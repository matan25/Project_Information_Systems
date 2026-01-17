"""
Authentication and registration logic for FLYTAU.

Routes:
- /login          : sign in as customer or manager (different views via ?role=...)
- /register       : create a new registered-customer account or upgrade a guest
- /logout         : clear the session and redirect to login
"""

from datetime import datetime, date
import re

from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from mysql.connector import Error

from db import get_db_connection

# If you later decide to hash passwords:
# from werkzeug.security import generate_password_hash, check_password_hash

auth_bp = Blueprint("auth", __name__)

# ----------------------------------------------------------------------
# Simple validation helpers
# ----------------------------------------------------------------------

EMAIL_RE = re.compile(r"^[^@]+@[^@]+\.[^@]+$")
PHONE_MIN_LEN = 7
PHONE_MAX_LEN = 15

# Names: English letters only, spaces, hyphens, apostrophes, length 2–50
NAME_RE = re.compile(r"^[A-Za-z][A-Za-z\s'\-]{1,49}$")

# Passport: EXACTLY 8 digits (numeric only)
PASSPORT_RE = re.compile(r"^\d{8}$")


def _normalize_phone(phone: str) -> str:
    """Remove spaces and dashes from a phone string."""
    return phone.replace(" ", "").replace("-", "")


def _is_valid_phone(phone: str) -> bool:
    """Simple phone validation: digits only after normalization, with length range."""
    if not phone:
        return False
    digits = _normalize_phone(phone)
    return digits.isdigit() and PHONE_MIN_LEN <= len(digits) <= PHONE_MAX_LEN


# ----------------------------------------------------------------------
# LOGIN (manager / customer via ?role=...)
# ----------------------------------------------------------------------
@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    """
    Unified backend for login, but with two different "views":

    - /login?role=manager  → manager login screen (Manager ID + password)
    - /login?role=customer → customer login screen (Email + password + guest option)

    POST body contains:
      - hidden field: user_type = "manager" / "customer"
      - identifier:  manager ID or email (depending on user_type)
      - password
    """

    if request.method == "POST":
        user_type = request.form.get("user_type")  # "customer" or "manager"
        identifier_raw = (request.form.get("identifier") or "").strip()
        password = request.form.get("password", "")

        if user_type not in ("customer", "manager"):
            user_type = "customer"

        if not identifier_raw or not password:
            if user_type == "manager":
                if not identifier_raw:
                    flash("Manager ID is required.", "error")
            else:
                if not identifier_raw:
                    flash("Email is required.", "error")
            if not password:
                flash("Password is required.", "error")
            return render_template("login.html", mode=user_type)

        conn = None
        cursor = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)

            # ----------------- MANAGER LOGIN -----------------
            if user_type == "manager":
                manager_id = identifier_raw

                cursor.execute(
                    """
                    SELECT Manager_id, Manager_Password, First_Name, Last_Name
                    FROM Managers
                    WHERE Manager_id = %s
                    """,
                    (manager_id,),
                )
                manager = cursor.fetchone()
                if not manager:
                    flash("Manager not found (invalid ID).", "error")
                    return render_template("login.html", mode="manager")

                if password != manager["Manager_Password"]:
                    flash("Incorrect password for manager.", "error")
                    return render_template("login.html", mode="manager")

                session.clear()
                session["role"] = "manager"
                session["manager_id"] = manager["Manager_id"]
                session["manager_name"] = (
                    f"{manager['First_Name']} {manager['Last_Name']}"
                )

                flash("Signed in as manager.", "success")
                return redirect(url_for("main.manager_home"))

            # ----------------- CUSTOMER LOGIN -----------------
            else:
                email = (identifier_raw or "").strip().lower()

                if not EMAIL_RE.match(email):
                    flash("Invalid email address format.", "error")
                    return render_template("login.html", mode="customer")

                cursor.execute(
                    """
                    SELECT Customer_Email,
                           Customer_Password,
                           First_Name,
                           Last_Name
                    FROM Register_Customers
                    WHERE LOWER(Customer_Email) = %s
                    """,
                    (email,),
                )
                customer = cursor.fetchone()

                if not customer:
                    cursor.execute(
                        """
                        SELECT Customer_Email
                        FROM Guest_Customers
                        WHERE LOWER(Customer_Email) = %s
                        """,
                        (email,),
                    )
                    guest = cursor.fetchone()

                    if guest:
                        flash(
                            "This email belongs to a guest booking only. "
                            "Please complete registration to create a full customer account.",
                            "error",
                        )
                        return redirect(url_for("auth.register", email=email))

                    flash(
                        "Customer not found. Please create an account first.",
                        "error",
                    )
                    return render_template("login.html", mode="customer")

                if password != customer["Customer_Password"]:
                    flash("Incorrect password for customer.", "error")
                    return render_template("login.html", mode="customer")

                session.clear()
                session["role"] = "customer"
                session["customer_email"] = customer["Customer_Email"]
                session["customer_name"] = (
                    f"{customer['First_Name']} {customer['Last_Name']}"
                )

                flash("Signed in as customer.", "success")
                return redirect(url_for("main.customer_home"))

        except Error as e:
            print("DB error in login():", e)
            flash("Internal error. Please try again later.", "error")
            return render_template("login.html", mode=user_type)

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    role_param = (request.args.get("role") or "").lower()
    mode = "manager" if role_param == "manager" else "customer"
    return render_template("login.html", mode=mode)


# ----------------------------------------------------------------------
# REGISTER (CUSTOMER) + UPGRADE / MERGE GUEST → REGISTER
# ----------------------------------------------------------------------
@auth_bp.route("/register", methods=["GET", "POST"])
def register():
    """
    Register a new customer account OR upgrade an existing 'guest' customer.

    Cases:
    1) Only Register_Customers row exists for this email  → error "already registered".
    2) Only Guest_Customers row exists  → create new Register row,
       move phones, update Orders, delete Guest rows.
    3) Both Guest and Register exist (old leftover)  → UPDATE existing Register row
       with new details, merge phones, update Orders, delete Guest rows.
    """
    today_str = date.today().isoformat()

    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password", "")
        confirm = request.form.get("confirm", "")
        first_name = (request.form.get("first_name") or "").strip()
        last_name = (request.form.get("last_name") or "").strip()
        passport_no = (request.form.get("passport_no") or "").strip()
        birth_date = (request.form.get("birth_date") or "").strip()

        # NEW: unlimited phones inputs named "phones"
        raw_phones = [(p or "").strip() for p in request.form.getlist("phones")]

        errors = []
        birth_date_value = None

        # --- Email ---
        if not email:
            errors.append("Email is required.")
        elif not EMAIL_RE.match(email):
            errors.append("Invalid email address format.")

        # --- Password + confirm ---
        if not password:
            errors.append("Password is required.")
        if not confirm:
            errors.append("Please confirm your password.")
        if password and confirm and password != confirm:
            errors.append("Passwords do not match.")
        if password and len(password) < 3:  # CHANGED: minimum 3 characters
            errors.append("Password must be at least 3 characters long.")

        # --- Names ---
        if not first_name:
            errors.append("First name is required.")
        elif not NAME_RE.match(first_name):
            errors.append(
                "First name may contain only English letters, spaces, "
                "hyphens and apostrophes (2–50 characters)."
            )

        if not last_name:
            errors.append("Last name is required.")
        elif not NAME_RE.match(last_name):
            errors.append(
                "Last name may contain only English letters, spaces, "
                "hyphens and apostrophes (2–50 characters)."
            )

        # --- Passport ---
        passport_for_db = None
        if not passport_no:
            errors.append("Passport number is required.")
        else:
            passport_normalized = passport_no.replace(" ", "")
            if not PASSPORT_RE.match(passport_normalized):
                errors.append(
                    "Passport number must be exactly 8 digits (numbers only). "
                    "Cannot create an account with an invalid passport."
                )
            else:
                passport_for_db = passport_normalized

        # --- Birth date ---
        if not birth_date:
            errors.append("Birth date is required.")
        else:
            try:
                birth_date_value = datetime.strptime(birth_date, "%Y-%m-%d").date()
                today = date.today()
                if birth_date_value > today:
                    errors.append("Birth date cannot be in the future.")
                else:
                    oldest_allowed = date(today.year - 120, today.month, today.day)
                    if birth_date_value < oldest_allowed:
                        errors.append("Birth date is too far in the past.")
            except ValueError:
                errors.append("Invalid birth date format. Please use YYYY-MM-DD.")

        # --- Phones (NOW REQUIRED: at least one) ---
        phones_clean = []
        for idx, p in enumerate(raw_phones, start=1):
            if not p:
                continue
            if not _is_valid_phone(p):
                errors.append(
                    f"Phone {idx} is invalid. Use digits only (7–15 digits; spaces/dashes allowed)."
                )
            else:
                normalized = _normalize_phone(p)
                if normalized not in phones_clean:
                    phones_clean.append(normalized)

        # REQUIRED: at least one phone
        if not phones_clean:
            errors.append("Please provide at least one phone number.")

        if errors:
            for msg in errors:
                flash(msg, "error")
            return render_template("register.html", today_str=today_str)

        conn = None
        cursor = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            conn.start_transaction()

            # Look for existing REGISTERED and/or GUEST for this email
            cursor.execute(
                """
                SELECT Customer_Email, First_Name, Last_Name, Passport_No, Birth_Date
                FROM Register_Customers
                WHERE LOWER(Customer_Email) = %s
                """,
                (email,),
            )
            registered = cursor.fetchone()

            cursor.execute(
                """
                SELECT Customer_Email, First_Name, Last_Name
                FROM Guest_Customers
                WHERE LOWER(Customer_Email) = %s
                """,
                (email,),
            )
            guest = cursor.fetchone()

            print("DEBUG register(): registered =", registered, "guest =", guest)

            # ------- CASE 1: only registered exists (no guest) -------
            if registered and not guest:
                flash("This email is already registered.", "error")
                conn.rollback()
                return render_template("register.html", today_str=today_str)

            # Collect phones from guest if exists
            guest_phones = []
            if guest:
                cursor.execute(
                    """
                    SELECT Phone_Number
                    FROM Guest_Customers_Phones
                    WHERE LOWER(Customer_Email) = %s
                    """,
                    (email,),
                )
                rows = cursor.fetchall() or []
                guest_phones = [r["Phone_Number"] for r in rows]

            # Merge phones: existing guest phones + new phones from form
            all_phones = []
            for p in guest_phones + phones_clean:
                norm = _normalize_phone(p)
                if norm and norm not in all_phones:
                    all_phones.append(norm)

            plain_password = password

            # ------- CASE 2: only guest exists -> create new registered -------
            if guest and not registered:
                cursor.execute(
                    """
                    INSERT INTO Register_Customers
                        (Customer_Email, First_Name, Last_Name,
                         Passport_No, Registration_Date, Birth_Date, Customer_Password)
                    VALUES (%s, %s, %s, %s, NOW(), %s, %s)
                    """,
                    (
                        email,
                        first_name,
                        last_name,
                        passport_for_db,
                        birth_date_value,
                        plain_password,
                    ),
                )

            # ------- CASE 3: both guest AND registered exist -> merge/update -------
            elif guest and registered:
                cursor.execute(
                    """
                    UPDATE Register_Customers
                    SET First_Name = %s,
                        Last_Name = %s,
                        Passport_No = %s,
                        Birth_Date = %s,
                        Customer_Password = %s
                    WHERE LOWER(Customer_Email) = %s
                    """,
                    (
                        first_name,
                        last_name,
                        passport_for_db,
                        birth_date_value,
                        plain_password,
                        email,
                    ),
                )

            # ------- CASE 4: neither guest nor registered (brand new user) -------
            elif not guest and not registered:
                cursor.execute(
                    """
                    INSERT INTO Register_Customers
                        (Customer_Email, First_Name, Last_Name,
                         Passport_No, Registration_Date, Birth_Date, Customer_Password)
                    VALUES (%s, %s, %s, %s, NOW(), %s, %s)
                    """,
                    (
                        email,
                        first_name,
                        last_name,
                        passport_for_db,
                        birth_date_value,
                        plain_password,
                    ),
                )

            # Insert phones into Register_Customers_Phones
            for p in all_phones:
                cursor.execute(
                    """
                    SELECT 1
                    FROM Register_Customers_Phones
                    WHERE LOWER(Customer_Email) = %s AND Phone_Number = %s
                    """,
                    (email, p),
                )
                if not cursor.fetchone():
                    cursor.execute(
                        """
                        INSERT INTO Register_Customers_Phones (Customer_Email, Phone_Number)
                        VALUES (%s, %s)
                        """,
                        (email, p),
                    )

            # If there was a guest profile -> migrate orders & delete guest rows
            if guest:
                cursor.execute(
                    """
                    UPDATE Orders
                    SET Customer_Type = 'Register'
                    WHERE LOWER(Customer_Email) = %s
                    """,
                    (email,),
                )

                cursor.execute(
                    """
                    DELETE FROM Guest_Customers_Phones
                    WHERE LOWER(Customer_Email) = %s
                    """,
                    (email,),
                )
                cursor.execute(
                    """
                    DELETE FROM Guest_Customers
                    WHERE LOWER(Customer_Email) = %s
                    """,
                    (email,),
                )

            conn.commit()

            if guest:
                flash(
                    "Your previous guest profile has been upgraded to a full customer account. "
                    "You can now sign in.",
                    "success",
                )
            else:
                flash("Account created successfully. You can now sign in.", "success")

            return redirect(url_for("auth.login", role="customer"))

        except Error as e:
            print("DB error in register():", e)
            if conn:
                conn.rollback()

            msg = str(e)
            if "Duplicate entry" in msg:
                lowered = msg.lower()
                if "passport" in lowered or "passport_no" in lowered:
                    flash(
                        "An account with this passport number already exists.",
                        "error",
                    )
                else:
                    flash(
                        "This email is already registered or used. Please use another email.",
                        "error",
                    )
            else:
                flash(
                    "Failed to create or update account. Please try again later.",
                    "error",
                )

            return render_template("register.html", today_str=today_str)

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    # GET
    prefill_email = (request.args.get("email") or "").strip().lower()
    return render_template(
        "register.html",
        today_str=today_str,
        prefill_email=prefill_email,
    )


# ----------------------------------------------------------------------
# LOGOUT
# ----------------------------------------------------------------------
@auth_bp.route("/logout")
def logout():
    """Clear the session and go back to the customer login page."""
    session.clear()
    flash("Signed out successfully.", "success")
    return redirect(url_for("auth.login", role="customer"))
