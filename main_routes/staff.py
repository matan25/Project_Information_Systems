from datetime import datetime
import re

from flask import render_template, redirect, url_for, request, flash
from mysql.connector import Error

from db import get_db_connection
from . import main_bp, _require_manager


# --------------------------------------------------
# Validation helpers
# --------------------------------------------------


def _is_hebrew_name(value: str) -> bool:
    """
    Allow names that contain Hebrew letters (plus spaces, apostrophe and dash).
    """
    if not value:
        return False
    pattern = r"^[\u0590-\u05FF\s'-]+$"
    return re.match(pattern, value) is not None


def _normalize_phone(phone: str) -> str:
    """Remove spaces and dashes from phone."""
    return phone.replace(" ", "").replace("-", "")


def _is_valid_phone(phone: str) -> bool:
    """
    Basic phone validation: digits only after removing spaces/dashes,
    length between 7 and 15.
    """
    if not phone:
        return False
    p = _normalize_phone(phone)
    if not p.isdigit():
        return False
    return 7 <= len(p) <= 15


def _validate_crew_form(form):
    """
    Validate form for creating a crew member.

    Start working date is taken automatically from the system clock.
    Long-haul certification is taken from the form.
    Returns: (clean_data_dict, errors_list, original_form_for_template)
    """
    errors = []

    role = (form.get("role") or "").strip()
    id_number = (form.get("id_number") or "").strip()
    first_name = (form.get("first_name") or "").strip()
    last_name = (form.get("last_name") or "").strip()
    city = (form.get("city") or "").strip()
    street = (form.get("street") or "").strip()
    house_number_str = (form.get("house_number") or "").strip()
    phone = (form.get("phone") or "").strip()
    long_haul_str = (form.get("long_haul") or "").strip()

    data = {
        "role": role or "pilot",
        "id_number": id_number,
        "first_name": first_name,
        "last_name": last_name,
        "city": city,
        "street": street,
        "house_number": house_number_str,
        "phone": phone,
        "long_haul": long_haul_str or "no",
    }

    # Role
    if role not in ("pilot", "attendant"):
        errors.append("Please select crew type: pilot or flight attendant.")

    # ID
    if not id_number:
        errors.append("ID number is required.")
    elif not id_number.isdigit() or len(id_number) != 9:
        errors.append("ID number must contain exactly 9 digits.")

    # Names (Hebrew allowed, message in English)
    if not first_name:
        errors.append("First name is required.")
    elif not _is_hebrew_name(first_name):
        errors.append(
            "First name must contain Hebrew letters (you may also use spaces, ' and -)."
        )

    if not last_name:
        errors.append("Last name is required.")
    elif not _is_hebrew_name(last_name):
        errors.append(
            "Last name must contain Hebrew letters (you may also use spaces, ' and -)."
        )

    # Address
    if not city:
        errors.append("City is required.")
    if not street:
        errors.append("Street is required.")

    # House number
    try:
        house_number = int(house_number_str)
        if house_number <= 0:
            raise ValueError()
    except ValueError:
        errors.append("House number must be a positive integer.")
        house_number = None

    # Phone
    if not phone:
        errors.append("Phone number is required.")
    elif not _is_valid_phone(phone):
        errors.append("Phone number is invalid. Please enter digits only (7–15 digits).")

    # Start working date – automatic (registration date)
    start_working_dt = datetime.now()

    # Long haul certified
    long_haul_certified = long_haul_str == "yes"

    clean = {
        "role": role,
        "id_number": id_number,
        "first_name": first_name,
        "last_name": last_name,
        "city": city,
        "street": street,
        "house_number": house_number,
        "phone": _normalize_phone(phone),
        "start_working_dt": start_working_dt,
        "long_haul_certified": long_haul_certified,
    }

    return clean, errors, data


# --------------------------------------------------
# Routes
# --------------------------------------------------


@main_bp.route("/manager/crew")
def manager_crew_list():
    """
    Manager view: list all pilots and flight attendants in two tables.
    """
    if not _require_manager():
        return redirect(url_for("auth.login"))

    conn = None
    cursor = None
    pilots = []
    attendants = []
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Pilots
        cursor.execute(
            """
            SELECT
                Pilot_id      AS Id,
                First_Name    AS FirstName,
                Last_Name     AS LastName,
                City,
                Street,
                House_Number,
                Phone_Number,
                Start_Working_Date,
                Long_Haul_Certified
            FROM Pilots
            ORDER BY Last_Name, First_Name
            """
        )
        pilots = cursor.fetchall()

        # Flight attendants
        cursor.execute(
            """
            SELECT
                Attendant_id        AS Id,
                First_Name          AS FirstName,
                Last_Name           AS LastName,
                City,
                Street,
                House_Number,
                Phone_Number,
                Start_Working_Date,
                Long_Haul_Certified
            FROM FlightAttendants
            ORDER BY Last_Name, First_Name
            """
        )
        attendants = cursor.fetchall()

    except Error as e:
        print("DB error in manager_crew_list:", e)
        flash("Failed to load crew members list.", "error")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return render_template(
        "manager_crew_list.html",
        pilots=pilots,
        attendants=attendants,
        lock_manager_nav=False,
    )


@main_bp.route("/manager/crew/new", methods=["GET", "POST"])
def manager_new_crew():
    """
    Create a new crew member (pilot or flight attendant).

    Start_Working_Date is set automatically.
    Total flight hours are NOT handled (column removed from DB).
    """
    if not _require_manager():
        return redirect(url_for("auth.login"))

    # default empty form
    default_form = {
        "role": "pilot",
        "id_number": "",
        "first_name": "",
        "last_name": "",
        "city": "",
        "street": "",
        "house_number": "",
        "phone": "",
        "long_haul": "no",
    }

    if request.method == "GET":
        return render_template(
            "manager_crew_form.html",
            form=default_form,
            lock_manager_nav=True,
        )

    # POST
    clean, errors, form_data = _validate_crew_form(request.form)

    if errors:
        for err in errors:
            flash(err, "error")
        return render_template(
            "manager_crew_form.html",
            form=form_data,
            lock_manager_nav=True,
        )

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Check duplicates for ID (both tables)
        cursor.execute(
            """
            SELECT 1 FROM Pilots WHERE Pilot_id = %s
            UNION
            SELECT 1 FROM FlightAttendants WHERE Attendant_id = %s
            """,
            (clean["id_number"], clean["id_number"]),
        )
        if cursor.fetchone():
            flash("A crew member with this ID number already exists.", "error")
            return render_template(
                "manager_crew_form.html",
                form=form_data,
                lock_manager_nav=True,
            )

        # Insert into proper table (without Total_Flight_Hours)
        if clean["role"] == "pilot":
            cursor.execute(
                """
                INSERT INTO Pilots (
                    Pilot_id,
                    First_Name,
                    Last_Name,
                    City,
                    Street,
                    House_Number,
                    Phone_Number,
                    Start_Working_Date,
                    Long_Haul_Certified
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    clean["id_number"],
                    clean["first_name"],
                    clean["last_name"],
                    clean["city"],
                    clean["street"],
                    clean["house_number"],
                    clean["phone"],
                    clean["start_working_dt"],
                    clean["long_haul_certified"],
                ),
            )
            crew_type_en = "Pilot"
        else:
            cursor.execute(
                """
                INSERT INTO FlightAttendants (
                    Attendant_id,
                    First_Name,
                    Last_Name,
                    City,
                    Street,
                    House_Number,
                    Phone_Number,
                    Start_Working_Date,
                    Long_Haul_Certified
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    clean["id_number"],
                    clean["first_name"],
                    clean["last_name"],
                    clean["city"],
                    clean["street"],
                    clean["house_number"],
                    clean["phone"],
                    clean["start_working_dt"],
                    clean["long_haul_certified"],
                ),
            )
            crew_type_en = "Flight attendant"

        conn.commit()
        flash(f"{crew_type_en} was added successfully.", "success")
        return redirect(url_for("main.manager_crew_list"))

    except Error as e:
        print("DB error in manager_new_crew:", e)
        flash("An error occurred while saving the crew member. Please try again.", "error")
        return render_template(
            "manager_crew_form.html",
            form=form_data,
            lock_manager_nav=True,
        )

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
