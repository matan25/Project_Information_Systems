"""
Customer and guest booking flows for FLYTAU – version with Register/Guest tables.

DB tables used here:
- Register_Customers
- Register_Customers_Phones
- Guest_Customers
- Guest_Customers_Phones
- Orders (with Customer_Email + Customer_Type)
"""

from datetime import datetime, timedelta
import re

from flask import (
    render_template,
    request,
    flash,
    redirect,
    url_for,
    session,
)

from mysql.connector import Error, errorcode

from db import get_db_connection
from . import main_bp, _require_customer


# -------------------------------------------------------------------
# Simple validation helpers
# -------------------------------------------------------------------

EMAIL_RE = re.compile(r"^[^@]+@[^@]+\.[^@]+$")
PHONE_MIN_LEN = 7
PHONE_MAX_LEN = 15

# Names – Hebrew/English letters, spaces, dash and '
NAME_RE = re.compile(r"^[A-Za-z\u0590-\u05FF][A-Za-z\u0590-\u05FF\s\-']*$")


def _normalize_phone_num(phone: str) -> str:
    """Remove spaces and dashes from a phone string."""
    return phone.replace(" ", "").replace("-", "")


def _is_valid_phone_num(phone: str) -> bool:
    """Simple phone validation: digits only after normalization, with length range."""
    if not phone:
        return False
    digits = _normalize_phone_num(phone)
    return digits.isdigit() and PHONE_MIN_LEN <= len(digits) <= PHONE_MAX_LEN


def _is_valid_name(name: str) -> bool:
    """
    Basic name validation: 2–50 chars, allowed letters (Heb/Eng), spaces, - and '.
    """
    if not name:
        return False
    if not (2 <= len(name) <= 50):
        return False
    return bool(NAME_RE.match(name))


# -------------------------------------------------------------------
# Helper: arrival time
# -------------------------------------------------------------------


def _compute_arrival(dep_dt: datetime, duration_minutes: int) -> datetime:
    """Compute arrival time from departure + route duration."""
    return dep_dt + timedelta(minutes=int(duration_minutes))


# -------------------------------------------------------------------
# Helper: update flight full/active status based on seats
# -------------------------------------------------------------------


def _update_flight_full_status(cursor, flight_id):
    """
    Update Flights.Status according to seat availability:

    - If there are 0 available seats on this flight → Status = 'Full-Occupied'
    - Otherwise → Status = 'Active'

    A seat is considered available only if:
    - Seat_Status = 'Available'
    - AND there is NO ticket for this seat whose order is NOT 'Cancelled-Customer'
      (i.e. any other status including NULL is considered occupying the seat).
    """
    cursor.execute(
        "SELECT Status FROM Flights WHERE Flight_id = %s FOR UPDATE",
        (flight_id,),
    )
    row = cursor.fetchone()
    if not row:
        return

    current_status = row["Status"]

    # Do not change status for flights that are already Cancelled/Completed
    if current_status in ("Cancelled", "Completed"):
        return

    cursor.execute(
        """
        SELECT COUNT(*) AS Available_Seats
        FROM FlightSeats fs
        WHERE fs.Flight_id = %s
          AND UPPER(TRIM(fs.Seat_Status)) = 'AVAILABLE'
          AND NOT EXISTS (
                SELECT 1
                FROM Tickets t
                JOIN Orders o
                  ON o.Order_code = t.Order_code
                WHERE t.FlightSeat_id = fs.FlightSeat_id
                  AND (o.Status IS NULL OR o.Status <> 'Cancelled-Customer')
          )
        """,
        (flight_id,),
    )
    row = cursor.fetchone() or {}
    available = int(row.get("Available_Seats") or 0)

    # IMPORTANT: use exactly the same value as in the DB / rest of the app
    new_status = "Full-Occupied" if available == 0 else "Active"

    if new_status != current_status:
        cursor.execute(
            """
            UPDATE Flights
            SET Status = %s
            WHERE Flight_id = %s
            """,
            (new_status, flight_id),
        )


# -------------------------------------------------------------------
# Helper: sync DB with manual SQL changes for cancelled orders
# -------------------------------------------------------------------


def _cleanup_cancelled_orders_seats(cursor):
    """
    Sync layer in case some Orders were manually marked 'Cancelled-Customer'
    in SQL but the seats were not released / flight status not updated.

    IMPORTANT (after adding Ticket_id surrogate key):
    We must NOT free seats that are already sold again to another
    non-cancelled order. So we only fix seats where all related
    orders are 'Cancelled-Customer' and the seat is still not 'Available'.

    Idempotent: safe to call before listing orders (customer or manager).
    """
    cursor.execute(
        """
        SELECT DISTINCT o.Order_code, o.Flight_id
        FROM Orders o
        JOIN Tickets t      ON o.Order_code     = t.Order_code
        JOIN FlightSeats fs ON fs.FlightSeat_id = t.FlightSeat_id
        LEFT JOIN Tickets t2
               ON t2.FlightSeat_id = fs.FlightSeat_id
        LEFT JOIN Orders o2
               ON o2.Order_code = t2.Order_code
              AND o2.Status <> 'Cancelled-Customer'
        WHERE o.Status = 'Cancelled-Customer'
          AND UPPER(TRIM(fs.Seat_Status)) <> 'AVAILABLE'
          AND o2.Order_code IS NULL
        """
    )
    rows = cursor.fetchall() or []

    for row in rows:
        order_code = row["Order_code"]
        flight_id = row["Flight_id"]

        # Release all seats for this cancelled order
        cursor.execute(
            """
            UPDATE FlightSeats fs
            JOIN Tickets t ON t.FlightSeat_id = fs.FlightSeat_id
            SET fs.Seat_Status = 'Available'
            WHERE t.Order_code = %s
            """,
            (order_code,),
        )

        # Update flight status based on new availability
        _update_flight_full_status(cursor, flight_id)


# -------------------------------------------------------------------
# Helper: set seat status for all seats in an order
# -------------------------------------------------------------------


def _set_seat_status_for_order(cursor, order_code, seat_status):
    """
    Update Seat_Status for all seats that belong to the given order.
    """
    cursor.execute(
        """
        UPDATE FlightSeats fs
        JOIN Tickets t ON t.FlightSeat_id = fs.FlightSeat_id
        SET fs.Seat_Status = %s
        WHERE t.Order_code = %s
        """,
        (seat_status, order_code),
    )


# -------------------------------------------------------------------
# Helper: auto-complete order when within 36h of departure
# -------------------------------------------------------------------


def _auto_complete_order_if_due(cursor, order, time_to_dep: timedelta) -> bool:
    """
    If an order is still 'Active' and its flight is not cancelled,
    change status to 'Completed' once it is within 36 hours of departure.

    Returns True if the status was updated, False otherwise.
    """
    if not order:
        return False

    if order.get("Order_Status") != "Active":
        return False

    # If flight itself is cancelled, do NOT mark as Completed here
    if order.get("Flight_Status") == "Cancelled":
        return False

    if time_to_dep is None:
        return False

    if time_to_dep <= timedelta(hours=36):
        cursor.execute(
            """
            UPDATE Orders
            SET Status = 'Completed'
            WHERE Order_code = %s
              AND Status = 'Active'
            """,
            (order["Order_code"],),
        )
        order["Order_Status"] = "Completed"
        return True

    return False


# -------------------------------------------------------------------
# Helper: Order code generation using IdCounters
# -------------------------------------------------------------------


def _get_next_order_code(cursor) -> str:
    """
    Generate the next Order_code in the format 'O000000001', 'O000000002', ...
    """
    try:
        cursor.execute(
            "SELECT NextNum FROM IdCounters WHERE Name = %s FOR UPDATE",
            ("Order",),
        )
        row = cursor.fetchone()

        if row is None:
            cursor.execute(
                """
                SELECT COALESCE(
                    MAX(CAST(SUBSTRING(Order_code, 2) AS UNSIGNED)), 0
                ) AS max_num
                FROM Orders
                FOR UPDATE
                """
            )
            m = cursor.fetchone() or {}
            current_max = int(m.get("max_num", 0) or 0)
            next_num = current_max + 1

            cursor.execute(
                "INSERT INTO IdCounters (Name, NextNum) VALUES (%s, %s)",
                ("Order", next_num + 1),
            )
            return f"O{next_num:09d}"

        next_num = int(row["NextNum"])
        cursor.execute(
            "UPDATE IdCounters SET NextNum = %s WHERE Name = %s",
            (next_num + 1, "Order"),
        )
        return f"O{next_num:09d}"

    except Error as e:
        if getattr(e, "errno", None) == errorcode.ER_NO_SUCH_TABLE:
            cursor.execute("SELECT MAX(Order_code) AS max_code FROM Orders")
            row = cursor.fetchone()
            max_code = row["max_code"] if row else None

            if not max_code:
                return "O000000001"

            try:
                num_part = int(max_code[1:])
                new_num = num_part + 1
                return f"O{new_num:09d}"
            except Exception:
                return f"O{datetime.now().strftime('%Y%m%d%H')}"
        else:
            raise


# -------------------------------------------------------------------
# Helper: customer lookup & guest↔registered logic
# -------------------------------------------------------------------


def _get_registered_customer(cursor, email: str):
    cursor.execute(
        """
        SELECT
            Customer_Email,
            First_Name,
            Last_Name,
            Passport_No,
            Birth_Date
        FROM Register_Customers
        WHERE LOWER(Customer_Email) = %s
        """,
        (email.lower(),),
    )
    return cursor.fetchone()


def _get_guest_customer(cursor, email: str):
    cursor.execute(
        """
        SELECT
            Customer_Email,
            First_Name,
            Last_Name
        FROM Guest_Customers
        WHERE LOWER(Customer_Email) = %s
        """,
        (email.lower(),),
    )
    return cursor.fetchone()


def _insert_guest_customer(cursor, email: str, first_name: str, last_name: str):
    cursor.execute(
        """
        INSERT INTO Guest_Customers (Customer_Email, First_Name, Last_Name)
        VALUES (%s, %s, %s)
        """,
        (email, first_name, last_name),
    )


def _insert_guest_phones(cursor, email: str, phones):
    for phone in phones:
        cursor.execute(
            """
            SELECT 1
            FROM Guest_Customers_Phones
            WHERE LOWER(Customer_Email) = %s AND Phone_Number = %s
            """,
            (email.lower(), phone),
        )
        if not cursor.fetchone():
            cursor.execute(
                """
                INSERT INTO Guest_Customers_Phones (Customer_Email, Phone_Number)
                VALUES (%s, %s)
                """,
                (email, phone),
            )


def _insert_registered_phones_from_list(cursor, email: str, phones):
    """
    Ensure these phones exist also under Register_Customers_Phones.
    Used ONLY when the customer is already registered.
    """
    for phone in phones:
        cursor.execute(
            """
            SELECT 1
            FROM Register_Customers_Phones
            WHERE LOWER(Customer_Email) = %s AND Phone_Number = %s
            """,
            (email.lower(), phone),
        )
        if not cursor.fetchone():
            cursor.execute(
                """
                INSERT INTO Register_Customers_Phones (Customer_Email, Phone_Number)
                VALUES (%s, %s)
                """,
                (email, phone),
            )


def _upgrade_guest_to_registered_move_data(
    cursor,
    email: str,
    first_name: str,
    last_name: str,
    passport_no: str,
    birth_date,
    password_plain: str,
    phones=None,
):
    """
    When a guest becomes registered:
    - Insert into Register_Customers (if not exists).
    - Move phones from Guest_* to Register_* (and also keep those we got now).
    - Delete from Guest_* tables.
    - All orders already use same email, so nothing to change in Orders.
    """
    email_l = email.lower()

    # Collect all phones (existing guest phones + new phones list)
    cursor.execute(
        """
        SELECT Phone_Number
        FROM Guest_Customers_Phones
        WHERE LOWER(Customer_Email) = %s
        """,
        (email_l,),
    )
    rows = cursor.fetchall() or []
    phones_all = {r["Phone_Number"] for r in rows}
    if phones:
        phones_all.update(phones)

    # Insert into Register_Customers (if not exists)
    cursor.execute(
        """
        SELECT 1
        FROM Register_Customers
        WHERE LOWER(Customer_Email) = %s
        """,
        (email_l,),
    )
    exists_reg = cursor.fetchone()

    if not exists_reg:
        cursor.execute(
            """
            INSERT INTO Register_Customers (
                Customer_Email,
                First_Name,
                Last_Name,
                Passport_No,
                Registration_Date,
                Birth_Date,
                Customer_Password
            )
            VALUES (%s, %s, %s, %s, NOW(), %s, %s)
            """,
            (email, first_name, last_name, passport_no, birth_date, password_plain),
        )
    else:
        # update existing registered record (e.g. if partial data existed)
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
            (first_name, last_name, passport_no, birth_date, password_plain, email_l),
        )

    # Insert phones into Register_Customers_Phones
    _insert_registered_phones_from_list(cursor, email, list(phones_all))

    # Delete from Guest_Customers_Phones and Guest_Customers
    cursor.execute(
        "DELETE FROM Guest_Customers_Phones WHERE LOWER(Customer_Email) = %s",
        (email_l,),
    )
    cursor.execute(
        "DELETE FROM Guest_Customers WHERE LOWER(Customer_Email) = %s",
        (email_l,),
    )
    # No need to touch Orders; all are already by email.


# -------------------------------------------------------------------
# Route: Flight search (public)
# -------------------------------------------------------------------


@main_bp.route("/flights/search", methods=["GET"])
def search_flights():
    """
    Public flight search.

    Updated requirements:
    - On entering the page, show *all* future active flights that have available seats.
    - Filters at the top:
        * Date field (optional)
        * Date type: by departure / by arrival
        * Origin airport (optional)
        * Destination airport (optional)
    - Show only flights with available seats.
    - If there are up to 3 seats left → seat count in red.
      If there are more than 3 seats → seat count in green.
    """
    origin = (request.args.get("origin") or "").strip()
    dest = (request.args.get("dest") or "").strip()
    date_str = (request.args.get("date") or "").strip()
    date_type = (request.args.get("date_type") or "dep").strip().lower()
    if date_type not in ("dep", "arr"):
        date_type = "dep"

    # For the date input: allow selecting from today and onward only
    today_str = datetime.now().strftime("%Y-%m-%d")

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    flights = []
    airports = []

    try:
        cursor.execute("SELECT Airport_code, City FROM Airports ORDER BY City")
        airports = cursor.fetchall()

        # Base: all future active flights
        query = """
            SELECT
                f.Flight_id,
                f.Dep_DateTime,
                DATE_ADD(f.Dep_DateTime, INTERVAL fr.Duration_Minutes MINUTE)
                    AS Arr_DateTime,
                a.Model AS AircraftModel,
                fr.Origin_Airport_code,
                fr.Destination_Airport_code,
                fr.Duration_Minutes,
                (
                    SELECT MIN(fs.Seat_Price)
                    FROM FlightSeats fs
                    WHERE fs.Flight_id = f.Flight_id
                      AND UPPER(TRIM(fs.Seat_Status)) = 'AVAILABLE'
                      AND NOT EXISTS (
                            SELECT 1
                            FROM Tickets t
                            JOIN Orders o
                              ON o.Order_code = t.Order_code
                            WHERE t.FlightSeat_id = fs.FlightSeat_id
                              AND (o.Status IS NULL OR o.Status <> 'Cancelled-Customer')
                      )
                ) AS Min_Price,
                (
                    SELECT COUNT(*)
                    FROM FlightSeats fs
                    WHERE fs.Flight_id = f.Flight_id
                      AND UPPER(TRIM(fs.Seat_Status)) = 'AVAILABLE'
                      AND NOT EXISTS (
                            SELECT 1
                            FROM Tickets t
                            JOIN Orders o
                              ON o.Order_code = t.Order_code
                            WHERE t.FlightSeat_id = fs.FlightSeat_id
                              AND (o.Status IS NULL OR o.Status <> 'Cancelled-Customer')
                      )
                ) AS Available_Seats
            FROM Flights f
            JOIN Flight_Routes fr ON f.Route_id = fr.Route_id
            JOIN Aircrafts a      ON f.Aircraft_id = a.Aircraft_id
            WHERE f.Status = 'Active'
              AND f.Dep_DateTime > NOW()
            ORDER BY f.Dep_DateTime
        """
        cursor.execute(query)
        flights_raw = cursor.fetchall()

        for f in flights_raw:
            # Only flights with available seats
            available = int(f.get("Available_Seats") or 0)
            if available <= 0:
                continue

            dep_dt = f["Dep_DateTime"]
            arr_dt = f["Arr_DateTime"]

            # Date filter according to user choice (departure / arrival)
            if date_str:
                if date_type == "dep":
                    if dep_dt.strftime("%Y-%m-%d") != date_str:
                        continue
                else:  # 'arr'
                    if arr_dt.strftime("%Y-%m-%d") != date_str:
                        continue

            # Filter by origin / destination if provided
            if origin and f["Origin_Airport_code"] != origin:
                continue
            if dest and f["Destination_Airport_code"] != dest:
                continue

            f["Dep_str"] = dep_dt.strftime("%H:%M")
            f["Arr_str"] = arr_dt.strftime("%H:%M")
            if f["Min_Price"] is None:
                f["Min_Price"] = 0

            flights.append(f)

    except Error as e:
        print("DB Error in search_flights:", e)
        flash("Error searching for flights.", "error")
    finally:
        cursor.close()
        conn.close()

    return render_template(
        "search_flights.html",
        airports=airports,
        flights=flights,
        search_params={
            "origin": origin,
            "dest": dest,
            "date": date_str,
            "date_type": date_type,
        },
        today_str=today_str,
    )


# -------------------------------------------------------------------
# Route: Seat selection (customer or guest)
# -------------------------------------------------------------------


@main_bp.route("/booking/<flight_id>/seats", methods=["GET"])
def select_seats(flight_id):
    """
    Step 1: show list of AVAILABLE seats for a specific flight.

    A seat is shown as available only if:
    - Seat_Status = 'Available'
    - AND there is NO ticket for this seat whose order is NOT 'Cancelled-Customer'
      (i.e. any other order – Active/Completed/Cancelled-System/NULL – is treated
       as occupied).
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    flight = None
    available_seats = []
    customer = None
    customer_phones = []

    try:
        cursor.execute(
            """
            SELECT
                f.Flight_id,
                f.Dep_DateTime,
                fr.Duration_Minutes,
                fr.Origin_Airport_code,
                fr.Destination_Airport_code,
                a.Model
            FROM Flights f
            JOIN Flight_Routes fr ON f.Route_id = fr.Route_id
            JOIN Aircrafts a      ON f.Aircraft_id = a.Aircraft_id
            WHERE f.Flight_id = %s
            """,
            (flight_id,),
        )
        flight = cursor.fetchone()

        if not flight:
            flash("Flight not found.", "error")
            return redirect(url_for("main.search_flights"))

        dep_dt = flight["Dep_DateTime"]
        duration = int(flight["Duration_Minutes"])
        arr_dt = _compute_arrival(dep_dt, duration)

        flight["Dep_str"] = dep_dt.strftime("%d/%m/%Y %H:%M")
        flight["Arr_str"] = arr_dt.strftime("%d/%m/%Y %H:%M")
        flight["Arr_DateTime"] = arr_dt

        # Registered customer details (including phones)
        if session.get("role") == "customer" and session.get("customer_email"):
            cursor.execute(
                """
                SELECT
                    Customer_Email,
                    First_Name,
                    Last_Name,
                    Passport_No,
                    Birth_Date
                FROM Register_Customers
                WHERE Customer_Email = %s
                """,
                (session["customer_email"],),
            )
            customer = cursor.fetchone()

            cursor.execute(
                """
                SELECT Phone_Number
                FROM Register_Customers_Phones
                WHERE Customer_Email = %s
                ORDER BY Phone_Number
                """,
                (session["customer_email"],),
            )
            rows = cursor.fetchall() or []
            customer_phones = [r["Phone_Number"] for r in rows]

        # Available seats (robust against inconsistent Seat_Status / old tickets)
        cursor.execute(
            """
            SELECT
                fs.FlightSeat_id,
                fs.Seat_Price,
                s.Row_Num,
                s.Col_Num,
                s.Seat_Class
            FROM FlightSeats fs
            JOIN Seats s ON fs.Seat_id = s.Seat_id
            WHERE fs.Flight_id = %s
              AND UPPER(TRIM(fs.Seat_Status)) = 'AVAILABLE'
              AND NOT EXISTS (
                    SELECT 1
                    FROM Tickets t
                    JOIN Orders o
                      ON o.Order_code = t.Order_code
                    WHERE t.FlightSeat_id = fs.FlightSeat_id
                      AND (o.Status IS NULL OR o.Status <> 'Cancelled-Customer')
              )
            ORDER BY s.Seat_Class DESC, s.Row_Num, s.Col_Num
            """,
            (flight_id,),
        )
        available_seats = cursor.fetchall()

    except Error as e:
        print("DB Error in select_seats:", e)
        flash("Error loading seats.", "error")
        return redirect(url_for("main.search_flights"))
    finally:
        cursor.close()
        conn.close()

    is_registered = (
        session.get("role") == "customer" and session.get("customer_email")
    )

    return render_template(
        "booking_seats.html",
        flight=flight,
        seats=available_seats,
        customer=customer,
        customer_phones=customer_phones,
        is_registered=is_registered,
    )


# -------------------------------------------------------------------
# Route: Prepare booking review (customer or guest)
# -------------------------------------------------------------------


@main_bp.route("/booking/<flight_id>/book", methods=["POST"])
def book_seats(flight_id):
    """
    Step 2: after seat selection, prepare a review page with price summary.

    No DB writes for guests here – only in confirm_booking().
    """
    selected_seat_ids = request.form.getlist("selected_seats")
    if not selected_seat_ids:
        flash("Please select at least one seat.", "error")
        return redirect(url_for("main.select_seats", flight_id=flight_id))

    is_registered = (
        session.get("role") == "customer" and session.get("customer_email")
    )

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    flight = None
    seats_info = []
    customer = None
    customer_phones = []
    guest_info = None
    total_price = 0.0

    try:
        # --- Flight info ---
        cursor.execute(
            """
            SELECT
                f.Flight_id,
                f.Dep_DateTime,
                fr.Duration_Minutes,
                fr.Origin_Airport_code,
                fr.Destination_Airport_code,
                a.Model
            FROM Flights f
            JOIN Flight_Routes fr ON f.Route_id = fr.Route_id
            JOIN Aircrafts a      ON f.Aircraft_id = a.Aircraft_id
            WHERE f.Flight_id = %s
            """,
            (flight_id,),
        )
        flight = cursor.fetchone()
        if not flight:
            flash("Flight not found.", "error")
            return redirect(url_for("main.search_flights"))

        dep_dt = flight["Dep_DateTime"]
        duration = int(flight["Duration_Minutes"])
        arr_dt = _compute_arrival(dep_dt, duration)
        flight["Dep_str"] = dep_dt.strftime("%d/%m/%Y %H:%M")
        flight["Arr_str"] = arr_dt.strftime("%d/%m/%Y %H:%M")
        flight["Arr_DateTime"] = arr_dt

        # --- Re-check seats still available (robust) ---
        format_strings = ",".join(["%s"] * len(selected_seat_ids))
        cursor.execute(
            f"""
            SELECT fs.FlightSeat_id
            FROM FlightSeats fs
            WHERE fs.FlightSeat_id IN ({format_strings})
              AND (
                    UPPER(TRIM(fs.Seat_Status)) <> 'AVAILABLE'
                    OR EXISTS (
                        SELECT 1
                        FROM Tickets t
                        JOIN Orders o
                          ON o.Order_code = t.Order_code
                        WHERE t.FlightSeat_id = fs.FlightSeat_id
                          AND (o.Status IS NULL OR o.Status <> 'Cancelled-Customer')
                    )
                  )
            """,
            tuple(selected_seat_ids),
        )
        taken_seats = cursor.fetchall()
        if taken_seats:
            flash(
                "Some of the selected seats were just taken. Please choose seats again.",
                "error",
            )
            return redirect(url_for("main.select_seats", flight_id=flight_id))

        # --- Load selected seats details ---
        cursor.execute(
            f"""
            SELECT
                fs.FlightSeat_id,
                fs.Seat_Price,
                s.Row_Num,
                s.Col_Num,
                s.Seat_Class
            FROM FlightSeats fs
            JOIN Seats s ON fs.Seat_id = s.Seat_id
            WHERE fs.FlightSeat_id IN ({format_strings})
            ORDER BY s.Seat_Class DESC, s.Row_Num, s.Col_Num
            """,
            tuple(selected_seat_ids),
        )
        seats_info = cursor.fetchall()
        total_price = sum(float(s["Seat_Price"] or 0) for s in seats_info)

        # --- Passenger: registered customer or guest ---
        if is_registered:
            customer_email = session["customer_email"]

            cursor.execute(
                """
                SELECT
                    Customer_Email,
                    First_Name,
                    Last_Name,
                    Passport_No,
                    Birth_Date
                FROM Register_Customers
                WHERE Customer_Email = %s
                """,
                (customer_email,),
            )
            customer = cursor.fetchone()

            cursor.execute(
                """
                SELECT Phone_Number
                FROM Register_Customers_Phones
                WHERE Customer_Email = %s
                ORDER BY Phone_Number
                """,
                (customer_email,),
            )
            rows = cursor.fetchall() or []
            customer_phones = [r["Phone_Number"] for r in rows]

            guest_info = None

        else:
            first_name = (request.form.get("guest_first_name") or "").strip()
            last_name = (request.form.get("guest_last_name") or "").strip()
            guest_email = (request.form.get("guest_email") or "").strip()

            guest_phone1 = (request.form.get("guest_phone") or "").strip()
            guest_phone2 = (request.form.get("guest_phone2") or "").strip()
            guest_phone3 = (request.form.get("guest_phone3") or "").strip()
            raw_phones = [guest_phone1, guest_phone2, guest_phone3]

            errors = []
            phones_clean = []

            if not first_name:
                errors.append("First name is required.")
            elif not _is_valid_name(first_name):
                errors.append(
                    "First name is invalid. Use 2–50 letters (Hebrew/English), spaces, - or '."
                )

            if not last_name:
                errors.append("Last name is required.")
            elif not _is_valid_name(last_name):
                errors.append(
                    "Last name is invalid. Use 2–50 letters (Hebrew/English), spaces, - or '."
                )

            if not guest_email:
                errors.append("Email is required.")
            elif len(guest_email) > 254:
                errors.append("Email address is too long.")
            elif not EMAIL_RE.match(guest_email):
                errors.append("Invalid email address format.")

            for idx, p in enumerate(raw_phones, start=1):
                if not p:
                    continue
                if not _is_valid_phone_num(p):
                    errors.append(
                        f"Phone {idx} is invalid. Use digits only (7–15 digits; spaces/dashes allowed)."
                    )
                else:
                    normalized = _normalize_phone_num(p)
                    if normalized not in phones_clean:
                        phones_clean.append(normalized)

            if not phones_clean:
                errors.append("Please provide at least one phone number.")

            if errors:
                for msg in errors:
                    flash(msg, "error")
                return redirect(url_for("main.select_seats", flight_id=flight_id))

            customer_email = guest_email
            guest_info = {
                "first_name": first_name,
                "last_name": last_name,
                "email": guest_email,
                "phones": phones_clean,
            }

        pending = {
            "flight_id": flight_id,
            "seat_ids": selected_seat_ids,
            "is_registered": bool(is_registered),
            "customer_email": customer_email,
        }
        if not is_registered and guest_info:
            pending.update(
                {
                    "guest_first_name": guest_info["first_name"],
                    "guest_last_name": guest_info["last_name"],
                    "guest_email": guest_info["email"],
                    "guest_phones": guest_info["phones"],
                }
            )
        session["pending_booking"] = pending

    except Error as e:
        print("DB Error in book_seats (review step):", e)
        flash("An error occurred while preparing the booking summary.", "error")
        return redirect(url_for("main.select_seats", flight_id=flight_id))
    finally:
        cursor.close()
        conn.close()

    return render_template(
        "booking_review.html",
        flight=flight,
        seats=seats_info,
        customer=customer,
        customer_phones=customer_phones,
        guest_info=guest_info,
        is_registered=bool(is_registered),
        total_price=total_price,
    )


# -------------------------------------------------------------------
# Route: Final booking confirmation (customer or guest)
# -------------------------------------------------------------------


@main_bp.route("/booking/confirm", methods=["POST"])
def confirm_booking():
    pending = session.get("pending_booking")
    if not pending:
        flash("No pending booking to confirm. Please start again.", "error")
        return redirect(url_for("main.search_flights"))

    flight_id = pending.get("flight_id")
    selected_seat_ids = pending.get("seat_ids") or []
    if not flight_id or not selected_seat_ids:
        session.pop("pending_booking", None)
        flash("Pending booking is incomplete. Please start again.", "error")
        return redirect(url_for("main.search_flights"))

    is_registered = pending.get("is_registered", False)

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        conn.start_transaction()

        format_strings = ",".join(["%s"] * len(selected_seat_ids))
        cursor.execute(
            f"""
            SELECT fs.FlightSeat_id
            FROM FlightSeats fs
            WHERE fs.FlightSeat_id IN ({format_strings})
              AND (
                    UPPER(TRIM(fs.Seat_Status)) <> 'AVAILABLE'
                    OR EXISTS (
                        SELECT 1
                        FROM Tickets t
                        JOIN Orders o
                          ON o.Order_code = t.Order_code
                        WHERE t.FlightSeat_id = fs.FlightSeat_id
                          AND (o.Status IS NULL OR o.Status <> 'Cancelled-Customer')
                    )
                  )
            FOR UPDATE
            """,
            tuple(selected_seat_ids),
        )
        taken_seats = cursor.fetchall()
        if taken_seats:
            conn.rollback()
            session.pop("pending_booking", None)
            flash(
                "Some of the selected seats were just taken. Please choose seats again.",
                "error",
            )
            return redirect(url_for("main.select_seats", flight_id=flight_id))

        if is_registered:
            customer_email = pending.get("customer_email")
            cursor.execute(
                """
                SELECT Customer_Email
                FROM Register_Customers
                WHERE Customer_Email = %s
                """,
                (customer_email,),
            )
            if not cursor.fetchone():
                is_registered = False

        if not is_registered:
            first_name = (pending.get("guest_first_name") or "").strip()
            last_name = (pending.get("guest_last_name") or "").strip()
            guest_email = (pending.get("guest_email") or "").strip()
            guest_phones = pending.get("guest_phones") or []

            if (
                not first_name
                or not last_name
                or not guest_email
                or not guest_phones
            ):
                conn.rollback()
                session.pop("pending_booking", None)
                flash("Guest details are missing. Please start booking again.", "error")
                return redirect(url_for("main.select_seats", flight_id=flight_id))

            customer_email = guest_email

            reg_row = _get_registered_customer(cursor, customer_email)
            if reg_row:
                # Guest uses an email that already belongs to a registered customer:
                # treat as registered and sync phones to the registered phones table.
                is_registered = True
                _insert_registered_phones_from_list(
                    cursor, customer_email, guest_phones
                )
            else:
                # Pure guest: keep data only in Guest_* tables (no FK violation).
                guest_row = _get_guest_customer(cursor, customer_email)
                if not guest_row:
                    _insert_guest_customer(cursor, customer_email, first_name, last_name)
                _insert_guest_phones(cursor, customer_email, guest_phones)

                session["guest_email"] = customer_email

        new_order_code = _get_next_order_code(cursor)
        # must match ENUM('Register','Guest') in Orders
        customer_type = "Register" if is_registered else "Guest"
        cursor.execute(
            """
            INSERT INTO Orders (
                Order_code,
                Order_Date,
                Status,
                Cancel_Date,
                Customer_Email,
                Flight_id,
                Customer_Type
            )
            VALUES (%s, NOW(), 'Active', NULL, %s, %s, %s)
            """,
            (new_order_code, customer_email, flight_id, customer_type),
        )

        for seat_id in selected_seat_ids:
            cursor.execute(
                """
                UPDATE FlightSeats
                SET Seat_Status = 'Sold'
                WHERE FlightSeat_id = %s
                  AND UPPER(TRIM(Seat_Status)) = 'AVAILABLE'
                """,
                (seat_id,),
            )
            if cursor.rowcount != 1:
                raise Exception(f"Seat {seat_id} is no longer available.")

            cursor.execute(
                """
                INSERT INTO Tickets (FlightSeat_id, Order_code)
                VALUES (%s, %s)
                """,
                (seat_id, new_order_code),
            )

        _update_flight_full_status(cursor, flight_id)

        conn.commit()
        session.pop("pending_booking", None)
        flash("Booking completed successfully.", "success")
        return redirect(
            url_for(
                "main.booking_confirmation",
                order_code=new_order_code,
                just_confirmed="1",
            )
        )

    except Exception as e:
        conn.rollback()
        print("Booking Error (confirm_booking):", e)
        flash("An error occurred during booking confirmation. Please try again.", "error")
        return redirect(url_for("main.select_seats", flight_id=flight_id))
    finally:
        cursor.close()
        conn.close()


# -------------------------------------------------------------------
# Booking confirmation (customer & guest, read-only)
# -------------------------------------------------------------------


@main_bp.route("/booking/order/<order_code>/summary")
def booking_confirmation(order_code):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    order = None
    tickets = []
    total_price = 0.0
    just_confirmed = request.args.get("just_confirmed") == "1"

    try:
        cursor.execute(
            """
            SELECT
                o.Order_code,
                o.Order_Date,
                o.Status AS Order_Status,
                o.Customer_Email,
                o.Customer_Type,
                rc.First_Name  AS Reg_First_Name,
                rc.Last_Name   AS Reg_Last_Name,
                gc.First_Name  AS Guest_First_Name,
                gc.Last_Name   AS Guest_Last_Name,
                o.Flight_id,
                f.Dep_DateTime,
                f.Status AS Flight_Status,
                fr.Duration_Minutes,
                fr.Origin_Airport_code,
                fr.Destination_Airport_code,
                a.Model AS AircraftModel
            FROM Orders o
            LEFT JOIN Flights       f  ON o.Flight_id = f.Flight_id
            LEFT JOIN Flight_Routes fr ON f.Route_id  = fr.Route_id
            LEFT JOIN Aircrafts     a  ON f.Aircraft_id = a.Aircraft_id
            LEFT JOIN Register_Customers rc
                   ON rc.Customer_Email = o.Customer_Email
            LEFT JOIN Guest_Customers gc
                   ON gc.Customer_Email = o.Customer_Email
            WHERE o.Order_code = %s
            """,
            (order_code,),
        )
        order = cursor.fetchone()
        if not order:
            flash("Order not found.", "error")
            return redirect(url_for("main.search_flights"))

        if order["Customer_Type"] == "Register" and order["Reg_First_Name"]:
            order["First_Name"] = order["Reg_First_Name"]
            order["Last_Name"] = order["Reg_Last_Name"]
        elif order["Guest_First_Name"]:
            order["First_Name"] = order["Guest_First_Name"]
            order["Last_Name"] = order["Guest_Last_Name"]
        else:
            order["First_Name"] = None
            order["Last_Name"] = None

        dep_dt = order["Dep_DateTime"]
        duration = int(order["Duration_Minutes"] or 0)

        if dep_dt:
            arr_dt = _compute_arrival(dep_dt, duration)
            order["Dep_str"] = dep_dt.strftime("%d/%m/%Y %H:%M")
            order["Arr_str"] = arr_dt.strftime("%d/%m/%Y %H:%M")
        else:
            order["Dep_str"] = "-"
            order["Arr_str"] = "-"

        order["OrderDate_str"] = order["Order_Date"].strftime("%d/%m/%Y %H:%M")

        if dep_dt:
            time_to_dep_for_completion = dep_dt - datetime.now()
        else:
            time_to_dep_for_completion = timedelta(days=99999)

        if _auto_complete_order_if_due(
            cursor,
            order,
            time_to_dep_for_completion,
        ):
            conn.commit()

        if (
            order["Order_Status"] == "Active"
            and order.get("Flight_Status") == "Cancelled"
        ):
            cursor.execute(
                """
                UPDATE Orders
                SET Status = 'Cancelled-System',
                    Cancel_Date = NOW()
                WHERE Order_code = %s
                  AND Status = 'Active'
                """,
                (order_code,),
            )
            order["Order_Status"] = "Cancelled-System"
            _set_seat_status_for_order(cursor, order_code, "Blocked")
            _update_flight_full_status(cursor, order["Flight_id"])
            conn.commit()

        cursor.execute(
            """
            SELECT
                t.FlightSeat_id,
                fs.Seat_Price,
                s.Row_Num,
                s.Col_Num,
                s.Seat_Class
            FROM Tickets t
            JOIN FlightSeats fs ON fs.FlightSeat_id = t.FlightSeat_id
            JOIN Seats       s  ON s.Seat_id        = fs.Seat_id
            WHERE t.Order_code = %s
            ORDER BY s.Seat_Class DESC, s.Row_Num, s.Col_Num
            """,
            (order_code,),
        )
        tickets = cursor.fetchall()

        raw_total = sum(float(t["Seat_Price"] or 0) for t in tickets)
        order["Original_Total"] = raw_total

        status = order["Order_Status"]
        order["Cancellation_Fee"] = None
        order["Refund_Amount"] = None

        if status == "Cancelled-System":
            total_price = 0.0
        elif status == "Cancelled-Customer":
            fee = round(raw_total * 0.05, 2)
            refund = max(raw_total - fee, 0.0)
            order["Cancellation_Fee"] = fee
            order["Refund_Amount"] = refund
            total_price = fee
        else:
            total_price = raw_total

        now = datetime.now()
        if dep_dt:
            time_to_dep = dep_dt - now
        else:
            time_to_dep = timedelta(days=99999)

        order["can_cancel_as_guest"] = (
            time_to_dep > timedelta(hours=36)
            and order["Order_Status"] not in ("Cancelled-Customer", "Cancelled-System")
        )

    except Error as e:
        print("DB error in booking_confirmation:", e)
        flash("Failed to load booking summary.", "error")
        return redirect(url_for("main.search_flights"))
    finally:
        cursor.close()
        conn.close()

    is_registered = session.get("role") == "customer" and session.get(
        "customer_email"
    )

    return render_template(
        "booking_confirmation.html",
        order=order,
        tickets=tickets,
        total_price=total_price,
        is_registered=is_registered,
        just_confirmed=just_confirmed,
    )


# -------------------------------------------------------------------
# Registered customer: login for orders (passport + birth date)
# -------------------------------------------------------------------


@main_bp.route("/customer/orders/login", methods=["GET", "POST"])
def customer_orders_login():
    """
    Login / verification for viewing customer orders based on passport + birth date.
    This is only for REGISTERED customers, so we use Register_Customers.
    """
    if session.get("role") == "manager":
        flash("Managers cannot use the customer orders login screen.", "error")
        return redirect(url_for("main.manager_home"))

    if request.method == "POST":
        passport = (request.form.get("passport") or "").strip()
        birth_str = (request.form.get("birth_date") or "").strip()

        errors = []
        if not passport:
            errors.append("Passport number is required.")
        if not birth_str:
            errors.append("Birth date is required.")

        if errors:
            for msg in errors:
                flash(msg, "error")
            return render_template("customer_orders_login.html")

        try:
            birth_date = datetime.strptime(birth_str, "%Y-%m-%d").date()
        except ValueError:
            flash("Invalid birth date format.", "error")
            return render_template("customer_orders_login.html")

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(
                """
                SELECT
                    Customer_Email,
                    First_Name,
                    Last_Name
                FROM Register_Customers
                WHERE Passport_No = %s
                  AND DATE(Birth_Date) = %s
                """,
                (passport, birth_date),
            )
            row = cursor.fetchone()
        except Error as e:
            print("DB error in customer_orders_login:", e)
            row = None
        finally:
            cursor.close()
            conn.close()

        if not row:
            flash("No registered customer found for these details.", "error")
            return render_template("customer_orders_login.html")

        if session.get("role") == "customer" and session.get("customer_email"):
            if row["Customer_Email"] != session["customer_email"]:
                flash(
                    "The provided passport and birth date do not match the customer currently signed in.",
                    "error",
                )
                return render_template("customer_orders_login.html")

            flash("Details verified successfully.", "success")
            return redirect(url_for("main.customer_orders"))

        session.clear()
        session["role"] = "customer"
        session["customer_email"] = row["Customer_Email"]
        session["customer_name"] = f"{row['First_Name']} {row['Last_Name']}"

        flash("You are now identified as a registered customer.", "success")
        return redirect(url_for("main.customer_orders"))

    return render_template("customer_orders_login.html")


# -------------------------------------------------------------------
# Registered customer: orders list + filter + cancellation
# -------------------------------------------------------------------


@main_bp.route("/customer/orders")
def customer_orders():
    if not _require_customer():
        return redirect(url_for("main.customer_orders_login"))

    status_filter = request.args.get("status", "all")
    valid_statuses = {
        "Active",
        "Completed",
        "Cancelled-Customer",
        "Cancelled-System",
    }
    if status_filter not in valid_statuses and status_filter != "all":
        status_filter = "all"

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    orders = []

    try:
        _cleanup_cancelled_orders_seats(cursor)
        conn.commit()

        base_query = """
            SELECT
                o.Order_code,
                o.Order_Date,
                o.Status               AS Order_Status,
                o.Flight_id,
                f.Dep_DateTime,
                f.Status               AS Flight_Status,
                fr.Origin_Airport_code,
                fr.Destination_Airport_code,
                COUNT(t.FlightSeat_id)          AS Ticket_Count,
                COALESCE(SUM(fs.Seat_Price), 0) AS Total_Price
            FROM Orders o
            LEFT JOIN Flights       f  ON o.Flight_id      = f.Flight_id
            LEFT JOIN Flight_Routes fr ON f.Route_id       = fr.Route_id
            LEFT JOIN Tickets       t  ON o.Order_code     = t.Order_code
            LEFT JOIN FlightSeats   fs ON t.FlightSeat_id  = fs.FlightSeat_id
            WHERE o.Customer_Email = %s
        """
        params = [session["customer_email"]]

        if status_filter != "all":
            base_query += " AND o.Status = %s"
            params.append(status_filter)

        base_query += """
            GROUP BY o.Order_code
            ORDER BY o.Order_Date DESC
        """

        cursor.execute(base_query, tuple(params))
        orders = cursor.fetchall()

        now = datetime.now()
        to_complete = []
        to_cancel_sys = []
        cancel_sys_flights = {}

        for o in orders:
            o["Date_str"] = o["Order_Date"].strftime("%d/%m/%Y")

            dep_dt = o["Dep_DateTime"]
            if dep_dt:
                o["Dep_str"] = dep_dt.strftime("%d/%m %H:%M")
                time_to_dep = dep_dt - now
            else:
                o["Dep_str"] = "-"
                time_to_dep = timedelta(days=99999)

            base_total = float(o["Total_Price"] or 0.0)
            o["Ticket_Count"] = int(o["Ticket_Count"] or 0)

            if o["Order_Status"] == "Active":
                if o.get("Flight_Status") == "Cancelled":
                    to_cancel_sys.append(o["Order_code"])
                    cancel_sys_flights[o["Order_code"]] = o["Flight_id"]
                    o["Order_Status"] = "Cancelled-System"
                    o["can_cancel"] = False
                else:
                    if _auto_complete_order_if_due(cursor, o, time_to_dep):
                        to_complete.append(o["Order_code"])
                        o["can_cancel"] = False
                    else:
                        o["can_cancel"] = time_to_dep > timedelta(hours=36)
            else:
                o["can_cancel"] = False

            if o["Order_Status"] == "Cancelled-Customer":
                fee = round(base_total * 0.05, 2)
                o["Total_Price"] = fee
            elif o["Order_Status"] == "Cancelled-System":
                o["Total_Price"] = 0.0
            else:
                o["Total_Price"] = base_total

        if to_complete:
            cursor.executemany(
                """
                UPDATE Orders
                SET Status = 'Completed'
                WHERE Order_code = %s
                  AND Status = 'Active'
                """,
                [(oc,) for oc in to_complete],
            )

        if to_cancel_sys:
            cursor.executemany(
                """
                UPDATE Orders
                SET Status = 'Cancelled-System',
                    Cancel_Date = NOW()
                WHERE Order_code = %s
                  AND Status = 'Active'
                """,
                [(oc,) for oc in to_cancel_sys],
            )

            for oc in to_cancel_sys:
                _set_seat_status_for_order(cursor, oc, "Blocked")
                flight_id = cancel_sys_flights.get(oc)
                if flight_id:
                    _update_flight_full_status(cursor, flight_id)

        if to_complete or to_cancel_sys:
            conn.commit()

    except Error as e:
        print("DB error in customer_orders:", e)
        flash("Failed to load your orders.", "error")
    finally:
        cursor.close()
        conn.close()

    return render_template(
        "customer_orders.html",
        orders=orders,
        status_filter=status_filter,
    )


# -------------------------------------------------------------------
# Registered customer: cancel order
# -------------------------------------------------------------------


@main_bp.route("/customer/orders/<order_code>/cancel", methods=["POST"])
def cancel_order(order_code):
    if not _require_customer():
        return redirect(url_for("main.customer_orders_login"))

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        conn.start_transaction()

        cursor.execute(
            """
            SELECT
                o.Order_code,
                o.Status AS Order_Status,
                o.Customer_Email,
                f.Flight_id,
                f.Dep_DateTime
            FROM Orders o
            JOIN Flights f ON o.Flight_id = f.Flight_id
            WHERE o.Order_code = %s
              AND o.Customer_Email = %s
            FOR UPDATE
            """,
            (order_code, session["customer_email"]),
        )
        order = cursor.fetchone()
        if not order:
            flash("Order not found or does not belong to you.", "error")
            conn.rollback()
            return redirect(url_for("main.customer_orders"))

        if order["Order_Status"] in ("Cancelled-Customer", "Cancelled-System"):
            flash("This order is already cancelled.", "info")
            conn.rollback()
            return redirect(url_for("main.customer_orders"))

        now = datetime.now()
        time_to_dep = order["Dep_DateTime"] - now
        if time_to_dep <= timedelta(hours=36):
            flash("Order can be cancelled only up to 36 hours before departure.", "error")
            conn.rollback()
            return redirect(url_for("main.customer_orders"))

        cursor.execute(
            """
            SELECT COALESCE(SUM(fs.Seat_Price), 0) AS Total_Price
            FROM Tickets t
            JOIN FlightSeats fs ON t.FlightSeat_id = fs.FlightSeat_id
            WHERE t.Order_code = %s
            """,
            (order_code,),
        )
        amount_row = cursor.fetchone() or {"Total_Price": 0}
        total_amount = float(amount_row["Total_Price"] or 0.0)
        fee = round(total_amount * 0.05, 2)
        refund = max(total_amount - fee, 0.0)

        _set_seat_status_for_order(cursor, order_code, "Available")

        cursor.execute(
            """
            UPDATE Orders
            SET Status = 'Cancelled-Customer',
                Cancel_Date = NOW()
            WHERE Order_code = %s
            """,
            (order_code,),
        )

        _update_flight_full_status(cursor, order["Flight_id"])

        conn.commit()
        flash(
            f"Order cancelled successfully. "
            f"Total amount was ${total_amount:.2f}. "
            f"Cancellation fee (5%) is ${fee:.2f}. "
            f"Refund amount: ${refund:.2f}.",
            "success",
        )
        return redirect(url_for("main.customer_orders"))

    except Error as e:
        conn.rollback()
        print("DB error in cancel_order:", e)
        flash("Failed to cancel the order.", "error")
        return redirect(url_for("main.customer_orders"))
    finally:
        cursor.close()
        conn.close()


# -------------------------------------------------------------------
# Guest: order lookup
# -------------------------------------------------------------------


@main_bp.route("/guest/order-lookup", methods=["GET", "POST"])
def guest_order_lookup():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip()
        order_code = (request.form.get("order_code") or "").strip()

        errors = []
        if not email:
            errors.append("Email is required.")
        elif not EMAIL_RE.match(email):
            errors.append("Invalid email address format.")
        if not order_code:
            errors.append("Order ID is required.")

        if errors:
            for msg in errors:
                flash(msg, "error")
            return render_template("guest_order_lookup.html")

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(
                """
                SELECT Order_code
                FROM Orders
                WHERE Order_code = %s
                  AND Customer_Email = %s
                  AND Customer_Type = 'Guest'
                """,
                (order_code, email),
            )
            row = cursor.fetchone()
        except Error as e:
            print("DB error in guest_order_lookup:", e)
            row = None
        finally:
            cursor.close()
            conn.close()

        if not row:
            flash("No guest order found for this email and order ID.", "error")
            return render_template("guest_order_lookup.html")

        session["guest_email"] = email

        return redirect(url_for("main.booking_confirmation", order_code=order_code))

    return render_template("guest_order_lookup.html")


# -------------------------------------------------------------------
# Guest: cancel order
# -------------------------------------------------------------------


@main_bp.route("/guest/orders/<order_code>/cancel", methods=["POST"])
def guest_cancel_order(order_code):
    if session.get("role") == "customer" and session.get("customer_email"):
        return redirect(url_for("main.cancel_order", order_code=order_code))

    guest_email = session.get("guest_email")
    if not guest_email:
        flash("For security reasons, please look up your booking again.", "error")
        return redirect(url_for("main.guest_order_lookup"))

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        conn.start_transaction()

        cursor.execute(
            """
            SELECT
                o.Order_code,
                o.Status AS Order_Status,
                o.Customer_Email,
                o.Customer_Type,
                f.Flight_id,
                f.Dep_DateTime
            FROM Orders o
            JOIN Flights f ON o.Flight_id = f.Flight_id
            WHERE o.Order_code = %s
              AND o.Customer_Email = %s
              AND o.Customer_Type = 'Guest'
            FOR UPDATE
            """,
            (order_code, guest_email),
        )
        order = cursor.fetchone()
        if not order:
            flash("Order not found or does not belong to this guest email.", "error")
            conn.rollback()
            return redirect(url_for("main.guest_order_lookup"))

        if order["Order_Status"] in ("Cancelled-Customer", "Cancelled-System"):
            flash("This order is already cancelled.", "info")
            conn.rollback()
            return redirect(url_for("main.booking_confirmation", order_code=order_code))

        now = datetime.now()
        time_to_dep = order["Dep_DateTime"] - now
        if time_to_dep <= timedelta(hours=36):
            flash("Order can be cancelled only up to 36 hours before departure.", "error")
            conn.rollback()
            return redirect(url_for("main.booking_confirmation", order_code=order_code))

        cursor.execute(
            """
            SELECT COALESCE(SUM(fs.Seat_Price), 0) AS Total_Price
            FROM Tickets t
            JOIN FlightSeats fs ON t.FlightSeat_id = fs.FlightSeat_id
            WHERE t.Order_code = %s
            """,
            (order_code,),
        )
        amount_row = cursor.fetchone() or {"Total_Price": 0}
        total_amount = float(amount_row["Total_Price"] or 0.0)
        fee = round(total_amount * 0.05, 2)
        refund = max(total_amount - fee, 0.0)

        _set_seat_status_for_order(cursor, order_code, "Available")

        cursor.execute(
            """
            UPDATE Orders
            SET Status = 'Cancelled-Customer',
                Cancel_Date = NOW()
            WHERE Order_code = %s
            """,
            (order_code,),
        )

        _update_flight_full_status(cursor, order["Flight_id"])

        conn.commit()
        flash(
            f"Order cancelled successfully. "
            f"Total amount was ${total_amount:.2f}. "
            f"Cancellation fee (5%) is ${fee:.2f}. "
            f"Refund amount: ${refund:.2f}.",
            "success",
        )
        return redirect(url_for("main.booking_confirmation", order_code=order_code))

    except Error as e:
        conn.rollback()
        print("DB error in guest_cancel_order:", e)
        flash("Failed to cancel the order.", "error")
        return redirect(url_for("main.booking_confirmation", order_code=order_code))
    finally:
        cursor.close()
        conn.close()
