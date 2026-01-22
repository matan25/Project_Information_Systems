"""
Customer and guest booking flows for FLYTAU – version with Register/Guest tables.

DB tables used here:
- Register_Customers
- Register_Customers_Phones
- Guest_Customers
- Guest_Customers_Phones
- Orders
- Tickets (now includes Paid_Price)
- FlightSeats (Seat_Status ENUM('Available','Sold','Blocked'))
- Flights (Status ENUM('Active','Full-Occupied','Completed','Cancelled'))

Core rule:
- Availability for customers is determined ONLY by FlightSeats.Seat_Status:
  * Available => available
  * Sold/Blocked => not available
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
NAME_RE = re.compile(r"^[A-Za-z\u0590-\u05FF][A-Za-z\u0590-\u05FF\s\-']*$")


def _normalize_phone_num(phone: str) -> str:
    return phone.replace(" ", "").replace("-", "")


def _is_valid_phone_num(phone: str) -> bool:
    if not phone:
        return False
    digits = _normalize_phone_num(phone)
    return digits.isdigit() and PHONE_MIN_LEN <= len(digits) <= PHONE_MAX_LEN


def _is_valid_name(name: str) -> bool:
    if not name:
        return False
    if not (2 <= len(name) <= 50):
        return False
    return bool(NAME_RE.match(name))


# -------------------------------------------------------------------
# Helper: arrival time
# -------------------------------------------------------------------

def _compute_arrival(dep_dt: datetime, duration_minutes: int) -> datetime:
    return dep_dt + timedelta(minutes=int(duration_minutes))


# -------------------------------------------------------------------
# sync Seat_Status from Tickets/Orders
# -------------------------------------------------------------------

def _sync_seat_status_from_orders(cursor, flight_id=None):
    """

    - If a FlightSeat is 'Available' but has a Ticket for an Order that is NOT
      Cancelled-Customer, it must not be Available.

    Handling:
    - Orders Cancelled-System => seat should be Blocked (if still Available)
    - Orders Active/Completed/(NULL/other) => seat should be Sold (if still Available)
    """

    flight_filter_sql = ""
    params = []
    if flight_id is not None:
        flight_filter_sql = " AND fs.Flight_id = %s "
        params.append(flight_id)

    # 1) If there is a ticket for an order cancelled-system => Blocked (only if currently Available)
    cursor.execute(
        f"""
        UPDATE FlightSeats fs
        JOIN Tickets t ON t.FlightSeat_id = fs.FlightSeat_id
        JOIN Orders  o ON o.Order_code    = t.Order_code
        SET fs.Seat_Status = 'Blocked'
        WHERE fs.Seat_Status = 'Available'
          {flight_filter_sql}
          AND UPPER(TRIM(COALESCE(o.Status,''))) = 'CANCELLED-SYSTEM'
        """,
        tuple(params),
    )

    # 2) If there is a ticket for an order that is NOT cancelled-customer and NOT cancelled-system => Sold
    cursor.execute(
        f"""
        UPDATE FlightSeats fs
        JOIN Tickets t ON t.FlightSeat_id = fs.FlightSeat_id
        JOIN Orders  o ON o.Order_code    = t.Order_code
        SET fs.Seat_Status = 'Sold'
        WHERE fs.Seat_Status = 'Available'
          {flight_filter_sql}
          AND UPPER(TRIM(COALESCE(o.Status,''))) NOT IN ('CANCELLED-CUSTOMER','CANCELLED-SYSTEM')
        """,
        tuple(params),
    )


# -------------------------------------------------------------------
# Helper: update flight full/active status based on Seat_Status ONLY
# -------------------------------------------------------------------

def _update_flight_full_status(cursor, flight_id):
    cursor.execute(
        "SELECT Status FROM Flights WHERE Flight_id = %s FOR UPDATE",
        (flight_id,),
    )
    row = cursor.fetchone()
    if not row:
        return

    current_status = row["Status"]
    if current_status in ("Cancelled", "Completed"):
        return

    cursor.execute(
        """
        SELECT COUNT(*) AS Available_Seats
        FROM FlightSeats fs
        WHERE fs.Flight_id = %s
          AND fs.Seat_Status = 'Available'
        """,
        (flight_id,),
    )
    row = cursor.fetchone() or {}
    available = int(row.get("Available_Seats") or 0)

    new_status = "Full-Occupied" if available == 0 else "Active"
    if new_status != current_status:
        cursor.execute(
            "UPDATE Flights SET Status = %s WHERE Flight_id = %s",
            (new_status, flight_id),
        )


# -------------------------------------------------------------------
# Helper: cleanup cancelled orders seats
# -------------------------------------------------------------------

def _cleanup_cancelled_orders_seats(cursor):
    """
    If Orders were manually marked Cancelled-Customer in SQL but seats not released,
    release only seats that are not re-sold to another non-cancelled-customer order.
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
              AND (
                    o2.Status IS NULL
                    OR UPPER(TRIM(o2.Status)) <> 'CANCELLED-CUSTOMER'
                  )
        WHERE UPPER(TRIM(o.Status)) = 'CANCELLED-CUSTOMER'
          AND fs.Seat_Status = 'Sold'             -- FIX: only Sold can be released
          AND o2.Order_code IS NULL
        """
    )
    rows = cursor.fetchall() or []

    for row in rows:
        order_code = row["Order_code"]
        flight_id = row["Flight_id"]

        cursor.execute(
            """
            UPDATE FlightSeats fs
            JOIN Tickets t ON t.FlightSeat_id = fs.FlightSeat_id
            SET fs.Seat_Status = 'Available'
            WHERE t.Order_code = %s
              AND fs.Seat_Status = 'Sold'         -- FIX: never override Blocked
            """,
            (order_code,),
        )

        _update_flight_full_status(cursor, flight_id)


# -------------------------------------------------------------------
# Helper: set seat status for all seats in an order
# -------------------------------------------------------------------

def _set_seat_status_for_order(cursor, order_code, seat_status):
    cursor.execute(
        """
        UPDATE FlightSeats fs
        JOIN Tickets t ON t.FlightSeat_id = fs.FlightSeat_id
        SET fs.Seat_Status = %s
        WHERE t.Order_code = %s
        """,
        (seat_status, order_code),
    )


def _reset_cancelled_seats_price_to_current_class_price(cursor, order_code: str):
    """
    Update Seat_Price for the seats that belong to this order (the seats being cancelled)
    to the CURRENT price of the same Flight + Seat_Class.
    """

    cursor.execute(
        """
        UPDATE FlightSeats fs_cancel
        JOIN Tickets t_cancel
          ON t_cancel.FlightSeat_id = fs_cancel.FlightSeat_id
         AND t_cancel.Order_code = %s
        JOIN Seats s_cancel
          ON s_cancel.Seat_id = fs_cancel.Seat_id

        JOIN (
            SELECT
                fs2.Flight_id,
                s2.Seat_Class,
                COALESCE(
                    MIN(CASE
                            WHEN fs2.Seat_Status IN ('Available','Blocked')
                            THEN fs2.Seat_Price
                        END),
                    MIN(fs2.Seat_Price)
                ) AS class_price
            FROM FlightSeats fs2
            JOIN Seats s2 ON s2.Seat_id = fs2.Seat_id

            -- exclude the seats that belong to THIS cancelled order
            LEFT JOIN Tickets tx
              ON tx.FlightSeat_id = fs2.FlightSeat_id
             AND tx.Order_code    = %s

            WHERE fs2.Seat_Price IS NOT NULL
              AND tx.FlightSeat_id IS NULL

            GROUP BY fs2.Flight_id, s2.Seat_Class
            HAVING class_price IS NOT NULL
        ) p
          ON p.Flight_id  = fs_cancel.Flight_id
         AND p.Seat_Class = s_cancel.Seat_Class

        SET fs_cancel.Seat_Price = p.class_price
        """,
        (order_code, order_code),
    )



# -------------------------------------------------------------------
# Helper: auto-complete order when within 36h of departure
# -------------------------------------------------------------------

def _auto_complete_order_if_due(cursor, order, time_to_dep: timedelta) -> bool:
    if not order:
        return False
    if order.get("Order_Status") != "Active":
        return False
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
                return f"O{(num_part + 1):09d}"
            except Exception:
                return f"O{datetime.now().strftime('%Y%m%d%H')}"
        raise


# -------------------------------------------------------------------
# Helper: customer lookup & guest↔registered logic
# -------------------------------------------------------------------

def _get_registered_customer(cursor, email: str):
    cursor.execute(
        """
        SELECT Customer_Email, First_Name, Last_Name, Passport_No, Birth_Date
        FROM Register_Customers
        WHERE LOWER(Customer_Email) = %s
        """,
        (email.lower(),),
    )
    return cursor.fetchone()


def _get_guest_customer(cursor, email: str):
    cursor.execute(
        """
        SELECT Customer_Email, First_Name, Last_Name
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
        normalized = _normalize_phone_num(phone)
        if not normalized:
            continue

        cursor.execute(
            """
            SELECT 1
            FROM Guest_Customers_Phones
            WHERE LOWER(Customer_Email) = %s
              AND REPLACE(REPLACE(Phone_Number,'-',''),' ','') = %s
            """,
            (email.lower(), normalized),
        )
        if not cursor.fetchone():
            cursor.execute(
                """
                INSERT INTO Guest_Customers_Phones (Customer_Email, Phone_Number)
                VALUES (%s, %s)
                """,
                (email, normalized),
            )


def _insert_registered_phones_from_list(cursor, email: str, phones):
    for phone in phones:
        normalized = _normalize_phone_num(phone)
        if not normalized:
            continue

        cursor.execute(
            """
            SELECT 1
            FROM Register_Customers_Phones
            WHERE LOWER(Customer_Email) = %s
              AND REPLACE(REPLACE(Phone_Number,'-',''),' ','') = %s
            """,
            (email.lower(), normalized),
        )
        if not cursor.fetchone():
            cursor.execute(
                """
                INSERT INTO Register_Customers_Phones (Customer_Email, Phone_Number)
                VALUES (%s, %s)
                """,
                (email, normalized),
            )


# -------------------------------------------------------------------
# Route: Flight search (public)
# -------------------------------------------------------------------

@main_bp.route("/flights/search", methods=["GET"])
def search_flights():
    origin = (request.args.get("origin") or "").strip()
    dest = (request.args.get("dest") or "").strip()
    date_str = (request.args.get("date") or "").strip()
    date_type = (request.args.get("date_type") or "dep").strip().lower()
    if date_type not in ("dep", "arr"):
        date_type = "dep"

    today_str = datetime.now().strftime("%Y-%m-%d")

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    flights = []
    airports = []

    try:
        _sync_seat_status_from_orders(cursor, flight_id=None)
        conn.commit()

        cursor.execute("SELECT Airport_code, City FROM Airports ORDER BY City")
        airports = cursor.fetchall()

        query = """
            SELECT
                f.Flight_id,
                f.Dep_DateTime,
                DATE_ADD(f.Dep_DateTime, INTERVAL fr.Duration_Minutes MINUTE) AS Arr_DateTime,
                a.Model AS AircraftModel,
                fr.Origin_Airport_code,
                fr.Destination_Airport_code,
                fr.Duration_Minutes,
                (
                    SELECT MIN(fs.Seat_Price)
                    FROM FlightSeats fs
                    WHERE fs.Flight_id = f.Flight_id
                      AND fs.Seat_Status = 'Available'
                ) AS Min_Price,
                (
                    SELECT COUNT(*)
                    FROM FlightSeats fs
                    WHERE fs.Flight_id = f.Flight_id
                      AND fs.Seat_Status = 'Available'
                ) AS Available_Seats
            FROM Flights f
            JOIN Flight_Routes fr ON f.Route_id = fr.Route_id
            JOIN Aircrafts a      ON f.Aircraft_id = a.Aircraft_id
            WHERE f.Status IN ('Active','Full-Occupied')
              AND f.Dep_DateTime > NOW()
            ORDER BY f.Dep_DateTime
        """
        cursor.execute(query)
        flights_raw = cursor.fetchall()

        for f in flights_raw:
            available = int(f.get("Available_Seats") or 0)
            if available <= 0:
                continue

            dep_dt = f["Dep_DateTime"]
            arr_dt = f["Arr_DateTime"]

            if date_str:
                if date_type == "dep":
                    if dep_dt.strftime("%Y-%m-%d") != date_str:
                        continue
                else:
                    if arr_dt.strftime("%Y-%m-%d") != date_str:
                        continue

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
        search_params={"origin": origin, "dest": dest, "date": date_str, "date_type": date_type},
        today_str=today_str,
    )


# -------------------------------------------------------------------
# Route: Seat selection
# -------------------------------------------------------------------

@main_bp.route("/booking/<flight_id>/seats", methods=["GET"])
def select_seats(flight_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    flight = None
    available_seats = []
    customer = None
    customer_phones = []

    try:
        cursor.execute(
            """
            SELECT f.Flight_id, f.Dep_DateTime, fr.Duration_Minutes,
                   fr.Origin_Airport_code, fr.Destination_Airport_code, a.Model
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

        # sync for this flight
        _sync_seat_status_from_orders(cursor, flight_id=flight_id)
        _update_flight_full_status(cursor, flight_id)
        conn.commit()

        dep_dt = flight["Dep_DateTime"]
        duration = int(flight["Duration_Minutes"])
        arr_dt = _compute_arrival(dep_dt, duration)

        flight["Dep_str"] = dep_dt.strftime("%d/%m/%Y %H:%M")
        flight["Arr_str"] = arr_dt.strftime("%d/%m/%Y %H:%M")
        flight["Arr_DateTime"] = arr_dt

        if session.get("role") == "customer" and session.get("customer_email"):
            cursor.execute(
                """
                SELECT Customer_Email, First_Name, Last_Name, Passport_No, Birth_Date
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

        cursor.execute(
            """
            SELECT fs.FlightSeat_id, fs.Seat_Price, s.Row_Num, s.Col_Num, s.Seat_Class
            FROM FlightSeats fs
            JOIN Seats s ON fs.Seat_id = s.Seat_id
            WHERE fs.Flight_id = %s
              AND fs.Seat_Status = 'Available'
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

    is_registered = session.get("role") == "customer" and session.get("customer_email")

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
    selected_seat_ids = request.form.getlist("selected_seats")
    if not selected_seat_ids:
        flash("Please select at least one seat.", "error")
        return redirect(url_for("main.select_seats", flight_id=flight_id))

    is_registered = session.get("role") == "customer" and session.get("customer_email")

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    flight = None
    seats_info = []
    customer = None
    customer_phones = []
    guest_info = None
    total_price = 0.0

    try:
        cursor.execute(
            """
            SELECT f.Flight_id, f.Dep_DateTime, fr.Duration_Minutes,
                   fr.Origin_Airport_code, fr.Destination_Airport_code, a.Model
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

        _sync_seat_status_from_orders(cursor, flight_id=flight_id)
        _update_flight_full_status(cursor, flight_id)
        conn.commit()

        dep_dt = flight["Dep_DateTime"]
        duration = int(flight["Duration_Minutes"])
        arr_dt = _compute_arrival(dep_dt, duration)
        flight["Dep_str"] = dep_dt.strftime("%d/%m/%Y %H:%M")
        flight["Arr_str"] = arr_dt.strftime("%d/%m/%Y %H:%M")
        flight["Arr_DateTime"] = arr_dt

        format_strings = ",".join(["%s"] * len(selected_seat_ids))
        cursor.execute(
            f"""
            SELECT fs.FlightSeat_id
            FROM FlightSeats fs
            WHERE fs.FlightSeat_id IN ({format_strings})
              AND fs.Seat_Status <> 'Available'
            """,
            tuple(selected_seat_ids),
        )
        if cursor.fetchall():
            flash("Some of the selected seats were just taken. Please choose seats again.", "error")
            return redirect(url_for("main.select_seats", flight_id=flight_id))

        cursor.execute(
            f"""
            SELECT fs.FlightSeat_id, fs.Seat_Price, s.Row_Num, s.Col_Num, s.Seat_Class
            FROM FlightSeats fs
            JOIN Seats s ON fs.Seat_id = s.Seat_id
            WHERE fs.FlightSeat_id IN ({format_strings})
            ORDER BY s.Seat_Class DESC, s.Row_Num, s.Col_Num
            """,
            tuple(selected_seat_ids),
        )
        seats_info = cursor.fetchall()
        total_price = sum(float(s["Seat_Price"] or 0) for s in seats_info)

        if is_registered:
            customer_email = session["customer_email"]
            cursor.execute(
                """
                SELECT Customer_Email, First_Name, Last_Name, Passport_No, Birth_Date
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
            raw_phones = [(p or "").strip() for p in request.form.getlist("guest_phones")]

            errors = []
            phones_clean = []

            if not first_name:
                errors.append("First name is required.")
            elif not _is_valid_name(first_name):
                errors.append("First name is invalid. Use 2–50 letters (Heb/Eng), spaces, - or '.")

            if not last_name:
                errors.append("Last name is required.")
            elif not _is_valid_name(last_name):
                errors.append("Last name is invalid. Use 2–50 letters (Heb/Eng), spaces, - or '.")

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
                    errors.append(f"Phone {idx} is invalid. Use digits only (7–15; spaces/dashes allowed).")
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
            guest_info = {"first_name": first_name, "last_name": last_name, "email": guest_email, "phones": phones_clean}

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
        print("DB Error in book_seats:", e)
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

        _sync_seat_status_from_orders(cursor, flight_id=flight_id)
        _update_flight_full_status(cursor, flight_id)

        format_strings = ",".join(["%s"] * len(selected_seat_ids))
        cursor.execute(
            f"""
            SELECT fs.FlightSeat_id
            FROM FlightSeats fs
            WHERE fs.FlightSeat_id IN ({format_strings})
              AND fs.Seat_Status <> 'Available'
            FOR UPDATE
            """,
            tuple(selected_seat_ids),
        )
        if cursor.fetchall():
            conn.rollback()
            session.pop("pending_booking", None)
            flash("Some of the selected seats were just taken. Please choose seats again.", "error")
            return redirect(url_for("main.select_seats", flight_id=flight_id))

        if is_registered:
            customer_email = pending.get("customer_email")
            cursor.execute(
                "SELECT Customer_Email FROM Register_Customers WHERE Customer_Email = %s",
                (customer_email,),
            )
            if not cursor.fetchone():
                is_registered = False

        if not is_registered:
            first_name = (pending.get("guest_first_name") or "").strip()
            last_name = (pending.get("guest_last_name") or "").strip()
            guest_email = (pending.get("guest_email") or "").strip()
            guest_phones = pending.get("guest_phones") or []

            if not first_name or not last_name or not guest_email or not guest_phones:
                conn.rollback()
                session.pop("pending_booking", None)
                flash("Guest details are missing. Please start booking again.", "error")
                return redirect(url_for("main.select_seats", flight_id=flight_id))

            customer_email = guest_email

            reg_row = _get_registered_customer(cursor, customer_email)
            if reg_row:
                session["guest_email"] = customer_email
                is_registered = True
                _insert_registered_phones_from_list(cursor, customer_email, guest_phones)
            else:
                guest_row = _get_guest_customer(cursor, customer_email)
                if not guest_row:
                    _insert_guest_customer(cursor, customer_email, first_name, last_name)
                _insert_guest_phones(cursor, customer_email, guest_phones)
                session["guest_email"] = customer_email

        new_order_code = _get_next_order_code(cursor)
        customer_type = "Register" if is_registered else "Guest"
        cursor.execute(
            """
            INSERT INTO Orders (Order_code, Order_Date, Status, Cancel_Date, Customer_Email, Flight_id, Customer_Type)
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
                  AND Seat_Status = 'Available'
                """,
                (seat_id,),
            )
            if cursor.rowcount != 1:
                raise Exception(f"Seat {seat_id} is no longer available.")

            # NEW: store historical paid price on ticket
            cursor.execute(
                "SELECT Seat_Price FROM FlightSeats WHERE FlightSeat_id = %s",
                (seat_id,),
            )
            price_row = cursor.fetchone() or {}
            paid_price = float(price_row.get("Seat_Price") or 0.0)

            cursor.execute(
                "INSERT INTO Tickets (FlightSeat_id, Order_code, Paid_Price) VALUES (%s, %s, %s)",
                (seat_id, new_order_code, paid_price),
            )

        _update_flight_full_status(cursor, flight_id)

        conn.commit()
        session.pop("pending_booking", None)
        flash("Booking completed successfully.", "success")
        return redirect(url_for("main.booking_confirmation", order_code=new_order_code, just_confirmed="1"))

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
    customer_phones = []

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
            LEFT JOIN Register_Customers rc ON rc.Customer_Email = o.Customer_Email
            LEFT JOIN Guest_Customers gc     ON gc.Customer_Email = o.Customer_Email
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

        if _auto_complete_order_if_due(cursor, order, time_to_dep_for_completion):
            conn.commit()

        if order["Order_Status"] == "Active" and order.get("Flight_Status") == "Cancelled":
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

        email = order.get("Customer_Email")
        if email:
            if order.get("Customer_Type") == "Register":
                cursor.execute(
                    """
                    SELECT Phone_Number
                    FROM Register_Customers_Phones
                    WHERE LOWER(Customer_Email) = %s
                    ORDER BY Phone_Number
                    """,
                    (email.lower(),),
                )
                customer_phones = [r["Phone_Number"] for r in (cursor.fetchall() or [])]
            else:
                cursor.execute(
                    """
                    SELECT Phone_Number
                    FROM Guest_Customers_Phones
                    WHERE LOWER(Customer_Email) = %s
                    ORDER BY Phone_Number
                    """,
                    (email.lower(),),
                )
                customer_phones = [r["Phone_Number"] for r in (cursor.fetchall() or [])]

        cursor.execute(
            """
            SELECT t.FlightSeat_id,
                   t.Paid_Price AS Seat_Price,
                   s.Row_Num, s.Col_Num, s.Seat_Class
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

    is_registered = session.get("role") == "customer" and session.get("customer_email")

    return render_template(
        "booking_confirmation.html",
        order=order,
        tickets=tickets,
        total_price=total_price,
        is_registered=is_registered,
        just_confirmed=just_confirmed,
        customer_phones=customer_phones,
    )


# -------------------------------------------------------------------
# Registered customer: orders list + filter + cancellation (through  registered customer area)
# -------------------------------------------------------------------

@main_bp.route("/customer/orders")
def customer_orders():
    if not _require_customer():
        return redirect(url_for("main.customer_orders_login"))

    status_filter = request.args.get("status", "all")
    valid_statuses = {"Active", "Completed", "Cancelled-Customer", "Cancelled-System"}
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
                COALESCE(SUM(t.Paid_Price), 0)  AS Total_Price
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

        base_query += " GROUP BY o.Order_code ORDER BY o.Order_Date DESC"

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

    return render_template("customer_orders.html", orders=orders, status_filter=status_filter)


# -------------------------------------------------------------------
# Registered customer: cancel order - set seats back to Available
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
            SELECT o.Order_code, o.Status AS Order_Status, o.Customer_Email,
                   f.Flight_id, f.Dep_DateTime
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

        # Total before any price reset (keeps historical purchase amounts intact)
        cursor.execute(
            """
            SELECT COALESCE(SUM(t.Paid_Price), 0) AS Total_Price
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

        # ===== NEW: reset cancelled seats price to current class price (only if there are Available seats now) =====
        _reset_cancelled_seats_price_to_current_class_price(cursor, order_code)

        # Release seats
        _set_seat_status_for_order(cursor, order_code, "Available")

        # Mark order cancelled
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
            f"Order cancelled successfully. Total was ${total_amount:.2f}. Fee (5%): ${fee:.2f}. Refund: ${refund:.2f}.",
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
# Guest: order lookup (view order details by input email and order_code)
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
            # allow lookup for BOTH Guest and Register orders
            cursor.execute(
                """
                SELECT Order_code, Customer_Type
                FROM Orders
                WHERE Order_code = %s
                  AND Customer_Email = %s
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
            flash("No order found for this email and order ID.", "error")
            return render_template("guest_order_lookup.html")

        session.pop("role", None)
        session.pop("customer_email", None)
        session.pop("customer_name", None)

        session["guest_email"] = email

        return redirect(url_for("main.booking_confirmation", order_code=order_code))

    return render_template("guest_order_lookup.html")


# -------------------------------------------------------------------
# Guest: cancel order set seats back to Available
# -------------------------------------------------------------------

@main_bp.route("/guest/orders/<order_code>/cancel", methods=["POST"])
def guest_cancel_order(order_code):

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
            SELECT o.Order_code, o.Status AS Order_Status, o.Customer_Email, o.Customer_Type,
                   f.Flight_id, f.Dep_DateTime
            FROM Orders o
            JOIN Flights f ON o.Flight_id = f.Flight_id
            WHERE o.Order_code = %s
              AND o.Customer_Email = %s
            FOR UPDATE
            """,
            (order_code, guest_email),
        )
        order = cursor.fetchone()
        if not order:
            flash("Order not found or does not belong to this email.", "error")
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

        # Total before any price reset (keeps historical purchase amounts intact)
        cursor.execute(
            """
            SELECT COALESCE(SUM(t.Paid_Price), 0) AS Total_Price
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

        _reset_cancelled_seats_price_to_current_class_price(cursor, order_code)

        # Release seats
        _set_seat_status_for_order(cursor, order_code, "Available")

        # Mark order cancelled
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
            f"Order cancelled successfully. Total was ${total_amount:.2f}. Fee (5%): ${fee:.2f}. Refund: ${refund:.2f}.",
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
