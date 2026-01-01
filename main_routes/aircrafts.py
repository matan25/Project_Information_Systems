"""
Aircraft management for FLYTAU.

Manager features:
- View full aircraft fleet.
- Create new aircraft (auto ID with AC + manufacturer initial + running number).
- Configure seat layout for a new aircraft:
    * Small aircraft: Economy seats only.
    * Large aircraft: Business rows first, then Economy rows.
- Seats are generated into Seats table using a concurrency-safe IdCounters
  mechanism for Seat_id (prefix S).

Assumptions:
- Aircrafts table:
    Aircraft_id (PK), Manufacturer, Model, Size, Purchase_Date
- Seats table:
    Seat_id (PK), Aircraft_id (FK), Row_Num, Col_Num, Seat_Class
- IdCounters(Name PK, NextNum BIGINT) exists and is used for numeric sequences.
"""

from datetime import datetime

from flask import render_template, redirect, url_for, request, flash
from mysql.connector import Error, errorcode

from db import get_db_connection
from . import main_bp, _require_manager


# -------------------------------------------------------------------
# Generic helpers for ID generation
# -------------------------------------------------------------------


def _get_next_seat_number(cursor) -> int:
    """
    Backward-compatible helper:
    Returns next numeric part for Seat_id prefix S.

    NOTE:
    This MAX-based approach is NOT safe under concurrency.
    We keep it as a fallback when IdCounters is not available.
    """
    cursor.execute(
        """
        SELECT MAX(CAST(SUBSTRING(Seat_id, 2) AS UNSIGNED)) AS max_num
        FROM Seats
        WHERE UPPER(LEFT(Seat_id, 1)) = 'S'
        """
    )
    row = cursor.fetchone()
    current_max = row["max_num"] or 0
    return int(current_max) + 1


def _reserve_seat_block(cursor, amount: int) -> int:
    """
    Concurrency-safe reservation for Seat_id numbers using IdCounters.

    Requires table:
        IdCounters(Name PK, NextNum BIGINT)

    Name used: 'Seat'
    """
    if amount <= 0:
        raise ValueError("amount must be positive")

    try:
        cursor.execute(
            "SELECT NextNum FROM IdCounters WHERE Name = %s FOR UPDATE",
            ("Seat",),
        )
        row = cursor.fetchone()

        if row is None:
            # First time: derive starting value from existing Seats.
            cursor.execute(
                """
                SELECT COALESCE(MAX(CAST(SUBSTRING(Seat_id, 2) AS UNSIGNED)), 0) AS max_num
                FROM Seats
                WHERE UPPER(LEFT(Seat_id, 1)) = 'S'
                FOR UPDATE
                """
            )
            m = cursor.fetchone()
            start = int((m or {}).get("max_num", 0) or 0) + 1

            try:
                cursor.execute(
                    "INSERT INTO IdCounters (Name, NextNum) VALUES (%s, %s)",
                    ("Seat", start + amount),
                )
            except Error as e:
                # Handle race: another transaction inserted Seat counter
                if getattr(e, "errno", None) == errorcode.ER_DUP_ENTRY:
                    cursor.execute(
                        "SELECT NextNum FROM IdCounters WHERE Name = %s FOR UPDATE",
                        ("Seat",),
                    )
                    row2 = cursor.fetchone()
                    if not row2:
                        raise
                    start = int(row2["NextNum"])
                    cursor.execute(
                        "UPDATE IdCounters SET NextNum = %s WHERE Name = %s",
                        (start + amount, "Seat"),
                    )
                else:
                    raise

            return start

        # Counter row already exists
        start = int(row["NextNum"])
        cursor.execute(
            "UPDATE IdCounters SET NextNum = %s WHERE Name = %s",
            (start + amount, "Seat"),
        )
        return start

    except Error as e:
        if getattr(e, "errno", None) == errorcode.ER_NO_SUCH_TABLE:
            print(
                "WARNING: IdCounters table missing; falling back to MAX()+1 (not concurrency-safe)."
            )
            return _get_next_seat_number(cursor)
        raise


def _get_next_aircraft_number(cursor) -> int:
    """
    Backward-compatible helper:
    Returns next numeric part for Aircraft_id, assuming pattern like:
        ACB001, ACA002, ACD003, etc.

    We always parse from position 4 onwards as the numeric part (if any),
    and we only consider IDs starting with 'AC'.
    """
    cursor.execute(
        """
        SELECT MAX(CAST(SUBSTRING(Aircraft_id, 4) AS UNSIGNED)) AS max_num
        FROM Aircrafts
        WHERE UPPER(LEFT(Aircraft_id, 2)) = 'AC'
        """
    )
    row = cursor.fetchone()
    current_max = row["max_num"] or 0
    return int(current_max) + 1


def _reserve_aircraft_number(cursor, amount: int = 1) -> int:
    """
    Concurrency-safe reservation for Aircraft numeric suffix using IdCounters.

    Patterns generated:
        - Aircraft_id = 'AC' + manufacturer_initial + <running 3-digit number>
          e.g. 'ACB001', 'ACA002', 'ACD003', ...

    We keep the numeric part global across all aircrafts for simplicity.

    IdCounters row name: 'Aircraft'
    """
    if amount <= 0:
        raise ValueError("amount must be positive")

    try:
        cursor.execute(
            "SELECT NextNum FROM IdCounters WHERE Name = %s FOR UPDATE",
            ("Aircraft",),
        )
        row = cursor.fetchone()

        if row is None:
            cursor.execute(
                """
                SELECT COALESCE(MAX(CAST(SUBSTRING(Aircraft_id, 4) AS UNSIGNED)), 0) AS max_num
                FROM Aircrafts
                WHERE UPPER(LEFT(Aircraft_id, 2)) = 'AC'
                FOR UPDATE
                """
            )
            m = cursor.fetchone()
            start = int((m or {}).get("max_num", 0) or 0) + 1

            try:
                cursor.execute(
                    "INSERT INTO IdCounters (Name, NextNum) VALUES (%s, %s)",
                    ("Aircraft", start + amount),
                )
            except Error as e:
                if getattr(e, "errno", None) == errorcode.ER_DUP_ENTRY:
                    cursor.execute(
                        "SELECT NextNum FROM IdCounters WHERE Name = %s FOR UPDATE",
                        ("Aircraft",),
                    )
                    row2 = cursor.fetchone()
                    if not row2:
                        raise
                    start = int(row2["NextNum"])
                    cursor.execute(
                        "UPDATE IdCounters SET NextNum = %s WHERE Name = %s",
                        (start + amount, "Aircraft"),
                    )
                else:
                    raise

            return start

        start = int(row["NextNum"])
        cursor.execute(
            "UPDATE IdCounters SET NextNum = %s WHERE Name = %s",
            (start + amount, "Aircraft"),
        )
        return start

    except Error as e:
        if getattr(e, "errno", None) == errorcode.ER_NO_SUCH_TABLE:
            print(
                "WARNING: IdCounters table missing; falling back to MAX()+1 (not concurrency-safe)."
            )
            return _get_next_aircraft_number(cursor)
        raise


def _build_aircraft_id(manufacturer: str, numeric_suffix: int) -> str:
    """
    Build Aircraft_id in the format:
        AC + <first letter of manufacturer> + 3-digit number

    Examples:
        manufacturer = 'Boeing',  numeric_suffix = 1  -> 'ACB001'
        manufacturer = 'Airbus',  numeric_suffix = 7  -> 'ACA007'
        manufacturer = 'Dasso',   numeric_suffix = 23 -> 'ACD023'
    """
    if not manufacturer:
        initial = "X"
    else:
        initial = manufacturer.strip()[0].upper()

    return f"AC{initial}{numeric_suffix:03d}"


# -------------------------------------------------------------------
# Routes
# -------------------------------------------------------------------


@main_bp.route("/manager/aircrafts")
def manager_aircrafts():
    """
    Show full aircraft fleet for the manager.

    Columns:
      - Aircraft ID
      - Manufacturer
      - Model
      - Size
      - Purchase date
      - Seat count (from Seats)
    """
    if not _require_manager():
        return redirect(url_for("auth.login"))

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute(
            """
            SELECT
                a.Aircraft_id,
                a.Manufacturer,
                a.Model,
                a.Size,
                a.Purchase_Date,
                COUNT(s.Seat_id) AS SeatCount
            FROM Aircrafts a
            LEFT JOIN Seats s ON s.Aircraft_id = a.Aircraft_id
            GROUP BY
                a.Aircraft_id,
                a.Manufacturer,
                a.Model,
                a.Size,
                a.Purchase_Date
            ORDER BY a.Aircraft_id
            """
        )
        aircrafts = cursor.fetchall()

        return render_template(
            "manager_aircrafts_list.html",
            aircrafts=aircrafts,
            lock_manager_nav=False,
        )

    except Error as e:
        print("DB error in manager_aircrafts:", e)
        flash("Failed to load aircraft fleet.", "error")
        return render_template(
            "manager_aircrafts_list.html",
            aircrafts=[],
            lock_manager_nav=False,
        )
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@main_bp.route("/manager/aircrafts/new", methods=["GET", "POST"])
def manager_new_aircraft():
    """
    Step 1: create a new aircraft record.

    Manager provides:
      - Manufacturer (Boeing / Airbus / Dasso)
      - Model (free text)
      - Size (Small / Large)

    The system:
      - Generates Aircraft_id using AC + manufacturer initial + running number.
      - Sets Purchase_Date to current timestamp.
      - Inserts row into Aircrafts.
      - Redirects to seat-layout configuration for the new aircraft.
    """
    if not _require_manager():
        return redirect(url_for("auth.login"))

    manufacturers = ["Boeing", "Airbus", "Dasso"]
    sizes = ["Small", "Large"]

    if request.method == "GET":
        form_aircraft = {
            "Manufacturer": "",
            "Model": "",
            "Size": "Small",
        }
        return render_template(
            "manager_aircrafts_form.html",
            aircraft=form_aircraft,
            manufacturers=manufacturers,
            sizes=sizes,
            lock_manager_nav=True,  # lock manager navigation during creation flow
        )

    # POST
    manufacturer = (request.form.get("manufacturer") or "").strip()
    model = (request.form.get("model") or "").strip()
    size = (request.form.get("size") or "").strip()

    form_aircraft = {
        "Manufacturer": manufacturer,
        "Model": model,
        "Size": size or "Small",
    }

    if not manufacturer or not model or size not in sizes:
        flash("Please fill all fields and select a valid size.", "error")
        return render_template(
            "manager_aircrafts_form.html",
            aircraft=form_aircraft,
            manufacturers=manufacturers,
            sizes=sizes,
            lock_manager_nav=True,
        )

    if manufacturer not in manufacturers:
        flash("Invalid manufacturer selected.", "error")
        return render_template(
            "manager_aircrafts_form.html",
            aircraft=form_aircraft,
            manufacturers=manufacturers,
            sizes=sizes,
            lock_manager_nav=True,
        )

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Reserve numeric suffix for Aircraft_id
        num = _reserve_aircraft_number(cursor, 1)
        aircraft_id = _build_aircraft_id(manufacturer, num)

        # Ensure uniqueness (defensive)
        cursor.execute(
            "SELECT 1 FROM Aircrafts WHERE Aircraft_id = %s",
            (aircraft_id,),
        )
        if cursor.fetchone():
            flash("Failed to generate a unique Aircraft ID. Please try again.", "error")
            conn.rollback()
            return render_template(
                "manager_aircrafts_form.html",
                aircraft=form_aircraft,
                manufacturers=manufacturers,
                sizes=sizes,
                lock_manager_nav=True,
            )

        purchase_date = datetime.now()

        cursor.execute(
            """
            INSERT INTO Aircrafts (Aircraft_id, Manufacturer, Model, Size, Purchase_Date)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (aircraft_id, manufacturer, model, size, purchase_date),
        )

        conn.commit()

        # Redirect to seat-layout configuration for this aircraft
        flash("Aircraft created successfully. Please configure its seats.", "success")
        return redirect(url_for("main.manager_aircraft_seats", aircraft_id=aircraft_id))

    except Error as e:
        print("DB error in manager_new_aircraft:", e)
        flash("Failed to create aircraft.", "error")
        return render_template(
            "manager_aircrafts_form.html",
            aircraft=form_aircraft,
            manufacturers=manufacturers,
            sizes=sizes,
            lock_manager_nav=True,
        )
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@main_bp.route("/manager/aircrafts/<aircraft_id>/seats", methods=["GET", "POST"])
def manager_aircraft_seats(aircraft_id):
    """
    Step 2: configure seat layout for an aircraft.

    Rules:
      - Small aircraft: Economy seats only.
      - Large aircraft: Business rows first, then Economy rows.
      - Row numbers are contiguous across classes:
            1..Business_rows, then continuing for Economy.
      - Seat_id generated with prefix 'S' and numeric suffix using IdCounters.

    Form fields:

      For Small:
        - eco_rows
        - eco_cols

      For Large:
        - biz_rows
        - biz_cols
        - eco_rows
        - eco_cols
    """
    if not _require_manager():
        return redirect(url_for("auth.login"))

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Load aircraft header
        cursor.execute(
            """
            SELECT
                Aircraft_id,
                Manufacturer,
                Model,
                Size,
                Purchase_Date
            FROM Aircrafts
            WHERE Aircraft_id = %s
            """,
            (aircraft_id,),
        )
        aircraft = cursor.fetchone()

        if not aircraft:
            flash("Aircraft not found.", "error")
            return redirect(url_for("main.manager_aircrafts"))

        size = aircraft["Size"]

        if request.method == "POST":
            # Parse form input
            def _parse_positive_int(field_name):
                val_str = (request.form.get(field_name) or "").strip()
                try:
                    val = int(val_str)
                    if val <= 0:
                        raise ValueError
                    return val
                except ValueError:
                    raise ValueError(f"Invalid value for {field_name}")

            try:
                if size == "Small":
                    eco_rows = _parse_positive_int("eco_rows")
                    eco_cols = _parse_positive_int("eco_cols")

                    biz_rows = 0
                    biz_cols = 0
                else:
                    biz_rows = _parse_positive_int("biz_rows")
                    biz_cols = _parse_positive_int("biz_cols")
                    eco_rows = _parse_positive_int("eco_rows")
                    eco_cols = _parse_positive_int("eco_cols")

            except ValueError:
                flash("All row and seat fields must be positive integers.", "error")
                return render_template(
                    "manager_aircraft_seats_form.html",
                    aircraft=aircraft,
                    size=size,
                    lock_manager_nav=True,
                )

            total_seats = 0
            if size == "Small":
                total_seats = eco_rows * eco_cols
            else:
                total_seats = biz_rows * biz_cols + eco_rows * eco_cols

            if total_seats <= 0:
                flash("Total number of seats must be positive.", "error")
                return render_template(
                    "manager_aircraft_seats_form.html",
                    aircraft=aircraft,
                    size=size,
                    lock_manager_nav=True,
                )

            # Remove existing seats (if any), then recreate
            cursor.execute(
                "DELETE FROM Seats WHERE Aircraft_id = %s",
                (aircraft_id,),
            )

            start_num = _reserve_seat_block(cursor, total_seats)
            next_num = start_num

            # Generate seats: Business first, then Economy
            current_row = 1

            # Large aircraft: Business rows first
            if size == "Large":
                for r in range(1, biz_rows + 1):
                    for c in range(1, biz_cols + 1):
                        seat_id = f"S{next_num:03d}"
                        cursor.execute(
                            """
                            INSERT INTO Seats
                                (Seat_id, Aircraft_id, Row_Num, Col_Num, Seat_Class)
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (seat_id, aircraft_id, current_row, c, "Business"),
                        )
                        next_num += 1
                    current_row += 1

            # Economy rows (Small or Large)
            for r in range(1, eco_rows + 1):
                for c in range(1, eco_cols + 1):
                    seat_id = f"S{next_num:03d}"
                    cursor.execute(
                        """
                        INSERT INTO Seats
                            (Seat_id, Aircraft_id, Row_Num, Col_Num, Seat_Class)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (seat_id, aircraft_id, current_row, c, "Economy"),
                    )
                    next_num += 1
                current_row += 1

            conn.commit()
            flash("Seats generated successfully and aircraft was added to the fleet.", "success")
            return redirect(url_for("main.manager_aircrafts"))

        # GET flow – show seat layout form
        return render_template(
            "manager_aircraft_seats_form.html",
            aircraft=aircraft,
            size=size,
            lock_manager_nav=True,  # lock navigation during seat layout step
        )

    except Error as e:
        print("DB error in manager_aircraft_seats:", e)
        flash("Failed to configure seats for this aircraft.", "error")
        return redirect(url_for("main.manager_aircrafts"))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
