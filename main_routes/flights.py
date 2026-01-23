"""
Flight management:
- list flights
- create flights (generate FlightSeats with FS000001...)
- edit flights (rules: no past dates, Completed only after arrival, Cancel only >=72h)
- seat pricing & seat status management
- automatic statuses:
    * Completed: arrival time passed
    * Full-occupied: all FlightSeats are Sold or Blocked
- history view (read-only):
    * for flights that cannot be edited anymore (past departure / cancelled / completed)
    * includes crew + seat statuses and prices

- arrival DateTime is treated as a derived field from Dep_DateTime + route Duration_Minutes.
"""

from datetime import datetime, timedelta

from flask import render_template, redirect, url_for, request, flash
from mysql.connector import Error, errorcode

from db import get_db_connection
from . import (
    main_bp,
    _require_manager,
    _flight_profile,
    _get_default_seat_price,
    LONG_FLIGHT_THRESHOLD_MINUTES,
)

# -----------------------------
# Helpers
# -----------------------------


def _compute_arrival(dep_dt: datetime, duration_minutes: int) -> datetime:
    """
    Utility: compute arrival time from departure + route duration.
    This is the single source of truth for arrival time in the app logic.
    """
    return dep_dt + timedelta(minutes=int(duration_minutes))


def _aircraft_has_conflict(cursor, aircraft_id, dep_dt, arr_dt, ignore_flight_id=None) -> bool:
    """
    Checks if the given aircraft already has a flight that overlaps
    with (dep_dt, arr_dt).
    Arrival is derived using Flight_Routes.Duration_Minutes.
    Cancelled flights are ignored.
    """
    params = [aircraft_id, dep_dt, arr_dt]
    ignore_clause = ""
    if ignore_flight_id is not None:
        ignore_clause = "AND f.Flight_id <> %s"
        params.append(ignore_flight_id)

    cursor.execute(
        f"""
        SELECT 1
        FROM Flights f
        JOIN Flight_Routes r ON f.Route_id = r.Route_id
        WHERE f.Aircraft_id = %s
          AND f.Status <> 'Cancelled'
          AND NOT (
                DATE_ADD(f.Dep_DateTime, INTERVAL r.Duration_Minutes MINUTE) <= %s
            OR  f.Dep_DateTime >= %s
          )
          {ignore_clause}
        LIMIT 1
        """,
        tuple(params),
    )
    return cursor.fetchone() is not None


def _aircraft_location_ok(
    cursor,
    aircraft_id,
    route_id,
    dep_dt: datetime,
    duration_minutes: int,
    ignore_flight_id=None,
) -> bool:
    """
    Enforce aircraft positioning rule:

    - For NEW flights:
        * An aircraft must start a flight from the airport where it last landed.
        * It must also land at the airport from which its next scheduled flight
          will depart.
        * The very first non-cancelled flight of an aircraft has no "starting"
          location constraint.
        * Cancelled flights are ignored for positioning.
    """
    if not aircraft_id or not route_id or dep_dt is None or duration_minutes is None:
        # Defensive fallback – do not block if data is incomplete
        return True

    # ---- Relaxed rule for existing flights (edit mode) ----
    if ignore_flight_id is not None:
        # עריכת טיסה קיימת: מניחים שהמטוס כבר רולוקייט ליעד הנדרש.
        # לא מפעילים כלל לוקיישן כדי לא "לשבור שרשרת" אחרי ביטולים באמצע.
        return True

    # From here on – strict rule (used for new flights only)

    # Get origin/destination of the route for the new / edited flight
    cursor.execute(
        """
        SELECT Origin_Airport_code, Destination_Airport_code
        FROM Flight_Routes
        WHERE Route_id = %s
        """,
        (route_id,),
    )
    route_row = cursor.fetchone()
    if not route_row:
        # Route was already validated elsewhere – if missing, do not fail here
        return True

    new_origin = route_row["Origin_Airport_code"]
    new_dest = route_row["Destination_Airport_code"]
    arr_dt = _compute_arrival(dep_dt, duration_minutes)

    # ---- Check previous flights: last arrival airport must match new_origin ----
    prev_params = [aircraft_id, arr_dt]
    prev_ignore_clause = ""
    if ignore_flight_id is not None:
        prev_ignore_clause = "AND f2.Flight_id <> %s"
        prev_params.append(ignore_flight_id)

    cursor.execute(
        f"""
        SELECT
            r2.Destination_Airport_code AS LastDest,
            DATE_ADD(f2.Dep_DateTime, INTERVAL r2.Duration_Minutes MINUTE) AS LastArrive
        FROM Flights f2
        JOIN Flight_Routes r2 ON f2.Route_id = r2.Route_id
        WHERE f2.Aircraft_id = %s
          AND f2.Status <> 'Cancelled'
          AND DATE_ADD(f2.Dep_DateTime, INTERVAL r2.Duration_Minutes MINUTE) <= %s
          {prev_ignore_clause}
        ORDER BY LastArrive DESC
        LIMIT 1
        """,
        tuple(prev_params),
    )
    prev_row = cursor.fetchone()

    # If there is a previous non-cancelled flight, its destination must match new_origin
    if prev_row is not None:
        if prev_row["LastDest"] != new_origin:
            return False
    # If there is no previous flight at all → this is the first flight: allowed.

    # ---- Check next flights: new_dest must match the origin of the next flight ----
    next_params = [aircraft_id, arr_dt]
    next_ignore_clause = ""
    if ignore_flight_id is not None:
        next_ignore_clause = "AND f2.Flight_id <> %s"
        next_params.append(ignore_flight_id)

    cursor.execute(
        f"""
        SELECT
            r2.Origin_Airport_code AS NextOrigin,
            f2.Dep_DateTime        AS NextDep
        FROM Flights f2
        JOIN Flight_Routes r2 ON f2.Route_id = r2.Route_id
        WHERE f2.Aircraft_id = %s
          AND f2.Status <> 'Cancelled'
          AND f2.Dep_DateTime >= %s
          {next_ignore_clause}
        ORDER BY f2.Dep_DateTime ASC
        LIMIT 1
        """,
        tuple(next_params),
    )
    next_row = cursor.fetchone()

    if next_row is not None:
        # The next flight's origin must match where we land now
        if next_row["NextOrigin"] != new_dest:
            return False

    return True


def _load_routes_and_aircrafts():
    """
    Loads routes and aircrafts for manager forms
    Returns (routes_list, aircrafts_list)
    """
    conn = None
    cursor = None
    routes = []
    aircrafts = []
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Routes + airport cities (for showing "TLV (Tel Aviv) → LHR (London)")
        cursor.execute(
            """
            SELECT
                fr.Route_id,
                fr.Origin_Airport_code,
                fr.Destination_Airport_code,
                fr.Duration_Minutes,
                ao.City AS Origin_City,
                ad.City AS Destination_City
            FROM Flight_Routes fr
            JOIN Airports ao ON ao.Airport_code = fr.Origin_Airport_code
            JOIN Airports ad ON ad.Airport_code = fr.Destination_Airport_code
            ORDER BY fr.Origin_Airport_code, fr.Destination_Airport_code
            """
        )
        routes = cursor.fetchall()

        # Only aircrafts with at least one seat
        cursor.execute(
            """
            SELECT
                a.Aircraft_id,
                a.Model,
                a.Manufacturer,
                a.Size,
                COUNT(s.Seat_id) AS SeatCount
            FROM Aircrafts a
            LEFT JOIN Seats s ON s.Aircraft_id = a.Aircraft_id
            GROUP BY
                a.Aircraft_id,
                a.Model,
                a.Manufacturer,
                a.Size
            HAVING COUNT(s.Seat_id) > 0
            ORDER BY a.Model
            """
        )
        aircrafts = cursor.fetchall()

    except Error as e:
        print("DB error in _load_routes_and_aircrafts:", e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return routes, aircrafts


def _get_next_flight_seat_number(cursor) -> int:
    """
    Returns next numeric part for FlightSeat_id prefix FS
    """
    cursor.execute(
        """
        SELECT MAX(CAST(SUBSTRING(FlightSeat_id, 3) AS UNSIGNED)) AS max_num
        FROM FlightSeats
        WHERE UPPER(LEFT(FlightSeat_id, 2)) = 'FS'
        """
    )
    row = cursor.fetchone()
    current_max = row["max_num"] or 0
    return int(current_max) + 1


def _reserve_flightseat_block(cursor, amount: int) -> int:
    """
    Concurrency-safe reservation for FS numbers using IdCounters.

    Requires table:
        IdCounters(Name PK, NextNum BIGINT)
    """
    if amount <= 0:
        raise ValueError("amount must be positive")

    try:
        cursor.execute(
            "SELECT NextNum FROM IdCounters WHERE Name = %s FOR UPDATE",
            ("FlightSeat",),
        )
        row = cursor.fetchone()

        if row is None:
            cursor.execute(
                """
                SELECT COALESCE(MAX(CAST(SUBSTRING(FlightSeat_id, 3) AS UNSIGNED)), 0) AS max_num
                FROM FlightSeats
                WHERE UPPER(LEFT(FlightSeat_id, 2)) = 'FS'
                FOR UPDATE
                """
            )
            m = cursor.fetchone()
            start = int((m or {}).get("max_num", 0) or 0) + 1

            try:
                cursor.execute(
                    "INSERT INTO IdCounters (Name, NextNum) VALUES (%s, %s)",
                    ("FlightSeat", start + amount),
                )
            except Error as e:
                if getattr(e, "errno", None) == errorcode.ER_DUP_ENTRY:
                    cursor.execute(
                        "SELECT NextNum FROM IdCounters WHERE Name = %s FOR UPDATE",
                        ("FlightSeat",),
                    )
                    row2 = cursor.fetchone()
                    if not row2:
                        raise
                    start = int(row2["NextNum"])
                    cursor.execute(
                        "UPDATE IdCounters SET NextNum = %s WHERE Name = %s",
                        (start + amount, "FlightSeat"),
                    )
                else:
                    raise

            return start

        start = int(row["NextNum"])
        cursor.execute(
            "UPDATE IdCounters SET NextNum = %s WHERE Name = %s",
            (start + amount, "FlightSeat"),
        )
        return start

    except Error as e:
        if getattr(e, "errno", None) == errorcode.ER_NO_SUCH_TABLE:
            print("WARNING: IdCounters table missing; falling back to MAX()+1 (not concurrency-safe).")
            return _get_next_flight_seat_number(cursor)
        raise


def _get_next_flight_id(cursor) -> str:
    """
    Generate the next Flight_id in the format 'FT001', 'FT002', ...
    Uses IdCounters(Name='Flight')
    """
    try:
        cursor.execute(
            "SELECT NextNum FROM IdCounters WHERE Name = %s FOR UPDATE",
            ("Flight",),
        )
        row = cursor.fetchone()

        if row is None:
            cursor.execute(
                """
                SELECT COALESCE(
                    MAX(CAST(SUBSTRING(Flight_id, 3) AS UNSIGNED)), 0
                ) AS max_num
                FROM Flights
                WHERE UPPER(LEFT(Flight_id, 2)) = 'FT'
                FOR UPDATE
                """
            )
            m = cursor.fetchone() or {}
            current_max = int(m.get("max_num", 0) or 0)
            next_num = current_max + 1

            cursor.execute(
                "INSERT INTO IdCounters (Name, NextNum) VALUES (%s, %s)",
                ("Flight", next_num + 1),
            )
            return f"FT{next_num:03d}"

        next_num = int(row["NextNum"])
        cursor.execute(
            "UPDATE IdCounters SET NextNum = %s WHERE Name = %s",
            (next_num + 1, "Flight"),
        )
        return f"FT{next_num:03d}"

    except Error as e:
        if getattr(e, "errno", None) == errorcode.ER_NO_SUCH_TABLE:
            cursor.execute(
                """
                SELECT MAX(Flight_id) AS max_id
                FROM Flights
                WHERE UPPER(LEFT(Flight_id, 2)) = 'FT'
                """
            )
            row = cursor.fetchone()
            max_id = row["max_id"] if row else None

            if not max_id:
                return "FT001"

            try:
                num_part = int(max_id[2:])
                new_num = num_part + 1
                return f"FT{new_num:03d}"
            except Exception:
                # Fallback in case of unexpected format
                return f"FT{datetime.now().strftime('%y%m%d%H')}"
        else:
            raise


# ===== Crew-availability helpers =====


def _required_crew_for_size(size: str):
    """
    Crew rules used at creation/edit-time
    For now:
      - Short-haul:
          * 2 pilots
          * 3 attendants
      - Long-haul (Large aircraft only):
          * 3 pilots
          * 6 attendants
    """
    if size == "Large":
        return 3, 6  # pilots, attendants
    return 2, 3


def _has_enough_crew_for_window(
    cursor,
    dep_dt: datetime,
    arr_dt: datetime,
    aircraft_size: str,
    origin_airport: str,
    dest_airport: str,
    ignore_flight_id=None,
) -> bool:
    """
    Check whether there are enough available qualified crew members
    (pilots + attendants) for the given flight time window.

    NEW flight (ignore_flight_id is None):
      - Enforces NO time overlaps with other assigned flights.
      - Enforces full location continuity relative to the closest previous/next
        NON-cancelled flights (crew must be at the correct airport).
      - If the route is long-haul (Duration > LONG_FLIGHT_THRESHOLD_MINUTES),
        requires Long_Haul_Certified = 1.

    EDIT flight (ignore_flight_id is not None):
      - Enforces NO time overlaps (excluding ignore_flight_id).
      - Skips location continuity checks (assumes possible relocation).
      - Long-haul certification requirement still applies.
    """
    req_pilots, req_attendants = _required_crew_for_size(aircraft_size)

    duration_minutes = int((arr_dt - dep_dt).total_seconds() // 60)
    is_long_route = duration_minutes > LONG_FLIGHT_THRESHOLD_MINUTES
    long_flag = 1 if is_long_route else 0

    edit_mode = ignore_flight_id is not None
    current_flight_id = ignore_flight_id if edit_mode else "__NEW__"

    # ---------- EDIT MODE: בודק רק חפיפות זמן (ללא לוקיישן) ----------
    if edit_mode:
        # --- Pilots: זמן + תעודת long-haul (אם נדרש) ---
        pilot_sql_query = """
            SELECT COUNT(*) AS cnt
            FROM Pilots p
            WHERE
              (%s = 0 OR COALESCE(p.Long_Haul_Certified, 0) = 1)
              AND NOT EXISTS (
                SELECT 1
                FROM FlightCrew_Pilots fcp
                JOIN Flights       f2 ON f2.Flight_id = fcp.Flight_id
                JOIN Flight_Routes r2 ON f2.Route_id  = r2.Route_id
                WHERE fcp.Pilot_id = p.Pilot_id
                  AND fcp.Flight_id <> %s
                  AND NOT (
                        DATE_ADD(f2.Dep_DateTime, INTERVAL r2.Duration_Minutes MINUTE) <= %s
                    OR  f2.Dep_DateTime >= %s
                  )
              )
        """
        pilot_params_query = (long_flag, current_flight_id, dep_dt, arr_dt)

        cursor.execute(pilot_sql_query, pilot_params_query)
        pilots_available = int(cursor.fetchone()["cnt"])

        # --- Attendants: long-haul ---
        attendant_sql_query = """
            SELECT COUNT(*) AS cnt
            FROM FlightAttendants fa
            WHERE
              (%s = 0 OR COALESCE(fa.Long_Haul_Certified, 0) = 1)
              AND NOT EXISTS (
                SELECT 1
                FROM FlightCrew_Attendants fca
                JOIN Flights       f2 ON f2.Flight_id = fca.Flight_id
                JOIN Flight_Routes r2 ON f2.Route_id  = r2.Route_id
                WHERE fca.Attendant_id = fa.Attendant_id
                  AND fca.Flight_id <> %s
                  AND NOT (
                        DATE_ADD(f2.Dep_DateTime, INTERVAL r2.Duration_Minutes MINUTE) <= %s
                    OR  f2.Dep_DateTime >= %s
                  )
              )
        """
        attendant_params_query = (long_flag, current_flight_id, dep_dt, arr_dt)

        cursor.execute(attendant_sql_query, attendant_params_query)
        attendants_available = int(cursor.fetchone()["cnt"])

        return (
            pilots_available >= req_pilots
            and attendants_available >= req_attendants
        )

    # ---------- NEW FLIGHT MODE: ----------

    # Pilots
    pilot_sql_query= """
        SELECT COUNT(*) AS cnt
        FROM Pilots p
        WHERE
          (%s = 0 OR COALESCE(p.Long_Haul_Certified, 0) = 1)
          AND
          NOT EXISTS (
            SELECT 1
            FROM FlightCrew_Pilots fcp
            JOIN Flights       f2 ON f2.Flight_id = fcp.Flight_id
            JOIN Flight_Routes r2 ON f2.Route_id  = r2.Route_id
            WHERE fcp.Pilot_id = p.Pilot_id
              AND fcp.Flight_id <> %s
              AND NOT (
                    DATE_ADD(f2.Dep_DateTime, INTERVAL r2.Duration_Minutes MINUTE) <= %s
                OR  f2.Dep_DateTime >= %s
              )
          )
          AND
          NOT EXISTS (
            SELECT 1
            FROM FlightCrew_Pilots fprev
            JOIN Flights       f2 ON f2.Flight_id = fprev.Flight_id
            JOIN Flight_Routes r2 ON r2.Route_id  = f2.Route_id
            WHERE fprev.Pilot_id = p.Pilot_id
              AND fprev.Flight_id <> %s
              AND f2.Dep_DateTime < %s
              AND f2.Status <> 'Cancelled'
              AND r2.Destination_Airport_code <> %s
              AND f2.Dep_DateTime = (
                    SELECT MAX(f3f.Dep_DateTime)
                    FROM FlightCrew_Pilots f3
                    JOIN Flights f3f ON f3f.Flight_id = f3.Flight_id
                    WHERE f3.Pilot_id = p.Pilot_id
                      AND f3.Flight_id <> %s
                      AND f3f.Dep_DateTime < %s
                      AND f3f.Status <> 'Cancelled'
              )
          )
          AND
          NOT EXISTS (
            SELECT 1
            FROM FlightCrew_Pilots fnext
            JOIN Flights       f2 ON f2.Flight_id = fnext.Flight_id
            JOIN Flight_Routes r2 ON r2.Route_id  = f2.Route_id
            WHERE fnext.Pilot_id = p.Pilot_id
              AND fnext.Flight_id <> %s
              AND f2.Dep_DateTime > %s
              AND f2.Status <> 'Cancelled'
              AND r2.Origin_Airport_code <> %s
              AND f2.Dep_DateTime = (
                    SELECT MIN(f3f.Dep_DateTime)
                    FROM FlightCrew_Pilots f3
                    JOIN Flights f3f ON f3f.Flight_id = f3.Flight_id
                    WHERE f3.Pilot_id = p.Pilot_id
                      AND f3.Flight_id <> %s
                      AND f3f.Dep_DateTime > %s
                      AND f3f.Status <> 'Cancelled'
              )
          )
    """
    pilot_params_query = (
        long_flag,
        current_flight_id,
        dep_dt,
        arr_dt,
        current_flight_id,
        dep_dt,
        origin_airport,
        current_flight_id,
        dep_dt,
        current_flight_id,
        arr_dt,
        dest_airport,
        current_flight_id,
        arr_dt,
    )

    cursor.execute(pilot_sql_query, pilot_params_query)
    pilots_available = int(cursor.fetchone()["cnt"])

    # Attendants
    attendant_sql_query = """
        SELECT COUNT(*) AS cnt
        FROM FlightAttendants fa
        WHERE
          (%s = 0 OR COALESCE(fa.Long_Haul_Certified, 0) = 1)
          AND
          NOT EXISTS (
            SELECT 1
            FROM FlightCrew_Attendants fca
            JOIN Flights       f2 ON f2.Flight_id = fca.Flight_id
            JOIN Flight_Routes r2 ON f2.Route_id  = r2.Route_id
            WHERE fca.Attendant_id = fa.Attendant_id
              AND fca.Flight_id <> %s
              AND NOT (
                    DATE_ADD(f2.Dep_DateTime, INTERVAL r2.Duration_Minutes MINUTE) <= %s
                OR  f2.Dep_DateTime >= %s
              )
          )
          AND
          NOT EXISTS (
            SELECT 1
            FROM FlightCrew_Attendants fprev
            JOIN Flights       f2 ON f2.Flight_id = fprev.Flight_id
            JOIN Flight_Routes r2 ON r2.Route_id  = f2.Route_id
            WHERE fprev.Attendant_id = fa.Attendant_id
              AND fprev.Flight_Id <> %s
              AND f2.Dep_DateTime < %s
              AND f2.Status <> 'Cancelled'
              AND r2.Destination_Airport_code <> %s
              AND f2.Dep_DateTime = (
                    SELECT MAX(f3f.Dep_DateTime)
                    FROM FlightCrew_Attendants f3
                    JOIN Flights f3f ON f3f.Flight_Id = f3.Flight_Id
                    WHERE f3.Attendant_Id = fa.Attendant_Id
                      AND f3.Flight_Id <> %s
                      AND f3f.Dep_DateTime < %s
                      AND f3f.Status <> 'Cancelled'
              )
          )
          AND
          NOT EXISTS (
            SELECT 1
            FROM FlightCrew_Attendants fnext
            JOIN Flights       f2 ON f2.Flight_Id = fnext.Flight_Id
            JOIN Flight_Routes r2 ON r2.Route_Id  = f2.Route_Id
            WHERE fnext.Attendant_Id = fa.Attendant_Id
              AND fnext.Flight_Id <> %s
              AND f2.Dep_DateTime > %s
              AND f2.Status <> 'Cancelled'
              AND r2.Origin_Airport_code <> %s
              AND f2.Dep_DateTime = (
                    SELECT MIN(f3f.Dep_DateTime)
                    FROM FlightCrew_Attendants f3
                    JOIN Flights f3f ON f3f.Flight_Id = f3.Flight_Id
                    WHERE f3.Attendant_Id = fa.Attendant_Id
                      AND f3.Flight_Id <> %s
                      AND f3f.Dep_DateTime > %s
                      AND f3f.Status <> 'Cancelled'
              )
          )
    """
    attendant_params_query = (
        long_flag,
        current_flight_id,
        dep_dt,
        arr_dt,
        current_flight_id,
        dep_dt,
        origin_airport,
        current_flight_id,
        dep_dt,
        current_flight_id,
        arr_dt,
        dest_airport,
        current_flight_id,
        arr_dt,
    )

    cursor.execute(attendant_sql_query, attendant_params_query)
    attendants_available = int(cursor.fetchone()["cnt"])

    return (
        pilots_available >= req_pilots
        and attendants_available >= req_attendants
    )


def _filter_aircrafts_for_window(
    cursor,
    all_aircrafts,
    dep_dt: datetime,
    duration_minutes: int,
    route_id,
    ignore_flight_id=None,
    check_crew: bool = False,
):
    """
    Filter aircrafts for a given time window and route.

    Conditions:
      * aircraft must have seats defined in Seats table,
      * long/short requirement (Large only for long-haul),
      * no overlapping flights for that aircraft (time),
      * positioning rule: aircraft must be at the origin airport at dep_dt,
      * if check_crew=True – there must be enough available crew members
        (pilots + attendants) for this aircraft size and window
    """
    if not dep_dt or duration_minutes is None:
        # Even in this fallback case, keep only aircrafts with seats
        return [ac for ac in all_aircrafts if int(ac.get("SeatCount", 0) or 0) > 0]

    arr_dt = _compute_arrival(dep_dt, duration_minutes)
    is_long = int(duration_minutes) > LONG_FLIGHT_THRESHOLD_MINUTES

    origin_airport = None
    dest_airport = None
    if check_crew and route_id:
        cursor.execute(
            """
            SELECT Origin_Airport_code, Destination_Airport_code
            FROM Flight_Routes
            WHERE Route_id = %s
            """,
            (route_id,),
        )
        r = cursor.fetchone()
        if r:
            origin_airport = r["Origin_Airport_code"]
            dest_airport = r["Destination_Airport_code"]

    filtered = []
    for ac in all_aircrafts:
        # Must have seats
        if int(ac.get("SeatCount", 0) or 0) <= 0:
            continue

        # Long-haul constraint
        if is_long and ac["Size"] != "Large":
            continue

        # Time overlap constraint
        if _aircraft_has_conflict(
            cursor,
            ac["Aircraft_id"],
            dep_dt,
            arr_dt,
            ignore_flight_id=ignore_flight_id,
        ):
            continue

        # Positioning / location constraint
        if not _aircraft_location_ok(
            cursor,
            ac["Aircraft_id"],
            route_id,
            dep_dt,
            duration_minutes,
            ignore_flight_id=ignore_flight_id,
        ):
            continue

        # Crew availability constraint (optional)
        if check_crew:
            if not origin_airport or not dest_airport:
                continue

            if not _has_enough_crew_for_window(
                cursor,
                dep_dt,
                arr_dt,
                ac["Size"],
                origin_airport,
                dest_airport,
                ignore_flight_id=ignore_flight_id,
            ):
                continue

        filtered.append(ac)

    return filtered


# ---------- Seat-layout helpers for EDIT mode ----------


def _get_aircraft_seat_signature(cursor, aircraft_id):
    """
    Returns a canonical 'seat layout signature' for an aircraft, used
    to restrict aircraft changes during flight edit.

    Signature encodes, per Seat_Class:
        - total seats
        - max row number
        - max column number
    """
    if not aircraft_id:
        return None

    cursor.execute(
        """
        SELECT
            Seat_Class,
            COUNT(*)     AS SeatCount,
            MAX(Row_Num) AS MaxRow,
            MAX(Col_Num) AS MaxCol
        FROM Seats
        WHERE Aircraft_id = %s
        GROUP BY Seat_Class
        ORDER BY Seat_Class
        """,
        (aircraft_id,),
    )
    rows = cursor.fetchall()
    if not rows:
        return None

    sig = tuple(
        (
            row["Seat_Class"],
            int(row["SeatCount"] or 0),
            int(row["MaxRow"] or 0),
            int(row["MaxCol"] or 0),
        )
        for row in rows
    )
    return sig


def _filter_aircrafts_same_layout(cursor, aircrafts, reference_aircraft_id):
    """
    For EDIT mode:
    Given a list of aircraft dicts, return only those whose seat layout
    matches the layout of reference_aircraft_id (same number of seats,
    rows and columns per Seat_Class).
    """
    ref_sig = _get_aircraft_seat_signature(cursor, reference_aircraft_id)
    if not ref_sig:
        # If reference layout cannot be determined – do not restrict further
        return aircrafts

    filtered = []
    cache = {}
    for ac in aircrafts:
        aid = ac["Aircraft_id"]
        if aid in cache:
            sig = cache[aid]
        else:
            sig = _get_aircraft_seat_signature(cursor, aid)
            cache[aid] = sig
        if sig == ref_sig:
            filtered.append(ac)
    return filtered


def _auto_update_completed(cursor, now_dt):
    """
    Mark flights as Completed automatically when arrival time passed
    and current status is Active OR Full-Occupied.
    """
    cursor.execute(
        """
        UPDATE Flights f
        JOIN Flight_Routes r ON f.Route_id = r.Route_id
        SET f.Status = 'Completed'
        WHERE f.Status IN ('Active', 'Full-Occupied')
          AND DATE_ADD(f.Dep_DateTime, INTERVAL r.Duration_Minutes MINUTE) < %s
        """,
        (now_dt,),
    )



# -------------------------------------------------------------------
# Seat-status sync helpers (Sold/Available) based on Orders+Tickets
# -------------------------------------------------------------------

def _sync_flight_seats_from_orders(cursor, flight_id=None):
    """
    Idempotent synchronization layer for FlightSeats.Seat_Status.

    Rules enforced (without touching 'Blocked'):
      1) If a seat is 'Available' but has at least one Ticket whose Order is NOT
         'Cancelled-Customer' (case/space-insensitive) -> set seat to 'Sold'.
      2) If a seat is 'Sold' but all Tickets for that seat belong ONLY to orders
         that are 'Cancelled-Customer' -> set seat back to 'Available'.

      matches the required table semantics:
      FlightSeats.Seat_Status ∈ {'Available','Sold','Blocked'}
    """
    flight_clause = ""
    params_sold = []
    params_rel = []

    if flight_id:
        flight_clause = " AND fs.Flight_id = %s "
        params_sold.append(flight_id)
        params_rel.append(flight_id)

    # (1) Available -> Sold if there exists a non-cancelled-customer order ticket
    cursor.execute(
        f"""
        UPDATE FlightSeats fs
        SET fs.Seat_Status = 'Sold'
        WHERE UPPER(TRIM(fs.Seat_Status)) = 'AVAILABLE'
          AND UPPER(TRIM(fs.Seat_Status)) <> 'BLOCKED'
          {flight_clause}
          AND EXISTS (
                SELECT 1
                FROM Tickets t
                JOIN Orders o ON o.Order_code = t.Order_code
                WHERE t.FlightSeat_id = fs.FlightSeat_id
                  AND (
                        o.Status IS NULL
                        OR UPPER(TRIM(o.Status)) <> 'CANCELLED-CUSTOMER'
                  )
          )
        """,
        tuple(params_sold),
    )

    # (2) Sold -> Available if there is no non-cancelled-customer order ticket
    cursor.execute(
        f"""
        UPDATE FlightSeats fs
        SET fs.Seat_Status = 'Available'
        WHERE UPPER(TRIM(fs.Seat_Status)) = 'SOLD'
          AND UPPER(TRIM(fs.Seat_Status)) <> 'BLOCKED'
          {flight_clause}
          AND NOT EXISTS (
                SELECT 1
                FROM Tickets t2
                JOIN Orders o2 ON o2.Order_code = t2.Order_code
                WHERE t2.FlightSeat_id = fs.FlightSeat_id
                  AND (
                        o2.Status IS NULL
                        OR UPPER(TRIM(o2.Status)) <> 'CANCELLED-CUSTOMER'
                  )
          )
        """,
        tuple(params_rel),
    )


def _sync_all_flight_seats_from_orders(cursor):
    """
    wrapper: sync seat statuses across ALL flights.
    """
    _sync_flight_seats_from_orders(cursor, flight_id=None)


def _auto_update_full_occupied(cursor, flight_id):
    """
    If all seats are Sold or Blocked -> set flight status to Full-occupied,
    else if status is Full-occupied and there is at least one Available seat
    -> set back to Active.
    Does not override Cancelled / Completed.
    """
    cursor.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(
                CASE
                    WHEN UPPER(TRIM(Seat_Status)) IN ('SOLD', 'BLOCKED') THEN 1
                    ELSE 0
                END
            ) AS non_free
        FROM FlightSeats
        WHERE Flight_id = %s
        """,
        (flight_id,),
    )
    occ = cursor.fetchone()
    if not occ or not occ["total"]:
        return

    cursor.execute("SELECT Status FROM Flights WHERE Flight_id = %s", (flight_id,))
    srow = cursor.fetchone()
    if not srow:
        return

    current_status = srow["Status"]
    if current_status in ("Cancelled", "Completed"):
        return

    total = int(occ["total"])
    non_free = int(occ["non_free"] or 0)

    if non_free == total and current_status != "Full-Occupied":
        cursor.execute(
            "UPDATE Flights SET Status = 'Full-Occupied' WHERE Flight_id = %s",
            (flight_id,),
        )
    elif non_free != total and current_status == "Full-Occupied":
        cursor.execute(
            "UPDATE Flights SET Status = 'Active' WHERE Flight_id = %s",
            (flight_id,),
        )


def _auto_update_full_occupied_all(cursor):
    """
    Global auto-update for Full-occupied across all flights.
    """
    cursor.execute(
        """
        UPDATE Flights f
        JOIN (
            SELECT
                Flight_id,
                COUNT(*) AS total,
                SUM(
                    CASE
                        WHEN UPPER(TRIM(Seat_Status)) IN ('SOLD', 'BLOCKED') THEN 1
                        ELSE 0
                    END
                ) AS non_free
            FROM FlightSeats
            GROUP BY Flight_id
        ) x ON x.Flight_id = f.Flight_id
        SET f.Status = 'Full-Occupied'
        WHERE UPPER(f.Status) NOT IN ('CANCELLED', 'COMPLETED')
          AND x.total > 0
          AND x.non_free = x.total
        """
    )

    cursor.execute(
        """
        UPDATE Flights f
        JOIN (
            SELECT
                Flight_id,
                COUNT(*) AS total,
                SUM(
                    CASE
                        WHEN UPPER(TRIM(Seat_Status)) IN ('SOLD', 'BLOCKED') THEN 1
                        ELSE 0
                    END
                ) AS non_free
            FROM FlightSeats
            GROUP BY Flight_id
        ) x ON x.Flight_id = f.Flight_id
        SET f.Status = 'Active'
        WHERE UPPER(f.Status) = 'FULL-OCCUPIED'
          AND x.total > 0
          AND x.non_free <> x.total
        """
    )


def _cleanup_cancelled_flights_crew(cursor):
    """
    Sync helper:
    If changed the Status of a flight  to 'Cancelled',
    this helper guarantees that the flight crew is not left assigned
    to that flight.
    """
    # Pilots
    cursor.execute(
        """
        DELETE fcp
        FROM FlightCrew_Pilots fcp
        JOIN Flights f ON f.Flight_id = fcp.Flight_id
        WHERE f.Status = 'Cancelled'
        """
    )

    # Attendants
    cursor.execute(
        """
        DELETE fca
        FROM FlightCrew_Attendants fca
        JOIN Flights f ON f.Flight_id = fca.Flight_id
        WHERE f.Status = 'Cancelled'
        """
    )


# -----------------------------
# Flights list – WITH FILTERS
# -----------------------------


@main_bp.route("/manager/flights")
def manager_flights():
    """
    Shows all flights for the manager, with optional filters.

    Filters:
      - status: one of {'all', 'Active', 'Completed', 'Cancelled', 'Full-Occupied'}
      - profile: {'all', 'short', 'long'} based on route duration
      - flight_id: substring match
      - origin: substring match on Origin_Airport_code
      - dest: substring match on Destination_Airport_code
      - dep_date: exact match on departure DATE (YYYY-MM-DD)
      - arr_date: exact match on arrival DATE (YYYY-MM-DD)
    """
    if not _require_manager():
        return redirect(url_for("auth.login"))

    status_filter = request.args.get("status", "all")
    profile_filter = request.args.get("profile", "all")
    flight_id_filter = (request.args.get("flight_id") or "").strip()
    origin_filter = (request.args.get("origin") or "").strip()
    dest_filter = (request.args.get("dest") or "").strip()

    dep_date_filter = (request.args.get("dep_date") or "").strip()
    arr_date_filter = (request.args.get("arr_date") or "").strip()

    dep_date_obj = None
    arr_date_obj = None
    if dep_date_filter:
        try:
            dep_date_obj = datetime.strptime(dep_date_filter, "%Y-%m-%d").date()
        except ValueError:
            dep_date_filter = ""
            dep_date_obj = None

    if arr_date_filter:
        try:
            arr_date_obj = datetime.strptime(arr_date_filter, "%Y-%m-%d").date()
        except ValueError:
            arr_date_filter = ""
            arr_date_obj = None

    valid_statuses = {"Active", "Completed", "Cancelled", "Full-Occupied"}
    if status_filter not in valid_statuses and status_filter != "all":
        status_filter = "all"

    if profile_filter not in {"short", "long"}:
        profile_filter = "all"

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        now = datetime.now()

        try:
            _auto_update_completed(cursor, now)

            _sync_all_flight_seats_from_orders(cursor)
            _auto_update_full_occupied_all(cursor)

            _cleanup_cancelled_flights_crew(cursor)
            conn.commit()
        except Error as e:
            print("DB error when auto-updating statuses / cleanup:", e)

        cursor.execute(
            """
            SELECT Airport_code, City
            FROM Airports
            ORDER BY City, Airport_code
            """
        )
        airports = cursor.fetchall()

        cursor.execute(
            """
            SELECT
                f.Flight_id,
                f.Dep_DateTime,
                f.Status,
                a.Aircraft_id,
                a.Model AS AircraftModel,
                a.Size  AS AircraftSize,
                r.Origin_Airport_code,
                r.Destination_Airport_code,
                r.Duration_Minutes,
                ao.City AS Origin_City,
                ad.City AS Destination_City
            FROM Flights f
            JOIN Aircrafts     a  ON f.Aircraft_id = a.Aircraft_id
            JOIN Flight_Routes r  ON f.Route_id    = r.Route_id
            JOIN Airports      ao ON ao.Airport_code = r.Origin_Airport_code
            JOIN Airports      ad ON ad.Airport_code = r.Destination_Airport_code
            ORDER BY f.Dep_DateTime
            """
        )
        flights_raw = cursor.fetchall()

        filtered_flights = []

        for fl in flights_raw:
            dep_dt = fl["Dep_DateTime"]
            duration = int(fl["Duration_Minutes"])
            arr_dt = _compute_arrival(dep_dt, duration)

            fl["Dep_str"] = dep_dt.strftime("%Y-%m-%d %H:%M")
            fl["Arr_str"] = arr_dt.strftime("%Y-%m-%d %H:%M")

            profile_code = _flight_profile(fl["Duration_Minutes"])
            fl["Profile_Code"] = profile_code
            fl["Profile"] = "Long-haul" if profile_code == "long" else "Short-haul"

            fl["can_edit"] = (dep_dt > now) and (fl["Status"] not in ("Cancelled", "Completed"))
            fl["can_view"] = not fl["can_edit"]

            if status_filter != "all" and fl["Status"] != status_filter:
                continue

            if profile_filter != "all" and fl["Profile_Code"] != profile_filter:
                continue

            if flight_id_filter and flight_id_filter.lower() not in (fl["Flight_id"] or "").lower():
                continue

            if origin_filter and origin_filter.lower() not in (fl["Origin_Airport_code"] or "").lower():
                continue

            if dest_filter and dest_filter.lower() not in (fl["Destination_Airport_code"] or "").lower():
                continue

            # Date filters (exact date match)
            if dep_date_obj and dep_dt.date() != dep_date_obj:
                continue

            if arr_date_obj and arr_dt.date() != arr_date_obj:
                continue

            filtered_flights.append(fl)

        return render_template(
            "manager_flights_list.html",
            flights=filtered_flights,
            airports=airports,
            status_filter=status_filter,
            profile_filter=profile_filter,
            flight_id_filter=flight_id_filter,
            origin_filter=origin_filter,
            dest_filter=dest_filter,
            dep_date_filter=dep_date_filter,
            arr_date_filter=arr_date_filter,
        )

    except Error as e:
        print("DB error in manager_flights:", e)
        flash("Failed to load flights.", "error")
        return render_template(
            "manager_flights_list.html",
            flights=[],
            airports=[],
            status_filter=status_filter,
            profile_filter=profile_filter,
            flight_id_filter=flight_id_filter,
            origin_filter=origin_filter,
            dest_filter=dest_filter,
            dep_date_filter=dep_date_filter,
            arr_date_filter=arr_date_filter,
        )
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()



# -----------------------------
# View history (read-only)
# -----------------------------


@main_bp.route("/manager/flights/<flight_id>/view", methods=["GET"])
def manager_view_flight(flight_id):
    """
    Read-only flight history/details page.
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
                f.Flight_id,
                f.Dep_DateTime,
                f.Status,
                f.Aircraft_id,
                f.Route_id,
                r.Origin_Airport_code,
                r.Destination_Airport_code,
                r.Duration_Minutes,
                a.Manufacturer AS AircraftManufacturer,
                a.Model        AS AircraftModel,
                a.Size         AS AircraftSize
            FROM Flights f
            JOIN Flight_Routes r ON f.Route_id = r.Route_id
            JOIN Aircrafts a     ON f.Aircraft_id = a.Aircraft_id
            WHERE f.Flight_id = %s
            """,
            (flight_id,),
        )
        flight = cursor.fetchone()
        if not flight:
            flash("Flight not found.", "error")
            return redirect(url_for("main.manager_flights"))

        try:
            # IMPORTANT: sync seat statuses for this flight, then update Full-Occupied
            _sync_flight_seats_from_orders(cursor, flight_id=flight_id)
            _auto_update_full_occupied(cursor, flight_id)
            conn.commit()

            cursor.execute("SELECT Status FROM Flights WHERE Flight_id = %s", (flight_id,))
            s = cursor.fetchone()
            if s:
                flight["Status"] = s["Status"]

            # ===== NEW: if flight is Cancelled -> block ALL seats automatically =====
            if flight.get("Status") == "Cancelled":
                cursor.execute(
                    """
                    UPDATE FlightSeats
                    SET Seat_Status = 'Blocked'
                    WHERE Flight_id = %s
                      AND Seat_Status <> 'Blocked'
                    """,
                    (flight_id,),
                )
                conn.commit()

        except Error as e:
            print("DB error when auto-updating Full-Occupied:", e)

        dep_dt = flight["Dep_DateTime"]
        duration = int(flight["Duration_Minutes"])
        arr_dt = _compute_arrival(dep_dt, duration)

        flight["Dep_str"] = dep_dt.strftime("%Y-%m-%d %H:%M")
        flight["Arr_str"] = arr_dt.strftime("%Y-%m-%d %H:%M")
        flight["Arr_DateTime"] = arr_dt

        # long_route: only duration strictly greater than the threshold (more than 6 hours)
        long_route = duration > LONG_FLIGHT_THRESHOLD_MINUTES

        cursor.execute(
            """
            SELECT
                fcp.Pilot_id,
                p.First_name,
                p.Last_name
            FROM FlightCrew_Pilots fcp
            JOIN Pilots p ON p.Pilot_id = fcp.Pilot_id
            WHERE fcp.Flight_id = %s
            ORDER BY p.Last_name, p.First_name
            """,
            (flight_id,),
        )
        pilots = cursor.fetchall()

        cursor.execute(
            """
            SELECT
                fca.Attendant_id,
                fa.First_name,
                fa.Last_name
            FROM FlightCrew_Attendants fca
            JOIN FlightAttendants fa ON fa.Attendant_id = fca.Attendant_id
            WHERE fca.Flight_id = %s
            ORDER BY fa.Last_name, fa.First_name
            """,
            (flight_id,),
        )
        attendants = cursor.fetchall()

        cursor.execute(
            """
            SELECT
                fs.FlightSeat_id,
                fs.Seat_Price,
                fs.Seat_Status,
                s.Row_Num,
                s.Col_Num,
                s.Seat_Class
            FROM FlightSeats fs
            JOIN Seats s ON fs.Seat_id = s.Seat_id
            WHERE fs.Flight_id = %s
            ORDER BY s.Row_Num, s.Col_Num
            """,
            (flight_id,),
        )
        seats = cursor.fetchall()

        return render_template(
            "manager_flight_view.html",
            flight=flight,
            long_route=long_route,
            pilots=pilots,
            attendants=attendants,
            seats=seats,
            LONG_FLIGHT_THRESHOLD_MINUTES=LONG_FLIGHT_THRESHOLD_MINUTES,
        )

    except Error as e:
        print("DB error in manager_view_flight:", e)
        flash("Failed to load flight history.", "error")
        return redirect(url_for("main.manager_flights"))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()



# -----------------------------
# Create flight (two-stage)
# -----------------------------


@main_bp.route("/manager/flights/new", methods=["GET", "POST"])
def manager_new_flight():
    """
    Create a new flight.

    Stage 1 (POST without aircraft_id):
      - Validate Route, Departure time
      - Compute duration and long/short
      - Filter available aircrafts for this window *including* crew availability
      - If at least one aircraft is available -> freeze schedule (route+time)
        and show aircraft list.
      - If no aircraft is available -> do NOT freeze; user can edit or go back.

    Stage 2 (POST with aircraft_id):
      - Validate aircraft
      - Check constraints & conflicts
      - Check there is enough available crew for this aircraft & window
        (time window + origin/destination airport rules)
      - Enforce aircraft positioning rule
      - Insert flight + FlightSeats, with auto-generated Flight_id (FT001, ...).
    """
    if not _require_manager():
        return redirect(url_for("auth.login"))

    routes, all_aircrafts = _load_routes_and_aircrafts()
    min_dep = datetime.now().strftime("%Y-%m-%dT%H:%M")

    def _render_form(flight, aircrafts, freeze_schedule):
        return render_template(
            "manager_flights_form.html",
            mode="create",
            routes=routes,
            aircrafts=aircrafts or [],
            flight=flight,
            long_route=False,
            min_dep=min_dep,
            LONG_FLIGHT_THRESHOLD_MINUTES=LONG_FLIGHT_THRESHOLD_MINUTES,
            freeze_schedule=freeze_schedule,
            lock_manager_nav=True,
            current_aircraft=None,
        )

    if request.method == "GET":
        empty_flight = {
            "Flight_id": "",
            "Route_id": "",
            "dep_value": "",
        }
        return _render_form(empty_flight, aircrafts=[], freeze_schedule=False)

    # POST
    route_id = request.form.get("route_id")
    dep_str = request.form.get("dep_datetime")
    aircraft_id = (request.form.get("aircraft_id") or "").strip() or None

    temp_flight = {
        "Flight_id": "",
        "Route_id": route_id,
        "dep_value": dep_str or "",
    }

    if not route_id or not dep_str:
        flash("Please fill route and departure time.", "error")
        return _render_form(temp_flight, aircrafts=[], freeze_schedule=False)

    try:
        dep_dt = datetime.strptime(dep_str, "%Y-%m-%dT%H:%M")
    except ValueError:
        flash("Invalid departure date/time format.", "error")
        return _render_form(temp_flight, aircrafts=[], freeze_schedule=False)

    now = datetime.now()
    if dep_dt <= now:
        flash("Departure time must be in the future.", "error")
        temp_flight["dep_value"] = dep_dt.strftime("%Y-%m-%dT%H:%M")
        return _render_form(temp_flight, aircrafts=[], freeze_schedule=False)

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Route must exist
        cursor.execute(
            """
            SELECT Duration_Minutes,
                   Origin_Airport_code,
                   Destination_Airport_code
            FROM Flight_Routes
            WHERE Route_id = %s
            """,
            (route_id,),
        )
        route_row = cursor.fetchone()
        if not route_row:
            flash("Selected route does not exist.", "error")
            temp_flight["dep_value"] = dep_dt.strftime("%Y-%m-%dT%H:%M")
            return _render_form(temp_flight, aircrafts=[], freeze_schedule=False)

        duration_minutes = int(route_row["Duration_Minutes"])
        origin_airport = route_row["Origin_Airport_code"]
        dest_airport = route_row["Destination_Airport_code"]
        is_long = duration_minutes > LONG_FLIGHT_THRESHOLD_MINUTES
        arr_dt = _compute_arrival(dep_dt, duration_minutes)

        # Filter available aircrafts for this window (including crew check with location)
        aircrafts_filtered = _filter_aircrafts_for_window(
            cursor,
            all_aircrafts,
            dep_dt,
            duration_minutes,
            route_id,
            ignore_flight_id=None,
            check_crew=True,
        )

        # ===== Stage 1: still no aircraft selected =====
        if not aircraft_id:
            if not aircrafts_filtered:
                flash(
                    "No aircraft currently satisfy all constraints for this route and time, "
                    "including crew availability. The flight was not created. "
                    "You can adjust the route or departure time and try again, "
                    "or return to the flights board.",
                    "error",
                )
                # Schedule not frozen; allow manager to change everything
                return _render_form(temp_flight, aircrafts=[], freeze_schedule=False)

            # At least one valid aircraft: freeze schedule and show list
            temp_flight["dep_value"] = dep_dt.strftime("%Y-%m-%dT%H:%M")
            return _render_form(
                temp_flight,
                aircrafts=aircrafts_filtered,
                freeze_schedule=True,
            )

        # ===== Stage 2: aircraft selected – create the flight =====

        # Validate the selected aircraft itself
        cursor.execute(
            "SELECT Size, Model FROM Aircrafts WHERE Aircraft_id = %s",
            (aircraft_id,),
        )
        aircraft = cursor.fetchone()
        if not aircraft:
            flash("Selected aircraft does not exist.", "error")
            return _render_form(temp_flight, aircrafts=aircrafts_filtered, freeze_schedule=True)

        # Long-haul restriction
        if is_long and aircraft["Size"] != "Large":
            flash(
                "Only large aircrafts are allowed to operate long-haul flights "
                f"(route duration {duration_minutes} minutes, aircraft model {aircraft['Model']}).",
                "error",
            )
            return _render_form(temp_flight, aircrafts=aircrafts_filtered, freeze_schedule=True)

        # Aircraft time-conflict check
        if _aircraft_has_conflict(cursor, aircraft_id, dep_dt, arr_dt):
            flash("This aircraft is already assigned to another overlapping flight.", "error")
            return _render_form(temp_flight, aircrafts=aircrafts_filtered, freeze_schedule=True)

        # Aircraft positioning rule (STRICT for new flights)
        if not _aircraft_location_ok(
            cursor,
            aircraft_id,
            route_id,
            dep_dt,
            duration_minutes,
            ignore_flight_id=None,
        ):
            flash(
                "This aircraft cannot be scheduled for this flight because its route "
                "is not consistent with the aircraft's previous or next flights (airport location).",
                "error",
            )
            return _render_form(temp_flight, aircrafts=aircrafts_filtered, freeze_schedule=True)

        # Final crew-availability check for the chosen aircraft (with location rules)
        if not _has_enough_crew_for_window(
            cursor,
            dep_dt,
            arr_dt,
            aircraft["Size"],
            origin_airport,
            dest_airport,
        ):
            flash(
                "There are not enough available qualified crew members for this aircraft "
                "and time window (including location rules). The flight has NOT been created "
                "and no Flight ID was reserved. Please change the route, departure time or aircraft, "
                "or return to the flights board.",
                "error",
            )
            return _render_form(temp_flight, aircrafts=aircrafts_filtered, freeze_schedule=True)

        # All constraints satisfied -> insert flight and its seats
        status = "Active"
        flight_id = _get_next_flight_id(cursor)

        cursor.execute(
            """
            INSERT INTO Flights
                (Flight_id, Dep_DateTime, Status, Aircraft_id, Route_id)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (flight_id, dep_dt, status, aircraft_id, route_id),
        )

        # Seats of the selected aircraft
        cursor.execute(
            """
            SELECT Seat_id, Seat_Class
            FROM Seats
            WHERE Aircraft_id = %s
            ORDER BY Row_Num, Col_Num
            """,
            (aircraft_id,),
        )
        seats = cursor.fetchall()

        if not seats:
            flash(
                "The selected aircraft has no seats defined in the Seats table. "
                "Please define seats for this aircraft before creating flights.",
                "error",
            )
            conn.rollback()
            return _render_form(temp_flight, aircrafts=aircrafts_filtered, freeze_schedule=True)

        # Reserve FlightSeat_id block and insert
        start_num = _reserve_flightseat_block(cursor, len(seats))
        next_num = start_num
        is_large_aircraft = aircraft["Size"] == "Large"

        for seat in seats:
            # Pricing policy(default prices - manager can change):
            #   Long-haul: Business=1200, Economy=400 (via _get_default_seat_price)
            #   Short-haul:
            #       Economy: 200 for all aircraft
            #       Business: 700 for Large aircraft, otherwise default (typically 1200)
            if not is_long:
                if seat["Seat_Class"] == "Economy":
                    price = 200.0
                elif seat["Seat_Class"] == "Business" and is_large_aircraft:
                    price = 700.0
                else:
                    price = _get_default_seat_price(seat["Seat_Class"])
            else:
                price = _get_default_seat_price(seat["Seat_Class"])

            flight_seat_id = f"FS{next_num:06d}"
            cursor.execute(
                """
                INSERT INTO FlightSeats
                    (FlightSeat_id, Seat_Price, Flight_id, Seat_id, Seat_Status)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (flight_seat_id, price, flight_id, seat["Seat_id"], "Available"),
            )
            next_num += 1

        conn.commit()
        flash(f"Flight {flight_id} created successfully. Please assign crew.", "success")
        return redirect(url_for("main.manager_flight_crew", flight_id=flight_id))

    except Error as e:
        print("DB error in manager_new_flight:", e)
        flash(f"Failed to create flight: {e}", "error")
        return _render_form(temp_flight, aircrafts=[], freeze_schedule=False)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# -----------------------------
# Edit flight
# -----------------------------

@main_bp.route("/manager/flights/<flight_id>/edit", methods=["GET", "POST"])
def manager_edit_flight(flight_id):
    """
    Edit an existing flight (single-stage, with aircraft filtering by route/time).
    After successful save we immediately continue to crew selection.
    Flight ID and route are fixed during edit; only schedule and status
    can be changed (aircraft is now fixed as well).

    EDIT RULE:
    - Aircraft cannot be changed at all once the flight has been created.
      The manager can adjust only the departure time and the status,
      while all constraints (overlap, crew, positioning) enforced
      for the already-selected aircraft.

    SPECIAL CASE:
    - When cancelling a flight (either via status='Cancelled' or via the
      dedicated "Cancel this flight" button), only a status change to 'Cancelled'
      is performed and the crew is cleared, without overlap/crew/location checks,
      and only if the 72-hours rule holds – based on the Dep_DateTime currently
      stored in the DB.
    """
    if not _require_manager():
        return redirect(url_for("auth.login"))

    routes, all_aircrafts = _load_routes_and_aircrafts()
    min_dep = datetime.now().strftime("%Y-%m-%dT%H:%M")

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute(
            """
            SELECT
                f.Flight_id,
                f.Dep_DateTime,
                f.Status,
                f.Aircraft_id,
                f.Route_id,
                r.Origin_Airport_code,
                r.Destination_Airport_code,
                r.Duration_Minutes
            FROM Flights f
            JOIN Flight_Routes r ON f.Route_id = r.Route_id
            WHERE f.Flight_id = %s
            """,
            (flight_id,),
        )
        flight = cursor.fetchone()
        if not flight:
            flash("Flight not found.", "error")
            return redirect(url_for("main.manager_flights"))

        now = datetime.now()

        if flight["Dep_DateTime"] <= now:
            flash("This flight has already departed and cannot be edited.", "error")
            return redirect(url_for("main.manager_flights"))

        duration_minutes = int(flight["Duration_Minutes"])
        # long_route: only duration strictly greater than the threshold (more than 6 hours)
        long_route = duration_minutes > LONG_FLIGHT_THRESHOLD_MINUTES

        # Current aircraft info (for display only)
        current_aircraft = None
        for ac in all_aircrafts:
            if ac["Aircraft_id"] == flight["Aircraft_id"]:
                current_aircraft = ac
                break

        # Base aircrafts that satisfy time/location/crew for CURRENT dep time
        # (Used for internal validation, even though aircraft cannot be changed anymore)
        base_filtered_aircrafts = _filter_aircrafts_for_window(
            cursor,
            all_aircrafts,
            flight["Dep_DateTime"],
            duration_minutes,
            flight["Route_id"],
            ignore_flight_id=flight_id,
            check_crew=True,
        )

        # EDIT RULE: keep only aircraft with same seat layout as current aircraft
        aircrafts_for_form = _filter_aircrafts_same_layout(
            cursor,
            base_filtered_aircrafts,
            flight["Aircraft_id"],
        )

        if request.method == "POST":
            # Did the user click the dedicated "Cancel this flight" button?
            cancel_clicked = request.form.get("cancel_flight") == "1"

            # Aircraft is locked – ignore any aircraft selection from the form
            aircraft_id = flight["Aircraft_id"]

            dep_str = None
            dep_dt = flight["Dep_DateTime"]  # locked DB value

            allowed_manager_statuses = {"Active", "Completed", "Cancelled"}
            raw_status = request.form.get("status", flight.get("Status", "Active"))
            new_status = (raw_status or "Active").strip() or "Active"

            # Manager cannot "force" Full-Occupied; it is controlled automatically.
            if new_status == "Full-Occupied":
                current_db_status = (flight.get("Status") or "").strip()
                if current_db_status == "Full-Occupied":
                    new_status = "Full-Occupied"
                else:
                    new_status = "Active"

            if new_status not in allowed_manager_statuses and new_status != "Full-Occupied":
                new_status = "Active"

            # === SPECIAL EARLY PATH: cancellation (button OR status) ===
            if cancel_clicked or new_status == "Cancelled":
                # Cancellation is based on the existing Dep_DateTime in DB –
                # manager cannot bypass the 72-hours rule by editing the time first.
                dep_dt_db = flight["Dep_DateTime"]
                time_to_dep = dep_dt_db - now
                if time_to_dep < timedelta(hours=72):
                    flash("A flight can be cancelled only up to 72 hours before departure.", "error")
                    flight["dep_value"] = dep_dt_db.strftime("%Y-%m-%dT%H:%M")
                    return render_template(
                        "manager_flights_form.html",
                        mode="edit",
                        routes=routes,
                        aircrafts=aircrafts_for_form,
                        flight=flight,
                        long_route=long_route,
                        min_dep=min_dep,
                        LONG_FLIGHT_THRESHOLD_MINUTES=LONG_FLIGHT_THRESHOLD_MINUTES,
                        freeze_schedule=True,
                        lock_manager_nav=True,
                        current_aircraft=current_aircraft,
                    )

                # Status change only + clear crew assignments
                cursor.execute(
                    """
                    UPDATE Flights
                    SET Status = 'Cancelled'
                    WHERE Flight_id = %s
                    """,
                    (flight_id,),
                )
                cursor.execute("DELETE FROM FlightCrew_Pilots WHERE Flight_id = %s", (flight_id,))
                cursor.execute("DELETE FROM FlightCrew_Attendants WHERE Flight_id = %s", (flight_id,))

                conn.commit()
                flash("Flight cancelled successfully. Crew assignments were cleared.", "success")
                return redirect(url_for("main.manager_flights"))

            # === Regular Active / Completed flow ===
            arr_dt = _compute_arrival(dep_dt, duration_minutes)

            # Validate aircraft size for long-haul route
            cursor.execute(
                "SELECT Size, Model FROM Aircrafts WHERE Aircraft_id = %s",
                (aircraft_id,),
            )
            aircraft = cursor.fetchone()
            if not aircraft:
                flash("Selected aircraft does not exist.", "error")
                flight["dep_value"] = dep_dt.strftime("%Y-%m-%dT%H:%M")
                return render_template(
                    "manager_flights_form.html",
                    mode="edit",
                    routes=routes,
                    aircrafts=aircrafts_for_form,
                    flight=flight,
                    long_route=long_route,
                    min_dep=min_dep,
                    LONG_FLIGHT_THRESHOLD_MINUTES=LONG_FLIGHT_THRESHOLD_MINUTES,
                    freeze_schedule=True,
                    lock_manager_nav=True,
                    current_aircraft=current_aircraft,
                )

            if long_route and aircraft["Size"] != "Large":
                flash(
                    "Only large aircrafts are allowed to operate long-haul flights "
                    f"(route duration {duration_minutes} minutes, aircraft model {aircraft['Model']}).",
                    "error",
                )
                flight["dep_value"] = dep_dt.strftime("%Y-%m-%dT%H:%M")
                return render_template(
                    "manager_flights_form.html",
                    mode="edit",
                    routes=routes,
                    aircrafts=aircrafts_for_form,
                    flight=flight,
                    long_route=long_route,
                    min_dep=min_dep,
                    LONG_FLIGHT_THRESHOLD_MINUTES=LONG_FLIGHT_THRESHOLD_MINUTES,
                    freeze_schedule=True,
                    lock_manager_nav=True,
                    current_aircraft=current_aircraft,
                )

            if new_status == "Completed" and arr_dt > now:
                flash("A flight can be marked as Completed only after its arrival time.", "error")
                flight["dep_value"] = dep_dt.strftime("%Y-%m-%dT%H:%M")
                return render_template(
                    "manager_flights_form.html",
                    mode="edit",
                    routes=routes,
                    aircrafts=aircrafts_for_form,
                    flight=flight,
                    long_route=long_route,
                    min_dep=min_dep,
                    LONG_FLIGHT_THRESHOLD_MINUTES=LONG_FLIGHT_THRESHOLD_MINUTES,
                    freeze_schedule=True,
                    lock_manager_nav=True,
                    current_aircraft=current_aircraft,
                )

            # Aircraft time-conflict check (with locked dep_dt/arr_dt)
            if _aircraft_has_conflict(cursor, aircraft_id, dep_dt, arr_dt, flight_id):
                flash("This aircraft is already assigned to another overlapping flight.", "error")
                flight["dep_value"] = dep_dt.strftime("%Y-%m-%dT%H:%M")
                return render_template(
                    "manager_flights_form.html",
                    mode="edit",
                    routes=routes,
                    aircrafts=aircrafts_for_form,
                    flight=flight,
                    long_route=long_route,
                    min_dep=min_dep,
                    LONG_FLIGHT_THRESHOLD_MINUTES=LONG_FLIGHT_THRESHOLD_MINUTES,
                    freeze_schedule=True,
                    lock_manager_nav=True,
                    current_aircraft=current_aircraft,
                )

            # Aircraft positioning rule (with locked dep_dt)
            if not _aircraft_location_ok(
                cursor,
                aircraft_id,
                flight["Route_id"],
                dep_dt,
                duration_minutes,
                ignore_flight_id=flight_id,
            ):
                flash(
                    "This aircraft cannot be scheduled for this flight because its route "
                    "is not consistent with the aircraft's previous or next flights (airport location).",
                    "error",
                )
                flight["dep_value"] = dep_dt.strftime("%Y-%m-%dT%H:%M")
                return render_template(
                    "manager_flights_form.html",
                    mode="edit",
                    routes=routes,
                    aircrafts=aircrafts_for_form,
                    flight=flight,
                    long_route=long_route,
                    min_dep=min_dep,
                    LONG_FLIGHT_THRESHOLD_MINUTES=LONG_FLIGHT_THRESHOLD_MINUTES,
                    freeze_schedule=True,
                    lock_manager_nav=True,
                    current_aircraft=current_aircraft,
                )

            # For Active / Completed flights, ensure there is enough crew (with locked dep_dt/arr_dt)
            if not _has_enough_crew_for_window(
                cursor,
                dep_dt,
                arr_dt,
                aircraft["Size"],
                flight["Origin_Airport_code"],
                flight["Destination_Airport_code"],
                ignore_flight_id=flight_id,
            ):
                flash(
                    "There are not enough available qualified crew members for this aircraft "
                    "and time window (including location rules). Please change the departure "
                    "time, or return to the flights board.",
                    "error",
                )
                flight["dep_value"] = dep_dt.strftime("%Y-%m-%dT%H:%M")
                return render_template(
                    "manager_flights_form.html",
                    mode="edit",
                    routes=routes,
                    aircrafts=aircrafts_for_form,
                    flight=flight,
                    long_route=long_route,
                    min_dep=min_dep,
                    LONG_FLIGHT_THRESHOLD_MINUTES=LONG_FLIGHT_THRESHOLD_MINUTES,
                    freeze_schedule=True,
                    lock_manager_nav=True,
                    current_aircraft=current_aircraft,
                )

            # update STATUS
            cursor.execute(
                """
                UPDATE Flights
                SET Status = %s
                WHERE Flight_id = %s
                """,
                (new_status, flight_id),
            )

            try:
                # keep seats in sync before recalculating Full-Occupied
                _sync_flight_seats_from_orders(cursor, flight_id=flight_id)
                _auto_update_full_occupied(cursor, flight_id)
            except Exception as e:
                print("Warning: failed to sync seats / auto-update Full-Occupied after edit:", e)

            conn.commit()

            flash("Flight details saved! Continue to crew assignment.", "success")
            return redirect(url_for("main.manager_flight_crew", flight_id=flight_id))

        flight["dep_value"] = flight["Dep_DateTime"].strftime("%Y-%m-%dT%H:%M")
        return render_template(
            "manager_flights_form.html",
            mode="edit",
            routes=routes,
            aircrafts=aircrafts_for_form,
            flight=flight,
            long_route=long_route,
            min_dep=min_dep,
            LONG_FLIGHT_THRESHOLD_MINUTES=LONG_FLIGHT_THRESHOLD_MINUTES,
            freeze_schedule=True,
            lock_manager_nav=True,
            current_aircraft=current_aircraft,
        )

    except Error as e:
        print("DB error in manager_edit_flight:", e)
        flash("Failed to load or update flight.", "error")
        return redirect(url_for("main.manager_flights"))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

