"""
Crew management:
- assign pilots & attendants to a flight
- only show crew members without time conflicts
- enforce required number of pilots/attendants per aircraft size
- enforce that a crew member can:
    * start a flight only from the destination airport of their last
      (non-cancelled) flight, unless they have no previous flights at all, and
    * land at the airport from which their next (non-cancelled) flight will
      depart (if such a flight already exists in the future).

NOTE:
- Arrival time for flights is derived from Dep_DateTime + route Duration_Minutes.
"""

from datetime import datetime, timedelta

from flask import render_template, redirect, url_for, request, flash
from mysql.connector import Error

from db import get_db_connection
from . import main_bp, _require_manager, LONG_FLIGHT_THRESHOLD_MINUTES


# -------------------------------------------------------------
# Helpers
# -------------------------------------------------------------


def _compute_arrival(dep_dt: datetime, duration_minutes: int) -> datetime:
    """Compute arrival from departure + duration."""
    return dep_dt + timedelta(minutes=int(duration_minutes))


def _get_flight_header(cursor, flight_id):
    """
    Load basic info about a flight, including computed arrival time.
    """
    cursor.execute(
        """
        SELECT
            f.Flight_id,
            f.Dep_DateTime,
            f.Status,
            r.Duration_Minutes,
            r.Origin_Airport_code,
            r.Destination_Airport_code,
            a.Model        AS AircraftModel,
            a.Size         AS Aircraft_Size
        FROM Flights f
        JOIN Flight_Routes r ON f.Route_id    = r.Route_id
        JOIN Aircrafts     a ON f.Aircraft_id = a.Aircraft_id
        WHERE f.Flight_id = %s
        """,
        (flight_id,),
    )
    flight = cursor.fetchone()
    if flight:
        dep_dt = flight["Dep_DateTime"]
        duration = int(flight["Duration_Minutes"])
        arr_dt = _compute_arrival(dep_dt, duration)
        flight["Arr_DateTime"] = arr_dt
        flight["Dep_str"] = dep_dt.strftime("%Y-%m-%d %H:%M")
        flight["Arr_str"] = arr_dt.strftime("%Y-%m-%d %H:%M")
        # האם הטיסה מוגדרת כ־Long-haul
        flight["Is_Long_Route"] = duration >= LONG_FLIGHT_THRESHOLD_MINUTES
    return flight


def _required_crew_for_flight(flight):
    """
    Return required numbers of pilots and attendants according to aircraft size.
    """
    size = flight.get("Aircraft_Size", "Small")
    if size == "Large":
        return 3, 6
    return 2, 3


def _load_available_crew(cursor, flight):
    """
    Return pilots / attendants that:
    * have no time conflicts with other flights, and
    * whose last non-cancelled flight (if any) lands at the origin airport
      of the current flight, and
    * whose first non-cancelled future flight (if any) departs from the
      destination airport of the current flight.

    בנוסף:
    * אם הטיסה Long-haul (משך ≥ LONG_FLIGHT_THRESHOLD_MINUTES) –
      נבחרים רק אנשי צוות שמסומנים COALESCE(Long_Haul_Certified, 0) = 1.
    * אם העמודה Long_Haul_Certified לא קיימת – יש fallback לשאילתא
      הישנה בלי התנאי הזה (כדי שלא תהיה קריסה).
    """
    dep_dt = flight["Dep_DateTime"]
    arr_dt = flight["Arr_DateTime"]
    origin_airport = flight["Origin_Airport_code"]
    dest_airport = flight["Destination_Airport_code"]
    current_flight_id = flight["Flight_id"]
    is_long_route = bool(flight.get("Is_Long_Route"))
    long_flag = 1 if is_long_route else 0

    # -------- Pilots --------
    pilot_sql_long = """
        SELECT p.Pilot_id, p.First_name, p.Last_name
        FROM Pilots p
        WHERE
          -- Long-haul qualification (only for long routes)
          (%s = 0 OR COALESCE(p.Long_Haul_Certified, 0) = 1)
          AND
          -- Time-overlap constraint
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
          -- Previous-flight location rule
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
          -- Next-flight location rule
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
        ORDER BY p.Last_name, p.First_name
    """
    pilot_params_long = (
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

    # fallback – השאילתא המקורית בלי Long_Haul_Certified
    pilot_sql_fallback = """
        SELECT p.Pilot_id, p.First_name, p.Last_name
        FROM Pilots p
        WHERE
          -- Time-overlap constraint
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
          -- Previous-flight location rule
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
          -- Next-flight location rule
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
        ORDER BY p.Last_name, p.First_name
    """
    pilot_params_fallback = pilot_params_long[1:]  # בלי long_flag

    try:
        cursor.execute(pilot_sql_long, pilot_params_long)
        pilots = cursor.fetchall()
    except Error as e:
        print("DB error in _load_available_crew (pilots, Long_Haul_Certified):", e)
        cursor.execute(pilot_sql_fallback, pilot_params_fallback)
        pilots = cursor.fetchall()

    # -------- Attendants --------
    attendant_sql_long = """
        SELECT fa.Attendant_id, fa.First_name, fa.Last_name
        FROM FlightAttendants fa
        WHERE
          -- Long-haul qualification (only for long routes)
          (%s = 0 OR COALESCE(fa.Long_Haul_Certified, 0) = 1)
          AND
          -- Time-overlap constraint
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
          -- Previous-flight location rule
          NOT EXISTS (
            SELECT 1
            FROM FlightCrew_Attendants fprev
            JOIN Flights       f2 ON f2.Flight_id = fprev.Flight_id
            JOIN Flight_Routes r2 ON r2.Route_id  = f2.Route_id
            WHERE fprev.Attendant_id = fa.Attendant_id
              AND fprev.Flight_id <> %s
              AND f2.Dep_DateTime < %s
              AND f2.Status <> 'Cancelled'
              AND r2.Destination_Airport_code <> %s
              AND f2.Dep_DateTime = (
                    SELECT MAX(f3f.Dep_DateTime)
                    FROM FlightCrew_Attendants f3
                    JOIN Flights f3f ON f3f.Flight_id = f3.Flight_id
                    WHERE f3.Attendant_id = fa.Attendant_id
                      AND f3.Flight_id <> %s
                      AND f3f.Dep_DateTime < %s
                      AND f3f.Status <> 'Cancelled'
              )
          )
          AND
          -- Next-flight location rule
          NOT EXISTS (
            SELECT 1
            FROM FlightCrew_Attendants fnext
            JOIN Flights       f2 ON f2.Flight_id = fnext.Flight_id
            JOIN Flight_Routes r2 ON r2.Route_id  = f2.Route_id
            WHERE fnext.Attendant_id = fa.Attendant_id
              AND fnext.Flight_id <> %s
              AND f2.Dep_DateTime > %s
              AND f2.Status <> 'Cancelled'
              AND r2.Origin_Airport_code <> %s
              AND f2.Dep_DateTime = (
                    SELECT MIN(f3f.Dep_DateTime)
                    FROM FlightCrew_Attendants f3
                    JOIN Flights f3f ON f3f.Flight_id = f3.Flight_id
                    WHERE f3.Attendant_id = fa.Attendant_id
                      AND f3.Flight_id <> %s
                      AND f3f.Dep_DateTime > %s
                      AND f3f.Status <> 'Cancelled'
              )
          )
        ORDER BY fa.Last_name, fa.First_name
    """
    attendant_params_long = (
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

    # fallback – השאילתא המקורית בלי Long_Haul_Certified
    attendant_sql_fallback = """
        SELECT fa.Attendant_id, fa.First_name, fa.Last_name
        FROM FlightAttendants fa
        WHERE
          -- Time-overlap constraint
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
          -- Previous-flight location rule
          NOT EXISTS (
            SELECT 1
            FROM FlightCrew_Attendants fprev
            JOIN Flights       f2 ON f2.Flight_id = fprev.Flight_id
            JOIN Flight_Routes r2 ON r2.Route_id  = f2.Route_id
            WHERE fprev.Attendant_id = fa.Attendant_id
              AND fprev.Flight_id <> %s
              AND f2.Dep_DateTime < %s
              AND f2.Status <> 'Cancelled'
              AND r2.Destination_Airport_code <> %s
              AND f2.Dep_DateTime = (
                    SELECT MAX(f3f.Dep_DateTime)
                    FROM FlightCrew_Attendants f3
                    JOIN Flights f3f ON f3f.Flight_id = f3.Flight_id
                    WHERE f3.Attendant_id = fa.Attendant_id
                      AND f3.Flight_id <> %s
                      AND f3f.Dep_DateTime < %s
                      AND f3f.Status <> 'Cancelled'
              )
          )
          AND
          -- Next-flight location rule
          NOT EXISTS (
            SELECT 1
            FROM FlightCrew_Attendants fnext
            JOIN Flights       f2 ON f2.Flight_id = fnext.Flight_id
            JOIN Flight_Routes r2 ON r2.Route_id  = f2.Route_id
            WHERE fnext.Attendant_id = fa.Attendant_id
              AND fnext.Flight_id <> %s
              AND f2.Dep_DateTime > %s
              AND f2.Status <> 'Cancelled'
              AND r2.Origin_Airport_code <> %s
              AND f2.Dep_DateTime = (
                    SELECT MIN(f3f.Dep_DateTime)
                    FROM FlightCrew_Attendants f3
                    JOIN Flights f3f ON f3f.Flight_id = f3.Flight_id
                    WHERE f3.Attendant_id = fa.Attendant_id
                      AND f3.Flight_id <> %s
                      AND f3f.Dep_DateTime > %s
                      AND f3f.Status <> 'Cancelled'
              )
          )
        ORDER BY fa.Last_name, fa.First_name
    """
    attendant_params_fallback = attendant_params_long[1:]

    try:
        cursor.execute(attendant_sql_long, attendant_params_long)
        attendants = cursor.fetchall()
    except Error as e:
        print("DB error in _load_available_crew (attendants, Long_Haul_Certified):", e)
        cursor.execute(attendant_sql_fallback, attendant_params_fallback)
        attendants = cursor.fetchall()

    return pilots, attendants


def _load_current_crew_ids(cursor, flight_id):
    """
    Return two lists of IDs (as strings): (pilot_ids, attendant_ids)
    that are currently assigned to the given flight.
    """
    cursor.execute(
        """
        SELECT Pilot_id
        FROM FlightCrew_Pilots
        WHERE Flight_id = %s
        """,
        (flight_id,),
    )
    pilot_ids = [str(row["Pilot_id"]) for row in cursor.fetchall()]

    cursor.execute(
        """
        SELECT Attendant_id
        FROM FlightCrew_Attendants
        WHERE Flight_id = %s
        """,
        (flight_id,),
    )
    attendant_ids = [str(row["Attendant_id"]) for row in cursor.fetchall()]

    return pilot_ids, attendant_ids


# -------------------------------------------------------------
# Route
# -------------------------------------------------------------


@main_bp.route("/manager/flights/<flight_id>/crew", methods=["GET", "POST"])
def manager_flight_crew(flight_id):
    """
    Assign crew (pilots and attendants) to a flight.

    After a successful save, the user continues to the seat-pricing screen.

    Safety rule:
    If, for any reason, there are fewer eligible crew members than required
    (for example because other flights were added later), this screen will not
    allow continuing to seat pricing and will send the manager back to the
    flight edit screen with an explanatory error message.
    """
    if not _require_manager():
        return redirect(url_for("auth.login"))

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        flight = _get_flight_header(cursor, flight_id)
        if not flight:
            flash("Flight not found.", "error")
            return redirect(url_for("main.manager_flights"))

        # אין שיבוץ צוות לטיסה שבוטלה / הושלמה
        if flight["Status"] in ("Cancelled", "Completed"):
            flash("This flight is not active and its crew cannot be changed.", "error")
            return redirect(url_for("main.manager_flights"))

        required_pilots, required_attendants = _required_crew_for_flight(flight)
        pilots, attendants = _load_available_crew(cursor, flight)

        # Safety net: אם אין מספיק אנשי צוות זמינים לפי כל החוקים – אי אפשר להמשיך
        if len(pilots) < required_pilots or len(attendants) < required_attendants:
            flash(
                "This flight currently does not have enough eligible crew members "
                f"({len(pilots)}/{required_pilots} pilot(s), "
                f"{len(attendants)}/{required_attendants} attendant(s)). "
                "Please edit the flight schedule or aircraft, or cancel the flight.",
                "error",
            )
            return redirect(url_for("main.manager_edit_flight", flight_id=flight_id))

        current_pilot_ids, current_att_ids = _load_current_crew_ids(cursor, flight_id)

        if request.method == "POST":
            now = datetime.now()
            if flight["Dep_DateTime"] <= now:
                flash("This flight has already departed and cannot be changed.", "error")
                return redirect(url_for("main.manager_flights"))

            pilot_ids_raw = request.form.getlist("pilots")
            att_ids_raw = request.form.getlist("attendants")

            pilot_ids = [int(x) for x in pilot_ids_raw if x.strip()]
            att_ids = [int(x) for x in att_ids_raw if x.strip()]

            current_pilot_ids = [str(p) for p in pilot_ids]
            current_att_ids = [str(a) for a in att_ids]

            if len(pilot_ids) != required_pilots:
                flash(
                    f"This aircraft requires exactly {required_pilots} pilot(s). "
                    f"You selected {len(pilot_ids)}.",
                    "error",
                )
            elif len(att_ids) != required_attendants:
                flash(
                    f"This aircraft requires exactly {required_attendants} attendant(s). "
                    f"You selected {len(att_ids)}.",
                    "error",
                )
            else:
                cursor.execute(
                    "DELETE FROM FlightCrew_Pilots WHERE Flight_id = %s",
                    (flight_id,),
                )
                cursor.execute(
                    "DELETE FROM FlightCrew_Attendants WHERE Flight_id = %s",
                    (flight_id,),
                )

                for pid in pilot_ids:
                    cursor.execute(
                        """
                        INSERT INTO FlightCrew_Pilots (Pilot_id, Flight_id)
                        VALUES (%s, %s)
                        """,
                        (pid, flight_id),
                    )

                for aid in att_ids:
                    cursor.execute(
                        """
                        INSERT INTO FlightCrew_Attendants (Attendant_id, Flight_id)
                        VALUES (%s, %s)
                        """,
                        (aid, flight_id),
                    )

                conn.commit()
                flash("Crew updated successfully. Continue to seat pricing.", "success")
                return redirect(url_for("main.manager_flight_seats", flight_id=flight_id))

        return render_template(
            "manager_flight_crew.html",
            flight=flight,
            pilots=pilots,
            attendants=attendants,
            current_pilot_ids=current_pilot_ids,
            current_att_ids=current_att_ids,
            required_pilots=required_pilots,
            required_attendants=required_attendants,
            lock_manager_nav=True,  # Lock manager navigation during the multi-step flow
        )

    except Error as e:
        print("DB error in manager_flight_crew:", e)
        flash("Failed to load or update crew.", "error")
        return redirect(url_for("main.manager_flights"))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
