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
    Also annotates the flight with:
      - Dep_str / Arr_str: formatted strings
      - Arr_DateTime: computed arrival datetime
      - Is_Long_Route: True iff duration is strictly greater than
        LONG_FLIGHT_THRESHOLD_MINUTES (i.e., > 6 hours, not including 6).
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
        flight["Is_Long_Route"] = duration > LONG_FLIGHT_THRESHOLD_MINUTES
    return flight


def _required_crew_for_flight(flight):
    """
    Return required numbers of pilots and attendants according to aircraft size.

    Rules:
      - Small/Medium aircraft:
          * 2 pilots
          * 3 attendants
      - Large aircraft:
          * 3 pilots
          * 6 attendants
    """
    size = flight.get("Aircraft_Size", "Small")
    if size == "Large":
        return 3, 6
    return 2, 3


def _load_available_crew(cursor, flight):
    """
     Return the count-based availability result for pilots/attendants who:
      * Have NO time overlap with any other assigned flights
        (excluding ignore_flight_id when provided).
      * For long-haul routes: must be Long_Haul_Certified = 1 (if the column exists).
      * Location continuity rules:
          - The latest flight BEFORE departure must end at the origin airport.
          - The earliest flight AFTER arrival must depart from the destination airport.
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
          AND NOT EXISTS (
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
          AND NOT EXISTS (
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

    cursor.execute(pilot_sql_long, pilot_params_long)
    pilots = cursor.fetchall()

    # -------- Attendants --------
    attendant_sql_long = """
        SELECT fa.Attendant_id, fa.First_name, fa.Last_name
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
          AND NOT EXISTS (
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
          AND NOT EXISTS (
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

    cursor.execute(attendant_sql_long, attendant_params_long)
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


def _load_crew_ui_state(cursor, flight_id, flight):
    """
    Helper for the crew-assignment screen:

    Returns:
      - pilots:      list of pilot rows to show in the UI
      - attendants:  list of attendant rows to show in the UI
      - current_pilot_ids: list[str] of Pilot_id currently assigned
      - current_att_ids:   list[str] of Attendant_id currently assigned
      - allowed_pilot_ids: set[int] of Pilot_id that are allowed to be selected
                           (either available now, or already assigned)
      - allowed_att_ids:   set[int] of Attendant_id that are allowed to be selected
    """
    # Current assignments
    current_pilot_ids, current_att_ids = _load_current_crew_ids(cursor, flight_id)

    # Crew that is currently eligible according to all rules
    pilots_available, attendants_available = _load_available_crew(cursor, flight)

    available_pilot_ids = {int(row["Pilot_id"]) for row in pilots_available}
    available_att_ids = {int(row["Attendant_id"]) for row in attendants_available}

    # Add currently assigned pilots that are not in the "available" set anymore
    extra_pilots = []
    missing_pilot_ids = [
        int(pid)
        for pid in current_pilot_ids
        if pid and int(pid) not in available_pilot_ids
    ]
    if missing_pilot_ids:
        placeholders = ",".join(["%s"] * len(missing_pilot_ids))
        cursor.execute(
            f"""
            SELECT Pilot_id, First_name, Last_name
            FROM Pilots
            WHERE Pilot_id IN ({placeholders})
            """,
            tuple(missing_pilot_ids),
        )
        extra_pilots = cursor.fetchall()

    # Same logic for attendants
    extra_attendants = []
    missing_att_ids = [
        int(aid)
        for aid in current_att_ids
        if aid and int(aid) not in available_att_ids
    ]
    if missing_att_ids:
        placeholders = ",".join(["%s"] * len(missing_att_ids))
        cursor.execute(
            f"""
            SELECT Attendant_id, First_name, Last_name
            FROM FlightAttendants
            WHERE Attendant_id IN ({placeholders})
            """,
            tuple(missing_att_ids),
        )
        extra_attendants = cursor.fetchall()

    # --- Deduplicate & sort pilots ---
    pilot_by_id = {}
    for row in (pilots_available + extra_pilots):
        pid = int(row["Pilot_id"])
        if pid not in pilot_by_id:
            pilot_by_id[pid] = row
    pilots = sorted(
        pilot_by_id.values(),
        key=lambda r: (r["Last_name"], r["First_name"]),
    )

    # --- sort attendants ---
    att_by_id = {}
    for row in (attendants_available + extra_attendants):
        aid = int(row["Attendant_id"])
        if aid not in att_by_id:
            att_by_id[aid] = row
    attendants = sorted(
        att_by_id.values(),
        key=lambda r: (r["Last_name"], r["First_name"]),
    )

    allowed_pilot_ids = available_pilot_ids | {
        int(pid) for pid in current_pilot_ids if pid
    }
    allowed_att_ids = available_att_ids | {
        int(aid) for aid in current_att_ids if aid
    }

    return (
        pilots,
        attendants,
        current_pilot_ids,
        current_att_ids,
        allowed_pilot_ids,
        allowed_att_ids,
    )


# -------------------------------------------------------------
# Route
# -------------------------------------------------------------


@main_bp.route("/manager/flights/<flight_id>/crew", methods=["GET", "POST"])
def manager_flight_crew(flight_id):
    """
    Assign crew (pilots and attendants) to a flight.

    After a successful save, the user continues to the seat-pricing screen.

    - Crew members who do NOT satisfy the time / availability constraints and (when enforced) the
      location-continuity constraints are NOT shown in the dropdown lists at all.

    - Crew members already assigned to this flight remain selectable even if an intermediate
      “connecting” flight in their chain was cancelled/changed — we assume the airline can
      reposition them to the next required destination.

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

        # No crew assignment for cancelled or completed flights
        if flight["Status"] in ("Cancelled", "Completed"):
            flash("This flight is not active and its crew cannot be changed.", "error")
            return redirect(url_for("main.manager_flights"))

        required_pilots, required_attendants = _required_crew_for_flight(flight)

        (
            pilots,
            attendants,
            current_pilot_ids,
            current_att_ids,
            allowed_pilot_ids,
            allowed_att_ids,
        ) = _load_crew_ui_state(cursor, flight_id, flight)

        if len(allowed_pilot_ids) < required_pilots or len(allowed_att_ids) < required_attendants:
            flash(
                "This flight currently does not have enough eligible crew members "
                f"(allowed pilots: {len(allowed_pilot_ids)}/{required_pilots}, "
                f"allowed attendants: {len(allowed_att_ids)}/{required_attendants}). "
                "Please edit the flight schedule or aircraft, or cancel the flight.",
                "error",
            )
            return redirect(url_for("main.manager_edit_flight", flight_id=flight_id))

        if request.method == "POST":
            now = datetime.now()
            if flight["Dep_DateTime"] <= now:
                flash("This flight has already departed and cannot be changed.", "error")
                return redirect(url_for("main.manager_flights"))

            pilot_ids_raw = request.form.getlist("pilots")
            att_ids_raw = request.form.getlist("attendants")

            pilot_ids = [int(x) for x in pilot_ids_raw if x.strip()]
            att_ids = [int(x) for x in att_ids_raw if x.strip()]

            # preserve the user's selections even on errors
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
                # All good – overwrite crew for this flight
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

        # GET flow or POST with validation errors
        return render_template(
            "manager_flight_crew.html",
            flight=flight,
            pilots=pilots,
            attendants=attendants,
            current_pilot_ids=current_pilot_ids,
            current_att_ids=current_att_ids,
            required_pilots=required_pilots,
            required_attendants=required_attendants,
            lock_manager_nav=True,
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
