"""
Seats management for flights:
- Manager view/update of seat prices and statuses per flight.
"""

from datetime import datetime, timedelta

from flask import render_template, redirect, url_for, request, flash
from mysql.connector import Error

from db import get_db_connection
from . import main_bp, _require_manager
from .flights import (
    _compute_arrival,
    _auto_update_full_occupied,
    _sync_flight_seats_from_orders,   # <-- NEW: sync seats from Orders+Tickets
)


@main_bp.route("/manager/flights/<flight_id>/seats", methods=["GET", "POST"])
def manager_flight_seats(flight_id):
    """
    Manager interface to view/update seat prices and statuses for a flight.
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
                a.Model AS AircraftModel,
                r.Origin_Airport_code,
                r.Destination_Airport_code,
                r.Duration_Minutes
            FROM Flights f
            JOIN Aircrafts     a ON f.Aircraft_id = a.Aircraft_id
            JOIN Flight_Routes r ON f.Route_id    = r.Route_id
            WHERE f.Flight_id = %s
            """,
            (flight_id,),
        )
        flight = cursor.fetchone()
        if not flight:
            flash("Flight not found.", "error")
            return redirect(url_for("main.manager_flights"))

        # ---- NEW: keep seat statuses and flight status consistent on every entry ----
        # This ensures that if a manager previously changed Blocked->Available,
        # the system will immediately set it back to Sold if a valid Ticket exists,
        # and also update Full-Occupied/Active accordingly.
        try:
            _sync_flight_seats_from_orders(cursor, flight_id=flight_id)
            _auto_update_full_occupied(cursor, flight_id)
            conn.commit()

            # refresh current flight status (might have changed to/from Full-Occupied)
            cursor.execute("SELECT Status FROM Flights WHERE Flight_id = %s", (flight_id,))
            srow = cursor.fetchone()
            if srow and "Status" in srow:
                flight["Status"] = srow["Status"]
        except Exception as e:
            print("Warning: failed to sync seats / update Full-Occupied on entry:", e)

        now = datetime.now()
        is_readonly = flight["Dep_DateTime"] <= now or flight["Status"] in ("Cancelled", "Completed")

        dep_dt = flight["Dep_DateTime"]
        duration = int(flight["Duration_Minutes"])
        arr_dt = _compute_arrival(dep_dt, duration)

        flight["Dep_str"] = dep_dt.strftime("%Y-%m-%d %H:%M")
        flight["Arr_str"] = arr_dt.strftime("%Y-%m-%d %H:%M")

        if request.method == "POST":
            if is_readonly:
                flash("This flight can no longer be edited.", "error")
                return redirect(url_for("main.manager_flights"))

            cursor.execute(
                """
                SELECT FlightSeat_id, Seat_Status
                FROM FlightSeats
                WHERE Flight_id = %s
                """,
                (flight_id,),
            )
            seat_rows = cursor.fetchall()

            allowed_statuses = {"Available", "Blocked"}

            for row in seat_rows:
                fs_id = row["FlightSeat_id"]
                current_status = row["Seat_Status"]

                # Sold seats cannot be modified by manager
                if current_status == "Sold":
                    continue

                price_str = request.form.get(f"price_{fs_id}")
                status = request.form.get(f"status_{fs_id}")

                if price_str is None or status is None:
                    continue

                if status not in allowed_statuses:
                    status = "Available"

                try:
                    price = float(price_str)
                except ValueError:
                    flash("Invalid price value for at least one of the seats.", "error")
                    return redirect(url_for("main.manager_flight_seats", flight_id=flight_id))

                if price < 0:
                    flash("Seat price cannot be negative.", "error")
                    return redirect(url_for("main.manager_flight_seats", flight_id=flight_id))

                cursor.execute(
                    """
                    UPDATE FlightSeats
                    SET Seat_Price = %s,
                        Seat_Status = %s
                    WHERE FlightSeat_id = %s
                    """,
                    (price, status, fs_id),
                )

            # ---- NEW: sync after manager changes, then update flight status ----
            # If manager set Blocked->Available but there is an active ticket,
            # the seat will become Sold again, and Full-Occupied will update properly.
            _sync_flight_seats_from_orders(cursor, flight_id=flight_id)
            _auto_update_full_occupied(cursor, flight_id)

            conn.commit()
            flash("Flight details saved successfully!", "success")
            return redirect(url_for("main.manager_flights"))

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
            "manager_flight_seats.html",
            flight=flight,
            seats=seats,
            is_readonly=is_readonly,
            lock_manager_nav=True,
        )

    except Error as e:
        print("DB error in manager_flight_seats:", e)
        flash("Failed to load or update flight seats.", "error")
        return redirect(url_for("main.manager_flights"))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
