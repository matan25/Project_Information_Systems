"""
Seats management for flights:
- Manager view/update of seat prices and statuses per flight.
"""

from datetime import datetime
from flask import render_template, redirect, url_for, request, flash
from mysql.connector import Error

from db import get_db_connection
from . import main_bp, _require_manager
from .flights import (
    _compute_arrival,
    _auto_update_full_occupied,
    _sync_flight_seats_from_orders,
)


@main_bp.route("/manager/flights/<flight_id>/seats", methods=["GET", "POST"])
def manager_flight_seats(flight_id):
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

        # Sync seats + update Full-Occupied
        try:
            _sync_flight_seats_from_orders(cursor, flight_id=flight_id)
            _auto_update_full_occupied(cursor, flight_id)
            conn.commit()

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

            # 1) Update class prices (Business/Economy) for ALL not-sold seats
            for seat_class in ("Business", "Economy"):
                raw = request.form.get(f"class_price_{seat_class}")
                if raw is None:
                    continue
                raw = raw.strip()
                if raw == "":
                    continue

                try:
                    new_price = float(raw)
                except ValueError:
                    flash(f"Invalid {seat_class} price value.", "error")
                    return redirect(url_for("main.manager_flight_seats", flight_id=flight_id))

                if new_price <= 0:
                    flash(f"{seat_class} price cannot be negative or 0.", "error")
                    return redirect(url_for("main.manager_flight_seats", flight_id=flight_id))

                cursor.execute(
                    """
                    UPDATE FlightSeats fs
                    JOIN Seats s ON s.Seat_id = fs.Seat_id
                    SET fs.Seat_Price = %s
                    WHERE fs.Flight_id = %s
                      AND s.Seat_Class = %s
                      AND fs.Seat_Status IN ('Available','Blocked')
                    """,
                    (new_price, flight_id, seat_class),
                )

            # 2) Update seat statuses per seat (non-sold only)
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

                if current_status == "Sold":
                    continue

                new_status = request.form.get(f"status_{fs_id}")
                if new_status is None:
                    continue
                if new_status not in allowed_statuses:
                    new_status = "Available"

                cursor.execute(
                    """
                    UPDATE FlightSeats
                    SET Seat_Status = %s
                    WHERE FlightSeat_id = %s
                      AND Seat_Status <> 'Sold'
                    """,
                    (new_status, fs_id),
                )

            _sync_flight_seats_from_orders(cursor, flight_id=flight_id)
            _auto_update_full_occupied(cursor, flight_id)

            conn.commit()
            flash("Flight details saved successfully!", "success")
            return redirect(url_for("main.manager_flights"))

        # GET: load seats
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
        seats = cursor.fetchall() or []

        cursor.execute(
            """
            SELECT
              s.Seat_Class,
              MAX(fs.Seat_Price) AS Class_Price
            FROM FlightSeats fs
            JOIN Seats s ON s.Seat_id = fs.Seat_id
            WHERE fs.Flight_id = %s
              AND fs.Seat_Status IN ('Available','Blocked')
            GROUP BY s.Seat_Class
            """,
            (flight_id,),
        )
        class_prices_rows = cursor.fetchall() or []
        class_prices = {r["Seat_Class"]: r["Class_Price"] for r in class_prices_rows}

        return render_template(
            "manager_flight_seats.html",
            flight=flight,
            seats=seats,
            is_readonly=is_readonly,
            class_prices=class_prices,   # <-- NEW
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
