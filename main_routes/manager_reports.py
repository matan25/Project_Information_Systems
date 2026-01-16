# main_routes/manager_reports.py
"""
Manager reports dashboard and individual report views.

Reports implemented (based on the provided SQL):

1) Flight load factor (occupancy) for completed flights.
2) Revenue by aircraft size, manufacturer and seat class,
   respecting order / cancellation business rules.
3) Cumulative flight hours per employee (pilots & attendants),
   separated into long and short flights.
4) Cancellation rate of purchases per month.
5) Monthly activity per aircraft, including utilization and dominant route.
"""

from flask import render_template, redirect, url_for, flash
from mysql.connector import Error

from db import get_db_connection
from . import main_bp, _require_manager


# ----------------------------------------------------------------------
# Reports menu
# ----------------------------------------------------------------------


@main_bp.route("/manager/reports")
def manager_reports_menu():
    """Main reports menu with 5 small tiles."""
    if not _require_manager():
        return redirect(url_for("auth.login", role="manager"))

    return render_template("manager_reports.html")


# ----------------------------------------------------------------------
# Report 1 – Flight load factor
# ----------------------------------------------------------------------

@main_bp.route("/manager/reports/flight-load-factor")
def manager_report_load_factor():
    """
    Report 1: Average load factor of flights that actually took place.

    For each completed flight:
    - Total_Seats: number of seats on that flight (all FlightSeats rows).
    - Sold_Seats : number of seats with Seat_Status = 'Sold'.
    - Load_Factor_Percent: Sold_Seats / Total_Seats * 100 (rounded to 2 decimals).
    - Arr_DateTime: calculated as Dep_DateTime + route Duration_Minutes.
    - Route: Origin -> Destination.
    """
    if not _require_manager():
        return redirect(url_for("auth.login", role="manager"))

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    rows = []

    try:
        cursor.execute(
            """
            SELECT
                f.Flight_id,
                f.Dep_DateTime,
                DATE_ADD(f.Dep_DateTime, INTERVAL r.Duration_Minutes MINUTE) AS Arr_DateTime,
                r.Origin_Airport_code,
                r.Destination_Airport_code,
                ao.City AS Origin_City,
                ad.City AS Destination_City,
                COUNT(fs.FlightSeat_id) AS Total_Seats,
                SUM(CASE WHEN fs.Seat_Status = 'Sold' THEN 1 ELSE 0 END) AS Sold_Seats
            FROM Flights f
            JOIN Flight_Routes r ON f.Route_id = r.Route_id
            JOIN Airports ao ON ao.Airport_code = r.Origin_Airport_code
            JOIN Airports ad ON ad.Airport_code = r.Destination_Airport_code
            JOIN FlightSeats fs ON f.Flight_id = fs.Flight_id
            WHERE f.Status = 'Completed'
            GROUP BY
                f.Flight_id,
                f.Dep_DateTime,
                r.Duration_Minutes,
                r.Origin_Airport_code,
                r.Destination_Airport_code,
                ao.City,
                ad.City
            ORDER BY f.Dep_DateTime
            """
        )
        rows = cursor.fetchall()

        for r in rows:
            dep_dt = r.get("Dep_DateTime")
            arr_dt = r.get("Arr_DateTime")

            r["Dep_str"] = dep_dt.strftime("%d/%m/%Y %H:%M") if dep_dt else "-"
            r["Arr_str"] = arr_dt.strftime("%d/%m/%Y %H:%M") if arr_dt else "-"

            r["Route_str"] = (
                f"{r.get('Origin_Airport_code')} ({r.get('Origin_City')}) → "
                f"{r.get('Destination_Airport_code')} ({r.get('Destination_City')})"
            )

            total = int(r.get("Total_Seats") or 0)
            sold = int(r.get("Sold_Seats") or 0)

            if total > 0:
                r["Load_Factor_Percent"] = round((sold / total) * 100.0, 2)
            else:
                r["Load_Factor_Percent"] = 0.0

    except Error as e:
        print("DB error in manager_report_load_factor:", e)
        flash("Failed to load load-factor report.", "error")
        rows = []
    finally:
        cursor.close()
        conn.close()

    return render_template("report_load_factor.html", flights=rows)



# ----------------------------------------------------------------------
# Report 2 – Revenue by aircraft / manufacturer / seat class
# ----------------------------------------------------------------------


@main_bp.route("/manager/reports/revenue-by-aircraft")
def manager_report_revenue_by_aircraft():
    """
    Report 2: Revenue by Aircraft Size, Manufacturer and Seat Class.

    Rules:
    - Active / Completed orders: 100% of ticket price.
    - Cancelled-Customer:
        * If cancelled ≥ 36 hours before departure → 5% of ticket price.
        * Otherwise → 0 revenue.
    - Cancelled-System: 0 revenue.
    - Cancelled flights are excluded (Flights.Status <> 'Cancelled').
    """
    if not _require_manager():
        return redirect(url_for("auth.login", role="manager"))

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    rows = []

    try:
        cursor.execute(
            """
            SELECT
                a.Size         AS Aircraft_Size,
                a.Manufacturer AS Aircraft_Manufacturer,
                s.Seat_Class   AS Seat_Class,
                COALESCE(
                    SUM(
                        CASE
                            WHEN o.Status IN ('Active', 'Completed')
                                THEN fs.Seat_Price
                            WHEN o.Status = 'Cancelled-Customer'
                                 AND o.Cancel_Date IS NOT NULL
                                 AND TIMESTAMPDIFF(
                                        HOUR,
                                        o.Cancel_Date,
                                        f.Dep_DateTime
                                     ) >= 36
                                THEN 0.05 * fs.Seat_Price
                            ELSE 0
                        END
                    ),
                    0
                ) AS Total_Revenue
            FROM Aircrafts a
            JOIN Seats s ON s.Aircraft_id = a.Aircraft_id
            LEFT JOIN FlightSeats fs ON fs.Seat_id = s.Seat_id
            LEFT JOIN Flights f
                   ON f.Flight_id = fs.Flight_id
                  AND f.Status <> 'Cancelled'
            LEFT JOIN Tickets t ON t.FlightSeat_id = fs.FlightSeat_id
            LEFT JOIN Orders o  ON o.Order_code     = t.Order_code
            GROUP BY
                a.Size, a.Manufacturer, s.Seat_Class
            ORDER BY
                a.Size, a.Manufacturer, s.Seat_Class
            """
        )
        rows = cursor.fetchall()

    except Error as e:
        print("DB error in manager_report_revenue_by_aircraft:", e)
        flash("Failed to load revenue report.", "error")
        rows = []
    finally:
        cursor.close()
        conn.close()

    return render_template("report_revenue_by_aircraft.html", rows=rows)


# ----------------------------------------------------------------------
# Report 3 – Employee flight hours (long vs short)
# ----------------------------------------------------------------------


@main_bp.route("/manager/reports/employee-hours")
def manager_report_employee_hours():
    """
    Report 3: Cumulative flight hours per employee (pilots & attendants),
    separated into long and short flights (long = Duration_Minutes > 360).
    """
    if not _require_manager():
        return redirect(url_for("auth.login", role="manager"))

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    rows = []

    try:
        cursor.execute(
            """
            -- PILOTS
            SELECT
                P.Pilot_id AS Employee_id,
                CONCAT(P.First_Name, ' ', P.Last_Name) AS Full_Name,
                'Pilot' AS Employee_Type,
                SUM(
                    CASE WHEN R.Duration_Minutes > 360
                         THEN R.Duration_Minutes ELSE 0 END
                ) / 60.0 AS Long_Hours,
                SUM(
                    CASE WHEN R.Duration_Minutes <= 360
                         THEN R.Duration_Minutes ELSE 0 END
                ) / 60.0 AS Short_Hours
            FROM Pilots P
            JOIN FlightCrew_Pilots CP   ON P.Pilot_id   = CP.Pilot_id
            JOIN Flights F              ON CP.Flight_id = F.Flight_id
            JOIN Flight_Routes R        ON F.Route_id   = R.Route_id
            WHERE F.Status = 'Completed'
            GROUP BY P.Pilot_id, Full_Name

            UNION ALL

            -- ATTENDANTS
            SELECT
                A.Attendant_id AS Employee_id,
                CONCAT(A.First_Name, ' ', A.Last_Name) AS Full_Name,
                'FlightAttendant' AS Employee_Type,
                SUM(
                    CASE WHEN R.Duration_Minutes > 360
                         THEN R.Duration_Minutes ELSE 0 END
                ) / 60.0 AS Long_Hours,
                SUM(
                    CASE WHEN R.Duration_Minutes <= 360
                         THEN R.Duration_Minutes ELSE 0 END
                ) / 60.0 AS Short_Hours
            FROM FlightAttendants A
            JOIN FlightCrew_Attendants CA ON A.Attendant_id = CA.Attendant_id
            JOIN Flights F                ON CA.Flight_id   = F.Flight_id
            JOIN Flight_Routes R          ON F.Route_id     = R.Route_id
            WHERE F.Status = 'Completed'
            GROUP BY A.Attendant_id, Full_Name

            ORDER BY Employee_Type, Full_Name
            """
        )
        rows = cursor.fetchall()

    except Error as e:
        print("DB error in manager_report_employee_hours:", e)
        flash("Failed to load employee hours report.", "error")
        rows = []
    finally:
        cursor.close()
        conn.close()

    return render_template("report_employee_hours.html", employees=rows)


# ----------------------------------------------------------------------
# Report 4 – Cancellation rate per month
# ----------------------------------------------------------------------


@main_bp.route("/manager/reports/cancellation-rate")
def manager_report_cancellation_rate():
    """
    Report 4: Cancellation rate of purchases per month.

    For each month:
    - Total_Orders
    - Cancelled_Orders (Cancelled-Customer or Cancelled-System)
    - Cancellation_Rate_Percent.
    """
    if not _require_manager():
        return redirect(url_for("auth.login", role="manager"))

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    rows = []

    try:
        cursor.execute(
            """
            SELECT
                DATE_FORMAT(Order_Date, '%Y-%m') AS YearMonth,
                COUNT(*) AS Total_Orders,
                SUM(
                    CASE WHEN Status IN ('Cancelled-Customer','Cancelled-System')
                         THEN 1 ELSE 0 END
                ) AS Cancelled_Orders,
                ROUND(
                    SUM(
                        CASE WHEN Status IN ('Cancelled-Customer','Cancelled-System')
                             THEN 1 ELSE 0 END
                    ) * 100.0 / COUNT(*),
                    2
                ) AS Cancellation_Rate_Percent
            FROM Orders
            GROUP BY DATE_FORMAT(Order_Date, '%Y-%m')
            ORDER BY YearMonth
            """
        )
        rows = cursor.fetchall()

    except Error as e:
        print("DB error in manager_report_cancellation_rate:", e)
        flash("Failed to load cancellation-rate report.", "error")
        rows = []
    finally:
        cursor.close()
        conn.close()

    return render_template("report_cancellation_rate.html", months=rows)


# ----------------------------------------------------------------------
# Report 5 – Monthly activity per aircraft
# ----------------------------------------------------------------------


@main_bp.route("/manager/reports/aircraft-monthly-activity")
def manager_report_aircraft_monthly_activity():
    """
    Report 5: Monthly activity per aircraft.

    For each aircraft & month:
    - Flights_Completed
    - Flights_Cancelled
    - Total_Flights
    - Utilization_Percent (share of month time in the air, completed flights only)
    - Dominant_Route (most common Origin–Destination pair in that month, based on completed flights only;
      if there is a tie, multiple routes are shown).

    Note: This report is limited to flights up to the current timestamp (no future departures).
    """
    if not _require_manager():
        return redirect(url_for("auth.login", role="manager"))

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    rows = []

    try:
        cursor.execute(
            """
            WITH flight_monthly AS (
                SELECT
                    f.Flight_id,
                    f.Aircraft_id,
                    DATE_FORMAT(f.Dep_DateTime, '%Y-%m-01') AS MonthStart,
                    fr.Origin_Airport_code,
                    fr.Destination_Airport_code,
                    fr.Duration_Minutes,
                    f.Status
                FROM Flights f
                JOIN Flight_Routes fr
                  ON f.Route_id = fr.Route_id
                WHERE f.Dep_DateTime <= NOW()
            ),

            aircraft_month_summary AS (
                SELECT
                    Aircraft_id,
                    MonthStart,
                    COUNT(*) AS Total_Flights,
                    SUM(CASE WHEN Status = 'Completed' THEN 1 ELSE 0 END)  AS Flights_Completed,
                    SUM(CASE WHEN Status = 'Cancelled' THEN 1 ELSE 0 END) AS Flights_Cancelled,
                    SUM(CASE WHEN Status = 'Completed' THEN Duration_Minutes ELSE 0 END) AS Total_Flight_Minutes
                FROM flight_monthly
                GROUP BY Aircraft_id, MonthStart
            ),

            top_route_rank AS (
                -- Rank routes by number of COMPLETED flights (ties allowed)
                SELECT
                    Aircraft_id,
                    MonthStart,
                    Origin_Airport_code,
                    Destination_Airport_code,
                    COUNT(*) AS Route_Flights,
                    DENSE_RANK() OVER (
                        PARTITION BY Aircraft_id, MonthStart
                        ORDER BY COUNT(*) DESC
                    ) AS rk
                FROM flight_monthly
                WHERE Status = 'Completed'
                GROUP BY
                    Aircraft_id,
                    MonthStart,
                    Origin_Airport_code,
                    Destination_Airport_code
            ),

            top_routes_agg AS (
                -- If multiple routes share rank 1, show them all (comma-separated)
                SELECT
                    Aircraft_id,
                    MonthStart,
                    GROUP_CONCAT(
                        CONCAT(Origin_Airport_code, '→', Destination_Airport_code)
                        ORDER BY Origin_Airport_code, Destination_Airport_code
                        SEPARATOR ', '
                    ) AS Dominant_Route
                FROM top_route_rank
                WHERE rk = 1
                GROUP BY Aircraft_id, MonthStart
            )

            SELECT
                ams.Aircraft_id,
                ac.Manufacturer,
                ac.Model,
                DATE_FORMAT(ams.MonthStart, '%Y-%m') AS Month,
                ams.Flights_Completed,
                ams.Flights_Cancelled,
                ams.Total_Flights,
                ROUND(
                    ams.Total_Flight_Minutes / (30 * 24 * 60) * 100,
                    2
                ) AS Utilization_Percent,
                tra.Dominant_Route
            FROM aircraft_month_summary AS ams
            JOIN Aircrafts AS ac
              ON ac.Aircraft_id = ams.Aircraft_id
            LEFT JOIN top_routes_agg AS tra
              ON tra.Aircraft_id = ams.Aircraft_id
             AND tra.MonthStart  = ams.MonthStart
            ORDER BY ams.MonthStart, ams.Aircraft_id
            """
        )

        rows = cursor.fetchall()

    except Error as e:
        print("DB error in manager_report_aircraft_monthly_activity:", e)
        flash("Failed to load aircraft monthly activity report.", "error")
        rows = []
    finally:
        cursor.close()
        conn.close()

    return render_template(
        "report_aircraft_monthly_activity.html",
        records=rows,
    )

