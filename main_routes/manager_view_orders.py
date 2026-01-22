"""
Manager view for all orders in the system.

Features:
- Show all orders (registered + guest customers) in manager interface
- Filter by status, flight ID, customer email
- Display ticket count and price logic consistent with customer views:
    * Active / Completed: full original total
    * Cancelled-Customer: 5% fee as amount charged, plus refund
    * Cancelled-System: amount charged = 0, full refund
"""

from datetime import datetime, timedelta

from flask import render_template, request, redirect, url_for, flash
from mysql.connector import Error

from db import get_db_connection
from . import main_bp, _require_manager
from .booking import _cleanup_cancelled_orders_seats  # optional helper, even if unused


@main_bp.route("/manager/orders")
def manager_orders():
    """Manager: list all orders with filters and full financial view."""
    if not _require_manager():
        return redirect(url_for("auth.login"))

    status_filter = request.args.get("status", "all")
    flight_id_filter = (request.args.get("flight_id") or "").strip() or None
    customer_email_filter = (request.args.get("customer_email") or "").strip() or None

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
        base_query = """
            SELECT
                o.Order_code,
                o.Order_Date,
                o.Status               AS Order_Status,
                o.Cancel_Date,
                o.Customer_Email,
                o.Customer_Type,
                -- name from Register or Guest, according to Customer_Type
                COALESCE(rc.First_Name, gc.First_Name) AS First_Name,
                COALESCE(rc.Last_Name,  gc.Last_Name)  AS Last_Name,
                o.Flight_id,
                f.Dep_DateTime,
                f.Status               AS Flight_Status,
                fr.Origin_Airport_code,
                fr.Destination_Airport_code,
                COUNT(t.FlightSeat_id) AS Ticket_Count,

                -- === UPDATED: use Tickets.Paid_Price (original paid) if exists, else fallback to current FlightSeats.Seat_Price ===
                COALESCE(SUM(COALESCE(t.Paid_Price, fs.Seat_Price)), 0) AS Raw_Total

            FROM Orders o
            LEFT JOIN Register_Customers rc
                   ON rc.Customer_Email = o.Customer_Email
                  AND o.Customer_Type   = 'Register'
            LEFT JOIN Guest_Customers gc
                   ON gc.Customer_Email = o.Customer_Email
                  AND o.Customer_Type   = 'Guest'
            LEFT JOIN Flights       f  ON o.Flight_id      = f.Flight_id
            LEFT JOIN Flight_Routes fr ON f.Route_id       = fr.Route_id
            LEFT JOIN Tickets       t  ON o.Order_code     = t.Order_code
            LEFT JOIN FlightSeats   fs ON t.FlightSeat_id  = fs.FlightSeat_id
            WHERE 1=1
        """
        params = []

        if status_filter != "all":
            base_query += " AND o.Status = %s"
            params.append(status_filter)

        if flight_id_filter:
            base_query += " AND o.Flight_id = %s"
            params.append(flight_id_filter)

        if customer_email_filter:
            base_query += " AND o.Customer_Email LIKE %s"
            params.append(f"%{customer_email_filter}%")

        base_query += """
            GROUP BY o.Order_code
            ORDER BY o.Order_Date DESC
        """

        cursor.execute(base_query, tuple(params))
        orders = cursor.fetchall()

        now = datetime.now()
        to_cancel_sys = []

        for o in orders:
            # Pretty strings
            o["OrderDate_str"] = (
                o["Order_Date"].strftime("%d/%m/%Y %H:%M")
                if o.get("Order_Date")
                else "-"
            )
            o["CancelDate_str"] = (
                o["Cancel_Date"].strftime("%d/%m/%Y %H:%M")
                if o.get("Cancel_Date")
                else None
            )

            dep_dt = o.get("Dep_DateTime")
            if dep_dt:
                o["Dep_str"] = dep_dt.strftime("%d/%m/%Y %H:%M")
                time_to_dep = dep_dt - now
            else:
                o["Dep_str"] = "-"
                time_to_dep = timedelta(days=99999)

            # Customer name
            first = (o.get("First_Name") or "").strip()
            last = (o.get("Last_Name") or "").strip()
            o["Customer_Name"] = (first + " " + last).strip() or None

            base_total = float(o.get("Raw_Total") or 0.0)
            o["Ticket_Count"] = int(o.get("Ticket_Count") or 0)
            o["Original_Total"] = base_total

            # Auto: flight cancelled → Cancelled-System
            if o["Order_Status"] == "Active" and o.get("Flight_Status") == "Cancelled":
                to_cancel_sys.append(o["Order_code"])
                o["Order_Status"] = "Cancelled-System"

            status = o["Order_Status"]
            o["Cancellation_Fee"] = None
            o["Refund_Amount"] = None

            # Displayed amounts (UNCHANGED LOGIC)
            if status == "Cancelled-Customer":
                fee = round(base_total * 0.05, 2)
                refund = max(base_total - fee, 0.0)
                o["Display_Total"] = fee
                o["Cancellation_Fee"] = fee
                o["Refund_Amount"] = refund
            elif status == "Cancelled-System":
                o["Display_Total"] = 0.0
                o["Refund_Amount"] = base_total
            else:
                o["Display_Total"] = base_total

        # Persist DB changes: Active → Cancelled-System when flight is cancelled
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
            conn.commit()

    except Error as e:
        print("DB error in manager_orders:", e)
        flash("Failed to load orders list.", "error")
    finally:
        cursor.close()
        conn.close()

    return render_template(
        "manager_orders.html",
        orders=orders,
        status_filter=status_filter,
        flight_id_filter=flight_id_filter,
        customer_email_filter=customer_email_filter,
    )
