# main_routes/home.py
from flask import render_template, session, redirect, url_for
from . import main_bp, _require_manager, _require_customer

@main_bp.route("/")
def index():
    """Public home page."""
    return render_template("home.html")

@main_bp.route("/customer/home")
def customer_home():
    """Customer landing page (after identifying registered customer)."""
    if not _require_customer():
        return redirect(url_for("auth.login", role="customer"))
    return render_template("customer_home.html")

@main_bp.route("/manager/home")
def manager_home():
    """Manager landing page."""
    if not _require_manager():
        return redirect(url_for("auth.login", role="manager"))
    manager_name = session.get("manager_name", "Manager")
    return render_template("manager_home.html", manager_name=manager_name, lock_manager_nav=False)
