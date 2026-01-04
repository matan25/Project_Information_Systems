"""
Main business logic package for FLYTAU.
Defines the main Blueprint and shared helpers,
and imports submodules (home, flights, crew, booking) that register routes.
"""

from flask import Blueprint, render_template, session, redirect, url_for, flash

main_bp = Blueprint("main", __name__)

# --------------------------------------------------
# Basic home routes
# --------------------------------------------------


@main_bp.route("/")
def index():
    """Public home page."""
    return render_template("home.html")


@main_bp.route("/customer/home")
def customer_home():
    """Customer landing page (after identifying registered customer)."""
    if not _require_customer():
        return redirect(url_for("main.customer_orders_login"))
    return render_template("customer_home.html")


@main_bp.route("/manager/home")
def manager_home():
    """Manager landing page."""
    if not _require_manager():
        return redirect(url_for("auth.login"))
    manager_name = session.get("manager_name", "Manager")
    return render_template(
        "manager_home.html",
        manager_name=manager_name,
        lock_manager_nav=False,  # Regular manager panel – navigation is unlocked
    )


# --------------------------------------------------
# Shared configuration
# --------------------------------------------------

# 6 hours → long-haul
LONG_FLIGHT_THRESHOLD_MINUTES = 360

# crew requirements by flight profile
CREW_REQUIREMENTS = {
    "short": {"pilots": 2, "attendants": 3, "long_required": False},
    "long": {"pilots": 3, "attendants": 6, "long_required": True},
}

# default price per seat class (base values)
SEAT_DEFAULT_PRICE_BY_CLASS = {
    "Business": 1200.0,
    # "Premium": 800.0,
    "Economy": 400.0,
}


# --------------------------------------------------
# Helper functions
# --------------------------------------------------


def _require_manager() -> bool:
    """Return True if current session is a manager, otherwise flash error."""
    if session.get("role") != "manager":
        flash("Manager account required.", "error")
        return False
    return True


def _require_customer() -> bool:
    """
    Return True if current session is a registered customer, otherwise flash error.

    Registered customers are identified in the session with:
        role = 'customer'
        customer_email = their email address.
    """
    if session.get("role") != "customer" or not session.get("customer_email"):
        flash("Registered customer identification is required.", "error")
        return False
    return True


def _flight_profile(duration_minutes: int) -> str:
    """Return 'short' or 'long' according to duration in minutes."""
    if duration_minutes >= LONG_FLIGHT_THRESHOLD_MINUTES:
        return "long"
    return "short"


def _crew_requirements(duration_minutes: int) -> dict:
    """Return required crew sizes and long-haul flag for a given duration."""
    profile = _flight_profile(duration_minutes)
    return CREW_REQUIREMENTS[profile]


def _get_default_seat_price(seat_class: str) -> float:
    """
    Return default price for seat_class (fallback to Economy).

    Per-flight overrides (for short-haul, etc.) are applied at the point
    of FlightSeats creation in flights.py.
    """
    if not seat_class:
        return SEAT_DEFAULT_PRICE_BY_CLASS.get("Economy", 400.0)
    return SEAT_DEFAULT_PRICE_BY_CLASS.get(
        seat_class,
        SEAT_DEFAULT_PRICE_BY_CLASS.get("Economy", 400.0),
    )


# VERY IMPORTANT: import submodules so their @main_bp.route
# decorators actually run and register routes.
from . import (
    home,
    flights,
    crew,
    booking,
    manager_orders,
    manager_reports,
    aircrafts,
    staff,
    seats
)
