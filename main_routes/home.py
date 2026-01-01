# # main_routes/home.py
# from flask import render_template, redirect, url_for, session
# from . import main_bp, _require_manager, _require_customer
#
# @main_bp.route("/")
# def index():
#     """
#     Landing page.
#     If user is already logged in, redirect to the relevant dashboard.
#     Otherwise show full-width home screen (no card).
#     """
#     role = session.get("role")
#     if role == "manager":
#         return redirect(url_for("main.manager_home"))
#     if role == "customer":
#         return redirect(url_for("main.customer_home"))
#
#     # not logged in → full-width hero page, no card
#     return render_template("home.html", no_card=True)

from flask import Blueprint, render_template, session, redirect, url_for

main_bp = Blueprint("main", __name__, url_prefix="")

@main_bp.route("/")
def index():
    return render_template("home.html")

@main_bp.route("/customer/home")
def customer_home():
    # אפשר לבדוק שהמשתמש הוא באמת customer
    if session.get("role") != "customer":
        return redirect(url_for("auth.login"))
    return render_template("customer_home.html")

@main_bp.route("/manager/home")
def manager_home():
    if session.get("role") != "manager":
        return redirect(url_for("auth.login"))
    return render_template("manager_home.html", manager_name="Manager")