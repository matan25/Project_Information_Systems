from flask import Flask, session, request, redirect, url_for, abort
from config import SECRET_KEY, DB_CONFIG, SESSION_SETTINGS
from auth_routes import auth_bp
from main_routes import main_bp

def create_main():
    main = Flask(__name__, template_folder="templates", static_folder="static")
    main.secret_key = SECRET_KEY
    main.config["DB_CONFIG"] = DB_CONFIG
    for k, v in SESSION_SETTINGS.items():
        main.config[k] = v

    main.register_blueprint(main_bp)
    main.register_blueprint(auth_bp)

    @main.after_request
    def remember_last_valid_url(response):
        """
        Store the last successful, non-static GET URL (HTTP 200) in the session,
        so we can redirect the user back to it on 404/405 errors.
        """
        # Save only valid pages (GET + 200)
        if request.method == "GET" and response.status_code == 200 and request.endpoint:
            # Ignore static files and logout endpoint
            if not request.endpoint.startswith("static") and request.endpoint != "auth.logout":
                full = request.full_path or request.path
                if full.endswith("?"):
                    full = full[:-1]
                session["last_valid_url"] = full
        return response

    def back_to_last(_err):
        """
        Error handler for 404/405: redirect to the last valid page when possible,
        otherwise fall back to a role-based home/login page.
        """
        # Do not interfere with static paths
        if request.path.startswith("/static/"):
            return _err

        last = session.get("last_valid_url")
        if last:
            return redirect(last, code=302)

        # Fallback if there is no stored last URL (for new session)
        role = session.get("role")
        if role == "manager":
            return redirect(url_for("main.manager_home"))
        if role == "customer" and session.get("customer_email"):
            return redirect(url_for("main.customer_home"))
        return redirect(url_for("auth.login", role="customer"))

    # handle 404, 405 to avoid a blank error page
    main.register_error_handler(404, back_to_last)
    main.register_error_handler(405, back_to_last)

    return main


main = create_main()

if __name__ == "__main__":
    # only for local running (change in deployment)
    main.run(debug=True, port=5050, use_reloader=False)

# app define (for deployment pythonanywhere)
app = main
