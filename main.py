# main.py
from flask import Flask
from config import SECRET_KEY, DB_CONFIG, SESSION_SETTINGS
from auth_routes import auth_bp
from main_routes import main_bp

def create_main():
    main = Flask(__name__, template_folder="templates", static_folder="static")

    # basic config
    main.secret_key = SECRET_KEY
    main.config["DB_CONFIG"] = DB_CONFIG
    for k, v in SESSION_SETTINGS.items():
        main.config[k] = v

    # register blueprints
    main.register_blueprint(main_bp)
    main.register_blueprint(auth_bp)
    #main.register_blueprint(main_bp)

    return main

main = create_main()

if __name__ == "__main__":
    main.run(debug=True)