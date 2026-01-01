import os

# Secret key for Flask session
SECRET_KEY = os.environ.get("FLYTAU_SECRET_KEY", "dev-secret-change-me")

# Local MySQL connection (Workbench: user=root, password=root)
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "flytau",
}

# Session security settings
SESSION_SETTINGS = {
    "SESSION_COOKIE_HTTPONLY": True,
    "SESSION_COOKIE_SAMESITE": "Lax",
    # Turn on later when using HTTPS:
    # "SESSION_COOKIE_SECURE": True,
}