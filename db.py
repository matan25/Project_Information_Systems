import mysql.connector
from flask import current_app


def get_db_connection():
    """
    Create and return a new MySQL connection using DB_CONFIG from main.config.
    Must be called inside an application context.
    """
    db_config = current_app.config["DB_CONFIG"]
    return mysql.connector.connect(**db_config)