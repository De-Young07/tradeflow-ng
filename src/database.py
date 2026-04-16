"""
TradeFlow NG — Database Initializer
"""

import sqlite3
import os

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite")
IS_POSTGRES  = DATABASE_URL.startswith("postgresql")

# Use db_adapter instead of direct sqlite3 calls
from db_adapter import query, execute, executemany, get_connection
# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH  = os.path.join(BASE_DIR, "data", "tradeflow.db")
SQL_PATH = os.path.join(BASE_DIR, "tradeflow_schema.sql")

def init_database():
    """Create the database and all tables from schema file."""
    print(f"Creating database at: {DB_PATH}")

    # Create data directory if it doesn't exist
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")  # Enforce foreign keys in SQLite

    with open(SQL_PATH, "r") as f:
        sql_script = f.read()

    conn.executescript(sql_script)
    conn.commit()
    conn.close()
    print("Database initialized successfully.")
    print("Tables created with seed data.")

def get_connection():
    """Return a live database connection. Use this everywhere."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row  # Returns rows as dicts, not tuples
    return conn

def test_connection():
    """Verify tables exist and seed data loaded correctly."""
    conn = get_connection()
    cursor = conn.cursor()

    tables = ["states", "markets", "commodities", "agents",
              "raw_submissions", "cleaned_prices", "forecasts",
              "optimization_runs", "optimization_recommendations",
              "actual_outcomes", "pipeline_logs"]

    print("\n--- DATABASE VERIFICATION ---")
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table:<35} → {count} rows")

    conn.close()
    print("----------------------------\n")

if __name__ == "__main__":
    init_database()
    test_connection()