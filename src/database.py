# src/database.py
"""
TradeFlow NG — Database Initializer
For SQLite only. PostgreSQL/Supabase schema is set up independently.
"""

import sqlite3
import os

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite")
IS_POSTGRES = DATABASE_URL.startswith("postgresql")

from db_adapter import query, execute, get_connection

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "tradeflow.db")
SQL_PATH = os.path.join(BASE_DIR, "tradeflow_schema.sql")


def init_database():
    """Initialize SQLite database. PostgreSQL/Supabase is pre-configured."""
    if IS_POSTGRES:
        print("✅ Using PostgreSQL/Supabase — schema initialization skipped.")
        print("   Ensure schema_postgresql.sql has been run on your Supabase database.")
        return

    print(f"Initializing SQLite database at: {DB_PATH}")

    # Create data directory if it doesn't exist
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")

    try:
        with open(SQL_PATH, "r") as f:
            sql_script = f.read()

        conn.executescript(sql_script)
        conn.commit()
        print("✅ SQLite database initialized successfully.")
        print("   Tables created with seed data.")
    except Exception as e:
        print(f"❌ Error initializing database: {e}")
        raise
    finally:
        conn.close()


def test_connection():
    """Verify tables exist and seed data loaded correctly."""
    if IS_POSTGRES:
        print("✅ PostgreSQL connection test:")
        try:
            result = query("SELECT COUNT(*) as count FROM states")
            count = result.iloc[0]["count"] if not result.empty else 0
            print(f"   States table: {count} records")
            return True
        except Exception as e:
            print(f"❌ PostgreSQL connection failed: {e}")
            return False
    else:
        conn = get_connection()
        cursor = conn.cursor()

        tables = [
            "states", "markets", "commodities", "agents",
            "raw_submissions", "cleaned_prices", "forecasts",
            "optimization_runs", "optimization_recommendations",
            "actual_outcomes", "pipeline_logs"
        ]

        print("\n--- SQLite DATABASE VERIFICATION ---")
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  {table:<35} → {count} rows")
            except Exception as e:
                print(f"  {table:<35} → ❌ Error: {e}")

        conn.close()
        print("-----------------------------------\n")


if __name__ == "__main__":
    init_database()
    test_connection()
