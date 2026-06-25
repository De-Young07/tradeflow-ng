# COMPLETE FILE: src/database.py
# Replace the entire file with this content

"""
TradeFlow NG — Database Initializer
Handles PostgreSQL/Supabase schema setup and SQLite initialization.
"""

import os
import sqlite3

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite")
IS_POSTGRES = DATABASE_URL.startswith("postgresql")

if IS_POSTGRES:
    try:
        import psycopg2
    except ImportError:
        raise ImportError("psycopg2 not installed. Run: pip install psycopg2-binary")

from db_adapter import query, execute, get_connection

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "tradeflow.db")
SQLITE_SCHEMA_PATH = os.path.join(BASE_DIR, "tradeflow_schema.sql")


def init_database():
    """Initialize database schema (SQLite or PostgreSQL)."""
    if IS_POSTGRES:
        _init_postgres()
    else:
        _init_sqlite()


def _init_postgres():
    """Initialize PostgreSQL/Supabase schema."""
    print("🔍 Detecting PostgreSQL/Supabase schema...")

    try:
        # Check if tables exist
        result = query("SELECT COUNT(*) as table_count FROM information_schema.tables WHERE table_schema = 'public'")
        table_count = result.iloc[0]["table_count"] if not result.empty else 0

        if table_count > 0:
            print(f"✅ PostgreSQL/Supabase schema already initialized ({table_count} tables found).")
            return

        print("❌ Schema not found. Please run schema_postgresql.sql manually:")
        print("   1. Go to Supabase → SQL Editor")
        print("   2. Copy contents of schema_postgresql.sql")
        print("   3. Paste and execute")
        print("   4. Refresh your app")

    except Exception as e:
        print(f"⚠️  Could not verify schema: {e}")
        print("   Ensure schema_postgresql.sql has been run on your Supabase database.")


def _init_sqlite():
    """Initialize SQLite database."""
    print(f"Initializing SQLite at: {DB_PATH}")

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")

    try:
        with open(SQLITE_SCHEMA_PATH, "r") as f:
            sql_script = f.read()

        conn.executescript(sql_script)
        conn.commit()
        print("✅ SQLite database initialized with seed data.")
    except Exception as e:
        print(f"❌ Error initializing SQLite: {e}")
        raise
    finally:
        conn.close()


def test_connection():
    """Verify database connection and tables."""
    if IS_POSTGRES:
        print("✅ Testing PostgreSQL/Supabase connection...")
        try:
            result = query("SELECT COUNT(*) as count FROM states")
            count = result.iloc[0]["count"] if not result.empty else 0
            print(f"   States table: {count} records ✅")
            return True
        except Exception as e:
            print(f"   ❌ Connection failed: {e}")
            return False
    else:
        print("✅ Testing SQLite connection...")
        conn = get_connection()
        cursor = conn.cursor()

        tables = [
            "states", "markets", "commodities", "agents",
            "raw_submissions", "cleaned_prices", "forecasts",
            "optimization_runs", "optimization_recommendations",
            "actual_outcomes", "pipeline_logs"
        ]

        all_ok = True
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"   {table:<35} → {count} rows ✅")
            except Exception as e:
                print(f"   {table:<35} → ❌ {str(e)[:50]}")
                all_ok = False
        cursor.close()
        conn.close()
        return all_ok


from sqlalchemy.ext.asyncio import AsyncSession

async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


if __name__ == "__main__":
    init_database()
    test_connection()
