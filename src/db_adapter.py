# src/db_adapter.py
"""
TradeFlow NG — Database Adapter
Optimized for Supabase PostgreSQL with SQLite fallback

Switch by setting DATABASE_URL environment variable:
  - Not set / "sqlite"  → local SQLite
  - postgresql://...    → PostgreSQL (Supabase)

On Streamlit Cloud, set DATABASE_URL in st.secrets [database] section.
"""

import os
import sqlite3
import pandas as pd
from contextlib import contextmanager
import re

# ── Detect environment ────────────────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite")

# Dynamic SQLite path — works on Windows, Mac, Linux, and Streamlit Cloud
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQLITE_PATH = os.path.join(BASE_DIR, "data", "tradeflow.db")

IS_POSTGRES = DATABASE_URL.startswith("postgresql") or DATABASE_URL.startswith("postgres")

if IS_POSTGRES:
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError:
        raise ImportError("psycopg2 not installed. Run: pip install psycopg2-binary")


# ════════════════════════════════════════════════════════════════════════════════
# SQL TRANSLATION — SQLite syntax → PostgreSQL syntax
# Applied to every query before it reaches PostgreSQL/Supabase.
# ════════════════════════════════════════════════════════════════════════════════

def _translate(sql):
    """
    Translate SQLite-specific SQL syntax to PostgreSQL.
    Comprehensive fixes for:
    1. Placeholders (? → %s)
    2. Date functions
    3. Boolean literals (0/1 → FALSE/TRUE) for bare AND qualified columns
    4. COALESCE boolean/integer mismatches
    5. Single-quoted column aliases → double-quoted
    6. String concatenation
    7. INSERT variants
    8. Type casts
    """
    s = sql

    # 1. Placeholders
    s = s.replace("?", "%s")

    # 2. Date functions
    date_patterns = [
        ("DATE('now', '-56 days')", "(CURRENT_DATE - INTERVAL '56 days')"),
        ("DATE('now', '-30 days')", "(CURRENT_DATE - INTERVAL '30 days')"),
        ("DATE('now', '-14 days')", "(CURRENT_DATE - INTERVAL '14 days')"),
        ("DATE('now', '-7 days')", "(CURRENT_DATE - INTERVAL '7 days')"),
        ("DATE('now', '-1 day')", "(CURRENT_DATE - INTERVAL '1 day')"),
        ("DATE('now', '+1 day')", "(CURRENT_DATE + INTERVAL '1 day')"),
        ("DATE('now', '+7 days')", "(CURRENT_DATE + INTERVAL '7 days')"),
        ("DATE('now','-56 days')", "(CURRENT_DATE - INTERVAL '56 days')"),
        ("DATE('now','-30 days')", "(CURRENT_DATE - INTERVAL '30 days')"),
        ("DATE('now','-14 days')", "(CURRENT_DATE - INTERVAL '14 days')"),
        ("DATE('now','-7 days')", "(CURRENT_DATE - INTERVAL '7 days')"),
        ("DATE('now','-1 day')", "(CURRENT_DATE - INTERVAL '1 day')"),
        ("DATE('now','+1 day')", "(CURRENT_DATE + INTERVAL '1 day')"),
        ("DATE('now','+7 days')", "(CURRENT_DATE + INTERVAL '7 days')"),
        ("DATE('now')", "CURRENT_DATE"),
    ]
    for sqlite_pattern, pg_pattern in date_patterns:
        s = s.replace(sqlite_pattern, pg_pattern)

    # 3. Boolean columns — handle BOTH bare names AND qualified names
    bool_cols = [
        "is_active", "is_hub", "is_outlier", "is_confirmed",
        "is_shock_flagged", "is_backhaul", "is_perishable",
        "missing_cost_flag",
    ]
    
    # Add qualified versions for common table aliases
    qualified_prefixes = ["f.", "r.", "ao.", "c.", "cp.", "s.", "corr.", "co.", "tc.", "t."]
    qualified_cols = [f"{prefix}{col}" for prefix in qualified_prefixes for col in bool_cols]
    all_cols = bool_cols + qualified_cols

    # Replace boolean literals (0/1 → FALSE/TRUE)
    for col in all_cols:
        s = s.replace(f"{col} = 1", f"{col} = TRUE")
        s = s.replace(f"{col}=1", f"{col} = TRUE")
        s = s.replace(f"{col} = 0", f"{col} = FALSE")
        s = s.replace(f"{col}=0", f"{col} = FALSE")
        s = s.replace(f"{col} = '1'", f"{col} = TRUE")
        s = s.replace(f"{col} = '0'", f"{col} = FALSE")
        # Handle CASE statements
        s = s.replace(f"THEN 1 ELSE 0 END AS {col}", f"THEN TRUE ELSE FALSE END AS {col}")

    # 4. COALESCE with boolean defaults — handle ALL column variations
    for col in all_cols:
        s = s.replace(f"COALESCE({col}, 0)", f"COALESCE({col}, FALSE)")
        s = s.replace(f"COALESCE({col}, 1)", f"COALESCE({col}, TRUE)")

    # 5. String concatenation
    s = s.replace(
        "state_id || commodity_id",
        "CAST(state_id AS TEXT) || CAST(commodity_id AS TEXT)"
    )
    s = s.replace(
        "cp.state_id || cp.commodity_id",
        "CAST(cp.state_id AS TEXT) || CAST(cp.commodity_id AS TEXT)"
    )

    # 6. Convert single-quoted column aliases to double-quoted identifiers
    #    Pattern: AS 'Column Name' → AS "Column Name"
    s = re.sub(r"\bAS\s+'([^']+)'", r'AS "\1"', s)

    # 7. INSERT variants
    s = s.replace("INSERT OR IGNORE INTO", "INSERT INTO")
    s = s.replace("INSERT OR REPLACE INTO", "INSERT INTO")
    s = s.replace("INSERT OR IGNORE", "INSERT")
    s = s.replace("INSERT OR REPLACE", "INSERT")

    # 8. Type casts
    s = s.replace("CAST(is_active AS INTEGER)", "is_active::int")
    s = s.replace("CAST(is_shock_flagged AS INTEGER)", "is_shock_flagged::int")
    s = s.replace("CAST(is_backhaul AS INTEGER)", "is_backhaul::int")

    return s


# ════════════════════════════════════════════════════════════════════════════════
# CONNECTION
# ════════════════════════════════════════════════════════════════════════════════

def get_connection():
    """Return a live database connection."""
    if IS_POSTGRES:
        try:
            return psycopg2.connect(DATABASE_URL)
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to Supabase PostgreSQL. "
                f"Check DATABASE_URL in secrets. Error: {e}"
            )
    else:
        # Ensure data directory exists
        os.makedirs(os.path.dirname(SQLITE_PATH), exist_ok=True)
        conn = sqlite3.connect(SQLITE_PATH, timeout=15)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row
        return conn


@contextmanager
def get_db():
    """Context manager — commits on success, rolls back on error."""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ══���═════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ════════════════════════════════════════════════════════════════════════════════

def query(sql, params=()):
    """Execute a SELECT and return a DataFrame."""
    if IS_POSTGRES:
        sql_pg = _translate(sql)
        conn = get_connection()
        try:
            return pd.read_sql(sql_pg, conn, params=params if params else None)
        finally:
            conn.close()
    else:
        conn = get_connection()
        try:
            return pd.read_sql(sql, conn, params=params)
        finally:
            conn.close()


def execute(sql, params=()):
    """Execute INSERT / UPDATE / DELETE."""
    if IS_POSTGRES:
        sql_pg = _translate(sql)
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_pg, params if params else None)
    else:
        with get_db() as conn:
            conn.execute(sql, params)


def executemany(sql, params_list):
    """Batch INSERT."""
    if IS_POSTGRES:
        sql_pg = _translate(sql)
        with get_db() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, sql_pg, params_list)
    else:
        with get_db() as conn:
            conn.executemany(sql, params_list)


def is_postgres():
    return IS_POSTGRES


def backend_name():
    return "PostgreSQL (Supabase)" if IS_POSTGRES else "SQLite"
