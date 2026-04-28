"""
TradeFlow NG — Database Adapter
Handles both SQLite (local) and PostgreSQL (cloud).

Switch by setting DATABASE_URL environment variable:
  - Not set / "sqlite"  → local SQLite
  - postgresql://...    → PostgreSQL (Supabase)

On Streamlit Cloud, set DATABASE_URL in st.secrets [database] section.
"""

import os
import sqlite3
import pandas as pd
from contextlib import contextmanager

# ── Detect environment ─────────────────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite")
SQLITE_PATH  = r"C:\Users\USER\Projects\TradeFlow\data\tradeflow.db"
IS_POSTGRES  = DATABASE_URL.startswith("postgresql") or \
               DATABASE_URL.startswith("postgres")

if IS_POSTGRES:
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError:
        raise ImportError("psycopg2 not installed. Run: pip install psycopg2-binary")


# ══════════════════════════════════════════════════════════════════════════════
# SQL TRANSLATION — SQLite syntax → PostgreSQL syntax
# Applied to every query before it reaches PostgreSQL.
# ══════════════════════════════════════════════════════════════════════════════

def _translate(sql):
    """
    Translate SQLite-specific SQL syntax to PostgreSQL.
    Covers: placeholders, date functions, boolean literals,
    string concat, INSERT variants, and type casts.
    """
    s = sql

    # 1. Placeholders
    s = s.replace("?", "%s")

    # 2. Date functions — with space variants and without
    s = s.replace("DATE('now', '-56 days')", "(CURRENT_DATE - INTERVAL '56 days')")
    s = s.replace("DATE('now', '-30 days')", "(CURRENT_DATE - INTERVAL '30 days')")
    s = s.replace("DATE('now', '-14 days')", "(CURRENT_DATE - INTERVAL '14 days')")
    s = s.replace("DATE('now', '-7 days')",  "(CURRENT_DATE - INTERVAL '7 days')")
    s = s.replace("DATE('now', '-1 day')",   "(CURRENT_DATE - INTERVAL '1 day')")
    s = s.replace("DATE('now', '+1 day')",   "(CURRENT_DATE + INTERVAL '1 day')")
    s = s.replace("DATE('now', '+7 days')",  "(CURRENT_DATE + INTERVAL '7 days')")
    s = s.replace("DATE('now','-56 days')",  "(CURRENT_DATE - INTERVAL '56 days')")
    s = s.replace("DATE('now','-30 days')",  "(CURRENT_DATE - INTERVAL '30 days')")
    s = s.replace("DATE('now','-14 days')",  "(CURRENT_DATE - INTERVAL '14 days')")
    s = s.replace("DATE('now','-7 days')",   "(CURRENT_DATE - INTERVAL '7 days')")
    s = s.replace("DATE('now','-1 day')",    "(CURRENT_DATE - INTERVAL '1 day')")
    s = s.replace("DATE('now','+1 day')",    "(CURRENT_DATE + INTERVAL '1 day')")
    s = s.replace("DATE('now','+7 days')",   "(CURRENT_DATE + INTERVAL '7 days')")
    s = s.replace("DATE('now')",             "CURRENT_DATE")

    # 3. Boolean columns — SQLite uses 0/1, PostgreSQL uses TRUE/FALSE
    #    Must use word boundaries to avoid partial replacements.
    #    Order matters: longer patterns first.
    bool_cols = [
        "is_active", "is_hub", "is_outlier", "is_confirmed",
        "is_shock_flagged", "is_backhaul", "is_perishable",
        "missing_cost_flag", "corr.is_active",
    ]
    for col in bool_cols:
        s = s.replace(f"{col} = 1",   f"{col} = TRUE")
        s = s.replace(f"{col}=1",     f"{col} = TRUE")
        s = s.replace(f"{col} = 0",   f"{col} = FALSE")
        s = s.replace(f"{col}=0",     f"{col} = FALSE")
        s = s.replace(f"{col} = '1'", f"{col} = TRUE")
        s = s.replace(f"{col} = '0'", f"{col} = FALSE")

    # 4. COALESCE with integer defaults for boolean columns
    for col in bool_cols:
        s = s.replace(f"COALESCE({col}, 0)",   f"COALESCE({col}, FALSE)")
        s = s.replace(f"COALESCE({col}, 1)",   f"COALESCE({col}, TRUE)")

    # 5. String concatenation — SQLite || works but needs explicit cast
    #    when mixing integer columns
    s = s.replace(
        "state_id || commodity_id",
        "CAST(state_id AS TEXT) || CAST(commodity_id AS TEXT)"
    )
    s = s.replace(
        "cp.state_id || cp.commodity_id",
        "CAST(cp.state_id AS TEXT) || CAST(cp.commodity_id AS TEXT)"
    )

    # 6. INSERT variants
    s = s.replace("INSERT OR IGNORE INTO", "INSERT INTO")
    s = s.replace("INSERT OR REPLACE INTO", "INSERT INTO")
    s = s.replace("INSERT OR IGNORE",       "INSERT")
    s = s.replace("INSERT OR REPLACE",      "INSERT")

    # 7. SQLite-specific type affinity casts that break PostgreSQL
    s = s.replace("CAST(is_active AS INTEGER)",       "is_active::int")
    s = s.replace("CAST(is_shock_flagged AS INTEGER)", "is_shock_flagged::int")
    s = s.replace("CAST(is_backhaul AS INTEGER)",      "is_backhaul::int")

    return s


# ══════════════════════════════════════════════════════════════════════════════
# CONNECTION
# ══════════════════════════════════════════════════════════════════════════════

def get_connection():
    """Return a live database connection."""
    if IS_POSTGRES:
        return psycopg2.connect(DATABASE_URL)
    else:
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


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════════

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
    return "PostgreSQL" if IS_POSTGRES else "SQLite"                cur.execute(sql_pg, params if params else None)
    else:
        with get_db() as conn:
            conn.execute(sql, params)


def executemany(sql, params_list):
    """Execute batch INSERT. Works on both backends."""
    if IS_POSTGRES:
        sql_pg = sql.replace("?", "%s")
        sql_pg = sql_pg.replace("INSERT OR IGNORE", "INSERT")
        with get_db() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, sql_pg, params_list)
    else:
        with get_db() as conn:
            conn.executemany(sql, params_list)


def is_postgres():
    return IS_POSTGRES


def backend_name():
    return "PostgreSQL" if IS_POSTGRES else "SQLite"
