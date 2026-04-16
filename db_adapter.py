"""
TradeFlow NG — Database Adapter
Handles both SQLite (local development) and PostgreSQL (production/cloud).

Set environment variable DATABASE_URL to switch:
  - Not set / "sqlite"  → uses local SQLite
  - postgresql://...    → uses PostgreSQL

On Streamlit Cloud, set DATABASE_URL in st.secrets or environment variables.
"""

import os
import sqlite3
import pandas as pd
from contextlib import contextmanager

# ── Detect environment ─────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite")
SQLITE_PATH  = r"C:\Users\USER\Projects\TradeFlow\data\tradeflow.db"
IS_POSTGRES  = DATABASE_URL.startswith("postgresql") or \
               DATABASE_URL.startswith("postgres")

# ── PostgreSQL driver (only imported if needed) ────────────
if IS_POSTGRES:
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError:
        raise ImportError(
            "psycopg2 not installed. Run: pip install psycopg2-binary"
        )


# ══════════════════════════════════════════════════════════
# CONNECTION
# ══════════════════════════════════════════════════════════

def get_connection():
    """Return a live database connection — SQLite or PostgreSQL."""
    if IS_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    else:
        conn = sqlite3.connect(SQLITE_PATH, timeout=15)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row
        return conn


@contextmanager
def get_db():
    """Context manager for safe connection handling."""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def query(sql, params=()):
    """Execute a SELECT and return a DataFrame. Works on both backends."""
    if IS_POSTGRES:
        # PostgreSQL uses %s placeholders, SQLite uses ?
        sql_pg = sql.replace("?", "%s")
        # Fix SQLite-specific functions
        sql_pg = sql_pg.replace("DATE('now')", "CURRENT_DATE")
        sql_pg = sql_pg.replace("DATE('now',", "CURRENT_DATE +")
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
    """Execute INSERT/UPDATE/DELETE. Works on both backends."""
    if IS_POSTGRES:
        sql_pg = sql.replace("?", "%s")
        sql_pg = sql_pg.replace("DATE('now')", "CURRENT_DATE")
        sql_pg = sql_pg.replace("INSERT OR IGNORE", "INSERT")
        sql_pg = sql_pg.replace("INSERT OR REPLACE", "INSERT")
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_pg, params if params else None)
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
