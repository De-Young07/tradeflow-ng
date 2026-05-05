# COMPLETE FILE: src/db_adapter.py
# Replace the entire file with this content

"""
TradeFlow NG — Database Adapter
Optimized for Supabase PostgreSQL with SQLite fallback.

Set DATABASE_URL environment variable:
  - Not set / "sqlite"  → local SQLite
  - postgresql://...    → PostgreSQL/Supabase
"""

import os
import sqlite3
import pandas as pd
from contextlib import contextmanager
import re

# ── Detect environment ────────────────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite")
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
# SQL TRANSLATION — SQLite → PostgreSQL
# ════════════════════════════════════════════════════════��═══════════════════════

def _translate(sql):
    """
    Translate SQLite syntax to PostgreSQL.
    Handles: placeholders, date functions, booleans, COALESCE, aliases, types, and HAVING clauses.
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
    for sqlite_pat, pg_pat in date_patterns:
        s = s.replace(sqlite_pat, pg_pat)

    # 3. Boolean columns — bare AND qualified
    bool_cols = [
        "is_active", "is_hub", "is_outlier", "is_confirmed",
        "is_shock_flagged", "is_backhaul", "is_perishable", "missing_cost_flag",
    ]
    qualified_prefixes = ["f.", "r.", "ao.", "c.", "cp.", "s.", "corr.", "co.", "tc.", "t."]
    all_cols = bool_cols + [f"{p}{col}" for p in qualified_prefixes for col in bool_cols]

    for col in all_cols:
        s = s.replace(f"{col} = 1", f"{col} = TRUE")
        s = s.replace(f"{col}=1", f"{col} = TRUE")
        s = s.replace(f"{col} = 0", f"{col} = FALSE")
        s = s.replace(f"{col}=0", f"{col} = FALSE")
        s = s.replace(f"{col} = '1'", f"{col} = TRUE")
        s = s.replace(f"{col} = '0'", f"{col} = FALSE")
        s = s.replace(f"THEN 1 ELSE 0 END AS {col}", f"THEN TRUE ELSE FALSE END AS {col}")

    # 4. COALESCE boolean fixes
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

    # 6. Single-quoted column aliases → double-quoted
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

    # 9. PostgreSQL case sensitivity: wrap HAVING column aliases in quotes
    # Pattern: HAVING ColumnName IS NOT NULL → HAVING "ColumnName" IS NOT NULL
    s = re.sub(
        r"HAVING\s+([A-Z]\w+)\s+(IS\s+NOT\s+NULL)",
        lambda m: f'HAVING "{m.group(1)}" {m.group(2)}',
        s,
        flags=re.IGNORECASE
    )

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
                f"Failed to connect to Supabase. "
                f"Check DATABASE_URL in secrets. Error: {e}"
            )
    else:
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


# ════════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ════════════════════════════════════════════════════════════════════════════════

def query(sql, params=()):
    """Execute SELECT and return DataFrame."""
    if IS_POSTGRES:
        sql_pg = _translate(sql)
        conn = get_connection()
        try:
            return pd.read_sql(sql_pg, conn, params=params if params else None)
        except Exception as e:
            raise Exception(f"PostgreSQL query failed: {e}\nSQL: {sql_pg}")
        finally:
            conn.close()
    else:
        conn = get_connection()
        try:
            return pd.read_sql(sql, conn, params=params)
        finally:
            conn.close()


def execute(sql, params=()):
    """Execute INSERT/UPDATE/DELETE."""
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
