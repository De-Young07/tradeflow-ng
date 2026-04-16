"""
TradeFlow NG — KoboToolbox Connector
Pulls weekly submissions from KoboToolbox API
and inserts into raw_submissions table.

STATUS: Ready but dormant until Kobo form is finalized and config.ini is set up.

SETUP INSTRUCTIONS:
    1. Log into KoboToolbox → Account Settings → API Key → Copy it
    2. Find your form's Asset UID from the form URL
    3. Create a file: TradeFlow/config.ini with contents:

        [kobo]
        api_token = your_token_here
        asset_uid = your_asset_uid_here
        base_url  = https://kf.kobotoolbox.org

    4. Run this script: python src/kobo_connector.py
"""

import sqlite3
import requests
import pandas as pd
import configparser
import os
from datetime import datetime, date, timedelta

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite")
IS_POSTGRES  = DATABASE_URL.startswith("postgresql")

# Use db_adapter instead of direct sqlite3 calls
from db_adapter import query, execute, executemany, get_connection

# ── Path config ────────────────────────────────────────────
DB_PATH     = r"C:\Users\USER\Projects\TradeFlow\data\tradeflow.db"
CONFIG_PATH = r"C:\Users\USER\Projects\TradeFlow\config.ini"



# ══════════════════════════════════════════════════════════
# 1. LOAD CONFIG
# ══════════════════════════════════════════════════════════

def load_config():
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(
            f"Config file not found at {CONFIG_PATH}\n"
            "Create it with your Kobo API token and asset UID first.\n"
            "See the docstring at the top of this file for instructions."
        )
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)
    return config["kobo"]


# ══════════════════════════════════════════════════════════
# 2. KOBO API FUNCTIONS
# ══════════════════════════════════════════════════════════

def fetch_kobo_submissions(since_date=None, limit=1000):
    """
    Pull submissions from KoboToolbox API.
    since_date: only pull submissions after this date (YYYY-MM-DD)
    """
    cfg = load_config()

    headers = {
        "Authorization": f"Token {cfg['api_token']}",
        "Content-Type":  "application/json",
    }

    url = f"{cfg['base_url']}/api/v2/assets/{cfg['asset_uid']}/data/"
    params = {"format": "json", "limit": limit}

    if since_date:
        params["query"] = f'{{"_submission_time":{{"$gt":"{since_date}"}}}}'

    print(f"  Fetching from Kobo: {url}")
    response = requests.get(url, headers=headers, params=params, timeout=30)

    if response.status_code != 200:
        raise ConnectionError(
            f"Kobo API error {response.status_code}: {response.text}"
        )

    data = response.json()
    submissions = data.get("results", [])
    print(f"  Retrieved {len(submissions)} submissions from Kobo.")
    return submissions


def get_last_pull_date():
    """Check pipeline_logs to find the last successful Kobo pull date."""
    conn = get_connection()
    result = conn.execute("""
        SELECT MAX(run_at) FROM pipeline_logs
        WHERE run_type = 'Kobo Ingestion' AND status = 'Success'
    """).fetchone()
    conn.close()
    last_run = result[0]
    if last_run:
        return last_run[:10]  # Return date portion only
    return None


# ══════════════════════════════════════════════════════════
# 3. FIELD MAPPING
#    Map your Kobo form field names to database columns.
#    UPDATE THESE when your form is finalized.
# ══════════════════════════════════════════════════════════

# These must match the exact field names in your Kobo form
FIELD_MAP = {
    "state":              "group_location/state_name",    # Update to your field name
    "market":             "group_location/market_name",
    "commodity":          "group_price/commodity_name",
    "price":              "group_price/price_per_unit",
    "unit":               "group_price/unit_of_measure",
    "quantity":           "group_price/quantity_available",
    "agent_phone":        "agent_phone_number",
    "submission_id":      "_id",
    "submission_time":    "_submission_time",
}

def parse_submission(submission, lookup_maps):
    """
    Parse a single Kobo submission dict into a database-ready record.
    Returns None if the record can't be parsed.
    """

    def get_field(key):
        field_name = FIELD_MAP.get(key, key)
        return submission.get(field_name)

    try:
        state_name     = str(get_field("state") or "").strip().lower()
        commodity_name = str(get_field("commodity") or "").strip().lower()
        price_raw      = get_field("price")
        submission_id  = str(get_field("submission_id"))
        submit_time    = get_field("submission_time")

        # Required field checks
        if not state_name or not commodity_name or price_raw is None:
            return None

        state_id     = lookup_maps["states"].get(state_name)
        commodity_id = lookup_maps["commodities"].get(commodity_name)

        if not state_id or not commodity_id:
            print(f"    ⚠ Unrecognized state '{state_name}' or commodity '{commodity_name}'")
            return None

        market_name = str(get_field("market") or "").strip().lower()
        market_id   = lookup_maps["markets"].get(market_name)

        agent_phone = str(get_field("agent_phone") or "").strip()
        agent_id    = lookup_maps["agents"].get(agent_phone)

        submission_date = submit_time[:10] if submit_time else str(date.today())

        return {
            "kobo_submission_id": submission_id,
            "agent_id":           agent_id,
            "state_id":           state_id,
            "market_id":          market_id,
            "commodity_id":       commodity_id,
            "reported_price":     float(price_raw),
            "reported_unit":      get_field("unit"),
            "quantity_available": float(get_field("quantity")) if get_field("quantity") else None,
            "submission_date":    submission_date,
            "source_channel":     "Kobo",
            "raw_json":           str(submission),
        }

    except Exception as e:
        print(f"    ✗ Failed to parse submission: {e}")
        return None


# ══════════════════════════════════════════════════════════
# 4. INSERT INTO RAW_SUBMISSIONS
# ══════════════════════════════════════════════════════════

def insert_raw_submissions(records):
    """Insert parsed records into raw_submissions. Skip duplicates."""
    conn = get_connection()
    inserted = 0
    skipped  = 0

    for record in records:
        if record is None:
            skipped += 1
            continue
        try:
            conn.execute("""
                INSERT OR IGNORE INTO raw_submissions (
                    kobo_submission_id, agent_id, state_id, market_id,
                    commodity_id, reported_price, reported_unit,
                    quantity_available, submission_date, source_channel, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record["kobo_submission_id"],
                record["agent_id"],
                record["state_id"],
                record["market_id"],
                record["commodity_id"],
                record["reported_price"],
                record["reported_unit"],
                record["quantity_available"],
                record["submission_date"],
                record["source_channel"],
                record["raw_json"],
            ))
            inserted += 1
        except Exception as e:
            skipped += 1
            print(f"    Skipped: {e}")

    conn.commit()
    conn.close()
    return inserted, skipped


# ══════════════════════════════════════════════════════════
# 5. LOOKUP MAP BUILDER (reused from csv_uploader)
# ══════════════════════════════════════════════════════════

def build_lookup_maps():
    conn = get_connection()
    states      = pd.read_sql("SELECT id, LOWER(name) as name FROM states", conn)
    markets     = pd.read_sql("SELECT id, LOWER(name) as name FROM markets", conn)
    commodities = pd.read_sql("SELECT id, LOWER(name) as name FROM commodities", conn)
    agents      = pd.read_sql("SELECT id, phone FROM agents", conn)
    conn.close()
    return {
        "states":      dict(zip(states["name"],      states["id"])),
        "markets":     dict(zip(markets["name"],     markets["id"])),
        "commodities": dict(zip(commodities["name"], commodities["id"])),
        "agents":      dict(zip(agents["phone"],     agents["id"])),
    }


# ══════════════════════════════════════════════════════════
# 6. PIPELINE LOG
# ══════════════════════════════════════════════════════════

def log_run(status, records_in, records_out, error=None, duration=None):
    conn = get_connection()
    conn.execute("""
        INSERT INTO pipeline_logs
        (run_type, status, records_in, records_out, error_message, duration_secs)
        VALUES (?, ?, ?, ?, ?, ?)
    """, ("Kobo Ingestion", status, records_in, records_out, error, duration))
    conn.commit()
    conn.close()


# ══════════════════════════════════════════════════════════
# 7. MAIN RUNNER
# ══════════════════════════════════════════════════════════

def run_kobo_ingestion(since_date=None):
    """
    Full Kobo ingestion pipeline.
    Pulls new submissions → parses → inserts → triggers cleaning.
    """
    start = datetime.now()

    print(f"\n{'='*50}")
    print(f"  KOBO INGESTION — {start.strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*50}\n")

    try:
        # Auto-detect since_date from last successful run
        if since_date is None:
            since_date = get_last_pull_date()
            if since_date:
                print(f"  Pulling submissions since last run: {since_date}")
            else:
                print("  No previous run found — pulling all submissions.")

        # Fetch
        print("[1/4] Fetching from Kobo API...")
        submissions = fetch_kobo_submissions(since_date=since_date)

        # Build lookup maps
        print("[2/4] Building lookup maps...")
        maps = build_lookup_maps()

        # Parse
        print("[3/4] Parsing submissions...")
        records = [parse_submission(s, maps) for s in submissions]
        valid   = [r for r in records if r is not None]
        print(f"  Parsed: {len(valid)} valid, {len(records)-len(valid)} unparseable.")

        # Insert
        print("[4/4] Inserting into database...")
        inserted, skipped = insert_raw_submissions(valid)

        duration = (datetime.now() - start).total_seconds()
        log_run("Success", len(submissions), inserted, duration=round(duration, 2))

        print(f"\n  ✓ Kobo ingestion complete in {round(duration, 1)}s")
        print(f"  Fetched:  {len(submissions)}")
        print(f"  Inserted: {inserted}")
        print(f"  Skipped:  {skipped}")
        print(f"{'='*50}\n")

        # Trigger cleaning
        if inserted > 0:
            from cleaning import run_cleaning_pipeline
            run_cleaning_pipeline(source="raw")

    except FileNotFoundError as e:
        print(f"\n  ⚠ Config not found — Kobo connector not yet activated.")
        print(f"  {e}\n")
        log_run("Skipped", 0, 0, error="Config not found")

    except Exception as e:
        duration = (datetime.now() - start).total_seconds()
        log_run("Failed", 0, 0, error=str(e), duration=round(duration, 2))
        print(f"\n  ✗ Kobo ingestion failed: {e}\n")
        raise


if __name__ == "__main__":
    run_kobo_ingestion()
