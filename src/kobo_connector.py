"""
TradeFlow NG — KoboToolbox Connector
Pulls weekly submissions from KoboToolbox API and inserts
into raw_submissions table.

CHANGES FROM PREVIOUS VERSION:
  - All conn.execute() replaced with db_adapter.execute/query
  - Agents looked up by agent_id (TFN-KW-001) not phone number
  - market_id AND state_id both captured per submission
  - log_run uses db_adapter — no direct connection needed

SETUP:
  1. KoboToolbox → Account Settings → API Key → copy it
  2. Get your form's Asset UID from the form URL
  3. Add to Streamlit secrets:
       [kobo]
       api_token = your_token_here
       asset_uid = your_asset_uid_here
       base_url  = https://kf.kobotoolbox.org
  4. OR create config.ini (local development only)
"""

import requests
import pandas as pd
import configparser
import os
import json
from datetime import datetime, date

from db_adapter import query as db_query, execute as db_execute

# ── Config path (local dev only — cloud uses st.secrets) ──
CONFIG_PATH = os.path.join(
    os.path.dirname(__file__), '..', 'config.ini'
)


# ═══════════════════════════════════════════════════════════
# 1. CONFIG LOADER
# ═══════════════════════════════════════════════════════════

def load_config():
    """
    Load Kobo config. Tries st.secrets first (cloud),
    then falls back to config.ini (local).
    """
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and 'kobo' in st.secrets:
            return {
                'api_token': st.secrets['kobo']['api_token'],
                'asset_uid': st.secrets['kobo']['asset_uid'],
                'base_url':  st.secrets['kobo'].get(
                    'base_url', 'https://kf.kobotoolbox.org'
                ),
            }
    except Exception:
        pass

    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(
            f"Kobo config not found at {CONFIG_PATH}\n"
            "Add [kobo] section to config.ini or Streamlit secrets."
        )
    cfg = configparser.ConfigParser()
    cfg.read(CONFIG_PATH)
    return dict(cfg['kobo'])


# ═══════════════════════════════════════════════════════════
# 2. KOBO API
# ═══════════════════════════════════════════════════════════

def fetch_kobo_submissions(since_date=None, limit=1000):
    """
    Pull submissions from KoboToolbox API.
    since_date: only pull after this date (YYYY-MM-DD string)
    """
    cfg = load_config()

    headers = {
        "Authorization": f"Token {cfg['api_token']}",
        "Content-Type":  "application/json",
    }
    url    = f"{cfg['base_url']}/api/v2/assets/{cfg['asset_uid']}/data/"
    params = {"format": "json", "limit": limit}

    if since_date:
        params["query"] = (
            f'{{"_submission_time":{{"$gt":"{since_date}T00:00:00"}}}}'
        )

    print(f"  Fetching from: {url}")
    response = requests.get(url, headers=headers, params=params, timeout=30)

    if response.status_code != 200:
        raise ConnectionError(
            f"Kobo API error {response.status_code}: {response.text}"
        )

    submissions = response.json().get("results", [])
    print(f"  Retrieved {len(submissions)} submissions.")
    return submissions


def get_last_pull_date():
    """Get date of last successful Kobo pull from pipeline_logs."""
    try:
        result = db_query("""
            SELECT MAX(run_at) AS last_run
            FROM   pipeline_logs
            WHERE  run_type = 'Kobo Ingestion'
              AND  status   = 'Success'
        """)
        if not result.empty and result.iloc[0]["last_run"]:
            return str(result.iloc[0]["last_run"])[:10]
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════
# 3. FIELD MAP
#    Left side  = your DB / logical name
#    Right side = exact field name from your Kobo form
#    *** UPDATE RIGHT SIDE after finalising your Kobo form ***
# ═══════════════════════════════════════════════════════════

FIELD_MAP = {
    # Identification
    "agent_id":       "group_identification/agent_id",        # e.g. TFN-KW-001
    "submission_id":  "_id",
    "submission_time":"_submission_time",

    # Location — BOTH market and state
    "state":          "group_location/state_name",
    "market":         "group_location/market_name",
    "gps":            "group_identification/_geolocation",

    # Commodity prices (repeat group — handled separately)
    "commodity":      "group_prices/commodity_name",
    "price":          "group_prices/price_per_unit",
    "unit":           "group_prices/unit_of_measure",
    "quantity":       "group_prices/quantity_available",
    "quality_grade":  "group_prices/quality_grade",
    "availability":   "group_prices/availability_level",

    # Transport
    "road_condition": "group_transport/road_condition",
    "transport_cost": "group_transport/transport_cost_estimate",
    "vehicle_type":   "group_transport/vehicle_type",

    # Observations
    "market_activity":"group_market/market_activity_level",
    "disruptions":    "group_market/disruptions",
    "notes":          "group_observations/additional_notes",
    "photo":          "group_observations/commodity_photo",
    "confidence":     "group_observations/price_confidence",
}


# ═══════════════════════════════════════════════════════════
# 4. LOOKUP MAP BUILDER
#    Agents looked up by agent_id (TFN-KW-001), NOT phone
# ═══════════════════════════════════════════════════════════

def build_lookup_maps():
    """Build name→id dicts for states, markets, commodities, agents."""
    states = db_query(
        "SELECT id, LOWER(name) AS name FROM states"
    )
    markets = db_query(
        "SELECT id, LOWER(name) AS name FROM markets"
    )
    commodities = db_query(
        "SELECT id, LOWER(name) AS name FROM commodities"
    )
    # Agents keyed by their text agent_id (e.g. TFN-KW-001)
    agents = db_query(
        "SELECT id, UPPER(agent_id) AS agent_id FROM agents WHERE is_active = 1"
    )

    return {
        "states":      dict(zip(states["name"],         states["id"])),
        "markets":     dict(zip(markets["name"],         markets["id"])),
        "commodities": dict(zip(commodities["name"],     commodities["id"])),
        "agents":      dict(zip(agents["agent_id"],      agents["id"])),
    }


# ═══════════════════════════════════════════════════════════
# 5. PARSE ONE SUBMISSION
# ═══════════════════════════════════════════════════════════

def _get(submission, logical_key):
    """Get a field value using FIELD_MAP."""
    field_name = FIELD_MAP.get(logical_key, logical_key)
    return submission.get(field_name)


def parse_submission(submission, lookup_maps):
    """
    Parse a single Kobo submission dict into one or more
    database-ready records (one per commodity in the repeat group).

    Returns list of records (empty list if parse fails).
    """
    try:
        # ── Agent lookup by agent_id text ──────────────────
        agent_id_text = str(_get(submission, "agent_id") or "").strip().upper()
        agent_db_id   = lookup_maps["agents"].get(agent_id_text)

        if not agent_db_id:
            print(f"    ⚠ Unknown agent_id: '{agent_id_text}' — skipping.")
            return []

        # ── State ──────────────────────────────────────────
        state_name = str(_get(submission, "state") or "").strip().lower()
        state_id   = lookup_maps["states"].get(state_name)
        if not state_id:
            print(f"    ⚠ Unknown state: '{state_name}'")
            return []

        # ── Market ─────────────────────────────────────────
        market_name = str(_get(submission, "market") or "").strip().lower()
        market_id   = lookup_maps["markets"].get(market_name)
        # market_id can be None if market not yet in DB — we still accept

        # ── Dates ──────────────────────────────────────────
        submit_time     = _get(submission, "submission_time")
        submission_date = str(submit_time)[:10] if submit_time else str(date.today())
        kobo_sub_id     = str(_get(submission, "submission_id") or "")

        # ── Road condition / transport (form-level fields) ─
        road_condition  = _get(submission, "road_condition")
        transport_cost  = _get(submission, "transport_cost")
        market_activity = _get(submission, "market_activity")
        confidence      = _get(submission, "confidence")
        notes           = _get(submission, "notes")

        # ── GPS ────────────────────────────────────────────
        gps = _get(submission, "gps")
        gps_str = None
        if gps and isinstance(gps, list) and len(gps) >= 2:
            gps_str = f"{gps[0]},{gps[1]}"

        # ── Commodity repeat group ─────────────────────────
        # Kobo repeat groups use a key like "group_prices" → list of dicts
        repeat_key = "group_prices"
        commodities_list = submission.get(repeat_key, [])

        # If the form uses flat fields (not repeat group) fall back to single
        if not commodities_list:
            commodity_name = str(_get(submission, "commodity") or "").strip().lower()
            price_raw      = _get(submission, "price")
            if commodity_name and price_raw is not None:
                commodities_list = [{
                    FIELD_MAP["commodity"]: commodity_name,
                    FIELD_MAP["price"]:     price_raw,
                    FIELD_MAP.get("unit",""):     _get(submission,"unit"),
                    FIELD_MAP.get("quantity",""): _get(submission,"quantity"),
                    FIELD_MAP.get("quality_grade",""): _get(submission,"quality_grade"),
                }]

        records = []
        for comm_entry in commodities_list:
            # field names inside repeat group may drop the group prefix
            def cget(k):
                full = FIELD_MAP.get(k, k)
                # Try full path first, then just the leaf
                leaf = full.split("/")[-1]
                return comm_entry.get(full) or comm_entry.get(leaf)

            comm_name = str(cget("commodity") or "").strip().lower()
            price_raw = cget("price")

            if not comm_name or price_raw is None:
                continue

            commodity_id = lookup_maps["commodities"].get(comm_name)
            if not commodity_id:
                print(f"    ⚠ Unknown commodity: '{comm_name}' — skipping row.")
                continue

            try:
                price_val = float(str(price_raw).replace(",", ""))
            except ValueError:
                print(f"    ⚠ Invalid price '{price_raw}' for {comm_name}")
                continue

            records.append({
                "kobo_submission_id": kobo_sub_id + f"_{comm_name[:4]}",
                "agent_id":           agent_db_id,
                "state_id":           state_id,
                "market_id":          market_id,
                "commodity_id":       commodity_id,
                "reported_price":     price_val,
                "reported_unit":      cget("unit"),
                "quantity_available": (
                    float(cget("quantity"))
                    if cget("quantity") else None
                ),
                "quality_grade":      cget("quality_grade"),
                "market_activity":    market_activity,
                "road_condition":     road_condition,
                "transport_cost_est": (
                    float(str(transport_cost).replace(",",""))
                    if transport_cost else None
                ),
                "price_confidence":   confidence,
                "submission_date":    submission_date,
                "gps_coordinates":    gps_str,
                "notes":              notes,
                "source_channel":     "Kobo",
                "raw_json":           json.dumps(submission)[:2000],
            })

        return records

    except Exception as e:
        print(f"    ✗ Parse error: {e}")
        return []


# ═══════════════════════════════════════════════════════════
# 6. INSERT RAW SUBMISSIONS — via db_execute
# ═══════════════════════════════════════════════════════════

def insert_raw_submissions(records):
    """
    Insert parsed records. Skips duplicates gracefully.
    Uses db_adapter.execute — works on both SQLite and PostgreSQL.
    """
    inserted = 0
    skipped  = 0

    for record in records:
        if record is None:
            skipped += 1
            continue
        try:
            db_execute("""
                INSERT INTO raw_submissions (
                    kobo_submission_id,
                    agent_id, state_id, market_id, commodity_id,
                    reported_price, reported_unit,
                    quantity_available, quality_grade,
                    market_activity, road_condition,
                    transport_cost_est, price_confidence,
                    submission_date, gps_coordinates,
                    notes, source_channel, raw_json
                ) VALUES (
                    ?, ?, ?, ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?, ?
                )
            """, (
                record["kobo_submission_id"],
                record["agent_id"],
                record["state_id"],
                record["market_id"],
                record["commodity_id"],
                record["reported_price"],
                record["reported_unit"],
                record["quantity_available"],
                record["quality_grade"],
                record["market_activity"],
                record["road_condition"],
                record["transport_cost_est"],
                record["price_confidence"],
                record["submission_date"],
                record["gps_coordinates"],
                record["notes"],
                record["source_channel"],
                record["raw_json"],
            ))
            inserted += 1
        except Exception as e:
            err = str(e).lower()
            if "unique" in err or "duplicate" in err:
                pass  # Duplicate submission — silently skip
            else:
                print(f"    Skipped record: {e}")
            skipped += 1

    return inserted, skipped


# ═══════════════════════════════════════════════════════════
# 7. PIPELINE LOG — via db_execute
# ═══════════════════════════════════════════════════════════

def log_run(status, records_in=0, records_out=0, error=None, duration=None):
    try:
        db_execute("""
            INSERT INTO pipeline_logs
                (run_type, status, records_in, records_out,
                 error_message, duration_secs)
            VALUES (?, ?, ?, ?, ?, ?)
        """, ("Kobo Ingestion", status, records_in, records_out,
              error, duration))
    except Exception as e:
        print(f"  Warning: could not write to pipeline_logs: {e}")


# ═══════════════════════════════════════════════════════════
# 8. MAIN PIPELINE
# ═══════════════════════════════════════════════════════════

def run_kobo_ingestion(since_date=None):
    """
    Full Kobo → raw_submissions pipeline.
    Agents identified by agent_id text (TFN-KW-001).
    Both market_id and state_id captured per record.
    Triggers cleaning pipeline after insertion.
    """
    start = datetime.now()
    print(f"\n{'='*52}")
    print(f"  KOBO INGESTION — {start.strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*52}\n")

    try:
        if since_date is None:
            since_date = get_last_pull_date()
            if since_date:
                print(f"  Pulling since last run: {since_date}")
            else:
                print("  No previous run — pulling all submissions.")

        print("[1/4] Fetching from Kobo API...")
        submissions = fetch_kobo_submissions(since_date=since_date)

        print("\n[2/4] Building lookup maps...")
        maps = build_lookup_maps()
        print(f"  States: {len(maps['states'])} | "
              f"Markets: {len(maps['markets'])} | "
              f"Commodities: {len(maps['commodities'])} | "
              f"Agents: {len(maps['agents'])}")

        print("\n[3/4] Parsing submissions...")
        all_records = []
        for s in submissions:
            all_records.extend(parse_submission(s, maps))
        print(f"  Parsed: {len(all_records)} commodity records "
              f"from {len(submissions)} submissions.")

        print("\n[4/4] Inserting into database...")
        inserted, skipped = insert_raw_submissions(all_records)

        duration = (datetime.now() - start).total_seconds()
        log_run("Success", len(submissions), inserted,
                duration=round(duration, 2))

        print(f"\n  ✓ Kobo ingestion complete in {round(duration,1)}s")
        print(f"  Submissions fetched: {len(submissions)}")
        print(f"  Records inserted:    {inserted}")
        print(f"  Records skipped:     {skipped}")
        print(f"{'='*52}\n")

        if inserted > 0:
            print("  → Triggering cleaning pipeline...")
            try:
                from cleaning import run_cleaning_pipeline
                run_cleaning_pipeline(source="raw")
            except ImportError:
                print("  ⚠ cleaning.py not found — run manually.")

        return inserted

    except FileNotFoundError as e:
        print(f"\n  ⚠ Kobo config not set — connector not yet activated.")
        print(f"  {e}\n")
        log_run("Skipped", 0, 0, error="Config not found")
        return 0

    except Exception as e:
        duration = (datetime.now() - start).total_seconds()
        log_run("Failed", 0, 0, error=str(e), duration=round(duration, 2))
        print(f"\n  ✗ Kobo ingestion failed: {e}\n")
        raise


if __name__ == "__main__":
    run_kobo_ingestion()
