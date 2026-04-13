"""
TradeFlow NG — CSV Uploader
Accepts manual agent price reports via CSV/Excel,
validates format, inserts into raw_submissions,
then triggers the cleaning pipeline.

Expected CSV columns:
    state, market, commodity, price, unit, quantity, date, agent_phone
"""

import sqlite3
import pandas as pd
import numpy as np
import os
from datetime import datetime

# ── Path config ────────────────────────────────────────────
DB_PATH      = r"C:\Users\USER\Projects\TradeFlow\data\tradeflow.db"
UPLOAD_DIR   = r"C:\Users\USER\Projects\TradeFlow\data\raw"

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    return conn


# ══════════════════════════════════════════════════════════
# LOOKUP HELPERS
# ══════════════════════════════════════════════════════════

def build_lookup_maps():
    """
    Build name → id maps for states, markets, commodities, agents.
    Makes CSV matching case-insensitive and flexible.
    """
    conn = get_connection()

    states = pd.read_sql("SELECT id, LOWER(name) as name FROM states", conn)
    markets = pd.read_sql("SELECT id, LOWER(name) as name, state_id FROM markets", conn)
    commodities = pd.read_sql("SELECT id, LOWER(name) as name FROM commodities", conn)
    agents = pd.read_sql("SELECT id, phone FROM agents", conn)

    conn.close()

    return {
        "states":      dict(zip(states["name"],      states["id"])),
        "markets":     dict(zip(markets["name"],     markets["id"])),
        "commodities": dict(zip(commodities["name"], commodities["id"])),
        "agents":      dict(zip(agents["phone"],     agents["id"])),
    }


# ══════════════════════════════════════════════════════════
# TEMPLATE GENERATOR
# ══════════════════════════════════════════════════════════

def generate_template(output_path=None):
    """
    Generate a blank CSV template for agents to fill.
    Share this with agents via WhatsApp or email.
    """
    if output_path is None:
        output_path = os.path.join(UPLOAD_DIR, "agent_report_template.csv")

    template_data = {
        "state":       ["Oyo", "Lagos"],
        "market":      ["Bodija Market", "Mile 12 Market"],
        "commodity":   ["Yam", "Tomato"],
        "price":       [25000, 6500],
        "unit":        ["Bag (100kg)", "Crate"],
        "quantity":    [50, 200],
        "date":        [datetime.today().strftime("%Y-%m-%d")] * 2,
        "agent_phone": ["08012345678", "08012345678"],
        "notes":       ["Good supply today", "Prices rising this week"],
    }

    df = pd.DataFrame(template_data)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"✓ Template saved to: {output_path}")
    return output_path


# ══════════════════════════════════════════════════════════
# CSV VALIDATOR
# ══════════════════════════════════════════════════════════

REQUIRED_COLUMNS = ["state", "commodity", "price", "date"]
OPTIONAL_COLUMNS = ["market", "unit", "quantity", "agent_phone", "notes"]

def validate_csv(df):
    """
    Check that the CSV has required columns and sensible values.
    Returns (is_valid, error_messages)
    """
    errors = []

    # Check required columns
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {missing}")
        return False, errors

    # Check price is numeric
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    bad_prices = df["price"].isna().sum()
    if bad_prices > 0:
        errors.append(f"{bad_prices} rows have non-numeric price values.")

    # Check date is parseable
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    bad_dates = df["date"].isna().sum()
    if bad_dates > 0:
        errors.append(f"{bad_dates} rows have unparseable date values.")

    # Check prices are positive
    neg_prices = (df["price"] <= 0).sum()
    if neg_prices > 0:
        errors.append(f"{neg_prices} rows have zero or negative prices.")

    is_valid = len(errors) == 0
    return is_valid, errors


# ══════════════════════════════════════════════════════════
# CSV INGESTER
# ══════════════════════════════════════════════════════════

def ingest_csv(filepath):
    """
    Main function. Load a CSV or Excel file, validate it,
    map names to IDs, and insert into raw_submissions.
    """
    print(f"\n{'='*50}")
    print(f"  CSV UPLOADER")
    print(f"  File: {os.path.basename(filepath)}")
    print(f"{'='*50}\n")

    # ── Load file ─────────────────────────────────────────
    ext = os.path.splitext(filepath)[1].lower()
    if ext in [".xlsx", ".xls"]:
        df = pd.read_excel(filepath)
    elif ext == ".csv":
        df = pd.read_csv(filepath)
    else:
        raise ValueError(f"Unsupported file type: {ext}. Use .csv or .xlsx")

    print(f"[1/4] Loaded {len(df)} rows from file.")

    # Normalize column names
    df.columns = df.columns.str.strip().str.lower()

    # ── Validate ──────────────────────────────────────────
    print("[2/4] Validating format...")
    is_valid, errors = validate_csv(df)
    if not is_valid:
        print("  ✗ Validation failed:")
        for e in errors:
            print(f"    - {e}")
        return False

    print(f"  ✓ Format valid.")

    # ── Map names to IDs ──────────────────────────────────
    print("[3/4] Mapping names to database IDs...")
    maps = build_lookup_maps()

    df["state_lower"]     = df["state"].str.strip().str.lower()
    df["commodity_lower"] = df["commodity"].str.strip().str.lower()

    df["state_id"]     = df["state_lower"].map(maps["states"])
    df["commodity_id"] = df["commodity_lower"].map(maps["commodities"])

    # Optional: market
    if "market" in df.columns:
        df["market_lower"] = df["market"].str.strip().str.lower()
        df["market_id"]    = df["market_lower"].map(maps["markets"])
    else:
        df["market_id"] = None

    # Optional: agent
    if "agent_phone" in df.columns:
        df["agent_id"] = df["agent_phone"].astype(str).map(maps["agents"])
    else:
        df["agent_id"] = None

    # Report unmapped values
    unmapped_states = df[df["state_id"].isna()]["state"].unique()
    unmapped_comms  = df[df["commodity_id"].isna()]["commodity"].unique()

    if len(unmapped_states) > 0:
        print(f"  ⚠ Unrecognized states (will skip): {list(unmapped_states)}")
    if len(unmapped_comms) > 0:
        print(f"  ⚠ Unrecognized commodities (will skip): {list(unmapped_comms)}")

    # Drop rows with missing required IDs
    df = df.dropna(subset=["state_id", "commodity_id"])
    print(f"  {len(df)} rows ready for insertion.")

    # ── Insert into raw_submissions ───────────────────────
    print("[4/4] Inserting into database...")
    conn = get_connection()
    inserted = 0
    skipped  = 0

    for _, row in df.iterrows():
        try:
            conn.execute("""
                INSERT INTO raw_submissions (
                    agent_id, state_id, market_id, commodity_id,
                    reported_price, reported_unit,
                    quantity_available, submission_date, source_channel
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                int(row["agent_id"])   if pd.notna(row.get("agent_id"))   else None,
                int(row["state_id"]),
                int(row["market_id"])  if pd.notna(row.get("market_id"))  else None,
                int(row["commodity_id"]),
                float(row["price"]),
                str(row.get("unit", "")) if pd.notna(row.get("unit", "")) else None,
                float(row["quantity"]) if pd.notna(row.get("quantity"))   else None,
                str(row["date"])[:10],
                "CSV Upload"
            ))
            inserted += 1
        except Exception as e:
            skipped += 1
            print(f"    Skipped row: {e}")

    conn.commit()
    conn.close()

    print(f"\n  ✓ Done: {inserted} inserted, {skipped} skipped.")
    print(f"{'='*50}\n")

    # ── Trigger cleaning pipeline ─────────────────────────
    if inserted > 0:
        print("  Triggering cleaning pipeline on new records...")
        from cleaning import run_cleaning_pipeline
        run_cleaning_pipeline(source="raw")

    return True


# ══════════════════════════════════════════════════════════
# QUICK TEST
# ══════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Generate a blank template first
    template_path = generate_template()
    print(f"\nTemplate created at: {template_path}")
    print("Fill it in and run: ingest_csv('path/to/your_file.csv')")
