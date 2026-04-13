"""
TradeFlow NG — Data Cleaning Pipeline
Cleans raw submissions and writes to cleaned_prices table.
Outlier detection: Z-score method
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import date, datetime
import os

# ── Path config ────────────────────────────────────────────
DB_PATH = r"C:\Users\USER\Projects\TradeFlow\data\tradeflow.db"

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    return conn


# ══════════════════════════════════════════════════════════
# 1. LOAD RAW DATA
# ══════════════════════════════════════════════════════════

def load_raw_submissions(since_date=None):
    """
    Load unprocessed raw submissions.
    If since_date is provided, only load from that date onward.
    """
    conn = get_connection()

    query = """
        SELECT
            rs.id               AS raw_id,
            rs.state_id,
            rs.market_id,
            rs.commodity_id,
            rs.reported_price,
            rs.reported_unit,
            rs.quantity_available,
            rs.submission_date,
            rs.source_channel,
            c.unit_of_measure   AS standard_unit,
            c.avg_weight_kg,
            c.perishability_class
        FROM raw_submissions rs
        JOIN commodities c ON rs.commodity_id = c.id
        WHERE rs.id NOT IN (
            SELECT raw_submission_id
            FROM cleaned_prices
            WHERE raw_submission_id IS NOT NULL
        )
    """

    if since_date:
        query += f" AND rs.submission_date >= '{since_date}'"

    df = pd.read_sql(query, conn)
    conn.close()

    print(f"  Loaded {len(df)} unprocessed raw submissions.")
    return df


def load_dummy_data():
    """
    Load already-cleaned dummy data for pipeline testing.
    Returns it as a DataFrame mimicking raw structure.
    """
    conn = get_connection()
    df = pd.read_sql("""
        SELECT
            cp.id           AS raw_id,
            cp.state_id,
            cp.market_id,
            cp.commodity_id,
            cp.price_per_unit   AS reported_price,
            c.unit_of_measure   AS reported_unit,
            cp.quantity_available,
            cp.price_date       AS submission_date,
            'Dummy'             AS source_channel,
            c.unit_of_measure   AS standard_unit,
            c.avg_weight_kg,
            c.perishability_class
        FROM cleaned_prices cp
        JOIN commodities c ON cp.commodity_id = c.id
        LIMIT 50
    """, conn)
    conn.close()
    print(f"  Loaded {len(df)} dummy records for testing.")
    return df


# ══════════════════════════════════════════════════════════
# 2. STANDARDIZATION
# ══════════════════════════════════════════════════════════

def standardize_prices(df):
    """
    Ensure all prices are in NGN per standard commodity unit.
    Calculates price_per_kg alongside for cross-commodity comparison.
    """
    df = df.copy()

    # Price per kg (for normalization)
    df["price_per_kg"] = df.apply(
        lambda row: round(row["reported_price"] / row["avg_weight_kg"], 2)
        if row["avg_weight_kg"] and row["avg_weight_kg"] > 0
        else None,
        axis=1
    )

    # Standardized price = reported price (already per unit from Kobo/CSV)
    df["price_per_unit"] = df["reported_price"]

    print(f"  Standardized prices for {len(df)} records.")
    return df


# ══════════════════════════════════════════════════════════
# 3. OUTLIER DETECTION — Z-SCORE
# ══════════════════════════════════════════════════════════

def detect_outliers_zscore(df, threshold=2.5):
    """
    Flag prices that deviate more than `threshold` standard deviations
    from the mean for that commodity across all states.

    threshold=2.5 is deliberately conservative for commodity markets —
    Nigerian prices can swing legitimately, so we don't want to
    over-flag real price spikes.
    """
    df = df.copy()
    df["is_outlier"]     = False
    df["outlier_reason"] = None

    for commodity_id in df["commodity_id"].unique():
        mask   = df["commodity_id"] == commodity_id
        subset = df.loc[mask, "price_per_unit"]

        if len(subset) < 3:
            # Not enough data points to compute z-score reliably
            continue

        mean   = subset.mean()
        std    = subset.std()

        if std == 0:
            continue

        z_scores = (subset - mean) / std
        outlier_mask = z_scores.abs() > threshold

        df.loc[mask & outlier_mask, "is_outlier"] = True
        df.loc[mask & outlier_mask, "outlier_reason"] = (
            df.loc[mask & outlier_mask].apply(
                lambda row: (
                    f"Z-score={round((row['price_per_unit'] - mean) / std, 2)}, "
                    f"mean={round(mean,0)}, std={round(std,0)}"
                ),
                axis=1
            )
        )

    flagged = df["is_outlier"].sum()
    print(f"  Outlier detection complete: {flagged} records flagged out of {len(df)}.")
    return df


# ══════════════════════════════════════════════════════════
# 4. VALIDATION
# ══════════════════════════════════════════════════════════

def validate_records(df):
    """
    Drop or flag records that fail basic validation.
    Returns clean df and a separate df of rejected records.
    """
    rejected = []

    # Must have a price
    no_price = df["price_per_unit"].isna()
    if no_price.any():
        rejected.append(df[no_price].copy().assign(reject_reason="Missing price"))
        df = df[~no_price]

    # Price must be positive
    neg_price = df["price_per_unit"] <= 0
    if neg_price.any():
        rejected.append(df[neg_price].copy().assign(reject_reason="Non-positive price"))
        df = df[~neg_price]

    # Must have state and commodity
    no_state = df["state_id"].isna() | df["commodity_id"].isna()
    if no_state.any():
        rejected.append(df[no_state].copy().assign(reject_reason="Missing state or commodity"))
        df = df[~no_state]

    # Must have a valid date
    df["submission_date"] = pd.to_datetime(df["submission_date"], errors="coerce")
    bad_date = df["submission_date"].isna()
    if bad_date.any():
        rejected.append(df[bad_date].copy().assign(reject_reason="Invalid date"))
        df = df[~bad_date]

    rejected_df = pd.concat(rejected) if rejected else pd.DataFrame()
    print(f"  Validation: {len(df)} passed, {len(rejected_df)} rejected.")
    return df, rejected_df


# ══════════════════════════════════════════════════════════
# 5. WRITE TO CLEANED_PRICES
# ══════════════════════════════════════════════════════════

def write_cleaned_prices(df):
    """
    Insert validated, standardized records into cleaned_prices table.
    Skips duplicates (same state + commodity + date + market).
    """
    if df.empty:
        print("  Nothing to write.")
        return 0

    conn = get_connection()
    inserted = 0
    skipped  = 0

    for _, row in df.iterrows():
        try:
            conn.execute("""
                INSERT OR IGNORE INTO cleaned_prices (
                    raw_submission_id,
                    state_id,
                    market_id,
                    commodity_id,
                    price_per_unit,
                    price_per_kg,
                    quantity_available,
                    price_date,
                    is_outlier,
                    outlier_reason,
                    is_confirmed,
                    cleaning_notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                None if row.get("source_channel") == "Dummy" else (int(row["raw_id"]) if pd.notna(row.get("raw_id")) else None),
                int(row["state_id"]),
                int(row["market_id"])            if pd.notna(row.get("market_id"))           else None,
                int(row["commodity_id"]),
                float(row["price_per_unit"]),
                float(row["price_per_kg"])       if pd.notna(row.get("price_per_kg"))        else None,
                float(row["quantity_available"]) if pd.notna(row.get("quantity_available"))  else None,
                str(row["submission_date"])[:10],  # Keep date only
                bool(row["is_outlier"]),
                row.get("outlier_reason"),
                False,                             # Not yet confirmed by 2nd agent
                "Auto-cleaned by pipeline"
            ))
            inserted += 1
        except Exception as e:
            skipped += 1
            print(f"    Skipped row: {e}")

    conn.commit()
    conn.close()
    print(f"  Written: {inserted} inserted, {skipped} skipped.")
    return inserted


# ══════════════════════════════════════════════════════════
# 6. PIPELINE LOG
# ══════════════════════════════════════════════════════════

def log_pipeline_run(run_type, status, records_in, records_out,
                     error_message=None, duration_secs=None):
    conn = get_connection()
    conn.execute("""
        INSERT INTO pipeline_logs
        (run_type, status, records_in, records_out, error_message, duration_secs)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (run_type, status, records_in, records_out, error_message, duration_secs))
    conn.commit()
    conn.close()


# ══════════════════════════════════════════════════════════
# 7. MAIN PIPELINE RUNNER
# ══════════════════════════════════════════════════════════

def run_cleaning_pipeline(source="dummy"):
    """
    Full cleaning pipeline.
    source: 'dummy'  → use existing dummy data for testing
            'raw'    → process new raw_submissions
            'csv'    → called from csv_uploader.py after CSV is ingested
    """
    start = datetime.now()
    print(f"\n{'='*50}")
    print(f"  CLEANING PIPELINE — {start.strftime('%Y-%m-%d %H:%M')}")
    print(f"  Source: {source.upper()}")
    print(f"{'='*50}")

    try:
        # Step 1: Load
        print("\n[1/5] Loading data...")
        if source == "dummy":
            df = load_dummy_data()
        else:
            df = load_raw_submissions()

        records_in = len(df)
        if records_in == 0:
            print("  No new records to process. Exiting.")
            return

        # Step 2: Standardize
        print("\n[2/5] Standardizing prices...")
        df = standardize_prices(df)

        # Step 3: Validate
        print("\n[3/5] Validating records...")
        df, rejected = validate_records(df)

        # Step 4: Detect outliers
        print("\n[4/5] Detecting outliers...")
        df = detect_outliers_zscore(df, threshold=2.5)

        # Step 5: Write
        print("\n[5/5] Writing to database...")
        inserted = write_cleaned_prices(df)

        duration = (datetime.now() - start).total_seconds()
        log_pipeline_run("Cleaning", "Success", records_in, inserted,
                         duration_secs=round(duration, 2))

        print(f"\n{'='*50}")
        print(f"  ✓ Pipeline complete in {round(duration, 1)}s")
        print(f"  Records in:  {records_in}")
        print(f"  Inserted:    {inserted}")
        print(f"  Outliers:    {df['is_outlier'].sum()}")
        print(f"  Rejected:    {len(rejected)}")
        print(f"{'='*50}\n")

    except Exception as e:
        duration = (datetime.now() - start).total_seconds()
        log_pipeline_run("Cleaning", "Failed", 0, 0,
                         error_message=str(e),
                         duration_secs=round(duration, 2))
        print(f"\n  ✗ Pipeline failed: {e}\n")
        raise


if __name__ == "__main__":
    run_cleaning_pipeline(source="dummy")
