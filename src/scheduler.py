"""
TradeFlow NG — Phase 6: Scheduler & Feedback Loop
Runs daily pipelines automatically and evaluates forecast accuracy
to decide whether Prophet needs retraining.

Schedule: Daily at a configurable time (default 06:00)
Retraining: Only when MAPE (forecast error) exceeds threshold (default 15%)

Run this as a background process:
    python src/scheduler.py

Or run once manually:
    python src/scheduler.py --run-now
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import logging
import sys
import os
import argparse

# ── Path config ────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH  = r"C:\Users\USER\Projects\TradeFlow\data\tradeflow.db"
sys.path.insert(0, os.path.join(BASE_DIR, 'src'))

# ── Logging ────────────────────────────────────────────────
LOG_PATH = os.path.join(BASE_DIR, 'logs', 'scheduler.log')
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger("tradeflow.scheduler")

# ── Config ─────────────────────────────────────────────────
SCHEDULE_HOUR   = 6      # Run at 06:00 daily
SCHEDULE_MINUTE = 0
MAPE_THRESHOLD  = 15.0   # Retrain if Mean Absolute Percentage Error > 15%
MIN_OUTCOMES    = 5      # Need at least this many outcomes to evaluate accuracy


# ══════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════

def get_connection():
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn

def query(sql, params=()):
    conn = get_connection()
    df = pd.read_sql(sql, conn, params=params)
    conn.close()
    return df

def log_pipeline(run_type, status, records_in=0, records_out=0,
                 error=None, duration=None):
    conn = get_connection()
    conn.execute("""
        INSERT INTO pipeline_logs
        (run_type, status, records_in, records_out, error_message, duration_secs)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (run_type, status, records_in, records_out, error, duration))
    conn.commit()
    conn.close()


# ══════════════════════════════════════════════════════════
# 1. ACCURACY EVALUATOR
#    Compares Prophet forecasts against actual outcomes
#    to compute MAPE per commodity
# ══════════════════════════════════════════════════════════

def compute_forecast_accuracy():
    """
    For each commodity, compare:
      - What Prophet predicted the sell price would be
      - What agents actually sold for (from actual_outcomes)

    Returns a DataFrame with MAPE per commodity.
    MAPE = Mean Absolute Percentage Error
         = mean(|predicted - actual| / actual) * 100
    """
    log.info("Evaluating forecast accuracy against actual outcomes...")

    outcomes = query("""
        SELECT
            ao.commodity_id,
            ao.actual_sell_price,
            ao.trip_date,
            ao.corridor_id,
            corr.dest_state_id,
            c.name AS commodity_name
        FROM actual_outcomes ao
        JOIN commodities c ON ao.commodity_id = c.id
        LEFT JOIN corridors corr ON ao.corridor_id = corr.id
        WHERE ao.actual_sell_price IS NOT NULL
          AND ao.actual_sell_price > 0
          AND ao.trip_date >= DATE('now', '-30 days')
    """)

    if len(outcomes) < MIN_OUTCOMES:
        log.info(
            f"  Only {len(outcomes)} outcomes in last 30 days "
            f"(need {MIN_OUTCOMES}). Skipping accuracy check."
        )
        return pd.DataFrame()

    results = []

    for commodity_id in outcomes["commodity_id"].unique():
        comm_outcomes = outcomes[outcomes["commodity_id"] == commodity_id]
        commodity_name = comm_outcomes.iloc[0]["commodity_name"]

        errors = []

        for _, row in comm_outcomes.iterrows():
            if not row["dest_state_id"]:
                continue

            # Find the forecast that was generated on or before the trip date
            # for this commodity at the destination state
            forecast = query("""
                SELECT predicted_price FROM forecasts
                WHERE commodity_id = ?
                  AND state_id     = ?
                  AND forecast_date = ?
                  AND generated_on <= ?
                ORDER BY generated_on DESC LIMIT 1
            """, (
                int(commodity_id),
                int(row["dest_state_id"]),
                str(row["trip_date"]),
                str(row["trip_date"]),
            ))

            if forecast.empty:
                continue

            predicted = float(forecast.iloc[0]["predicted_price"])
            actual    = float(row["actual_sell_price"])

            if actual > 0:
                ape = abs(predicted - actual) / actual * 100
                errors.append(ape)

        if errors:
            mape = np.mean(errors)
            results.append({
                "commodity_id":   commodity_id,
                "commodity_name": commodity_name,
                "mape":           round(mape, 2),
                "n_outcomes":     len(errors),
                "needs_retrain":  mape > MAPE_THRESHOLD,
            })
            log.info(
                f"  {commodity_name}: MAPE = {mape:.1f}% "
                f"({'⚠ RETRAIN' if mape > MAPE_THRESHOLD else '✓ OK'})"
            )

    return pd.DataFrame(results)


# ══════════════════════════════════════════════════════════
# 2. FEEDBACK LOOP
#    Incorporate actual outcomes back into cleaned_prices
#    so Prophet can learn from real results
# ══════════════════════════════════════════════════════════

def incorporate_actual_outcomes():
    """
    Pull actual buy/sell prices from actual_outcomes and insert them
    into cleaned_prices as confirmed real data points.
    This is how the system learns from experience.
    """
    log.info("Incorporating actual outcomes into cleaned_prices...")

    # Find outcomes not yet incorporated
    new_outcomes = query("""
        SELECT
            ao.id,
            ao.commodity_id,
            corr.origin_state_id AS state_id,
            corr.dest_state_id,
            ao.actual_buy_price,
            ao.actual_sell_price,
            ao.actual_quantity,
            ao.trip_date,
            c.avg_weight_kg
        FROM actual_outcomes ao
        JOIN commodities c ON ao.commodity_id = c.id
        LEFT JOIN corridors corr ON ao.corridor_id = corr.id
        WHERE ao.id NOT IN (
            SELECT COALESCE(raw_submission_id, -1)
            FROM cleaned_prices
            WHERE cleaning_notes = 'From actual outcome'
        )
        AND ao.actual_buy_price IS NOT NULL
        AND ao.trip_date IS NOT NULL
    """)

    if new_outcomes.empty:
        log.info("  No new outcomes to incorporate.")
        return 0

    conn = get_connection()
    inserted = 0

    for _, row in new_outcomes.iterrows():
        # Insert buy price at origin state
        if row["state_id"] and row["actual_buy_price"]:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO cleaned_prices (
                        state_id, commodity_id, price_per_unit,
                        price_per_kg, quantity_available,
                        price_date, is_outlier, is_confirmed,
                        cleaning_notes
                    ) VALUES (?, ?, ?, ?, ?, ?, 0, 1, 'From actual outcome')
                """, (
                    int(row["state_id"]),
                    int(row["commodity_id"]),
                    float(row["actual_buy_price"]),
                    float(row["actual_buy_price"]) / float(row["avg_weight_kg"] or 100),
                    float(row["actual_quantity"]) if row["actual_quantity"] else None,
                    str(row["trip_date"])[:10],
                ))
                inserted += 1
            except Exception as e:
                log.warning(f"  Could not insert buy price: {e}")

        # Insert sell price at destination state
        if row["dest_state_id"] and row["actual_sell_price"]:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO cleaned_prices (
                        state_id, commodity_id, price_per_unit,
                        price_per_kg, price_date,
                        is_outlier, is_confirmed, cleaning_notes
                    ) VALUES (?, ?, ?, ?, ?, 0, 1, 'From actual outcome')
                """, (
                    int(row["dest_state_id"]),
                    int(row["commodity_id"]),
                    float(row["actual_sell_price"]),
                    float(row["actual_sell_price"]) / float(row["avg_weight_kg"] or 100),
                    str(row["trip_date"])[:10],
                ))
                inserted += 1
            except Exception as e:
                log.warning(f"  Could not insert sell price: {e}")

    conn.commit()
    conn.close()
    log.info(f"  Incorporated {inserted} actual price points into cleaned_prices.")
    return inserted


# ══════════════════════════════════════════════════════════
# 3. SELECTIVE RETRAINING
#    Only retrain commodities where MAPE > threshold
# ══════════════════════════════════════════════════════════

def run_selective_retraining(accuracy_df):
    """
    Retrain Prophet only for commodities where MAPE exceeds threshold.
    This is more efficient than retraining everything weekly.
    """
    if accuracy_df.empty:
        log.info("No accuracy data — running full forecast pipeline.")
        needs_retrain = None  # All commodities
    else:
        to_retrain = accuracy_df[accuracy_df["needs_retrain"]]
        if to_retrain.empty:
            log.info(
                f"All commodities within {MAPE_THRESHOLD}% MAPE threshold. "
                "No retraining needed."
            )
            return
        needs_retrain = to_retrain["commodity_id"].tolist()
        log.info(
            f"Retraining {len(needs_retrain)} commodity/ies: "
            f"{to_retrain['commodity_name'].tolist()}"
        )

    try:
        from forecasting import run_forecasting_pipeline
        # Pass commodity filter if we have one
        if needs_retrain:
            run_forecasting_for_commodities(needs_retrain)
        else:
            run_forecasting_pipeline(periods=7)
        log.info("  Retraining complete.")
    except Exception as e:
        log.error(f"  Retraining failed: {e}")
        raise


def run_forecasting_for_commodities(commodity_ids):
    """
    Run Prophet forecasting for specific commodities only.
    Imports forecasting module internals selectively.
    """
    import warnings
    warnings.filterwarnings("ignore")

    from forecasting import (
        load_training_data, train_prophet,
        generate_forecast, write_forecasts,
        log_run as fc_log
    )

    start    = datetime.now()
    total_in = 0
    total_out = 0

    # Get state-commodity combos for these commodities only
    combos = query("""
        SELECT DISTINCT cp.state_id, cp.commodity_id,
               s.name AS state_name, c.name AS commodity_name
        FROM cleaned_prices cp
        JOIN states s ON cp.state_id = s.id
        JOIN commodities c ON cp.commodity_id = c.id
        WHERE cp.commodity_id IN ({})
          AND cp.is_outlier = 0 AND cp.is_confirmed = 1
        ORDER BY c.name, s.name
    """.format(",".join("?" * len(commodity_ids))), tuple(commodity_ids))

    total_in = len(combos)

    for _, row in combos.iterrows():
        try:
            df = load_training_data(row["state_id"], row["commodity_id"])
            if df is None:
                continue
            hist_mean = df["y"].mean()
            hist_std  = df["y"].std()
            model     = train_prophet(df)
            forecast  = generate_forecast(model, periods=7)
            inserted, _ = write_forecasts(
                forecast, row["state_id"], row["commodity_id"],
                hist_mean, hist_std
            )
            total_out += inserted
            log.info(f"    {row['commodity_name']}/{row['state_name']}: {inserted} days")
        except Exception as e:
            log.warning(f"    Failed {row['commodity_name']}/{row['state_name']}: {e}")

    duration = (datetime.now() - start).total_seconds()
    fc_log("Success", total_in, total_out, duration=round(duration, 2))


# ══════════════════════════════════════════════════════════
# 4. MAIN DAILY PIPELINE
#    The full sequence that runs every day
# ══════════════════════════════════════════════════════════

def run_daily_pipeline():
    """
    Full daily automated pipeline:

    1. Ingest new Kobo submissions (if connector active)
    2. Run cleaning pipeline on raw data
    3. Incorporate actual outcomes into cleaned prices (feedback loop)
    4. Evaluate forecast accuracy against actual outcomes
    5. Retrain Prophet selectively where accuracy has dropped
    6. Run optimization to generate fresh recommendations
    7. Log everything
    """
    start = datetime.now()
    log.info("=" * 60)
    log.info(f"DAILY PIPELINE STARTED — {start.strftime('%Y-%m-%d %H:%M')}")
    log.info("=" * 60)

    steps_completed = []
    steps_failed    = []

    # ── Step 1: Kobo ingestion ─────────────────────────────
    log.info("\n[1/6] Kobo ingestion...")
    try:
        from kobo_connector import run_kobo_ingestion
        run_kobo_ingestion()
        steps_completed.append("Kobo ingestion")
    except FileNotFoundError:
        log.info("  Kobo config not set up yet — skipping.")
        steps_completed.append("Kobo ingestion (skipped — no config)")
    except Exception as e:
        log.warning(f"  Kobo ingestion failed: {e}")
        steps_failed.append(f"Kobo ingestion: {e}")

    # ── Step 2: Cleaning ───────────────────────────────────
    log.info("\n[2/6] Running cleaning pipeline...")
    try:
        from cleaning import run_cleaning_pipeline
        run_cleaning_pipeline(source="raw")
        steps_completed.append("Cleaning")
    except Exception as e:
        log.error(f"  Cleaning failed: {e}")
        steps_failed.append(f"Cleaning: {e}")

    # ── Step 3: Feedback loop ──────────────────────────────
    log.info("\n[3/6] Feedback loop — incorporating actual outcomes...")
    try:
        n = incorporate_actual_outcomes()
        steps_completed.append(f"Feedback loop ({n} prices added)")
    except Exception as e:
        log.error(f"  Feedback loop failed: {e}")
        steps_failed.append(f"Feedback loop: {e}")

    # ── Step 4: Accuracy evaluation ────────────────────────
    log.info("\n[4/6] Evaluating forecast accuracy...")
    accuracy_df = pd.DataFrame()
    try:
        accuracy_df = compute_forecast_accuracy()
        steps_completed.append("Accuracy evaluation")
    except Exception as e:
        log.error(f"  Accuracy evaluation failed: {e}")
        steps_failed.append(f"Accuracy evaluation: {e}")

    # ── Step 5: Selective retraining ───────────────────────
    log.info("\n[5/6] Selective Prophet retraining...")
    try:
        run_selective_retraining(accuracy_df)
        steps_completed.append("Forecasting")
    except Exception as e:
        log.error(f"  Forecasting failed: {e}")
        steps_failed.append(f"Forecasting: {e}")

    # ── Step 6: Optimization ───────────────────────────────
    log.info("\n[6/6] Running optimization...")
    try:
        from optimization import run_optimization_pipeline
        run_optimization_pipeline()
        steps_completed.append("Optimization")
    except Exception as e:
        log.error(f"  Optimization failed: {e}")
        steps_failed.append(f"Optimization: {e}")

    # ── Summary ────────────────────────────────────────────
    duration = (datetime.now() - start).total_seconds()
    status   = "Success" if not steps_failed else "Partial"

    log.info("\n" + "=" * 60)
    log.info(f"DAILY PIPELINE COMPLETE — {round(duration/60, 1)} minutes")
    log.info(f"Status: {status}")
    log.info(f"Completed: {', '.join(steps_completed)}")
    if steps_failed:
        log.warning(f"Failed: {', '.join(steps_failed)}")
    log.info("=" * 60 + "\n")

    log_pipeline(
        "Daily Pipeline", status,
        records_in=len(steps_completed) + len(steps_failed),
        records_out=len(steps_completed),
        error="; ".join(steps_failed) if steps_failed else None,
        duration=round(duration, 2),
    )


# ══════════════════════════════════════════════════════════
# 5. ACCURACY REPORT
#    Readable summary of model performance
# ══════════════════════════════════════════════════════════

def print_accuracy_report():
    """
    Print a human-readable accuracy report.
    Call this manually to check model health.
    """
    print("\n" + "=" * 50)
    print("  TRADEFLOW NG — FORECAST ACCURACY REPORT")
    print(f"  {date.today().strftime('%d %b %Y')}")
    print("=" * 50)

    accuracy_df = compute_forecast_accuracy()

    if accuracy_df.empty:
        print("\n  Not enough outcome data yet to evaluate accuracy.")
        print(f"  Need at least {MIN_OUTCOMES} logged outcomes in the last 30 days.")
        print("  Use the Feedback tab to log completed trades.\n")
        return

    for _, row in accuracy_df.sort_values("mape", ascending=False).iterrows():
        status = "⚠ NEEDS RETRAINING" if row["needs_retrain"] else "✓ OK"
        print(
            f"\n  {row['commodity_name']:<12} "
            f"MAPE: {row['mape']:>6.1f}%   "
            f"Outcomes: {row['n_outcomes']:>3}   "
            f"{status}"
        )

    above = accuracy_df["needs_retrain"].sum()
    print(f"\n  {above}/{len(accuracy_df)} commodities need retraining "
          f"(threshold: {MAPE_THRESHOLD}%)")
    print("=" * 50 + "\n")


# ══════════════════════════════════════════════════════════
# 6. SCHEDULER
# ══════════════════════════════════════════════════════════

def start_scheduler():
    """
    Start the APScheduler blocking scheduler.
    Runs the daily pipeline at SCHEDULE_HOUR:SCHEDULE_MINUTE every day.
    """
    scheduler = BlockingScheduler(timezone="Africa/Lagos")

    scheduler.add_job(
        run_daily_pipeline,
        trigger=CronTrigger(
            hour=SCHEDULE_HOUR,
            minute=SCHEDULE_MINUTE,
        ),
        id="daily_pipeline",
        name="TradeFlow NG Daily Pipeline",
        replace_existing=True,
        misfire_grace_time=3600,  # Allow up to 1hr late start
    )

    log.info("=" * 60)
    log.info("  TRADEFLOW NG SCHEDULER STARTED")
    log.info(f"  Daily pipeline scheduled at {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} (Lagos time)")
    log.info(f"  Retraining threshold: MAPE > {MAPE_THRESHOLD}%")
    log.info(f"  Log file: {LOG_PATH}")
    log.info("  Press Ctrl+C to stop.")
    log.info("=" * 60)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped.")


# ══════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TradeFlow NG Scheduler")
    parser.add_argument(
        "--run-now",
        action="store_true",
        help="Run the daily pipeline immediately, then exit."
    )
    parser.add_argument(
        "--accuracy",
        action="store_true",
        help="Print forecast accuracy report and exit."
    )
    args = parser.parse_args()

    if args.accuracy:
        print_accuracy_report()
    elif args.run_now:
        log.info("Manual run triggered.")
        run_daily_pipeline()
    else:
        start_scheduler()
