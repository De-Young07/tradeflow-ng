"""
TradeFlow NG — Phase 6: Scheduler & Feedback Loop
Runs daily pipelines automatically and evaluates forecast accuracy
to decide whether Prophet needs retraining.

Schedule: Daily at a configurable time (default 06:00)
Retraining: Only when MAPE exceeds threshold (default 15%)
"""

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import logging
import sys
import os
import argparse
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite")
IS_POSTGRES  = DATABASE_URL.startswith("postgresql")

from db_adapter import query, execute, executemany, get_connection

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, 'src'))

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

SCHEDULE_HOUR   = 6
SCHEDULE_MINUTE = 0
MAPE_THRESHOLD  = 15.0
MIN_OUTCOMES    = 5


def log_pipeline(run_type, status, records_in=0, records_out=0, error=None, duration=None):
    """Log pipeline execution with proper error handling."""
    try:
        execute("""
            INSERT INTO pipeline_logs (
                run_type, status, records_in, records_out,
                error_message, duration_secs, executed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """, (run_type, status, records_in, records_out, error, duration))
    except Exception as e:
        log.error(f"Failed to log pipeline: {e}")


def compute_forecast_accuracy():
    """Compare forecasts against actual outcomes."""
    log.info("Computing forecast accuracy...")

    try:
        results = query("""
            SELECT 
                f.commodity_id,
                AVG(ABS((ao.actual_sell_price - f.predicted_price) / NULLIF(f.predicted_price, 0))) * 100 AS mape
            FROM forecasts f
            JOIN actual_outcomes ao ON f.state_id = ao.state_id
                AND f.commodity_id = ao.commodity_id
                AND DATE(ao.trip_date) = f.forecast_date
            GROUP BY f.commodity_id
            HAVING COUNT(*) >= %s
        """, (MIN_OUTCOMES,))

        if not results.empty:
            needs_retraining = results[results["mape"] > MAPE_THRESHOLD]
            log.info(f"Forecast accuracy computed. Need retraining: {len(needs_retraining)} commodities")
            return needs_retraining
        return results
    except Exception as e:
        log.error(f"Accuracy computation failed: {e}")
        return None


def run_daily_pipeline():
    """Execute all daily tasks."""
    log.info("Starting daily pipeline...")
    start_time = datetime.now()

    try:
        from cleaning import run_cleaning_pipeline
        from forecasting import run_forecasting_pipeline
        from optimization import run_optimization_pipeline

        log.info("[1/3] Running cleaning pipeline...")
        run_cleaning_pipeline()

        log.info("[2/3] Running forecasting pipeline...")
        run_forecasting_pipeline()

        log.info("[3/3] Running optimization pipeline...")
        run_optimization_pipeline()

        duration = (datetime.now() - start_time).total_seconds()
        log_pipeline("daily", "success", duration=duration)
        log.info(f"✓ Daily pipeline completed in {duration:.1f}s")

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        log_pipeline("daily", "failed", error=str(e), duration=duration)
        log.error(f"Pipeline failed: {e}")


def start_scheduler():
    """Start APScheduler."""
    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_daily_pipeline,
        CronTrigger(hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE),
        id="daily_pipeline",
        name="Daily TradeFlow Pipeline",
    )
    log.info(f"Scheduler started. Daily run at {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d}")
    scheduler.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-now", action="store_true", help="Run once immediately")
    args = parser.parse_args()

    if args.run_now:
        log.info("Running pipeline immediately...")
        run_daily_pipeline()
    else:
        start_scheduler()
