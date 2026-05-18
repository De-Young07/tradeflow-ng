"""
TradeFlow NG — Scheduler / Pipeline Runner

TWO MODES:
  LOCAL:  python src/scheduler.py --run-now
  CLOUD:  GitHub Actions cron (see .github/workflows/scheduler.yml)
          Runs automatically every day at 06:00 WAT (05:00 UTC). Free.

PIPELINE ORDER:
  Kobo ingestion → Cleaning → Forecasting → Optimization

CLI:
  python src/scheduler.py                  # full pipeline
  python src/scheduler.py --kobo-only
  python src/scheduler.py --forecast-only
  python src/scheduler.py --optimize-only
  python src/scheduler.py --accuracy
  python src/scheduler.py --clear-dummy
"""

import os
import sys
import argparse
from datetime import datetime, date

SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


def _ensure_db_env():
    if os.environ.get("DATABASE_URL"):
        return
    try:
        import streamlit as st
        url = st.secrets.get("database", {}).get("DATABASE_URL")
        if url:
            os.environ["DATABASE_URL"] = url
            return
    except Exception:
        pass
    try:
        import configparser
        cfg_path = os.path.join(SRC_DIR, '..', 'config.ini')
        if os.path.exists(cfg_path):
            cfg = configparser.ConfigParser()
            cfg.read(cfg_path)
            url = cfg.get('database', 'DATABASE_URL', fallback=None)
            if url:
                os.environ["DATABASE_URL"] = url
    except Exception:
        pass

_ensure_db_env()
from db_adapter import query as db_query, execute as db_execute, backend_name


def run_kobo():
    print("\n── STAGE 1: KOBO INGESTION ─────────────────────────")
    try:
        from kobo_connector import run_kobo_ingestion
        return run_kobo_ingestion()
    except FileNotFoundError:
        print("  ⚠ Kobo config not set — skipping.")
        return 0
    except Exception as e:
        print(f"  ✗ Kobo failed: {e}")
        return 0


def run_cleaning():
    print("\n── STAGE 2: DATA CLEANING ──────────────────────────")
    try:
        from cleaning import run_cleaning_pipeline
        return run_cleaning_pipeline(source="raw")
    except Exception as e:
        print(f"  ✗ Cleaning failed: {e}")
        return 0


def run_forecasting():
    print("\n── STAGE 3: FORECASTING ────────────────────────────")
    try:
        from forecasting import run_forecasting_pipeline
        return run_forecasting_pipeline(periods=7)
    except Exception as e:
        print(f"  ✗ Forecasting failed: {e}")
        return 0


def run_optimization():
    print("\n── STAGE 4: OPTIMIZATION ───────────────────────────")
    try:
        from optimization import run_optimization_pipeline
        recs = run_optimization_pipeline()
        return len(recs) if recs is not None and not recs.empty else 0
    except Exception as e:
        print(f"  ✗ Optimization failed: {e}")
        return 0


def run_full_pipeline():
    start = datetime.now()
    print(f"\n{'='*56}")
    print(f"  TRADEFLOW NG — FULL PIPELINE")
    print(f"  {start.strftime('%Y-%m-%d %H:%M:%S')} | DB: {backend_name()}")
    print(f"{'='*56}")

    k = run_kobo()
    c = run_cleaning()
    f = run_forecasting()
    o = run_optimization()

    dur = round((datetime.now() - start).total_seconds(), 2)
    print(f"\n{'='*56}")
    print(f"  COMPLETE in {dur}s")
    print(f"  Kobo: {k}  Cleaned: {c}  Forecasts: {f}  Recs: {o}")
    print(f"{'='*56}\n")

    try:
        db_execute("""
            INSERT INTO pipeline_logs
                (run_type, status, records_in, records_out, duration_secs)
            VALUES (?, ?, ?, ?, ?)
        """, ("Full Pipeline", "Success", k, o, dur))
    except Exception:
        pass

    return {"kobo": k, "cleaned": c, "forecasts": f, "optimizations": o}


def print_accuracy_report():
    print(f"\n{'='*56}\n  FORECAST ACCURACY — {date.today()}\n{'='*56}")
    try:
        df = db_query("""
            SELECT c.name AS commodity, s.name AS state,
                   ao.trade_date, f.predicted_price,
                   ao.actual_sell_price,
                   ABS(f.predicted_price - ao.actual_sell_price)
                       / NULLIF(ao.actual_sell_price,0)*100 AS mape_pct
            FROM   actual_outcomes ao
            JOIN   forecasts   f ON f.state_id=ao.state_id
                               AND f.commodity_id=ao.commodity_id
                               AND f.forecast_date=ao.trade_date
            JOIN   states      s ON s.id=ao.state_id
            JOIN   commodities c ON c.id=ao.commodity_id
            WHERE  ao.actual_sell_price IS NOT NULL
            ORDER BY mape_pct DESC LIMIT 50
        """)
        if df.empty:
            print("  No actual outcomes logged yet.\n")
            return
        avg = float(df["mape_pct"].mean())
        print(f"  Average MAPE: {round(avg,1)}%  (target: <15%)\n")
        for _, r in df.iterrows():
            flag = " ⚠" if r["mape_pct"] > 15 else ""
            print(f"  {r['commodity']:<14} {r['state']:<14} "
                  f"₦{r['predicted_price']:>10,.0f} "
                  f"₦{r['actual_sell_price']:>10,.0f} "
                  f"{r['mape_pct']:>6.1f}%{flag}")
    except Exception as e:
        print(f"  ✗ {e}")


def clear_dummy_data():
    print("\n  This removes all simulated data permanently.")
    if input("  Type YES to confirm: ").strip().upper() != "YES":
        print("  Cancelled."); return
    try:
        db_execute("""DELETE FROM cleaned_prices
                      WHERE source_channel IN ('dummy','simulation')
                         OR source_channel IS NULL""")
        db_execute("""DELETE FROM raw_submissions
                      WHERE source_channel IN ('dummy','simulation')
                         OR source_channel IS NULL""")
        print("  ✓ Dummy data cleared.")
    except Exception as e:
        print(f"  ✗ {e}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="TradeFlow NG Scheduler")
    p.add_argument("--run-now",       action="store_true")
    p.add_argument("--kobo-only",     action="store_true")
    p.add_argument("--forecast-only", action="store_true")
    p.add_argument("--optimize-only", action="store_true")
    p.add_argument("--accuracy",      action="store_true")
    p.add_argument("--clear-dummy",   action="store_true")
    args = p.parse_args()

    if args.kobo_only:       run_kobo()
    elif args.forecast_only: run_forecasting()
    elif args.optimize_only: run_optimization()
    elif args.accuracy:      print_accuracy_report()
    elif args.clear_dummy:   clear_dummy_data()
    else:                    run_full_pipeline()
