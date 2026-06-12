"""
TradeFlow NG — Prophet Forecasting Module
Generates 7-day price forecasts per commodity per state.
All DB access goes through db_adapter (handles SQLite ↔ PostgreSQL).
"""

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import warnings
warnings.filterwarnings("ignore")
import os

from db_adapter import query as db_query, execute as db_execute, get_connection

try:
    from prophet import Prophet
except ImportError:
    raise ImportError("Prophet not installed. Run: pip install prophet")

# Minimum data points required to train Prophet
MIN_ROWS = 10


# ═══════════════════════════════════════════════════════════
# 1. LOAD TRAINING DATA
# ═══════════════════════════════════════════════════════════

def load_training_data(state_id, commodity_id):
    """
    Load cleaned, confirmed prices for one state+commodity.
    Routes through db_adapter so is_outlier/is_confirmed
    booleans are translated correctly for PostgreSQL.
    """
    df = db_query("""
        SELECT price_date     AS ds,
               price_per_unit AS y
        FROM   cleaned_prices
        WHERE  state_id     = ?
          AND  commodity_id = ?
          AND  is_outlier   = FALSE 
          AND  is_confirmed = TRUE
          AND  price_date   IS NOT NULL
        ORDER BY price_date
    """, (state_id, commodity_id))

    if len(df) < MIN_ROWS:
        return None

    df["ds"] = pd.to_datetime(df["ds"])
    df["y"]  = pd.to_numeric(df["y"], errors="coerce")
    df = df.dropna()
    return df


# ═══════════════════════════════════════════════════════════
# 2. TRAIN PROPHET
# ═══════════════════════════════════════════════════════════

def train_prophet(df, commodity_name=""):
    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
        changepoint_prior_scale=0.3,
        seasonality_prior_scale=10.0,
        interval_width=0.80,
        uncertainty_samples=500,
    )
    model.add_country_holidays(country_name="NG")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        model.fit(df)
    return model


# ═══════════════════════════════════════════════════════════
# 3. GENERATE FORECAST
# ═══════════════════════════════════════════════════════════

def generate_forecast(model, df_train, periods=7):
    """
    Dynamically calculates periods needed to bridge from the last
    training data point to today + 7 days ahead.
    """
    last_date = pd.Timestamp(df_train["ds"].max())
    today     = pd.Timestamp(date.today())
    
    # How many days from last training point to today
    days_gap      = max((today - last_date).days, 0)
    # Total periods = gap + how many future days we want
    total_periods = days_gap + periods

    future   = model.make_future_dataframe(periods=total_periods, freq="D")
    forecast = model.predict(future)

    # Now filter to only FUTURE dates (after today)
    forecast = forecast[forecast["ds"] > today].head(periods).copy()
    forecast = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    forecast["yhat"]       = forecast["yhat"].clip(lower=0)
    forecast["yhat_lower"] = forecast["yhat_lower"].clip(lower=0)
    forecast["yhat_upper"] = forecast["yhat_upper"].clip(lower=0)

    return forecast


# ═══════════════════════════════════════════════════════════
# 4. SHOCK DETECTION
# ═══════════════════════════════════════════════════════════

def detect_shock(forecast_row, historical_mean, historical_std,
                 uncertainty_threshold=0.3, zscore_threshold=2.5):
    predicted  = forecast_row["yhat"]
    lower      = forecast_row["yhat_lower"]
    upper      = forecast_row["yhat_upper"]
    band_width = upper - lower
    reasons    = []

    if predicted > 0:
        ratio = band_width / predicted
        if ratio > uncertainty_threshold:
            reasons.append(
                f"Wide uncertainty band: {round(ratio*100,1)}% of predicted price"
            )

    if historical_std and historical_std > 0:
        z = abs(predicted - historical_mean) / historical_std
        if z > zscore_threshold:
            reasons.append(
                f"Z-score={round(z,2)} vs historical mean={round(historical_mean,0)}"
            )

    is_flagged   = len(reasons) > 0
    shock_reason = " | ".join(reasons) if reasons else None
    return is_flagged, shock_reason


# ═══════════════════════════════════════════════════════════
# 5. WRITE FORECASTS TO DB
# ═══════════════════════════════════════════════════════════

def write_forecasts(forecast_df, state_id, commodity_id,
                    historical_mean, historical_std,
                    model_version="prophet_v1.0"):
    """
    Delete-then-insert approach — avoids ON CONFLICT translation issues.
    Deletes existing forecasts for this state-commodity before inserting fresh ones.
    """
    today    = str(date.today())
    inserted = 0
    skipped  = 0

    if forecast_df.empty:
        return 0, 0

    # Step 1 — Delete any existing forecasts for this combo
    # so we can write fresh ones cleanly
    for _, row in forecast_df.iterrows():
        ...
        db_execute("""
            INSERT INTO forecasts (
                state_id, commodity_id, forecast_date, generated_on,
                predicted_price, lower_bound, upper_bound,
                model_version, is_shock_flagged, shock_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (...))
        inserted += 1

    # Step 2 — Insert fresh forecasts
    for _, row in forecast_df.iterrows():
        is_shock, shock_reason = detect_shock(row, historical_mean, historical_std)
        try:
            db_execute("""
                INSERT INTO forecasts (
                    state_id, commodity_id,
                    forecast_date, generated_on,
                    predicted_price, lower_bound, upper_bound,
                    model_version, is_shock_flagged, shock_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                int(state_id),
                int(commodity_id),
                str(row["ds"])[:10],
                today,
                round(float(row["yhat"]),       2),
                round(float(row["yhat_lower"]), 2),
                round(float(row["yhat_upper"]), 2),
                model_version,
                bool(is_shock),
                shock_reason,
            ))
            inserted += 1
        except Exception as e:
            skipped += 1
            print(f"      Skipped row: {e}")

    return inserted, skipped


# ═══════════════════════════════════════════════════════════
# 6. PIPELINE LOG
# ═══════════════════════════════════════════════════════════

def log_run(status, records_in=0, records_out=0, error=None, duration=None):
    """Log via db_adapter — handles both SQLite and PostgreSQL."""
    try:
        db_execute("""
            INSERT INTO pipeline_logs
                (run_type, status, records_in, records_out,
                 error_message, duration_secs)
            VALUES (?, ?, ?, ?, ?, ?)
        """, ("Forecasting", status, records_in, records_out, error, duration))
    except Exception as e:
        print(f"  Warning: could not write to pipeline_logs: {e}")



# ═══════════════════════════════════════════════════════════
# 7. MAIN PIPELINE
# ═══════════════════════════════════════════════════════════

def run_forecasting_pipeline(periods=7, model_version="prophet_v1.0"):
    """
    Load all active state+commodity combos → train Prophet →
    generate 7-day forecast → write to forecasts table.
    """
    start = datetime.now()
    print(f"\n{'='*52}")
    print(f"  FORECASTING PIPELINE — {start.strftime('%Y-%m-%d %H:%M')}")
    print(f"  Horizon: {periods} days ahead")
    print(f"{'='*52}\n")

    # Load combos — use db_adapter so boolean filters work
    combos = db_query("""
        SELECT DISTINCT
            cp.state_id,
            cp.commodity_id,
            s.name AS state_name,
            c.name AS commodity_name
        FROM   cleaned_prices cp
        JOIN   states      s ON cp.state_id     = s.id
        JOIN   commodities c ON cp.commodity_id = c.id
        WHERE  cp.is_outlier   = 0
          AND  cp.is_confirmed = 1
        ORDER BY c.name, s.name
    """)

    total_combos   = len(combos)
    total_inserted = 0
    total_skipped  = 0
    total_shocks   = 0
    skipped_combos = []

    print(f"  Found {total_combos} state-commodity combinations to forecast.\n")

    for i, row in combos.iterrows():
        state_id       = row["state_id"]
        commodity_id   = row["commodity_id"]
        state_name     = row["state_name"]
        commodity_name = row["commodity_name"]
        label          = f"{commodity_name} / {state_name}"
        print(f"  [{i+1}/{total_combos}] {label}")

        try:
            df = load_training_data(state_id, commodity_id)
            if df is None:
                print(f"    ⚠ Insufficient data (need {MIN_ROWS} rows) — skipping.")
                skipped_combos.append(label)
                continue

            hist_mean = float(df["y"].mean())
            hist_std  = float(df["y"].std())

            model    = train_prophet(df, commodity_name)
            forecast = generate_forecast(model, df, periods=periods)

            shocks_this = sum(
                detect_shock(r, hist_mean, hist_std)[0]
                for _, r in forecast.iterrows()
            )

            inserted, skipped = write_forecasts(
                forecast, state_id, commodity_id,
                hist_mean, hist_std, model_version
            )

            total_inserted += inserted
            total_skipped  += skipped
            total_shocks   += shocks_this

            shock_tag = f" ⚠ {shocks_this} HIGH-RISK days" if shocks_this else ""
            print(f"    ✓ {inserted} forecast days written.{shock_tag}")

        except Exception as e:
            print(f"    ✗ Failed: {e}")
            skipped_combos.append(label)

    duration = (datetime.now() - start).total_seconds()
    log_run("Success", total_combos, total_inserted, duration=round(duration, 2))

    print(f"\n{'='*52}")
    print(f"  ✓ Forecasting complete in {round(duration,1)}s")
    print(f"  Combinations:   {total_combos}")
    print(f"  Forecast days:  {total_inserted}")
    print(f"  High-risk days: {total_shocks}")
    print(f"  Skipped combos: {len(skipped_combos)}")
    for s in skipped_combos:
        print(f"    - {s}")
    print(f"{'='*52}\n")

    return total_inserted

# ═══════════════════════════════════════════════════════════
# 8. PREVIEW HELPER
# ═══════════════════════════════════════════════════════════

def preview_forecasts(commodity_name=None, state_name=None, n=7):
    """Print latest forecasts. Use in Jupyter."""
    sql    = """
        SELECT f.forecast_date, s.name AS state, c.name AS commodity,
               f.predicted_price, f.lower_bound, f.upper_bound,
               f.is_shock_flagged, f.shock_reason
        FROM   forecasts f
        JOIN   states      s ON f.state_id     = s.id
        JOIN   commodities c ON f.commodity_id = c.id
        WHERE  f.generated_on = CURRENT_DATE
    """
    params = []
    if commodity_name:
        sql += " AND c.name = ?"; params.append(commodity_name)
    if state_name:
        sql += " AND s.name = ?"; params.append(state_name)
    sql += " ORDER BY c.name, s.name, f.forecast_date LIMIT ?"
    params.append(n * 10)

    df = db_query(sql, tuple(params))

    if df.empty:
        print("No forecasts for today. Run run_forecasting_pipeline() first.")
        return df

    df["predicted_price"] = df["predicted_price"].apply(lambda x: f"₦{x:,.0f}")
    df["lower_bound"]     = df["lower_bound"].apply(lambda x: f"₦{x:,.0f}")
    df["upper_bound"]     = df["upper_bound"].apply(lambda x: f"₦{x:,.0f}")
    df["risk"]            = df["is_shock_flagged"].apply(
                                lambda x: "⚠ HIGH RISK" if x else "✓ Normal")
    df = df.drop(columns=["is_shock_flagged", "shock_reason"])

    print(f"\n{'='*80}")
    print(f"  FORECAST PREVIEW — {date.today()}")
    print(f"{'='*80}")
    print(df.to_string(index=False))
    print(f"{'='*80}\n")
    return df


if __name__ == "__main__":
    run_forecasting_pipeline(periods=7)
    preview_forecasts()
