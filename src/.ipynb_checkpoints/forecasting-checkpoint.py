"""
TradeFlow NG — Prophet Forecasting Module
Trains Prophet models on cleaned price data and generates
7-day ahead forecasts per commodity per state.

Shock detection: High-uncertainty forecasts are included
but marked as high-risk for the optimization layer.
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import warnings
warnings.filterwarnings("ignore")

# ── Prophet import with helpful error ─────────────────────
try:
    from prophet import Prophet
except ImportError:
    raise ImportError(
        "Prophet not installed. Run: pip install prophet"
    )

# ── Path config ────────────────────────────────────────────
DB_PATH = r"C:\Users\USER\Projects\TradeFlow\data\tradeflow.db"

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    return conn


# ══════════════════════════════════════════════════════════
# 1. LOAD TRAINING DATA
# ══════════════════════════════════════════════════════════

def load_training_data(state_id, commodity_id, min_rows=8):
    """
    Load cleaned price history for a specific state + commodity.
    Prophet needs at minimum ~8 data points to fit reliably.
    Returns None if insufficient data.
    """
    conn = get_connection()
    df = pd.read_sql("""
        SELECT
            price_date      AS ds,
            price_per_unit  AS y
        FROM cleaned_prices
        WHERE state_id     = ?
          AND commodity_id = ?
          AND is_outlier   = 0
          AND is_confirmed = 1
        ORDER BY price_date ASC
    """, conn, params=(state_id, commodity_id))
    conn.close()

    if len(df) < min_rows:
        return None

    df["ds"] = pd.to_datetime(df["ds"])
    df["y"]  = pd.to_numeric(df["y"], errors="coerce")
    df = df.dropna()
    return df


# ══════════════════════════════════════════════════════════
# 2. TRAIN PROPHET MODEL
# ══════════════════════════════════════════════════════════

def train_prophet(df, commodity_name=""):
    """
    Train a Prophet model on price history.
    Configuration is tuned for Nigerian commodity markets:
    - Weekly seasonality on (markets are weekly)
    - Daily seasonality off (we have weekly data)
    - Yearly seasonality on (harvest cycles matter)
    - Higher changepoint flexibility for volatile markets
    """
    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
        changepoint_prior_scale=0.3,      # Higher = more flexible trend
        seasonality_prior_scale=10.0,     # Allow strong seasonality
        interval_width=0.80,              # 80% confidence interval
        uncertainty_samples=500,
    )

    # Nigerian public holidays affect market activity
    model.add_country_holidays(country_name="NG")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        model.fit(df)

    return model


# ══════════════════════════════════════════════════════════
# 3. GENERATE FORECASTS
# ══════════════════════════════════════════════════════════

def generate_forecast(model, periods=7):
    """
    Generate forecast for the next `periods` days.
    Returns a DataFrame with ds, yhat, yhat_lower, yhat_upper.
    Only returns future dates (not historical fitted values).
    """
    future = model.make_future_dataframe(periods=periods, freq="D")
    forecast = model.predict(future)

    # Keep only the future portion
    today = pd.Timestamp(date.today())
    forecast = forecast[forecast["ds"] >= today].copy()

    # Keep only what we need
    forecast = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()

    # Clip negative predictions (prices can't be negative)
    forecast["yhat"]       = forecast["yhat"].clip(lower=0)
    forecast["yhat_lower"] = forecast["yhat_lower"].clip(lower=0)
    forecast["yhat_upper"] = forecast["yhat_upper"].clip(lower=0)

    return forecast


# ══════════════════════════════════════════════════════════
# 4. SHOCK DETECTION
# ══════════════════════════════════════════════════════════

def detect_shock(forecast_row, historical_mean, historical_std,
                 uncertainty_threshold=0.3, zscore_threshold=2.5):
    """
    Flag a forecast as high-risk if:
    1. Uncertainty band is too wide relative to the predicted price
       (uncertainty_ratio > threshold), OR
    2. Predicted price is far from historical norm (z-score > threshold)

    Returns (is_shock_flagged, shock_reason)
    """
    predicted    = forecast_row["yhat"]
    lower        = forecast_row["yhat_lower"]
    upper        = forecast_row["yhat_upper"]
    band_width   = upper - lower

    reasons = []

    # Check 1: Uncertainty ratio
    if predicted > 0:
        uncertainty_ratio = band_width / predicted
        if uncertainty_ratio > uncertainty_threshold:
            reasons.append(
                f"Wide uncertainty band: {round(uncertainty_ratio*100, 1)}% of predicted price"
            )

    # Check 2: Z-score vs historical
    if historical_std and historical_std > 0:
        z = abs(predicted - historical_mean) / historical_std
        if z > zscore_threshold:
            reasons.append(
                f"Z-score={round(z, 2)} vs historical mean={round(historical_mean, 0)}"
            )

    is_flagged   = len(reasons) > 0
    shock_reason = " | ".join(reasons) if reasons else None
    return is_flagged, shock_reason


# ══════════════════════════════════════════════════════════
# 5. WRITE FORECASTS TO DATABASE
# ══════════════════════════════════════════════════════════

def write_forecasts(forecast_df, state_id, commodity_id,
                    historical_mean, historical_std,
                    model_version="prophet_v1.0"):
    """
    Insert forecast rows into the forecasts table.
    Skips dates already forecasted today (idempotent).
    """
    conn = get_connection()
    today = str(date.today())
    inserted = 0
    skipped  = 0

    for _, row in forecast_df.iterrows():
        is_shock, shock_reason = detect_shock(
            row, historical_mean, historical_std
        )

        try:
            conn.execute("""
                INSERT OR IGNORE INTO forecasts (
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
                shock_reason
            ))
            inserted += 1
        except Exception as e:
            skipped += 1
            print(f"      Skipped forecast row: {e}")

    conn.commit()
    conn.close()
    return inserted, skipped


# ══════════════════════════════════════════════════════════
# 6. PIPELINE LOG
# ══════════════════════════════════════════════════════════

def log_run(status, records_in, records_out, error=None, duration=None):
    conn = get_connection()
    conn.execute("""
        INSERT INTO pipeline_logs
        (run_type, status, records_in, records_out, error_message, duration_secs)
        VALUES (?, ?, ?, ?, ?, ?)
    """, ("Forecast", status, records_in, records_out, error, duration))
    conn.commit()
    conn.close()


# ══════════════════════════════════════════════════════════
# 7. MAIN FORECASTING RUNNER
# ══════════════════════════════════════════════════════════

def run_forecasting_pipeline(periods=7, model_version="prophet_v1.0"):
    """
    Full forecasting pipeline.
    Loops over all active state + commodity combinations,
    trains Prophet, generates 7-day forecast, writes to DB.
    """
    start = datetime.now()

    print(f"\n{'='*52}")
    print(f"  FORECASTING PIPELINE — {start.strftime('%Y-%m-%d %H:%M')}")
    print(f"  Horizon: {periods} days ahead")
    print(f"{'='*52}\n")

    # Load all active state + commodity combinations
    conn = get_connection()
    combos = pd.read_sql("""
        SELECT DISTINCT
            cp.state_id,
            cp.commodity_id,
            s.name  AS state_name,
            c.name  AS commodity_name
        FROM cleaned_prices cp
        JOIN states      s ON cp.state_id     = s.id
        JOIN commodities c ON cp.commodity_id = c.id
        WHERE cp.is_outlier   = 0
          AND cp.is_confirmed = 1
        ORDER BY c.name, s.name
    """, conn)
    conn.close()

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
            # Load training data
            df = load_training_data(state_id, commodity_id)
            if df is None:
                print(f"    ⚠ Insufficient data — skipping.")
                skipped_combos.append(label)
                continue

            # Historical stats for shock detection
            hist_mean = df["y"].mean()
            hist_std  = df["y"].std()

            # Train model
            model = train_prophet(df, commodity_name)

            # Generate forecast
            forecast = generate_forecast(model, periods=periods)

            # Count shocks before writing
            shocks_this = sum(
                detect_shock(r, hist_mean, hist_std)[0]
                for _, r in forecast.iterrows()
            )

            # Write to database
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
    log_run("Success", total_combos, total_inserted,
            duration=round(duration, 2))

    print(f"\n{'='*52}")
    print(f"  ✓ Forecasting complete in {round(duration, 1)}s")
    print(f"  Combinations:   {total_combos}")
    print(f"  Forecast days:  {total_inserted}")
    print(f"  High-risk days: {total_shocks}")
    print(f"  Skipped combos: {len(skipped_combos)}")
    if skipped_combos:
        for s in skipped_combos:
            print(f"    - {s}")
    print(f"{'='*52}\n")

    return total_inserted


# ══════════════════════════════════════════════════════════
# 8. QUICK INSPECTION HELPER
# ══════════════════════════════════════════════════════════

def preview_forecasts(commodity_name=None, state_name=None, n=7):
    """
    Print a readable preview of the latest forecasts.
    Use in Jupyter to inspect results after running pipeline.

    Usage:
        preview_forecasts()                          # All
        preview_forecasts(commodity_name="Yam")      # One commodity
        preview_forecasts(state_name="Lagos")        # One state
    """
    conn = get_connection()

    query = """
        SELECT
            f.forecast_date,
            s.name          AS state,
            c.name          AS commodity,
            f.predicted_price,
            f.lower_bound,
            f.upper_bound,
            f.is_shock_flagged,
            f.shock_reason
        FROM forecasts f
        JOIN states      s ON f.state_id     = s.id
        JOIN commodities c ON f.commodity_id = c.id
        WHERE f.generated_on = DATE('now')
    """
    params = []
    if commodity_name:
        query  += " AND c.name = ?"
        params.append(commodity_name)
    if state_name:
        query  += " AND s.name = ?"
        params.append(state_name)

    query += " ORDER BY c.name, s.name, f.forecast_date LIMIT ?"
    params.append(n * 10)

    df = pd.read_sql(query, conn, params=params)
    conn.close()

    if df.empty:
        print("No forecasts found for today. Run run_forecasting_pipeline() first.")
        return df

    # Format for readability
    df["predicted_price"] = df["predicted_price"].apply(lambda x: f"₦{x:,.0f}")
    df["lower_bound"]     = df["lower_bound"].apply(lambda x: f"₦{x:,.0f}")
    df["upper_bound"]     = df["upper_bound"].apply(lambda x: f"₦{x:,.0f}")
    df["risk"]            = df["is_shock_flagged"].apply(
                                lambda x: "⚠ HIGH RISK" if x else "✓ Normal"
                            )
    df = df.drop(columns=["is_shock_flagged", "shock_reason"])

    print(f"\n{'='*80}")
    print(f"  FORECAST PREVIEW — Generated {date.today()}")
    print(f"{'='*80}")
    print(df.to_string(index=False))
    print(f"{'='*80}\n")
    return df


if __name__ == "__main__":
    run_forecasting_pipeline(periods=7)
    print("\nPreview of results:")
    preview_forecasts()
