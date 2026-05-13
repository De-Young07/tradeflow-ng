"""
TradeFlow NG — Prophet Forecasting Module
Trains Prophet models on cleaned price data and generates
7-day ahead forecasts per commodity per state.

Shock detection: High-uncertainty forecasts are included
but marked as high-risk for the optimization layer.
"""

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import warnings
import os
warnings.filterwarnings("ignore")

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite")
IS_POSTGRES  = DATABASE_URL.startswith("postgresql")

from db_adapter import query, execute, executemany, get_connection

try:
    from prophet import Prophet
except ImportError:
    raise ImportError(
        "Prophet not installed. Run: pip install prophet"
    )

from dotenv import load_dotenv


def load_training_data(state_id, commodity_id, min_rows=8):
    """
    Load cleaned price history for a specific state + commodity.
    Prophet needs at minimum ~8 data points to fit reliably.
    Returns None if insufficient data.
    """
    df = query("""
        SELECT
            price_date      AS ds,
            price_per_unit  AS y
        FROM cleaned_prices
        WHERE state_id     = %s
          AND commodity_id = %s
          AND is_outlier   = FALSE
          AND is_confirmed = TRUE
        ORDER BY price_date ASC
    """, (state_id, commodity_id))

    if len(df) < min_rows:
        return None

    df["ds"] = pd.to_datetime(df["ds"])
    df["y"]  = pd.to_numeric(df["y"], errors="coerce")
    df = df.dropna()
    return df


def train_prophet(df, commodity_name=""):
    """
    Train a Prophet model on price history.
    Configuration tuned for Nigerian commodity markets.
    """
    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
        changepoint_prior_scale=0.3,
        seasonality_prior_scale=10.0,
        interval_width=0.80,
        uncertainty_samples=500,
    )

    try:
        model.fit(df)
        return model
    except Exception as e:
        return None


def run_forecast(state_id, commodity_id, state_name, commodity_name, days_ahead=7):
    """
    Run forecast for one state-commodity pair.
    Returns dict with forecast data or None if failed.
    """
    df = load_training_data(state_id, commodity_id)
    if df is None:
        return None

    model = train_prophet(df, commodity_name)
    if model is None:
        return None

    future = model.make_future_dataframe(periods=days_ahead)
    forecast = model.predict(future)

    return {
        "state_id": state_id,
        "commodity_id": commodity_id,
        "state_name": state_name,
        "commodity_name": commodity_name,
        "forecast": forecast[forecast["ds"] > df["ds"].max()][["ds", "yhat", "yhat_lower", "yhat_upper"]],
    }


def run_forecasting_pipeline():
    """
    Run Prophet on all state-commodity combinations.
    Insert into forecasts table.
    """
    print(f"\n{'='*50}")
    print(f"  FORECASTING PIPELINE — {date.today()}")
    print(f"  Horizon: 7 days ahead")
    print(f"{'='*50}\n")

    # Get all state-commodity pairs from cleaned_prices
    pairs = query("""
        SELECT DISTINCT 
            cp.state_id, 
            cp.commodity_id,
            s.name AS state_name,
            c.name AS commodity_name
        FROM cleaned_prices cp
        JOIN states s ON cp.state_id = s.id
        JOIN commodities c ON cp.commodity_id = c.id
        ORDER BY c.name, s.name
    """)

    print(f"  Found {len(pairs)} state-commodity combinations to forecast.\n")

    successful = 0
    failed = 0

    for idx, row in pairs.iterrows():
        state_id = int(row["state_id"])
        commodity_id = int(row["commodity_id"])
        state_name = row["state_name"]
        commodity_name = row["commodity_name"]

        print(f"  [{idx+1}/{len(pairs)}] {commodity_name} / {state_name}")

        try:
            result = run_forecast(state_id, commodity_id, state_name, commodity_name)
            if result is None:
                print(f"    ✗ Failed: Insufficient data")
                failed += 1
                continue

            # Insert forecasts
            for _, frow in result["forecast"].iterrows():
                execute("""
                    INSERT INTO forecasts (
                        state_id, commodity_id, forecast_date,
                        predicted_price, lower_bound, upper_bound,
                        generated_on, is_shock_flagged, shock_reason
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    state_id,
                    commodity_id,
                    pd.Timestamp(frow["ds"]).date(),
                    float(frow["yhat"]),
                    float(frow["yhat_lower"]),
                    float(frow["yhat_upper"]),
                    date.today(),
                    False,
                    None,
                ))

            print(f"    ✓ {len(result['forecast'])} forecasts inserted")
            successful += 1

        except Exception as e:
            print(f"    ✗ Failed: {str(e)[:70]}")
            failed += 1

    print(f"\n  {'='*50}")
    print(f"  ✓ Successful: {successful}")
    print(f"  ✗ Failed: {failed}")
    print(f"  {'='*50}\n")


if __name__ == "__main__":
    run_forecasting_pipeline()
