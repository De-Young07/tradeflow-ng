"""
TradeFlow NG — Prophet Forecasting Module
Generates 7-day price forecasts per commodity per state.
All DB access goes through db_adapter (handles SQLite ↔ PostgreSQL).
"""

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta, timezone
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

# ── Reliability under stale training data ──────────────────
# When the newest confirmed training point is older than this many days, the
# trend can't be trusted: train with flat growth (level + seasonality only)
# and mark the forecast STALE so the optimizer can suppress recs built on it.
STALENESS_MAX_DAYS = 30
# Even when data is not "stale" (> STALENESS_MAX_DAYS), a linear trend must not
# be extrapolated far beyond the last confirmed point, or it runs away and rails
# to the clamp band below — yielding a flat, zero-width, artificially-extreme
# forecast. When the first forecast day is more than this many days past the last
# confirmed point, train with flat growth (level + seasonality) instead: a
# conservative in-band forecast that is still trusted (NOT marked stale), just no
# longer trend-extrapolated.
MAX_TREND_EXTRAP_DAYS = 14
# Yearly seasonality needs a multi-year window to be identifiable. On a short
# training span Prophet's yearly Fourier basis is unconstrained and extrapolates
# explosively just past the data (₦600k+ swings that the clamp then masks as a
# railed ceiling — the real cause of the flat, zero-width artifact). Enable yearly
# only once the span reaches this many days; it auto-turns-on as history
# accumulates. Weekly seasonality (needs only weeks) and the trend still model the
# real signal meanwhile.
YEARLY_MIN_SPAN_DAYS = 730
# Weekly seasonality likewise needs enough observations to identify a 7-day
# cycle. On a handful of sparse points it overfits and swings hard enough to rail
# against the clamp floor/ceiling. Require at least this many training rows before
# fitting weekly; below it, fall back to level + trend only.
WEEKLY_MIN_ROWS = 20
# Bound every forecast to a plausible band around observed history:
# [hist_min·(1-CLAMP_MARGIN), hist_max·(1+CLAMP_MARGIN)]. Replaces the old
# clip(lower=0) that turned negative runaways into a false 0.0 and left
# positive runaways unbounded.
CLAMP_MARGIN = 0.5
# Marker prefix written into shock_reason for stale forecasts; the optimizer
# keys off this exact token — keep it in sync with optimization.STALE_MARKER.
STALE_MARKER = "STALE_TRAINING"


def _utc_today():
    """Canonical pipeline date. UTC so the write side (generated_on) agrees
    with the optimizer's server-side reads regardless of machine timezone."""
    return datetime.now(timezone.utc).date()


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

def train_prophet(df, commodity_name="", flat_growth=False):
    # flat_growth: the last confirmed point sits far enough behind the forecast
    # window that a linear trend can't be trusted (stale data, or simply a long
    # gap to the target). Flat growth => level + seasonality only, so the forecast
    # can't run away and rail to the clamp band.
    # Seasonalities are gated on data sufficiency — each explodes out-of-sample
    # when under-identified: yearly on <2yr span (see YEARLY_MIN_SPAN_DAYS),
    # weekly on too few rows (see WEEKLY_MIN_ROWS). Below threshold we fall back
    # to level + trend, which the clamp then keeps in-band.
    n_rows     = len(df)
    span_days  = int((df["ds"].max() - df["ds"].min()).days) if n_rows else 0
    use_yearly = span_days >= YEARLY_MIN_SPAN_DAYS
    use_weekly = n_rows >= WEEKLY_MIN_ROWS and span_days >= 14
    model = Prophet(
        growth="flat" if flat_growth else "linear",
        yearly_seasonality=use_yearly,
        weekly_seasonality=use_weekly,
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
    today     = pd.Timestamp(_utc_today())
    
    # How many days from last training point to today
    days_gap      = max((today - last_date).days, 0)
    # Total periods = gap + how many future days we want
    total_periods = days_gap + periods

    future   = model.make_future_dataframe(periods=total_periods, freq="D")
    forecast = model.predict(future)

    # Now filter to only FUTURE dates (after today)
    forecast = forecast[forecast["ds"] > today].head(periods).copy()
    forecast = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()

    # Clamp to a plausible band around observed history. Bounds a runaway
    # trend instead of masking it with clip(lower=0) (which turned negative
    # runaways into a false 0.0 and left positive runaways unbounded).
    hist_min = float(df_train["y"].min())
    hist_max = float(df_train["y"].max())
    floor    = max(0.0, hist_min * (1 - CLAMP_MARGIN))
    ceil     = hist_max * (1 + CLAMP_MARGIN)
    for col in ("yhat", "yhat_lower", "yhat_upper"):
        forecast[col] = forecast[col].clip(lower=floor, upper=ceil)

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
                    model_version="prophet_v2.0",
                    data_age_days=0, stale=False):
    """
    Delete-then-insert approach — avoids ON CONFLICT translation issues.
    Deletes existing forecasts for this state-commodity before inserting fresh ones.

    `stale`/`data_age_days` carry the training-data-age signal: for stale
    series each row is force-flagged and its shock_reason is prefixed with
    STALE_TRAINING:<n>d so the optimizer can suppress recs built on it.
    """
    today       = str(_utc_today())
    row_version = f"{model_version}_flat" if stale else model_version
    inserted = 0
    skipped  = 0

    if forecast_df.empty:
        return 0, 0

    # Step 1 — Delete any existing forecasts for this combo
    # so we can write fresh ones cleanly
    try:
        db_execute("""
            DELETE FROM forecasts
            WHERE state_id     = ?
              AND commodity_id = ?
              AND generated_on = ?
        """, (int(state_id), int(commodity_id), today))
    except Exception as e:
        print(f"      Warning on delete: {e}")

    # Step 2 — Insert fresh forecasts
    for _, row in forecast_df.iterrows():
        is_shock, shock_reason = detect_shock(row, historical_mean, historical_std)
        if stale:
            tag = f"{STALE_MARKER}:{int(data_age_days)}d"
            shock_reason = f"{tag} | {shock_reason}" if shock_reason else tag
            is_shock = True
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
                row_version,
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

def run_forecasting_pipeline(periods=7, model_version="prophet_v2.0"):
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
        WHERE  cp.is_outlier   = FALSE
          AND  cp.is_confirmed = TRUE
        ORDER BY c.name, s.name
    """)

    total_combos   = len(combos)
    total_inserted = 0
    total_skipped  = 0
    total_shocks   = 0
    skipped_combos = []
    errors         = []

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

            # Age of the newest confirmed training point (UTC-consistent).
            last_train_date = pd.Timestamp(df["ds"].max())
            data_age_days   = max(
                (pd.Timestamp(_utc_today()) - last_train_date).days, 0
            )
            # Two escalating thresholds off the same data-age signal:
            #  • far_from_data → flat growth (trend can't span the gap) but the
            #    forecast is still trusted/usable by the optimizer.
            #  • stale → additionally flag STALE_TRAINING so the optimizer
            #    suppresses recs built on it (data simply too old to trust).
            # The first forecast day is (data_age_days + 1) past the last point.
            far_from_data = (data_age_days + 1) > MAX_TREND_EXTRAP_DAYS
            stale         = data_age_days > STALENESS_MAX_DAYS
            use_flat      = stale or far_from_data

            model    = train_prophet(df, commodity_name, flat_growth=use_flat)
            forecast = generate_forecast(model, df, periods=periods)

            shocks_this = sum(
                detect_shock(r, hist_mean, hist_std)[0]
                for _, r in forecast.iterrows()
            )

            inserted, skipped = write_forecasts(
                forecast, state_id, commodity_id,
                hist_mean, hist_std, model_version,
                data_age_days=data_age_days, stale=stale,
            )

            total_inserted += inserted
            total_skipped  += skipped
            total_shocks   += shocks_this

            if stale:
                shock_tag = f" ⚠ STALE ({data_age_days}d old) — flat + flagged"
            elif far_from_data:
                extra     = f" — {shocks_this} HIGH-RISK days" if shocks_this else ""
                shock_tag = (f" ⓘ flat growth ({data_age_days}d gap; trend not "
                             f"extrapolated){extra}")
            elif shocks_this:
                shock_tag = f" ⚠ {shocks_this} HIGH-RISK days"
            else:
                shock_tag = ""
            print(f"    ✓ {inserted} forecast days written.{shock_tag}")

        except Exception as e:
            print(f"    ✗ Failed: {e}")
            skipped_combos.append(label)
            errors.append(f"{label}: {e}")

    duration = (datetime.now() - start).total_seconds()
    # Honest status: a run that wrote nothing is a FAILURE, not a success.
    # (Previously this always logged "Success", which is why 0-output went
    #  unnoticed for two months and silently starved the optimizer.)
    if total_inserted > 0:
        status, err = "Success", None
    elif errors:
        status, err = "Failed", f"0 forecasts written; first error: {errors[0]}"
    elif total_skipped:
        status, err = "Failed", f"0 forecasts written; {total_skipped} rows rejected on insert"
    else:
        status, err = "Failed", f"0 forecasts written; all {total_combos} combos had insufficient data"
    log_run(status, total_combos, total_inserted, error=err, duration=round(duration, 2))

    icon = "✓" if status == "Success" else "✗"
    print(f"\n{'='*52}")
    print(f"  {icon} Forecasting {status} in {round(duration,1)}s")
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
        WHERE  f.generated_on = (SELECT MAX(generated_on) FROM forecasts)
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
