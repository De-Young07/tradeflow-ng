"""
TradeFlow NG — Forecasts Router
"""
from fastapi import APIRouter, Depends
import asyncpg
from typing import Optional
from models.database import get_db
from auth import require_admin

router = APIRouter(dependencies=[Depends(require_admin)])


@router.get("/")
async def get_forecasts(
    state: Optional[str] = None,
    commodity: Optional[str] = None,
    db: asyncpg.Connection = Depends(get_db),
):
    # Forecast data
    sql = """
        SELECT f.forecast_date AS date,
               s.name AS state, c.name AS commodity,
               f.predicted_price, f.lower_bound, f.upper_bound,
               f.is_shock_flagged
        FROM   forecasts f
        JOIN   states      s ON f.state_id     = s.id
        JOIN   commodities c ON f.commodity_id = c.id
        WHERE  f.generated_on = (SELECT MAX(generated_on) FROM forecasts)
    """
    params = []
    if state:
        params.append(state)
        sql += f" AND s.name = ${len(params)}"
    if commodity:
        params.append(commodity)
        sql += f" AND c.name = ${len(params)}"
    sql += " ORDER BY f.forecast_date"

    forecast_rows = [dict(r) for r in await db.fetch(sql, *params)]

    # Historical data (last 56 days)
    hist_sql = """
        SELECT cp.price_date AS date,
               s.name AS state, c.name AS commodity,
               AVG(cp.price_per_unit) AS price
        FROM   cleaned_prices cp
        JOIN   states      s ON cp.state_id     = s.id
        JOIN   commodities c ON cp.commodity_id = c.id
        WHERE  cp.price_date >= CURRENT_DATE - INTERVAL '56 days'
          AND  cp.is_outlier IS NOT TRUE
    """
    hist_params = []
    if state:
        hist_params.append(state)
        hist_sql += f" AND s.name = ${len(hist_params)}"
    if commodity:
        hist_params.append(commodity)
        hist_sql += f" AND c.name = ${len(hist_params)}"
    hist_sql += " GROUP BY cp.price_date, s.name, c.name ORDER BY cp.price_date"

    hist_rows = [dict(r) for r in await db.fetch(hist_sql, *hist_params)]

    # Summary stats
    if forecast_rows:
        prices = [r["predicted_price"] for r in forecast_rows]
        shocks = sum(1 for r in forecast_rows if r["is_shock_flagged"])
        summary = {
            "next_week_avg":    round(sum(prices) / len(prices), 0),
            "price_range_low":  min(r["lower_bound"] for r in forecast_rows),
            "price_range_high": max(r["upper_bound"] for r in forecast_rows),
            "high_risk_days":   shocks,
        }
    else:
        summary = {"next_week_avg": 0, "price_range_low": 0,
                   "price_range_high": 0, "high_risk_days": 0}

    return {
        "data": {
            "historical": hist_rows,
            "forecast":   forecast_rows,
            "summary":    summary,
        },
        "status": "ok",
        "error": None,
    }


@router.get("/states")
async def forecast_states(db: asyncpg.Connection = Depends(get_db)):
    rows = await db.fetch(
        "SELECT DISTINCT s.name FROM forecasts f JOIN states s ON f.state_id=s.id ORDER BY s.name"
    )
    return {"data": [r["name"] for r in rows], "status": "ok"}


@router.get("/commodities")
async def forecast_commodities(db: asyncpg.Connection = Depends(get_db)):
    rows = await db.fetch(
        "SELECT DISTINCT c.name FROM forecasts f JOIN commodities c ON f.commodity_id=c.id ORDER BY c.name"
    )
    return {"data": [r["name"] for r in rows], "status": "ok"}
