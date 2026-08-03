"""
TradeFlow NG — Admin Router
All admin-only endpoints.
"""

from fastapi import APIRouter, Depends, HTTPException
import asyncpg
from typing import Optional

from models.database import get_db
from models.schemas import (
    CreateAgentRequest,
    FeedbackRequest,
)
from auth import require_admin
from security import hash_password

router = APIRouter(dependencies=[Depends(require_admin)])


# ── Overview ─────────────────────────────────────────────────
@router.get("/overview")
async def get_overview(db: AsyncSession = Depends(get_db)):
    # Latest optimization run stats
    opt = await db.execute(text("""
        SELECT
            SUM(r.expected_profit_ngn)  AS total_profit,
            COUNT(*)                    AS n_recommendations,
            AVG(r.profit_margin_pct)    AS avg_margin,
            SUM(CASE WHEN r.is_backhaul    IS NOT FALSE AND r.is_backhaul    THEN 1 ELSE 0 END) AS n_backhauls,
            SUM(CASE WHEN r.is_shock_flagged IS NOT FALSE AND r.is_shock_flagged THEN 1 ELSE 0 END) AS n_shocks
        FROM optimization_recommendations r
        WHERE r.run_id = (SELECT MAX(id) FROM optimization_runs)
    """))
    opt_row = opt.mappings().first() or {}

    # Agent count
    agents_res = await db.execute(text(
        "SELECT COUNT(*) AS n FROM agents WHERE is_active IS NOT FALSE"
    ))
    n_agents = (agents_res.mappings().first() or {}).get("n", 0)

    # Price records
    prices_res = await db.execute(text(
        "SELECT COUNT(*) AS n FROM cleaned_prices"
    ))
    n_prices = (prices_res.mappings().first() or {}).get("n", 0)

    # Last forecast and optimization dates
    dates_res = await db.execute(text("""
        SELECT
            (SELECT MAX(generated_on) FROM forecasts)     AS last_forecast,
            (SELECT MAX(run_date)     FROM optimization_runs) AS last_opt
    """))
    dates_row = dates_res.mappings().first() or {}

    # Pipeline logs (last 20)
    logs_res = await db.execute(text("""
        SELECT id, run_type, status, records_in, records_out,
               error_message, duration_secs, run_at
        FROM   pipeline_logs
        ORDER BY run_at DESC
        LIMIT 20
    """)]

    return {
        "data": {
            "total_profit":          float(opt_row.get("total_profit") or 0),
            "n_recommendations":     int(opt_row.get("n_recommendations") or 0),
            "avg_margin":            round(float(opt_row.get("avg_margin") or 0), 1),
            "n_backhauls":           int(opt_row.get("n_backhauls") or 0),
            "n_shock_flags":         int(opt_row.get("n_shocks") or 0),
            "n_agents":              int(n_agents),
            "n_price_records":       int(n_prices),
            "last_forecast_date":    str(dates_row.get("last_forecast") or ""),
            "last_optimization_date":str(dates_row.get("last_opt") or ""),
            "pipeline_logs":         logs,
        },
        "status": "ok",
        "error": None,
    }


# ── Recommendations ───────────────────────────────────────────
@router.get("/recommendations")
async def get_recommendations(
    run_id: Optional[str] = "latest",
    commodity: Optional[str] = None,
    risk_only: bool = False,
    backhaul_only: bool = False,
    db: asyncpg.Connection = Depends(get_db),
):
    if run_id == "latest":
        run_id = await db.fetchval("SELECT MAX(id) FROM optimization_runs")
        if not run_id:
            return {"data": [], "status": "ok", "error": None}

    params = [int(run_id)]
    sql = """
        SELECT r.id, r.run_id, r.corridor_id, r.commodity_id,
               c.name AS commodity_name,
               so.name AS origin,
               sd.name AS destination,
               r.recommended_quantity,
               r.buy_price, r.sell_price, r.transport_cost,
               (r.sell_price - r.buy_price - r.transport_cost) AS profit_per_unit,
               r.expected_profit_ngn, r.profit_margin_pct,
               r.is_shock_flagged, r.is_backhaul,
               r.status
        FROM   optimization_recommendations r
        JOIN   commodities c  ON r.commodity_id = c.id
        JOIN   corridors   co ON r.corridor_id  = co.id
        JOIN   states so ON co.origin_state_id = so.id
        JOIN   states sd ON co.dest_state_id   = sd.id
        WHERE  r.run_id = $1
    """

    if commodity:
        params.append(commodity)
        sql += f" AND c.name = ${len(params)}"
    if risk_only:
        sql += " AND r.is_shock_flagged IS NOT FALSE AND r.is_shock_flagged"
    if backhaul_only:
        sql += " AND r.is_backhaul IS NOT FALSE AND r.is_backhaul"

    sql += " ORDER BY r.expected_profit_ngn DESC"

    rows = [dict(r) for r in await db.fetch(sql, *params)]
    return {"data": rows, "status": "ok", "error": None}


# ── Agents ───────────────────────────────────────────────────
@router.get("/agents")
async def list_agents(db: asyncpg.Connection = Depends(get_db)):
    rows = await db.fetch("""
        SELECT a.id, a.full_name, a.agent_id, a.phone,
               s.name AS state, m.name AS market,
               a.is_active,
               COUNT(rs.id) AS submission_count
        FROM   agents a
        LEFT JOIN states  s  ON a.state_id  = s.id
        LEFT JOIN markets m  ON a.market_id = m.id
        LEFT JOIN raw_submissions rs ON rs.agent_id = a.id
        GROUP BY a.id, a.full_name, a.agent_id, a.phone,
                 s.name, m.name, a.is_active
        ORDER BY a.agent_id
    """")
    return {"data": [dict(r) for r in rows], "status": "ok", "error": None}


@router.post("/agents")
async def create_agent(body: CreateAgentRequest, db: asyncpg.Connection = Depends(get_db)):
    aid = body.agent_id.strip().upper()
    # Check duplicate agent_id
    exists = await db.fetchval("SELECT id FROM agents WHERE agent_id = $1", aid)
    if exists:
        raise HTTPException(status_code=409, detail="Agent ID already exists")

    await db.execute("""
        INSERT INTO agents (full_name, agent_id, password, phone,
                            state_id, market_id, is_active)
        VALUES ($1, $2, $3, $4, $5, $6, TRUE)
    """, body.full_name, aid, hash_password(body.password), body.phone,
         body.state_id, body.market_id)
    return {"data": {"agent_id": body.agent_id, "created": True}, "status": "ok"}


# ── Price trends ──────────────────────────────────────────────
@router.get("/prices/trend")
async def price_trend(
    commodity: str = "Yam",
    days: int = 56,
    db: asyncpg.Connection = Depends(get_db),
):
    res = await db.execute(text("""
        SELECT s.name AS state, cp.price_date AS date,
               AVG(cp.price_per_unit) AS price
        FROM   cleaned_prices cp
        JOIN   states      s ON cp.state_id     = s.id
        JOIN   commodities c ON cp.commodity_id = c.id
        WHERE  c.name = $1
          AND  cp.price_date >= CURRENT_DATE - (INTERVAL '1 day' * {int(days)})
          AND  cp.is_outlier IS NOT TRUE
        GROUP BY s.name, cp.price_date
        ORDER BY cp.price_date, s.name
    """), {"commodity": commodity, "days": days})
    rows = [dict(r) for r in res.mappings().all()]
    return {"data": rows, "status": "ok", "error": None}


# ── Tableau / profit matrix ───────────────────────────────────
@router.get("/tableau")
async def get_tableau(commodity: str = "Yam", db: AsyncSession = Depends(get_db)):
    res = await db.execute(text("""
        SELECT so.name AS origin,
               sd.name AS destination,
               c.name  AS commodity,
               r.expected_profit_ngn / NULLIF(r.recommended_quantity, 0) AS profit_per_unit,
               r.profit_margin_pct,
               r.expected_profit_ngn > 0 AS is_profitable
        FROM   optimization_recommendations r
        JOIN   corridors co ON r.corridor_id  = co.id
        JOIN   states   so  ON co.origin_state_id = so.id
        JOIN   states   sd  ON co.dest_state_id   = sd.id
        JOIN   commodities c ON r.commodity_id   = c.id
        WHERE  r.run_id = (SELECT MAX(id) FROM optimization_runs)
          AND  c.name   = :commodity
        ORDER BY profit_per_unit DESC
    """), {"commodity": commodity})
    rows = [dict(r) for r in res.mappings().all()]
    return {"data": rows, "status": "ok", "error": None}


# ── Feedback ──────────────────────────────────────────────────
@router.post("/feedback")
async def log_feedback(body: FeedbackRequest, db: AsyncSession = Depends(get_db)):
    # Get corridor and commodity from recommendation
    rec = await db.execute(text("""
        SELECT r.corridor_id, r.commodity_id,
               co.dest_state_id AS state_id
        FROM   optimization_recommendations r
        JOIN   corridors co ON r.corridor_id = co.id
        WHERE  r.id = :id
    """), {"id": body.recommendation_id})
    row = rec.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Recommendation not found")

    await db.execute(text("""
        INSERT INTO actual_outcomes
            (recommendation_id, state_id, commodity_id,
             actual_buy_price, actual_sell_price,
             actual_transport_cost, actual_quantity,
             trade_date, outcome_notes)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    """, body.recommendation_id, row["state_id"], row["commodity_id"],
         body.actual_buy_price, body.actual_sell_price,
         body.actual_transport_cost, body.actual_quantity,
         body.trade_date, body.notes)
    return {"data": {"logged": True}, "status": "ok", "error": None}


# ── Database inspector ───────────────────────────────────────
@router.get("/db/stats")
async def db_stats(db: AsyncSession = Depends(get_db)):
    tables = [
        "states", "markets", "commodities", "agents",
        "raw_submissions", "cleaned_prices", "forecasts",
        "optimization_runs", "optimization_recommendations",
        "actual_outcomes", "pipeline_logs",
    ]
    counts = {}
    for table in tables:
        try:
            res = await db.execute(text(f"SELECT COUNT(*) AS n FROM {table}"))
            counts[table] = int((res.mappings().first() or {}).get("n", 0))
        except Exception:
            counts[table] = -1
    return {"data": counts, "status": "ok", "error": None}


# ── Lookups for dropdowns ───────────────────────────────────
@router.get("/lookups")
async def get_lookups(db: asyncpg.Connection = Depends(get_db)):
    states = await db.fetch("SELECT id, name FROM states ORDER BY name")
    markets = await db.fetch("SELECT id, name, state_id FROM markets ORDER BY name")
    commodities = await db.fetch("SELECT id, name FROM commodities ORDER BY name")
    return {
        "data": {
            "states":      [dict(r) for r in states],
            "markets":     [dict(r) for r in markets],
            "commodities": [dict(r) for r in commodities],
        },
        "status": "ok",
    }
