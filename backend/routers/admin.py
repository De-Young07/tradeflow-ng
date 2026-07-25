"""
TradeFlow NG — Admin Router
All admin-only endpoints.
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import Optional

from models.database import get_db
from models.schemas import (
    CreateAgentRequest,
    FeedbackRequest,
)
from auth import require_admin

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
    """))
    logs = [dict(r) for r in logs_res.mappings().all()]

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
    db: AsyncSession = Depends(get_db),
):
    if run_id == "latest":
        res = await db.execute(text("SELECT MAX(id) AS id FROM optimization_runs"))
        run_id = (res.mappings().first() or {}).get("id")
        if not run_id:
            return {"data": [], "status": "ok", "error": None}

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
        WHERE  r.run_id = :run_id
    """
    params = {"run_id": int(run_id)}

    if commodity:
        sql += " AND c.name = :commodity"
        params["commodity"] = commodity
    if risk_only:
        sql += " AND r.is_shock_flagged IS NOT FALSE AND r.is_shock_flagged"
    if backhaul_only:
        sql += " AND r.is_backhaul IS NOT FALSE AND r.is_backhaul"

    sql += " ORDER BY r.expected_profit_ngn DESC"

    res = await db.execute(text(sql), params)
    rows = [dict(r) for r in res.mappings().all()]

    return {"data": rows, "status": "ok", "error": None}


# ── Agents ────────────────────────────────────────────────────
@router.get("/agents")
async def list_agents(db: AsyncSession = Depends(get_db)):
    res = await db.execute(text("""
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
    """))
    rows = [dict(r) for r in res.mappings().all()]
    return {"data": rows, "status": "ok", "error": None}


@router.post("/agents")
async def create_agent(body: CreateAgentRequest, db: AsyncSession = Depends(get_db)):
    # Check duplicate agent_id
    exists = await db.execute(text(
        "SELECT id FROM agents WHERE agent_id = :aid"
    ), {"aid": body.agent_id.strip().upper()})
    if exists.mappings().first():
        raise HTTPException(status_code=409, detail="Agent ID already exists")

    await db.execute(text("""
        INSERT INTO agents (full_name, agent_id, password, phone,
                            state_id, market_id, is_active)
        VALUES (:name, :aid, :pwd, :phone, :sid, :mid, TRUE)
    """), {
        "name":  body.full_name,
        "aid":   body.agent_id.strip().upper(),
        "pwd":   body.password,
        "phone": body.phone,
        "sid":   body.state_id,
        "mid":   body.market_id,
    })
    await db.commit()
    return {"data": {"agent_id": body.agent_id, "created": True}, "status": "ok"}


# ── Price trends ──────────────────────────────────────────────
@router.get("/prices/trend")
async def price_trend(
    commodity: str = "Yam",
    days: int = 56,
    db: AsyncSession = Depends(get_db),
):
    res = await db.execute(text("""
        SELECT s.name AS state, cp.price_date AS date,
               AVG(cp.price_per_unit) AS price
        FROM   cleaned_prices cp
        JOIN   states      s ON cp.state_id     = s.id
        JOIN   commodities c ON cp.commodity_id = c.id
        WHERE  c.name = :commodity
          AND  cp.price_date >= CURRENT_DATE - INTERVAL ':days days'
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
             trade_date, notes)
        VALUES
            (:rec_id, :sid, :cid,
             :buy, :sell, :transport, :qty,
             :trade_date, :notes)
    """), {
        "rec_id":    body.recommendation_id,
        "sid":       row["state_id"],
        "cid":       row["commodity_id"],
        "buy":       body.actual_buy_price,
        "sell":      body.actual_sell_price,
        "transport": body.actual_transport_cost,
        "qty":       body.actual_quantity,
        "trade_date":str(body.trade_date),
        "notes":     body.notes,
    })
    await db.commit()
    return {"data": {"logged": True}, "status": "ok"}


# ── Database inspector ────────────────────────────────────────
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
    return {"data": counts, "status": "ok"}


# ── Lookups for dropdowns ─────────────────────────────────────
@router.get("/lookups")
async def get_lookups(db: AsyncSession = Depends(get_db)):
    states = await db.execute(text("SELECT id, name FROM states ORDER BY name"))
    markets = await db.execute(text("SELECT id, name, state_id FROM markets ORDER BY name"))
    commodities = await db.execute(text("SELECT id, name FROM commodities ORDER BY name"))
    return {
        "data": {
            "states":      [dict(r) for r in states.mappings().all()],
            "markets":     [dict(r) for r in markets.mappings().all()],
            "commodities": [dict(r) for r in commodities.mappings().all()],
        },
        "status": "ok",
    }
