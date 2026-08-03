"""
TradeFlow NG — Agent Router
Endpoints for field agents.
"""

from fastapi import APIRouter, Depends, HTTPException
import asyncpg
from datetime import date

from models.database import get_db
from models.schemas import SubmitPriceRequest, FeedbackRequest
from auth import require_agent

router = APIRouter(dependencies=[Depends(require_agent)])


@router.get("/recommendations")
async def agent_recommendations(
    token: dict = Depends(require_agent),
    db: asyncpg.Connection = Depends(get_db),
):
    state_id = token.get("state_id")
    rows = await db.fetch("""
        SELECT r.id, r.run_id,
               c.name AS commodity_name,
               so.name AS origin, sd.name AS destination,
               r.recommended_quantity,
               r.buy_price, r.sell_price, r.transport_cost,
               r.expected_profit_ngn, r.profit_margin_pct,
               r.is_shock_flagged, r.is_backhaul,
               r.status
        FROM   optimization_recommendations r
        JOIN   corridors co ON r.corridor_id  = co.id
        JOIN   states   so  ON co.origin_state_id = so.id
        JOIN   states   sd  ON co.dest_state_id   = sd.id
        JOIN   commodities c ON r.commodity_id   = c.id
        WHERE  r.run_id = (SELECT MAX(id) FROM optimization_runs)
          AND  (co.origin_state_id = $1 OR co.dest_state_id = $1)
        ORDER BY r.expected_profit_ngn DESC
    """, state_id)
    return {"data": [dict(r) for r in rows], "status": "ok"}


@router.get("/prices/local")
async def local_prices(
    token: dict = Depends(require_agent),
    db: asyncpg.Connection = Depends(get_db),
):
    state_id = token.get("state_id")
    rows = await db.fetch("""
        SELECT c.name AS commodity, cp.price_per_unit AS price,
               cp.price_date, m.name AS market,
               cp.quantity_available, cp.is_outlier
        FROM   cleaned_prices cp
        JOIN   commodities c ON cp.commodity_id = c.id
        LEFT JOIN markets  m ON cp.market_id    = m.id
        WHERE  cp.state_id  = $1
          AND  cp.price_date = (
              SELECT MAX(cp2.price_date)
              FROM cleaned_prices cp2
              WHERE cp2.state_id = cp.state_id
                AND cp2.commodity_id = cp.commodity_id
          )
          AND  cp.is_outlier IS NOT TRUE
        ORDER BY c.name
    """, state_id)
    return {"data": [dict(r) for r in rows], "status": "ok"}


@router.post("/prices/submit")
async def submit_price(
    body: SubmitPriceRequest,
    token: dict = Depends(require_agent),
    db: asyncpg.Connection = Depends(get_db),
):
    agent_db_id = token.get("agent_db_id")
    state_id    = token.get("state_id")

    await db.execute("""
        INSERT INTO raw_submissions
            (agent_id, state_id, market_id, commodity_id,
             reported_price, reported_unit,
             quantity_available, quality_grade,
             road_condition, notes,
             submission_date, source_channel)
        VALUES
            ($1, $2, $3, $4,
             $5, NULL,
             $6, $7,
             $8, $9,
             $10, 'Agent App')
    """, agent_db_id, state_id, body.market_id, body.commodity_id,
         body.reported_price, body.quantity_available, body.quality_grade,
         body.road_condition, body.notes,
         body.obs_date or date.today())
    return {"data": {"submitted": True}, "status": "ok"}


@router.get("/submissions/recent")
async def recent_submissions(
    token: dict = Depends(require_agent),
    db: asyncpg.Connection = Depends(get_db),
):
    agent_db_id = token.get("agent_db_id")
    rows = await db.fetch("""
        SELECT rs.id, c.name AS commodity, m.name AS market,
               rs.reported_price, rs.submission_date,
               rs.quality_grade, rs.source_channel
        FROM   raw_submissions rs
        JOIN   commodities c ON rs.commodity_id = c.id
        LEFT JOIN markets  m ON rs.market_id    = m.id
        WHERE  rs.agent_id = $1
        ORDER BY rs.submission_date DESC, rs.id DESC
        LIMIT 15
    """, agent_db_id)
    return {"data": [dict(r) for r in rows], "status": "ok"}


@router.post("/report")
async def report_outcome(
    body: FeedbackRequest,
    token: dict = Depends(require_agent),
    db: asyncpg.Connection = Depends(get_db),
):
    row = await db.fetchrow("""
        SELECT r.corridor_id, r.commodity_id,
               co.dest_state_id AS state_id
        FROM   optimization_recommendations r
        JOIN   corridors co ON r.corridor_id = co.id
        WHERE  r.id = $1
    """, body.recommendation_id)
    if not row:
        raise HTTPException(status_code=404, detail="Recommendation not found")

    await db.execute("""
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
    return {"data": {"logged": True}, "status": "ok"}


# ── Lookups for agent forms ───────────────────────────────────
@router.get("/lookups")
async def agent_lookups(
    token: dict = Depends(require_agent),
    db: asyncpg.Connection = Depends(get_db),
):
    state_id = token.get("state_id")
    commodities = await db.fetch("SELECT id, name FROM commodities ORDER BY name")
    markets = await db.fetch(
        "SELECT id, name FROM markets WHERE state_id = $1 ORDER BY name", state_id
    )
    return {
        "data": {
            "commodities": [dict(r) for r in commodities],
            "markets":     [dict(r) for r in markets],
        },
        "status": "ok",
    }
