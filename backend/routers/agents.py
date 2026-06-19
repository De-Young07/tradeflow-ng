"""
TradeFlow NG — Agent Router
Endpoints for field agents.
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from datetime import date

from models.database import get_db
from models.schemas import SubmitPriceRequest, FeedbackRequest
from auth import require_agent

router = APIRouter(dependencies=[Depends(require_agent)])


@router.get("/recommendations")
async def agent_recommendations(
    token: dict = Depends(require_agent),
    db: AsyncSession = Depends(get_db),
):
    state_id = token.get("state_id")
    res = await db.execute(text("""
        SELECT r.id, r.run_id,
               c.name AS commodity_name,
               so.name AS origin, sd.name AS destination,
               r.recommended_quantity,
               r.buy_price, r.sell_price, r.transport_cost,
               r.expected_profit_ngn, r.profit_margin_pct,
               r.is_shock_flagged, r.is_backhaul,
               r.backhaul_note, r.status
        FROM   optimization_recommendations r
        JOIN   corridors co ON r.corridor_id  = co.id
        JOIN   states   so  ON co.origin_state_id = so.id
        JOIN   states   sd  ON co.dest_state_id   = sd.id
        JOIN   commodities c ON r.commodity_id   = c.id
        WHERE  r.run_id = (SELECT MAX(id) FROM optimization_runs)
          AND  (co.origin_state_id = :sid OR co.dest_state_id = :sid)
        ORDER BY r.expected_profit_ngn DESC
    """), {"sid": state_id})
    rows = [dict(r) for r in res.mappings().all()]
    return {"data": rows, "status": "ok"}


@router.get("/prices/local")
async def local_prices(
    token: dict = Depends(require_agent),
    db: AsyncSession = Depends(get_db),
):
    state_id = token.get("state_id")
    res = await db.execute(text("""
        SELECT c.name AS commodity, cp.price_per_unit AS price,
               cp.price_date, m.name AS market,
               cp.quantity_available, cp.is_outlier
        FROM   cleaned_prices cp
        JOIN   commodities c ON cp.commodity_id = c.id
        LEFT JOIN markets  m ON cp.market_id    = m.id
        WHERE  cp.state_id  = :sid
          AND  cp.price_date = (
              SELECT MAX(cp2.price_date)
              FROM cleaned_prices cp2
              WHERE cp2.state_id = cp.state_id
                AND cp2.commodity_id = cp.commodity_id
          )
          AND  cp.is_outlier IS NOT TRUE
        ORDER BY c.name
    """), {"sid": state_id})
    rows = [dict(r) for r in res.mappings().all()]
    return {"data": rows, "status": "ok"}


@router.post("/prices/submit")
async def submit_price(
    body: SubmitPriceRequest,
    token: dict = Depends(require_agent),
    db: AsyncSession = Depends(get_db),
):
    agent_db_id = token.get("agent_db_id")
    state_id    = token.get("state_id")

    await db.execute(text("""
        INSERT INTO raw_submissions
            (agent_id, state_id, market_id, commodity_id,
             reported_price, reported_unit,
             quantity_available, quality_grade,
             road_condition, notes,
             submission_date, source_channel)
        VALUES
            (:agent_id, :state_id, :market_id, :commodity_id,
             :price, NULL,
             :qty, :quality,
             :road, :notes,
             :sub_date, 'Agent App')
    """), {
        "agent_id":    agent_db_id,
        "state_id":    state_id,
        "market_id":   body.market_id,
        "commodity_id":body.commodity_id,
        "price":       body.reported_price,
        "qty":         body.quantity_available,
        "quality":     body.quality_grade,
        "road":        body.road_condition,
        "notes":       body.notes,
        "sub_date":    str(body.obs_date or date.today()),
    })
    await db.commit()
    return {"data": {"submitted": True}, "status": "ok"}


@router.get("/submissions/recent")
async def recent_submissions(
    token: dict = Depends(require_agent),
    db: AsyncSession = Depends(get_db),
):
    agent_db_id = token.get("agent_db_id")
    res = await db.execute(text("""
        SELECT rs.id, c.name AS commodity, m.name AS market,
               rs.reported_price, rs.submission_date,
               rs.quality_grade, rs.source_channel
        FROM   raw_submissions rs
        JOIN   commodities c ON rs.commodity_id = c.id
        LEFT JOIN markets  m ON rs.market_id    = m.id
        WHERE  rs.agent_id = :aid
        ORDER BY rs.submission_date DESC, rs.id DESC
        LIMIT 15
    """), {"aid": agent_db_id})
    rows = [dict(r) for r in res.mappings().all()]
    return {"data": rows, "status": "ok"}


@router.post("/report")
async def report_outcome(
    body: FeedbackRequest,
    token: dict = Depends(require_agent),
    db: AsyncSession = Depends(get_db),
):
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
        VALUES (:rec_id, :sid, :cid, :buy, :sell, :transport, :qty, :date, :notes)
    """), {
        "rec_id":    body.recommendation_id,
        "sid":       row["state_id"],
        "cid":       row["commodity_id"],
        "buy":       body.actual_buy_price,
        "sell":      body.actual_sell_price,
        "transport": body.actual_transport_cost,
        "qty":       body.actual_quantity,
        "date":      str(body.trade_date),
        "notes":     body.notes,
    })
    await db.commit()
    return {"data": {"logged": True}, "status": "ok"}


# ── Lookups for agent forms ───────────────────────────────────
@router.get("/lookups")
async def agent_lookups(
    token: dict = Depends(require_agent),
    db: AsyncSession = Depends(get_db),
):
    state_id = token.get("state_id")
    commodities = await db.execute(text(
        "SELECT id, name FROM commodities ORDER BY name"
    ))
    markets = await db.execute(text(
        "SELECT id, name FROM markets WHERE state_id = :sid ORDER BY name"
    ), {"sid": state_id})

    return {
        "data": {
            "commodities": [dict(r) for r in commodities.mappings().all()],
            "markets":     [dict(r) for r in markets.mappings().all()],
        },
        "status": "ok",
    }
