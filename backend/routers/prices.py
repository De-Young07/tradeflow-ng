"""
TradeFlow NG — Prices Router (public endpoint)
"""
from fastapi import APIRouter, Depends
import asyncpg
from models.database import get_db

router = APIRouter()

@router.get("/latest")
async def latest_prices(db: asyncpg.Connection = Depends(get_db)):
    rows = await db.fetch("""
        SELECT s.name AS state, c.name AS commodity,
               cp.price_per_unit AS price, cp.price_date,
               m.name AS market
        FROM   cleaned_prices cp
        JOIN   states      s ON cp.state_id     = s.id
        JOIN   commodities c ON cp.commodity_id = c.id
        LEFT JOIN markets  m ON cp.market_id    = m.id
        WHERE  cp.price_date = (
            SELECT MAX(cp2.price_date)
            FROM   cleaned_prices cp2
            WHERE  cp2.state_id     = cp.state_id
              AND  cp2.commodity_id = cp.commodity_id
        )
        AND cp.is_outlier IS NOT TRUE
        ORDER BY c.name, s.name
    """)
    return {"data": [dict(r) for r in rows], "status": "ok", "error": None}
