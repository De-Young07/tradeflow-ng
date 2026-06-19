"""
TradeFlow NG — Recommendations Router (public summary)
"""
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from models.database import get_db
from auth import require_admin

router = APIRouter(dependencies=[Depends(require_admin)])


@router.get("/summary")
async def recommendations_summary(db: AsyncSession = Depends(get_db)):
    res = await db.execute(text("""
        SELECT COUNT(*) AS total,
               SUM(expected_profit_ngn) AS total_profit,
               AVG(profit_margin_pct)   AS avg_margin,
               SUM(CASE WHEN is_backhaul IS NOT FALSE AND is_backhaul THEN 1 ELSE 0 END) AS backhauls,
               SUM(CASE WHEN is_shock_flagged IS NOT FALSE AND is_shock_flagged THEN 1 ELSE 0 END) AS shocks
        FROM optimization_recommendations
        WHERE run_id = (SELECT MAX(id) FROM optimization_runs)
    """))
    row = dict(res.mappings().first() or {})
    return {"data": row, "status": "ok"}
