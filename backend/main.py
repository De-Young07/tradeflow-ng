"""
TradeFlow NG — FastAPI Backend
Production-grade API for the TradeFlow NG agricultural trade intelligence platform.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import os

from routers import admin, agents, prices, forecasts, recommendations, pipeline
from auth import router as auth_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    from models.database import init_pool, close_pool
    await init_pool()
    print("TradeFlow NG API starting — database pool ready.")
    yield
    await close_pool()
    print("TradeFlow NG API shut down.")


app = FastAPI(
    title="TradeFlow NG API",
    description="AI-powered agricultural trade intelligence for Nigeria.",
    version="2.0.0",
    lifespan=lifespan,
)

ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "https://app.tradeflowng.com",
    "https://tradeflowng.com",
    "https://www.tradeflowng.com",
    # Add your actual Vercel deployment URL:
    "https://tradeflow-ng.vercel.app",        # ← your Vercel URL
    "https://tradeflow-ng-git-main.vercel.app", # ← preview URL
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router,            prefix="/auth",           tags=["Auth"])
app.include_router(admin.router,           prefix="/admin",          tags=["Admin"])
app.include_router(agents.router,          prefix="/agent",          tags=["Agent"])
app.include_router(prices.router,          prefix="/prices",         tags=["Prices"])
app.include_router(forecasts.router,       prefix="/forecasts",      tags=["Forecasts"])
app.include_router(recommendations.router, prefix="/recommendations", tags=["Recommendations"])
app.include_router(pipeline.router,        prefix="/pipeline",       tags=["Pipeline"])


@app.get("/health")
async def health_check():
    from models.database import pool
    from datetime import datetime
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT MAX(run_at) AS last_run FROM pipeline_logs WHERE status = 'Success'"
            )
            last_pipeline = str(row["last_run"]) if row and row["last_run"] else None
        db_status = "connected"
    except Exception as e:
        db_status = f"error: {str(e)[:60]}"
        last_pipeline = None

    return {
        "status": "ok",
        "db": db_status,
        "last_pipeline": last_pipeline,
        "timestamp": datetime.utcnow().isoformat(),
    }
