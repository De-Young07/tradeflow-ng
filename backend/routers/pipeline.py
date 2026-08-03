  
  """
TradeFlow NG — Pipeline Router
Triggers cleaning → forecasting → optimization from API.
"""
import time
import sys
import os
from fastapi import APIRouter, Depends, BackgroundTasks
import asyncpg
from models.database import get_db
from models.schemas import PipelineResult
from auth import require_admin

router = APIRouter(dependencies=[Depends(require_admin)])

# Add src/ to path so existing ML modules are importable
SRC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "..", "src")
if os.path.exists(SRC_DIR) and SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


def _run_pipeline_sync() -> dict:
    """Run the full ML pipeline synchronously in a background thread."""
     
    start = time.time()
    result = {
        "records_cleaned": 0,
        "forecasts_written": 0,
        "recommendations_generated": 0,
        "error": None,
    }
    try:
        from cleaning import run_cleaning_pipeline
        cleaned = run_cleaning_pipeline(source="raw") or 0
        result["records_cleaned"] = cleaned
    except Exception as e:
        result["error"] = f"Cleaning failed: {e}"
        result["status"] = "partial"
        result["duration_secs"] = round(time.time() - start, 2)
        return result

    try:
        from forecasting import run_forecasting_pipeline
        forecasts = run_forecasting_pipeline(periods=7) or 0
        result["forecasts_written"] = forecasts
    except Exception as e:
        result["error"] = f"Forecasting failed: {e}"
        result["status"] = "partial"
        result["duration_secs"] = round(time.time() - start, 2)
        return result

    try:
        from optimization import run_optimization_pipeline
        recs = run_optimization_pipeline()
        result["recommendations_generated"] = len(recs) if recs is not None else 0
    except Exception as e:
        result["error"] = f"Optimization failed: {e}"
        result["status"] = "partial"
        result["duration_secs"] = round(time.time() - start, 2)
        return result

    result["status"] = "success"
    result["duration_secs"] = round(time.time() - start, 2)
    return result


@router.post("/run")
async def run_pipeline(background_tasks: BackgroundTasks):
    """
    Trigger the full pipeline asynchronously.
    Returns immediately with a job ID; pipeline runs in background.
    """
    import uuid
    job_id = str(uuid.uuid4())[:8]
    # For production: use a task queue (Celery/RQ). For MVP: run in background.
    background_tasks.add_task(_run_pipeline_sync)
    return {
        "data": {
            "job_id": job_id,
            "message": "Pipeline started. Check /health for last run time.",
            "stages": ["cleaning", "forecasting", "optimization"],
        },
        "status": "ok",
        "error": None,
    }


@router.post("/run/cleaning")
async def run_cleaning_only(background_tasks: BackgroundTasks):
    def _clean():
        try:
            from cleaning import run_cleaning_pipeline
            run_cleaning_pipeline(source="raw")
        except Exception as e:
            print(f"Cleaning error: {e}")
    background_tasks.add_task(_clean)
    return {"data": {"started": "cleaning"}, "status": "ok"}


@router.post("/run/forecasting")
async def run_forecasting_only(background_tasks: BackgroundTasks):
    def _forecast():
        try:
            from forecasting import run_forecasting_pipeline
            run_forecasting_pipeline(periods=7)
        except Exception as e:
            print(f"Forecasting error: {e}")
    background_tasks.add_task(_forecast)
    return {"data": {"started": "forecasting"}, "status": "ok"}


@router.post("/run/optimization")
async def run_optimization_only(background_tasks: BackgroundTasks):
    def _optimize():
        try:
            from optimization import run_optimization_pipeline
            run_optimization_pipeline()
        except Exception as e:
            print(f"Optimization error: {e}")
    background_tasks.add_task(_optimize)
    return {"data": {"started": "optimization"}, "status": "ok"}


@router.get("/logs")
async def pipeline_logs(db: asyncpg.Connection = Depends(get_db)):
    rows = await db.fetch("""
        SELECT id, run_type, status, records_in, records_out,
               error_message, duration_secs, run_at
        FROM   pipeline_logs
        ORDER BY run_at DESC
        LIMIT 50
    """)
    return {"data": [dict(r) for r in rows], "status": "ok"}
