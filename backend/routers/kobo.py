"""
TradeFlow NG — KoBoToolbox ingestion webhook.

Receives a KoBoToolbox submission (posted by a KoBo REST Service) and lands one
row per commodity into `raw_submissions`. Authenticated by a shared secret in a
custom header (KoBo REST Services support custom HTTP headers), NOT a user JWT —
KoBo cannot obtain one.

Flow:  KoBo form → POST /kobo/submission → raw_submissions → cleaning.py → cleaned_prices

Design decisions (per project owner):
  - Price stored in reported_price = wholesale_price (the optimizer routes wholesale).
    retail_price is preserved in raw_json.
  - state_id is derived from the agent (the form has no state field), so every
    submission inherits the agent's registered state.
  - Idempotent: kobo_submission_id has a UNIQUE constraint; re-delivered webhooks
    are skipped via ON CONFLICT DO NOTHING.
"""

import os
import json
from datetime import date, datetime

import asyncpg
from fastapi import APIRouter, Depends, Header, HTTPException

from models.database import get_db

router = APIRouter()

# Shared secret KoBo must send in the X-Kobo-Secret header. Set on Render.
KOBO_WEBHOOK_SECRET = os.getenv("KOBO_WEBHOOK_SECRET", "")

# ── Field names as they appear in the KoBo submission JSON ────────────
# The repeat group and its leaf fields (confirmed from the form's XLSForm export).
REPEAT_GROUP   = "commodity_prices"
F_AGENT_ID     = "agent_id"
F_MARKET_NAME  = "market_name"
F_ROAD_COND    = "road_condition"
F_HIRE_COST    = "hire_cost"
F_CONFIDENCE   = "confidence_level"
F_MARKET_DAY   = "is_market_day"
F_NOTABLE      = "notable_event"
F_SUBMIT_TIME  = "_submission_time"
F_UUID         = "_uuid"

# Repeat-group leaf fields
R_COMMODITY    = "commodity_name"
R_AVAILABLE    = "prices_available"
R_UNIT         = "unit_of_measure"
R_WHOLESALE    = "wholesale_price"
R_RETAIL       = "retail_price"
R_SUPPLY       = "supply_level"
R_QUALITY      = "quality_grade"
R_DIRECTION    = "price_direction"


def _to_float(v):
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _kobo_leaf(entry: dict, leaf: str):
    """
    Repeat-group entries may key fields either by bare leaf name (`commodity_name`)
    or namespaced (`commodity_prices/commodity_name`). Accept both.
    """
    if leaf in entry:
        return entry[leaf]
    return entry.get(f"{REPEAT_GROUP}/{leaf}")


async def _require_secret(x_kobo_secret: str = Header(default="")):
    if not KOBO_WEBHOOK_SECRET:
        # Fail closed: if no secret is configured, reject rather than accept anon posts.
        raise HTTPException(status_code=503, detail="KoBo webhook not configured")
    if x_kobo_secret != KOBO_WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Invalid webhook secret")


@router.post("/submission", dependencies=[Depends(_require_secret)])
async def kobo_submission(payload: dict, db: asyncpg.Connection = Depends(get_db)):
    """
    Ingest one KoBo submission. Returns a per-commodity breakdown of what was
    inserted vs. skipped (unknown agent/commodity, no price, duplicate).
    """
    started = datetime.utcnow()

    # ── Resolve agent (and derive state from the agent) ──────────────
    agent_id_text = str(payload.get(F_AGENT_ID) or "").strip().upper()
    agent = await db.fetchrow(
        "SELECT id, state_id FROM agents WHERE UPPER(agent_id) = $1", agent_id_text
    )
    if not agent:
        await _log(db, "Failed", 1, 0, f"Unknown agent_id '{agent_id_text}'", started)
        raise HTTPException(status_code=422, detail=f"Unknown agent_id '{agent_id_text}'")

    agent_db_id = agent["id"]
    state_id    = agent["state_id"]

    # ── Resolve market (optional — may be unmapped) ──────────────────
    market_name = str(payload.get(F_MARKET_NAME) or "").strip()
    market_id = None
    if market_name:
        m = await db.fetchrow(
            "SELECT id FROM markets WHERE LOWER(name) = LOWER($1)", market_name
        )
        market_id = m["id"] if m else None

    # ── Form-level fields shared by every commodity row ──────────────
    submit_time = payload.get(F_SUBMIT_TIME)
    sub_date = (
        datetime.fromisoformat(str(submit_time).replace("Z", "+00:00")).date()
        if submit_time else date.today()
    )
    uuid_str        = str(payload.get(F_UUID) or "").strip()
    road_condition  = payload.get(F_ROAD_COND)
    transport_est   = _to_float(payload.get(F_HIRE_COST))
    confidence      = payload.get(F_CONFIDENCE)
    notes           = payload.get(F_NOTABLE)
    raw_json        = json.dumps(payload)[:4000]

    # ── Repeat group: one raw_submissions row per commodity ──────────
    entries = payload.get(REPEAT_GROUP) or []
    if not isinstance(entries, list):
        entries = []

    inserted, skipped, details = 0, 0, []

    for entry in entries:
        comm_name = str(_kobo_leaf(entry, R_COMMODITY) or "").strip().lower()

        # Honor the "prices_available" gate — skip rows the agent marked no-data.
        if str(_kobo_leaf(entry, R_AVAILABLE) or "yes").strip().lower() == "no":
            skipped += 1
            details.append({"commodity": comm_name, "status": "no_price_reported"})
            continue

        price = _to_float(_kobo_leaf(entry, R_WHOLESALE))
        if not comm_name or price is None:
            skipped += 1
            details.append({"commodity": comm_name or "?", "status": "missing_commodity_or_price"})
            continue

        comm = await db.fetchrow(
            "SELECT id FROM commodities WHERE LOWER(name) = $1", comm_name
        )
        if not comm:
            skipped += 1
            details.append({"commodity": comm_name, "status": "unknown_commodity"})
            continue

        # Idempotent per (submission, commodity): unique kobo_submission_id.
        kobo_sub_id = f"{uuid_str}_{comm_name[:8]}" if uuid_str else None

        result = await db.execute("""
            INSERT INTO raw_submissions (
                kobo_submission_id, agent_id, state_id, market_id, commodity_id,
                reported_price, reported_unit, quality_grade,
                road_condition, transport_cost_est, price_confidence,
                submission_date, notes, source_channel, raw_json
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8,
                $9, $10, $11,
                $12, $13, 'Kobo', $14
            )
            ON CONFLICT (kobo_submission_id) DO NOTHING
        """,
            kobo_sub_id, agent_db_id, state_id, market_id, comm["id"],
            price, _kobo_leaf(entry, R_UNIT), _kobo_leaf(entry, R_QUALITY),
            road_condition, transport_est, confidence,
            sub_date, notes, raw_json,
        )
        if result.endswith("0"):  # "INSERT 0 0" → conflict, nothing inserted
            skipped += 1
            details.append({"commodity": comm_name, "status": "duplicate"})
        else:
            inserted += 1
            details.append({"commodity": comm_name, "status": "inserted"})

    await _log(db, "Success", len(entries), inserted, None, started)

    return {
        "data": {
            "agent_id": agent_id_text,
            "market_id": market_id,
            "state_id": state_id,
            "inserted": inserted,
            "skipped": skipped,
            "commodities": details,
        },
        "status": "ok",
    }


async def _log(db, status, records_in, records_out, error, started):
    duration = round((datetime.utcnow() - started).total_seconds(), 2)
    try:
        await db.execute("""
            INSERT INTO pipeline_logs
                (run_type, status, records_in, records_out, error_message, duration_secs)
            VALUES ('Kobo Ingestion', $1, $2, $3, $4, $5)
        """, status, records_in, records_out, error, duration)
    except Exception:
        pass  # logging must never break ingestion
