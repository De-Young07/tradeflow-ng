# KoBoToolbox → TradeFlow NG Integration

How field submissions from the KoBoToolbox form reach the database.

## Flow

```
KoBo form  ──REST Service (webhook)──►  POST /kobo/submission  ──►  raw_submissions
                                              │                          │
                                        X-Kobo-Secret header       cleaning.py
                                        (shared secret)                  ▼
                                                                   cleaned_prices ──► forecasting / optimization
```

One submission carries a `commodity_prices` **repeat group**; the webhook writes
**one `raw_submissions` row per commodity**.

## Endpoint

`POST /kobo/submission`

- **Auth:** header `X-Kobo-Secret: <KOBO_WEBHOOK_SECRET>`. Missing/wrong → `401`.
  If the env var is unset the endpoint fails closed with `503` (never accepts anonymous posts).
- **Body:** the raw KoBo submission JSON.
- **Response:** per-commodity breakdown — `inserted` / `skipped` with a reason
  (`no_price_reported`, `unknown_commodity`, `missing_commodity_or_price`, `duplicate`).

### Field mapping (form → raw_submissions)

| raw_submissions        | KoBo field                          | Notes |
|------------------------|-------------------------------------|-------|
| `reported_price`       | `commodity_prices[].wholesale_price`| wholesale is canonical; retail preserved in `raw_json` |
| `reported_unit`        | `commodity_prices[].unit_of_measure`| |
| `quality_grade`        | `commodity_prices[].quality_grade`  | e.g. `grade_a` |
| `commodity_id`         | `commodity_prices[].commodity_name` | resolved via `commodities` (case-insensitive) |
| `agent_id`             | `agent_id`                          | resolved via `agents.agent_id` (e.g. `TFN-KG-001`) |
| `state_id`             | — derived from the agent            | the form has no state field |
| `market_id`            | `market_name`                       | resolved via `markets`; null if unmapped |
| `road_condition`       | `road_condition`                    | form-level, applied to every row |
| `transport_cost_est`   | `hire_cost`                         | |
| `price_confidence`     | `confidence_level`                  | |
| `notes`                | `notable_event`                     | |
| `submission_date`      | `_submission_time`                  | date portion |
| `kobo_submission_id`   | `_uuid` + `_<commodity>`            | UNIQUE → idempotent re-delivery |
| `source_channel`       | constant `'Kobo'`                   | |

Rows where `prices_available == "no"` are skipped (agent reported no data).

## Setup

### 1. Render env var

Add to the backend service on Render:

```
KOBO_WEBHOOK_SECRET = <a long random string>
```

(The existing `KOBO_API_TOKEN` / `KOBO_ASSET_UID` are for API polling and are not
used by this webhook path.)

### 2. KoBo REST Service

In the KoBo form → **Settings → REST Services → Register a New Service**:

- **Endpoint URL:** `https://tradeflow-ng.onrender.com/kobo/submission`
- **Custom HTTP Headers:** add one —
  - Name: `X-Kobo-Secret`
  - Value: the same string as `KOBO_WEBHOOK_SECRET`
- Type: JSON (default).

KoBo will POST every new submission. Re-delivery is safe (idempotent).

### 3. Verify

After a real submission, check ingestion:

```bash
curl -s https://tradeflow-ng.onrender.com/pipeline/logs \
  -H "Authorization: Bearer <admin-jwt>" | jq '.data[0]'
# run_type "Kobo Ingestion", status "Success"
```

Then run the cleaning pipeline (or wait for the daily cron) to promote the rows
into `cleaned_prices`.
