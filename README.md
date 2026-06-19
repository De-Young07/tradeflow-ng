# TradeFlow NG v2.0

**Flowing Trade. Feeding Nigeria.**

AI-powered agricultural trade intelligence platform — rebuilt from Streamlit to a production Next.js + FastAPI stack.

---

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Next.js 14    │    │    FastAPI       │    │   Supabase      │
│   (Vercel)      │◄──►│   (Railway)     │◄──►│  PostgreSQL     │
│  app.tradeflow  │    │  api.tradeflow  │    │  (unchanged)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                               ▲
                               │ daily 06:00 WAT
                       ┌───────────────┐
                       │ GitHub Actions│
                       │  (scheduler)  │
                       └───────────────┘
```

**ML pipeline (unchanged):** Prophet forecasting + PuLP optimization in `src/`

---

## Quick Start — Local Development

### 1. Backend

```bash
cd backend
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Create .env from template
cp .env.example .env
# Fill in DATABASE_URL, JWT_SECRET, ADMIN_USERNAME, ADMIN_PASSWORD

uvicorn main:app --reload --port 8000
# API docs: http://localhost:8000/docs
```

### 2. Frontend

```bash
cd frontend
npm install

# Create .env.local from template
cp .env.example .env.local
# Fill in NEXT_PUBLIC_API_URL=http://localhost:8000
# Fill in JWT_SECRET (same as backend)

npm run dev
# App: http://localhost:3000
```

### 3. Test credentials (local dev)
- **Admin:** username `admin` / password `tradeflow2026`
- **Agent:** use an agent_id + password from your `agents` table

---

## Deployment

### Backend → Railway

1. Connect GitHub repo at [railway.app](https://railway.app)
2. Select `/backend` as root directory
3. Add environment variables from `.env.example`
4. Railway auto-detects `railway.toml` — deploys on push to `main`
5. Set custom domain: `api.tradeflowng.com`

### Frontend → Vercel

1. Connect GitHub repo at [vercel.com](https://vercel.com)
2. Select `/frontend` as root directory  
3. Add environment variables:
   - `NEXT_PUBLIC_API_URL=https://api.tradeflowng.com`
   - `JWT_SECRET=<same value as backend>`
4. Vercel auto-detects Next.js — deploys on push to `main`
5. Set custom domain: `app.tradeflowng.com`

### GitHub Actions Secrets

Add these in repo Settings → Secrets → Actions:

| Secret | Value |
|--------|-------|
| `DATABASE_URL` | Supabase session-mode URL (port 5432) |
| `JWT_SECRET` | Same 64-char hex as backend/frontend |
| `KOBO_API_TOKEN` | KoboToolbox API token |
| `KOBO_ASSET_UID` | KoboToolbox form UID |
| `ADMIN_USERNAME` | Admin login username |
| `ADMIN_PASSWORD` | Admin login password |
| `API_URL` | `https://api.tradeflowng.com` |

---

## Repository Structure

```
tradeflow-ng/
├── backend/                  # FastAPI → Railway
│   ├── main.py
│   ├── auth.py
│   ├── routers/
│   │   ├── admin.py          # All admin endpoints
│   │   ├── agents.py         # Agent endpoints
│   │   ├── prices.py         # Public prices
│   │   ├── forecasts.py      # Forecast data
│   │   ├── recommendations.py
│   │   └── pipeline.py       # Trigger ML pipeline
│   ├── models/
│   │   ├── database.py       # SQLAlchemy async
│   │   └── schemas.py        # Pydantic models
│   ├── requirements.txt
│   ├── railway.toml
│   └── .env.example
│
├── frontend/                 # Next.js 14 → Vercel
│   ├── app/
│   │   ├── layout.tsx
│   │   ├── login/
│   │   │   ├── admin/page.tsx
│   │   │   └── agent/page.tsx
│   │   ├── admin/
│   │   │   ├── layout.tsx        # Sidebar shell
│   │   │   ├── page.tsx          # Overview dashboard
│   │   │   ├── recommendations/  # Trade route cards
│   │   │   ├── forecasts/        # Prophet chart
│   │   │   ├── tableau/          # Profit heatmap
│   │   │   ├── feedback/         # Log outcomes
│   │   │   └── data/             # Pipeline + agents
│   │   ├── agent/
│   │   │   ├── layout.tsx        # Dark green + tab bar
│   │   │   ├── page.tsx          # My Trades
│   │   │   ├── submit/           # Submit price
│   │   │   └── report/           # Report outcome
│   │   └── api/auth/             # Cookie handlers
│   ├── lib/
│   │   ├── api.ts            # All API calls
│   │   └── auth.ts           # JWT + session
│   ├── styles/globals.css
│   ├── middleware.ts          # Route protection
│   └── .env.example
│
├── src/                      # ML pipeline (unchanged)
│   ├── forecasting.py
│   ├── optimization.py
│   ├── cleaning.py
│   ├── kobo_connector.py
│   ├── scheduler.py
│   └── db_adapter.py
│
└── .github/workflows/
    └── scheduler.yml         # Daily 06:00 WAT cron
```

---

## API Reference

**Auth**
- `POST /auth/admin/login` — Admin JWT
- `POST /auth/agent/login` — Agent JWT + profile

**Admin** *(require admin JWT)*
- `GET  /admin/overview` — Dashboard metrics
- `GET  /admin/recommendations` — Trade routes
- `GET  /admin/forecasts` — Price forecasts + history  
- `GET  /admin/prices/trend` — Multi-state price trends
- `GET  /admin/tableau` — Profit matrix data
- `GET  /admin/agents` — Agent list
- `POST /admin/agents` — Create agent
- `POST /admin/feedback` — Log trade outcome
- `GET  /admin/db/stats` — Table row counts
- `GET  /admin/lookups` — States, markets, commodities

**Pipeline** *(require admin JWT)*
- `POST /pipeline/run` — Full pipeline (async)
- `POST /pipeline/run/cleaning` — Cleaning only
- `POST /pipeline/run/forecasting` — Forecasting only
- `POST /pipeline/run/optimization` — Optimization only
- `GET  /pipeline/logs` — Pipeline history

**Agent** *(require agent JWT)*
- `GET  /agent/recommendations` — Routes for agent's state
- `GET  /agent/prices/local` — Local market prices
- `POST /agent/prices/submit` — Submit price report
- `GET  /agent/submissions/recent` — Last 15 submissions
- `POST /agent/report` — Log trade outcome
- `GET  /agent/lookups` — Commodities + local markets

**Public**
- `GET  /prices/latest` — Latest prices per commodity/state
- `GET  /health` — System health check

---

## Brand

| Token | Value |
|-------|-------|
| Primary green | `#1A6B3C` |
| Dark green | `#0D1F14` |
| Gold | `#C8860A` |
| Cream | `#F5F2EB` |
| Heading font | Plus Jakarta Sans |
| Body font | Inter |

---

*TradeFlow NG · Flowing Trade. Feeding Nigeria.*
