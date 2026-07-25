# ═══════════════════════════════════════════════════════════════
# TradeFlow NG — FastAPI Backend
# Place this file at REPO ROOT (not inside backend/)
#
# Render settings:
#   Root Directory  → (leave empty)
#   Dockerfile Path → Dockerfile   ← auto-detected at repo root
#
# Local build (from repo root):
#   docker build -t tradeflow-backend .
# ═══════════════════════════════════════════════════════════════

# ── Stage 1: Build ──────────────────────────────────────────────
FROM python:3.11-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libpq-dev libffi-dev libssl-dev \
    python3-dev build-essential pkg-config \
    libgomp1 gfortran \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Cached layer — only re-runs if requirements.txt changes
COPY backend/requirements.txt .

RUN pip install --upgrade pip setuptools wheel && \
    pip install --prefix=/install --no-cache-dir -r requirements.txt


# ── Stage 2: Runtime ────────────────────────────────────────────
FROM python:3.11-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 libgomp1 curl \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r tradeflow && \
    useradd -r -g tradeflow -s /bin/false tradeflow

COPY --from=builder /install /usr/local

WORKDIR /app

# Copy FastAPI backend (context = repo root, so path is backend/)
COPY backend/ .

# Copy ML pipeline (src/ is at repo root — valid path from repo root context)
COPY src/ ./src/

ENV PYTHONPATH="/app/src:/app"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

RUN chown -R tradeflow:tradeflow /app
USER tradeflow

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

CMD uvicorn main:app \
    --host 0.0.0.0 \
    --port ${PORT} \
    --workers 1 \
    --loop uvloop \
    --http httptools \
    --log-level info \
    --no-access-log
