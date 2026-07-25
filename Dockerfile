# ═══════════════════════════════════════════════════════════════
# TradeFlow NG — FastAPI Backend
# Multi-stage build for Google Cloud Run
# Python 3.11 — matches development environment exactly
# ═══════════════════════════════════════════════════════════════

# ── Stage 1: Build dependencies ─────────────────────────────────
FROM python:3.11-slim AS builder

# System packages needed to compile Prophet, psycopg2, asyncpg
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    libffi-dev \
    libssl-dev \
    python3-dev \
    build-essential \
    pkg-config \
    # Required by Prophet / pystan
    libgomp1 \
    # Required by some numpy/scipy wheels
    gfortran \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy requirements first — Docker cache layer stays valid
# unless requirements.txt changes
COPY requirements.txt .

# Install all Python dependencies into a prefix we can copy later
RUN pip install --upgrade pip setuptools wheel && \
    pip install --prefix=/install --no-cache-dir -r requirements.txt


# ── Stage 2: Runtime image ───────────────────────────────────────
FROM python:3.11-slim AS runtime

# Runtime system libraries (no compilers needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    libgomp1 \
    # Useful for health probes and debugging without bloating the image
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN groupadd -r tradeflow && useradd -r -g tradeflow -s /bin/false tradeflow

# Copy installed Python packages from builder
COPY --from=builder /install /usr/local

WORKDIR /app

# Copy backend source
COPY . .

# Copy src/ ML pipeline so pipeline.py can import forecasting, optimization, etc.
# Adjust this path if your repo structure differs:
#   tradeflow-ng/
#     backend/   ← this Dockerfile lives here
#     src/       ← ML pipeline lives here
# We expect the Docker build context to be the repo root,
# so COPY src/ ./src/ will work.
# If you build from inside backend/, change the path accordingly.
COPY ../src ./src 2>/dev/null || true

# Set Python path so `from forecasting import ...` resolves
ENV PYTHONPATH="/app/src:/app:${PYTHONPATH}"

# Prevent Python from writing .pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Cloud Run injects PORT; default to 8080 if not set
ENV PORT=8080

# Set ownership
RUN chown -R tradeflow:tradeflow /app

USER tradeflow

# Health check — Cloud Run will use the /health endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# Cloud Run requires the app to listen on 0.0.0.0:$PORT
# Single worker is correct for Cloud Run (it scales via instances, not workers)
CMD uvicorn main:app \
    --host 0.0.0.0 \
    --port ${PORT} \
    --workers 1 \
    --loop uvloop \
    --http httptools \
    --log-level info \
    --no-access-log
