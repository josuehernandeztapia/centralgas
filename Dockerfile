# ============================================================
# Central Gas Agent — Production Dockerfile
# Multi-stage build: smaller runtime image, faster cold starts on Fly.io
# ============================================================

# ---------- Stage 1: Builder ----------
FROM python:3.12-slim AS builder

WORKDIR /build

# Build deps (gcc/libpq for psycopg2, snap7 needs libsnap7 at runtime)
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
# Install to /install (a clean prefix) so we can copy to /usr/local in runtime.
# This makes packages available system-wide — both root (fly ssh) and appuser can import them.
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ---------- Stage 2: Runtime ----------
FROM python:3.12-slim AS runtime

# Runtime deps only (no compilers)
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
        curl \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

# Set timezone to Aguascalientes (UTC-6, no DST)
ENV TZ=America/Mexico_City
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Copy installed packages system-wide (so both root and appuser can import them)
COPY --from=builder /install /usr/local

# Non-root user for security (used by uvicorn at runtime)
RUN useradd --create-home --uid 1000 appuser

WORKDIR /app
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Copy app code (set owner so non-root user can read)
COPY --chown=appuser:appuser . .

USER appuser

# Healthcheck — Fly.io also checks via fly.toml [[checks]]
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8000/health || exit 1

EXPOSE 8000

# Default cmd (overridden by fly.toml [processes] section)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
