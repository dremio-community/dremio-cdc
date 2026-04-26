# ── Stage 1: Build React frontend ─────────────────────────────────────────────
FROM node:20-slim AS frontend-builder

WORKDIR /build/frontend
COPY ui/frontend/package*.json ./
RUN npm ci --silent

COPY ui/frontend/ ./
RUN npm run build


# ── Stage 2: Python runtime ────────────────────────────────────────────────────
FROM python:3.11-slim

LABEL org.opencontainers.image.title="Dremio CDC"
LABEL org.opencontainers.image.description="Stream database changes into Dremio — no Kafka required"
LABEL org.opencontainers.image.source="https://github.com/dremio-community/dremio-cdc"
LABEL org.opencontainers.image.authors="Mark Shainman"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# System deps for psycopg2, pyodbc, and oracledb
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
        unixodbc-dev \
        libaio1 \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY core/       ./core/
COPY sources/    ./sources/
COPY ui/backend/ ./ui/backend/
COPY main.py     .
COPY run_ui.py   .
COPY debezium/   ./debezium/
COPY config.example.yml .

# Inject pre-built React frontend
COPY --from=frontend-builder /build/frontend/dist/ ./ui/frontend/dist/

# Create data directory for SQLite offset store and DLQ
RUN mkdir -p /app/data

# Port for the web UI
EXPOSE 7070

# Healthcheck via the status endpoint
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -sf http://localhost:7070/api/status || exit 1

# Default: launch the web UI; override CMD to run headless
# Mount your config.yml to /app/config.yml
# Mount a volume to /app/data for persistent offset storage
CMD ["python", "main.py", "--ui", "--config", "/app/config.yml", "--no-browser", "--port", "7070"]
