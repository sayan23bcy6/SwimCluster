# ── SwimCluster Dockerfile ────────────────────────────────────────────────────
#
# Multi-stage build:
#   builder – installs dependencies into a venv
#   runtime – slim image, non-root user, copies the venv

FROM python:3.11-slim AS builder

WORKDIR /app

# Install build tools.
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Copy dependency files first to leverage layer caching.
COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source and install the package as a normal install.
COPY README.md ./
COPY swimcluster/ swimcluster/
RUN pip install --no-cache-dir .

# ── Runtime image ─────────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

# Create a non-root user for security.
RUN groupadd -r swim && useradd -r -g swim swim

WORKDIR /app

# Copy installed packages and entrypoint from builder.
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin/swimcluster /usr/local/bin/swimcluster

RUN mkdir -p /app/data && chown -R swim:swim /app

USER swim

# Expose ports: gossip/probe (UDP+TCP) and HTTP API.
EXPOSE 7946 8080

# Default command: run a cluster node.
# Override SWIM_NODE_ID, SWIM_PORT, SWIM_HTTP_PORT, SWIM_SEEDS via env.
CMD ["swimcluster", "node"]
