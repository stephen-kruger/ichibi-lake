FROM node:lts-bookworm-slim

# Install curl for healthchecks and DuckDB CLI. socat is used by the
# duckdb-ui service to expose the localhost-only DuckDB UI server on 0.0.0.0
# so it is reachable through Docker's published port.
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates git unzip socat && \
    rm -rf /var/lib/apt/lists/*

# Ensure data directories exist with correct permissions
RUN mkdir -p /app/data_files /var/lib/postgresql/data && \
    chmod 755 /app/data_files /var/lib/postgresql/data

WORKDIR /app

# Install production deps first so the layer is cached across code changes.
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev || npm install --omit=dev

# Copy the rest of the gateway source. The build context is the repo root
# (see docker-compose.yaml), so this picks up src/, swagger.yaml, etc.
COPY src ./src
COPY swagger.yaml ./swagger.yaml

# Install DuckDB CLI for debugging / inspection via `docker exec ... duckdb -ui`.
RUN arch=$(dpkg --print-architecture) && \
    case "$arch" in \
      amd64) cli_arch=linux-amd64 ;; \
      arm64) cli_arch=linux-arm64 ;; \
      *) echo "Unsupported arch: $arch"; exit 1 ;; \
    esac && \
    curl -fsSL "https://github.com/duckdb/duckdb/releases/download/v1.5.4/duckdb_cli-${cli_arch}.zip" \
      -o /tmp/duckdb_cli.zip && \
    unzip -o /tmp/duckdb_cli.zip duckdb -d /usr/local/bin/ && \
    rm /tmp/duckdb_cli.zip && \
    chmod +x /usr/local/bin/duckdb

# The Node gateway installs the DuckDB extensions it needs (ducklake,
# postgres, duckpgq) at startup. DuckPGQ ships from the community repo, so
# opt in to that here.
ENV DUCKDB_ALLOW_COMMUNITY_EXTENSIONS=1 \
    NODE_ENV=production

# Note: PORT is set at runtime via compose.
# EXPOSE uses the default so the image is self-documenting.
EXPOSE 3000

CMD ["node", "src/index.js"]
