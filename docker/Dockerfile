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

# Install DuckDB CLI for debugging / inspection via `docker exec ... duckdb -ui`
# and for the duckdb-ui compose service that browses the DuckLake catalog.
#
# IMPORTANT: keep this CLI version in lockstep with the DuckDB version bundled by
# `@duckdb/node-api` in package.json (currently 2.0.0-r.1 => DuckDB v2.0.0). The
# Node gateway writes/migrates the DuckLake catalog with its bundled `ducklake`
# extension; an older CLI ships an older `ducklake` that cannot read the newer
# catalog format and fails ATTACH with "Not implemented Error: Only DuckLake
# versions ... are supported", which leaves the DuckDB UI showing only the empty
# in-memory `memory` database instead of the lake's tables.
# Since v2.0.0 the CLI release assets are `duckdb-cli-<platform>.tar.gz`
# (previously `duckdb_cli-<platform>.zip`).
RUN arch=$(dpkg --print-architecture) && \
    case "$arch" in \
      amd64) cli_arch=linux-amd64 ;; \
      arm64) cli_arch=linux-arm64 ;; \
      *) echo "Unsupported arch: $arch"; exit 1 ;; \
    esac && \
    curl -fsSL "https://github.com/duckdb/duckdb/releases/download/v2.0.0/duckdb-cli-${cli_arch}.tar.gz" \
      | tar -xz -C /usr/local/bin/ duckdb && \
    chmod +x /usr/local/bin/duckdb

# Pre-install the DuckDB extensions the gateway (ducklake, postgres, duckpgq)
# and the duckdb-ui service (ui) need, so containers never download them at
# startup. Runtime INSTALL is then a no-op. Without this, every fresh container
# fetched them during _attachDuckLake(); DuckDB's built-in HTTP client has no
# read timeout, so an occasional stalled connection (observed with the
# community-extensions download for duckpgq) hung startup indefinitely and
# every request queued behind it. Here a stall is bounded by `timeout` and
# retried. The CLI and @duckdb/node-api must be the same DuckDB version (see
# above) because extensions are stored per version under
# ~/.duckdb/extensions/<version>/, and both run as root so they share that path.
RUN for attempt in 1 2 3; do \
      timeout 180 duckdb -c "INSTALL ducklake; INSTALL postgres; INSTALL ui; INSTALL duckpgq FROM community;" && break; \
      echo "DuckDB extension install attempt $attempt failed; retrying..."; \
      [ "$attempt" = 3 ] && exit 1; \
    done && \
    ls /root/.duckdb/extensions/*/*/

# The Node gateway still runs INSTALL for its extensions at startup (a no-op
# here, since they are pre-installed above). DuckPGQ ships from the community
# repo, so opt in to that here.
ENV DUCKDB_ALLOW_COMMUNITY_EXTENSIONS=1 \
    NODE_ENV=production

# Note: PORT is set at runtime via compose.
# EXPOSE uses the default so the image is self-documenting.
EXPOSE 3000

CMD ["node", "src/index.js"]
