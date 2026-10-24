FROM node:lts-bookworm-slim

# curl is used by the compose healthcheck.
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
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
# Admin console static assets (served at /console) and its recovery CLI.
COPY public ./public
COPY scripts/console-admin.js ./scripts/console-admin.js
COPY scripts/install-extensions.js ./scripts/install-extensions.js

# Pre-install the DuckDB extensions the gateway loads (ducklake, postgres,
# duckpgq) so containers never download them at startup; the gateway's runtime
# INSTALL is then a no-op. Without this, every fresh container fetched them
# during _attachDuckLake(); DuckDB's built-in HTTP client has no read timeout,
# so an occasional stalled connection (observed with the community-extensions
# download for duckpgq) hung startup indefinitely with every request queued
# behind it. Here a stall is bounded by `timeout` and retried. Installing with
# the gateway's own @duckdb/node-api guarantees the extension version matches.
RUN for attempt in 1 2 3; do \
      timeout 180 node scripts/install-extensions.js && break; \
      echo "DuckDB extension install attempt $attempt failed; retrying..."; \
      [ "$attempt" = 3 ] && exit 1; \
    done

# The Node gateway still runs INSTALL for its extensions at startup (a no-op
# here, since they are pre-installed above). DuckPGQ ships from the community
# repo, so opt in to that here.
ENV DUCKDB_ALLOW_COMMUNITY_EXTENSIONS=1 \
    NODE_ENV=production

# Note: PORT is set at runtime via compose.
# EXPOSE uses the default so the image is self-documenting.
EXPOSE 3000

CMD ["node", "src/index.js"]
