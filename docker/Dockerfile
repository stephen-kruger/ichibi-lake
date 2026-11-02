# ichibi-lake gateway, self-contained: besides the Node gateway the image ships
# Postgres 17 (the DuckLake catalog) and a single-node Kafka (KRaft), which
# docker/entrypoint.sh starts on 127.0.0.1 when DUCKLAKE_PG_HOST / KAFKA_BROKERS
# point nowhere else. So `docker run -p 3000:3000 -v ichibi-lake:/data <image>`
# is a complete lake, while docker/compose.yaml (and any scaled-out setup)
# runs the same image against separate Postgres and Kafka servers.
FROM node:lts-bookworm-slim

ARG POSTGRES_MAJOR=17
ARG KAFKA_VERSION=4.3.1
ARG SCALA_VERSION=2.13

# curl: compose/image healthchecks and downloads. tini: PID 1 (signals,
# zombies). Postgres from the PGDG repository (Debian bookworm ships 15),
# without the Debian package's default cluster: the entrypoint creates one in
# /data. Java 17 runs Kafka.
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates gnupg tini && \
    install -d /usr/share/postgresql-common/pgdg && \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.gpg && \
    echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.gpg] https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    mkdir -p /etc/postgresql-common/createcluster.d && \
    echo 'create_main_cluster = false' > /etc/postgresql-common/createcluster.d/no-main-cluster.conf && \
    apt-get update && \
    apt-get install -y --no-install-recommends postgresql-${POSTGRES_MAJOR} openjdk-17-jre-headless && \
    apt-get purge -y gnupg && apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Kafka (checksum-verified), run by its own user.
RUN curl -fsSL -o /tmp/kafka.tgz https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    curl -fsSL https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz.sha512 \
      | tr -d ' \n' | cut -d: -f2 | tr 'A-F' 'a-f' | sed 's#$#  /tmp/kafka.tgz\n#' | sha512sum -c - && \
    mkdir -p /opt/kafka && tar -xzf /tmp/kafka.tgz -C /opt/kafka --strip-components=1 && \
    rm -rf /tmp/kafka.tgz /opt/kafka/site-docs && \
    useradd --system --home-dir /nonexistent --shell /usr/sbin/nologin kafka

ENV PATH=/usr/lib/postgresql/${POSTGRES_MAJOR}/bin:/opt/kafka/bin:$PATH

# Gateway data directories used by docker/compose.yaml, and /data for the
# self-contained setup (lake files, blobs, settings, Postgres, Kafka).
RUN mkdir -p /app/data_files /app/blobs /app/config /data && \
    chmod 755 /app/data_files /app/blobs /app/config /data

WORKDIR /app

# Install production deps first so the layer is cached across code changes.
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev || npm install --omit=dev

# Copy the rest of the gateway source. The build context is the repo root
# (see docker/compose.override.yaml), so this picks up src/, swagger.yaml, etc.
COPY src ./src
COPY swagger.yaml ./swagger.yaml
# Admin console static assets (served at /console) and its recovery CLI.
COPY public ./public
COPY scripts/console-admin.js ./scripts/console-admin.js
COPY scripts/install-extensions.js ./scripts/install-extensions.js

# Pre-install the DuckDB extensions the gateway loads (ducklake, postgres,
# duckpgq, plus httpfs and aws for s3:// data paths) so containers never
# download them at startup; the gateway's runtime INSTALL is then a no-op.
# Without this, every fresh container fetched them
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

COPY docker/entrypoint.sh /usr/local/bin/ichibi-lake-entrypoint
COPY docker/kafka-log4j2.properties /opt/kafka/config/ichibi-log4j2.properties
RUN chmod 755 /usr/local/bin/ichibi-lake-entrypoint

# The Node gateway still runs INSTALL for its extensions at startup (a no-op
# here, since they are pre-installed above). DuckPGQ ships from the community
# repo, so opt in to that here.
#
# Defaults for the self-contained setup: everything persistent lives under
# /data (mount a volume there). docker/compose.yaml sets its own paths.
ENV DUCKDB_ALLOW_COMMUNITY_EXTENSIONS=1 \
    NODE_ENV=production \
    PORT=3000 \
    DUCKLAKE_DATA_PATH=/data/ducklake \
    USE_FILESYSTEM_BLOBS=1 \
    BLOBS_PATH=/data/blobs \
    STORAGE_CONFIG_FILE=/data/config/storage.json \
    EMBEDDED_POSTGRES=auto \
    EMBEDDED_KAFKA=auto

# No VOLUME: with separate Postgres/Kafka servers /data is unused, and an
# anonymous volume per container would only pile up. Mount one for keeps.
EXPOSE 3000

# Liveness only (no auth, no database access); compose files may override it.
HEALTHCHECK --interval=10s --timeout=5s --start-period=60s --retries=5 \
    CMD curl -sf "http://localhost:${PORT:-3000}/health" > /dev/null || exit 1

ENTRYPOINT ["tini", "--", "ichibi-lake-entrypoint"]
CMD ["node", "src/index.js"]
