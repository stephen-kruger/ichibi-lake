#!/bin/bash
# Container entrypoint (run by tini, see docker/Dockerfile).
#
# Starts the bundled Postgres (DuckLake catalog) and Kafka on 127.0.0.1 when
# the gateway is not pointed at other servers, then the gateway itself:
#
#   EMBEDDED_POSTGRES=auto (default): on unless DUCKLAKE_PG_HOST names another
#                         host (anything but empty, localhost, 127.0.0.1, ::1).
#   EMBEDDED_KAFKA=auto (default):    on unless KAFKA_BROKERS is set.
#   1 / 0:                            force on / off.
#
# Their data lives in /data/postgres and /data/kafka. With neither on, this
# just execs the command (docker/compose.yaml runs them as separate services).
# Otherwise every process is supervised: if one exits, the others are stopped
# and the container exits with its status, for the restart policy to handle.
set -euo pipefail

log() { echo "[entrypoint] $*"; }

# Start a server in the background as another user. The background subshell
# execs it (no wrapper process), so $! and our signals reach the server itself.
start_as() { local u="$1"; shift; ( exec setpriv --reuid="$u" --regid="$u" --init-groups -- "$@" ) & }

is_local_host() {
    case "${1:-}" in ''|localhost|127.0.0.1|::1|0.0.0.0) return 0 ;; *) return 1 ;; esac
}

want() {  # want <setting value> <auto result: 0/1>
    case "${1,,}" in
        1|true|yes|on) return 0 ;;
        0|false|no|off) return 1 ;;
        auto|'') [ "$2" = 1 ] ;;
        *) log "unknown value '$1' (use auto, 1 or 0)"; exit 2 ;;
    esac
}

wait_port() {  # wait_port <port> <seconds> <name>
    for _ in $(seq 1 "$2"); do
        if (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; then return 0; fi
        sleep 1
    done
    log "$3 did not open port $1 within $2 s"
    return 1
}

pg_auto=0; is_local_host "${DUCKLAKE_PG_HOST:-}" && pg_auto=1
kafka_auto=0; [ -z "${KAFKA_BROKERS:-}" ] && kafka_auto=1
run_pg=0; want "${EMBEDDED_POSTGRES:-auto}" "$pg_auto" && run_pg=1
run_kafka=0; want "${EMBEDDED_KAFKA:-auto}" "$kafka_auto" && run_kafka=1

if [ "$run_pg" = 0 ] && [ "$run_kafka" = 0 ]; then
    exec "$@"
fi

if [ "$(id -u)" != 0 ]; then
    log "the bundled Postgres/Kafka need the container to start as root (they run as their own users);"
    log "run as root, or set EMBEDDED_POSTGRES=0 and EMBEDDED_KAFKA=0 and use external servers"
    exit 1
fi

declare -A pids=()

stop_all() {
    # Gateway first, so it stops writing before its catalog and broker go away.
    for name in gateway kafka postgres; do
        local pid="${pids[$name]:-}"
        [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null || continue
        log "stopping $name"
        # Postgres: SIGINT is a fast shutdown (SIGTERM would wait for clients).
        if [ "$name" = postgres ]; then kill -INT "$pid" 2>/dev/null || true; else kill -TERM "$pid" 2>/dev/null || true; fi
        for _ in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
        kill -KILL "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
}

on_signal() {
    log "received $1"
    stop_all
    exit 0
}
trap 'on_signal TERM' TERM
trap 'on_signal INT' INT

# --- Postgres ------------------------------------------------------------------

if [ "$run_pg" = 1 ]; then
    export DUCKLAKE_PG_HOST=127.0.0.1
    export DUCKLAKE_PG_PORT="${DUCKLAKE_PG_PORT:-5432}"
    export DUCKLAKE_PG_USER="${DUCKLAKE_PG_USER:-postgres}"
    export DUCKLAKE_PG_PASSWORD="${DUCKLAKE_PG_PASSWORD:-postgres}"
    export DUCKLAKE_PG_DB="${DUCKLAKE_PG_DB:-ducklake}"
    pgdata=/data/postgres
    mkdir -p "$pgdata" /var/run/postgresql
    chown -R postgres:postgres "$pgdata" /var/run/postgresql
    chmod 700 "$pgdata"

    if [ ! -s "$pgdata/PG_VERSION" ]; then
        log "initialising Postgres in $pgdata"
        pwfile=$(mktemp)
        printf '%s\n' "$DUCKLAKE_PG_PASSWORD" > "$pwfile"
        chown postgres "$pwfile"
        # Local socket: trust (only this container's root/postgres can reach it).
        # TCP (the gateway, 127.0.0.1 only): password.
        runuser -u postgres -- initdb -D "$pgdata" --username="$DUCKLAKE_PG_USER" --pwfile="$pwfile" \
            --encoding=UTF8 --auth-local=trust --auth-host=scram-sha-256 > /dev/null
        rm -f "$pwfile"
    fi

    log "starting Postgres"
    # Same quiet logging as the db service in docker/compose.yaml.
    start_as postgres postgres -D "$pgdata" \
        -c listen_addresses=127.0.0.1 -c port="$DUCKLAKE_PG_PORT" \
        -c log_statement=none -c log_min_messages=warning -c log_min_duration_statement=2000 \
        -c logging_collector=on -c log_directory=log -c log_filename=postgresql-%a.log \
        -c log_rotation_age=1d -c log_truncate_on_rotation=on
    pids[postgres]=$!

    for _ in $(seq 1 60); do
        runuser -u postgres -- pg_isready -q -h /var/run/postgresql -p "$DUCKLAKE_PG_PORT" && break
        kill -0 "${pids[postgres]}" 2>/dev/null || { log "Postgres exited during startup"; exit 1; }
        sleep 1
    done
    psql_local() { runuser -u postgres -- psql -q -v ON_ERROR_STOP=1 -h /var/run/postgresql -p "$DUCKLAKE_PG_PORT" -U "$DUCKLAKE_PG_USER" -d postgres "$@"; }
    # Keep the password in step with DUCKLAKE_PG_PASSWORD, and create the catalog database.
    psql_local -v pw="$DUCKLAKE_PG_PASSWORD" -v who="$DUCKLAKE_PG_USER" <<< "ALTER ROLE :\"who\" PASSWORD :'pw';"
    if [ -z "$(psql_local -tA -v db="$DUCKLAKE_PG_DB" <<< "SELECT 1 FROM pg_database WHERE datname = :'db';")" ]; then
        log "creating database $DUCKLAKE_PG_DB"
        psql_local -v db="$DUCKLAKE_PG_DB" <<< 'CREATE DATABASE :"db";'
    fi
fi

# --- Kafka ---------------------------------------------------------------------

if [ "$run_kafka" = 1 ]; then
    export KAFKA_BROKERS=127.0.0.1:9092
    kdata=/data/kafka
    kconf=/tmp/kafka-server.properties
    mkdir -p "$kdata"
    chown -R kafka:kafka "$kdata"
    # The same settings as the kafka service in docker/compose.yaml.
    cat > "$kconf" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs=$kdata
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
message.max.bytes=10485760
replica.fetch.max.bytes=10485760
log.retention.hours=${KAFKA_LOG_RETENTION_HOURS:-48}
log.retention.bytes=${KAFKA_LOG_RETENTION_BYTES:-1073741824}
log.segment.bytes=${KAFKA_LOG_SEGMENT_BYTES:-268435456}
log.retention.check.interval.ms=${KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS:-60000}
EOF
    chown kafka "$kconf"
    if [ ! -s "$kdata/meta.properties" ]; then
        log "formatting Kafka storage in $kdata"
        runuser -u kafka -- env LOG_DIR=/tmp/kafka-logs kafka-storage.sh format --cluster-id "$(kafka-storage.sh random-uuid)" --config "$kconf" > /dev/null
    fi
    log "starting Kafka"
    # Warnings and errors only, on the container log (docker/kafka-log4j2.properties);
    # EXTRA_ARGS without -loggc: no GC log files.
    start_as kafka env LOG_DIR=/tmp/kafka-logs EXTRA_ARGS="-name kafkaServer" \
        KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=/opt/kafka/config/ichibi-log4j2.properties" \
        KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms256m -Xmx512m}" \
        kafka-server-start.sh "$kconf"
    pids[kafka]=$!
    # The gateway connects lazily, so a slow broker only delays Kafka features.
    wait_port 9092 90 Kafka && log "Kafka is listening on 127.0.0.1:9092" || true
fi

# --- Gateway -------------------------------------------------------------------

log "starting gateway: $*"
"$@" &
pids[gateway]=$!

# Wait for the first process to exit, then stop the rest.
set +e
wait -n "${pids[@]}"
status=$?
for name in "${!pids[@]}"; do
    kill -0 "${pids[$name]}" 2>/dev/null || log "$name exited (status $status)"
done
stop_all
exit "$status"
