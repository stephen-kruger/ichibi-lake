#!/usr/bin/env bash
# Smoke-test harness for scripts/superlake-wizard.js.
# Pipes scripted answers to the wizard for each topology and verifies the
# expected output files. Used during development; safe to delete.
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
WIZARD="$ROOT/scripts/superlake-wizard.js"
LOGDIR="$(mktemp -d)"
fail=0

run_case() {
    local label=$1 input=$2 dir=$3; shift 3
    local log="$LOGDIR/$(echo "$label" | tr ' /' '__').log"
    echo "=== $label ==="
    if printf '%s' "$input" | node "$WIZARD" >"$log" 2>&1; then
        echo "  exit OK"
    else
        echo "  exit FAIL  (log: $log)"
        tail -25 "$log" | sed 's/^/    /'
        fail=1
    fi
    for f in "$@"; do
        if [ -f "$ROOT/superlake/$dir/$f" ]; then
            echo "  ✓ $dir/$f"
        else
            echo "  ✗ MISSING: $dir/$f"
            fail=1
        fi
    done
}

# Each input is the exact sequence of answers, in order, separated by \n.

# Option 1: choice=1, N=3, basePort=3333, kafka=y, apiKey=(blank → generated), sqlRO=y
run_case 'Option 1 (single-host)' \
    $'1\n3\n3333\ny\n\ny\n' \
    'option1-singlehost' \
    compose.yaml .env README.md

# Option 2 server: choice=2, role=1, serverIp, subnet, share, apiKey=blank, sqlRO=y
run_case 'Option 2 server (NFS catalog)' \
    $'2\n1\n192.168.1.10\n192.168.1.0/24\n/srv/ichibi-lake\n\ny\n' \
    'option2-nfs-server' \
    compose.yaml setup-nfs-server.sh .env README.md

# Option 2 client: choice=2, role=2, serverIp, share, mount, hostPort, kafka=n, apiKey, sqlRO
run_case 'Option 2 client (NFS gateway)' \
    $'2\n2\n192.168.1.10\n/srv/ichibi-lake\n/mnt/ichibi-lake\n3334\nn\nMYAPIKEY\ny\n' \
    'option2-nfs-client' \
    compose.yaml setup-nfs-client.sh .env README.md

# Option 3 server: choice=3, role=1, ip, bucket, access, secret(blank→gen), apiKey(blank→gen), sqlRO
run_case 'Option 3 server (MinIO catalog)' \
    $'3\n1\n192.168.1.10\nichibi-lake\nichibi-lake\n\n\ny\n' \
    'option3-minio-server' \
    compose.yaml .env README.md db-s3-patch.md

# Option 3 client: choice=3, role=2, ip, bucket, access, secret, hostPort, apiKey, sqlRO
run_case 'Option 3 client (MinIO gateway)' \
    $'3\n2\n192.168.1.10\nichibi-lake\nichibi-lake\nSECRET123\n3335\nMYAPIKEY\ny\n' \
    'option3-minio-client' \
    compose.yaml .env README.md db-s3-patch.md

# Option 4 server: choice=4, role=1, ip, user, share, apiKey=blank, sqlRO
run_case 'Option 4 server (SSHFS host)' \
    $'4\n1\n192.168.1.10\nichibi\n/home/ichibi/ichibi-lake\n\ny\n' \
    'option4-sshfs-server' \
    compose.yaml setup-sshfs-server.sh .env README.md

# Option 4 client: choice=4, role=2, ip, user, share, mount, sshKey, hostPort, apiKey, sqlRO
run_case 'Option 4 client (SSHFS gateway)' \
    $'4\n2\n192.168.1.10\nichibi\n/home/ichibi/ichibi-lake\n/mnt/ichibi-lake\n/root/.ssh/id_ed25519\n3336\nMYAPIKEY\ny\n' \
    'option4-sshfs-client' \
    compose.yaml mount-sshfs.sh .env README.md

# Option 5: choice=5, ip, name, hostPort, peers, apiKey=blank, sqlRO
run_case 'Option 5 (Federation)' \
    $'5\n192.168.1.10\nlake_local\n3337\nlake_dubai@10.0.0.11:5432,lake_riyadh@10.0.0.12:5432\n\ny\n' \
    'option5-federation-lake_local' \
    compose.yaml .env README.md db-federation-patch.md

echo
if [ "$fail" = 0 ]; then
    echo '==== ALL SMOKE TESTS PASSED ===='
else
    echo '==== SMOKE FAILURES ===='
    echo "Logs preserved in: $LOGDIR"
    exit 1
fi
