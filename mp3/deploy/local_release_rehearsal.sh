#!/bin/bash
# Disposable ten-process local release rehearsal. Never runs against remote hosts.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ "${MP3_TARGET:-}" != local ]; then
    echo "Set MP3_TARGET=local explicitly; refusing to run otherwise." >&2
    exit 2
fi
REPO_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
TEMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/mp3-release-local.XXXXXX")"
export MP3_LOCAL_ROOT="$TEMP_ROOT/hosts"
export MP3_NO_TMUX=1

cleanup() {
    if MP3_TARGET=local MP3_LOCAL_ROOT="$MP3_LOCAL_ROOT" "$SCRIPT_DIR/stop_mp" all >/dev/null 2>&1; then
        if [ -z "$(MP3_TARGET=local MP3_LOCAL_ROOT="$MP3_LOCAL_ROOT" "$SCRIPT_DIR/status_mp" all \
            | grep -E 'node=UP|log_query=UP')" ]; then
            rm -rf "$TEMP_ROOT"
            return
        fi
    fi
    echo "Rehearsal cleanup left services running; preserving its temporary data at $TEMP_ROOT" >&2
}
trap cleanup EXIT INT TERM

echo "Local-only rehearsal root: $TEMP_ROOT"
"$SCRIPT_DIR/deploy_mp" all
for n in $(seq 1 10); do
    mkdir -p "$MP3_LOCAL_ROOT/vm$(printf '%02d' "$n")/data"
    for dataset in $(seq 1 20); do
        printf 'release-rehearsal-vm-%s-dataset-%s\n' "$n" "$dataset" \
            > "$MP3_LOCAL_ROOT/vm$(printf '%02d' "$n")/data/business_$dataset"
    done
done
"$SCRIPT_DIR/run_mp" all
prep_ok=0
for _ in $(seq 1 30); do
    if MP3_TARGET=local MP3_LOCAL_ROOT="$MP3_LOCAL_ROOT" "$SCRIPT_DIR/check_prep"; then
        prep_ok=1
        break
    fi
    sleep 1
done
[ "$prep_ok" = 1 ] || { echo "local check_prep did not converge" >&2; exit 1; }

assert_command() {
    local vm="$1" command="$2" output="$3"
    LAST_RESULT=$(MP3_TARGET=local MP3_LOCAL_ROOT="$MP3_LOCAL_ROOT" "$SCRIPT_DIR/cmd_mp" "$vm" "$command")
    printf '%s\n' "$LAST_RESULT" | grep -Fq "$output" || {
        printf 'Command did not contain expected output: %s\n%s\n' "$output" "$LAST_RESULT" >&2
        return 1
    }
}

wait_for_all_owners() {
    local key="$1" expected_file="$2" end owner owner_id output all
    end=$((SECONDS + 15))
    while [ "$SECONDS" -lt "$end" ]; do
        all=1
        for owner in $(MP3_TARGET=local MP3_LOCAL_ROOT="$MP3_LOCAL_ROOT" bash -c ". '$SCRIPT_DIR/lib.sh'; owners '$key'"); do
            owner_id=$(printf 'vm%02d' "$owner")
            rm -f "$MP3_LOCAL_ROOT/$owner_id/run/out/release-wait-$key"
            if ! output=$(MP3_TARGET=local MP3_LOCAL_ROOT="$MP3_LOCAL_ROOT" \
                "$SCRIPT_DIR/cmd_mp" "$owner" "getfromreplica $owner_id $key @out/release-wait-$key") \
                || ! printf '%s\n' "$output" | grep -Fq '"status": "OK"' \
                || ! cmp -s "$MP3_LOCAL_ROOT/$owner_id/run/out/release-wait-$key" "$expected_file"; then
                all=0
                break
            fi
        done
        [ "$all" = 1 ] && return 0
        sleep 0.2
    done
    echo "Timed out waiting for all five prescribed copies of $key" >&2
    return 1
}

# Required command coverage in Strong mode.
assert_command 1 'put release_key @data/business_1' '"status": "OK"'
assert_command 2 'get release_key @out/release_key.get' '"status": "OK"'
cmp "$MP3_LOCAL_ROOT/vm02/run/out/release_key.get" "$MP3_LOCAL_ROOT/vm01/data/business_1"
assert_command 3 'ls release_key' '"owners"'
assert_command 4 'liststore' '"keys"'
FOUND_REPLICA=0
for OWNER_NUMBER in $(MP3_TARGET=local MP3_LOCAL_ROOT="$MP3_LOCAL_ROOT" \
    bash -c ". '$SCRIPT_DIR/lib.sh'; owners release_key"); do
    if REPLICA_RESULT=$(MP3_TARGET=local MP3_LOCAL_ROOT="$MP3_LOCAL_ROOT" \
        "$SCRIPT_DIR/cmd_mp" "$OWNER_NUMBER" \
        "getfromreplica vm$(printf '%02d' "$OWNER_NUMBER") release_key @out/release_key.replica") \
        && printf '%s\n' "$REPLICA_RESULT" | grep -Fq '"status": "OK"' \
        && cmp -s "$MP3_LOCAL_ROOT/vm$(printf '%02d' "$OWNER_NUMBER")/run/out/release_key.replica" \
            "$MP3_LOCAL_ROOT/vm01/data/business_1"; then
        FOUND_REPLICA=1
        break
    fi
done
[ "$FOUND_REPLICA" = 1 ] || { echo "no successful prescribed replica served release_key" >&2; exit 1; }
assert_command 5 'list_mem_ids' '"members"'
assert_command 1 'multiput(release_multi, VM1, VM2)' '"status": "OK"'

# Exercise each supported mode through the existing command path.
assert_command 3 'consistency_mode eventual-noRR' '"status": "OK"'
assert_command 3 'put norr_key @data/business_3' '"status": "OK"'
wait_for_all_owners norr_key "$MP3_LOCAL_ROOT/vm03/data/business_3"
assert_command 4 'get norr_key @out/norr_key.get' '"status": "OK"'
cmp "$MP3_LOCAL_ROOT/vm04/run/out/norr_key.get" "$MP3_LOCAL_ROOT/vm03/data/business_3"

assert_command 3 'consistency_mode eventual-RR' '"status": "OK"'
assert_command 3 'put rr_key @data/business_3' '"status": "OK"'
wait_for_all_owners rr_key "$MP3_LOCAL_ROOT/vm03/data/business_3"
assert_command 4 'get rr_key @out/rr_key.get' '"status": "OK"'
cmp "$MP3_LOCAL_ROOT/vm04/run/out/rr_key.get" "$MP3_LOCAL_ROOT/vm03/data/business_3"

# Crash one prescribed owner after switching back to Strong; restart must recover its DB.
assert_command 3 'consistency_mode strong' '"status": "OK"'
assert_command 3 'put restart_key @data/business_3' '"status": "OK"'
OWNER=$(MP3_TARGET=local MP3_LOCAL_ROOT="$MP3_LOCAL_ROOT" bash -c ". '$SCRIPT_DIR/lib.sh'; owners restart_key" | awk '{print $1}')
MP3_TARGET=local MP3_LOCAL_ROOT="$MP3_LOCAL_ROOT" "$SCRIPT_DIR/kill_mp" "$OWNER"
MP3_TARGET=local MP3_LOCAL_ROOT="$MP3_LOCAL_ROOT" "$SCRIPT_DIR/run_mp" "$OWNER"
assert_command 4 'get restart_key @out/restart_key.get' '"status": "OK"'
cmp "$MP3_LOCAL_ROOT/vm04/run/out/restart_key.get" "$MP3_LOCAL_ROOT/vm03/data/business_3"

# Run the original MP1 client against all ten original servers using manifest log_query ports.
CLIENT_DIR="$TEMP_ROOT/client"
mkdir -p "$CLIENT_DIR"
python3 - "$MP3_LOCAL_ROOT/vm01/run/manifest.json" "$CLIENT_DIR/machines.txt" <<'PY'
import json, sys
manifest = json.load(open(sys.argv[1], encoding="utf-8"))
with open(sys.argv[2], "w", encoding="utf-8") as out:
    for node in manifest["nodes"]:
        endpoint = node["log_query"]
        out.write(f"{endpoint['host']}:{endpoint['port']}\n")
PY
LOG_RESULT=$(cd "$CLIENT_DIR" && python3 -B "$REPO_ROOT/mp1/client.py" <<< 'receivedput restart_key')
printf '%s\n' "$LOG_RESULT" | grep -Fq '10 machines responded, 0 failed'
printf '%s\n' "$LOG_RESULT" | grep -Eq 'Total matches across all machines: [1-9][0-9]*'

STATUS=$(MP3_TARGET=local MP3_LOCAL_ROOT="$MP3_LOCAL_ROOT" "$SCRIPT_DIR/status_mp" all)
printf '%s\n' "$STATUS" | grep -F 'node=UP' | wc -l | grep -q '10'
echo "PASS: ten local nodes, eight commands, Strong/Eventual-noRR/Eventual-RR, crash/recovery and MP1 log query"
