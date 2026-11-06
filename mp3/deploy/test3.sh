#!/bin/bash
# Demo Test 3: concurrent puts under Eventual-RR (R=2, W=2, membership retained).
#   ./test3.sh mode [via_vm]                         switch all ten nodes to eventual-RR
#   ./test3.sh put <writer_vm> <key> <file>          failure-free put
#   ./test3.sh get <reader_vm> <key> <file>          get (wait a while after the put first)
#   ./test3.sh kill <key> [vm]                       crash one replica of <key> (random live if omitted)
#   ./test3.sh multiput <key> <vm> <vm> ...          concurrent puts from >= 7 live client VMs;
#                                                    each client puts its own dataset file business_<vm>
#   ./test3.sh restart <vm>                          restart the failed replica with its data
#   ./test3.sh gfr <key> <vm>                        value + version on that replica (no repair)
#   ./test3.sh readrepair <key>                      read-repair events from every replica's log
#   ./test3.sh verify <key>                          getfromreplica on all five replicas: same
#                                                    version, same bytes, equals one input file
. "$(dirname "$0")/demo_lib.sh"
step="${1:-}"; shift || true
case "$step" in
mode)
    via=$(vmn "${1:-1}")
    say "Test 3: switch all ten nodes to eventual-RR (membership retained)"
    cmd "$via" "consistency_mode eventual-RR"
    ;;
put)
    writer=$(vmn "${1:?writer vm}"); key="${2:?key}"; file="${3:?file}"
    say "Test 3: failure-free put of $key"
    put_file "$writer" "$key" "$file"
    ;;
get)
    reader=$(vmn "${1:?reader vm}"); key="${2:?key}"; file="${3:?file}"
    say "Test 3: get $key on VM $reader, diff with $file"
    fetch_check "$reader" "$key" "$file"
    ;;
kill)
    key="${1:?key}"; name_ok "$key"
    if [ -n "${2:-}" ]; then victim=$(vmn "$2"); else victim=$(pick_two "$key" | cut -d' ' -f1); fi
    say "Test 3: replicas of $key (ring order): $(owners "$key"); crashing VM $victim"
    "$DEPLOY_DIR/kill_mp" "$victim"
    ;;
multiput)
    key="${1:?key}"; shift; name_ok "$key"
    [ $# -ge 7 ] || die "multiput needs at least seven live client VMs"
    labels=""; for n in "$@"; do labels+=", VM$(vmn "$n")"; done
    for n in "$@"; do [ -n "$(vm_pid "$(vmn "$n")")" ] || die "client VM $n is not running"; done
    say "Test 3: multiput($key$labels) launched from VM $(vmn "$1")"
    out=$(cmd "$(vmn "$1")" "multiput($key$labels)") || true
    echo "$out" | head -1
    # Per-client result. In Eventual mode a put that is overtaken by a newer concurrent put
    # at the replicas is reported as an error by design; the newest version wins everywhere.
    echo "$out" | tail -n 1 | python3 -c '
import sys, json
d = json.loads(sys.stdin.read())
print("multiput status:", d.get("status"), "-", d.get("message", ""))
for n in d.get("nodes", []):
    print("  %-5s %-6s version=%s file_sha=%s %s" % (n["vm"], n["status"], n.get("version"), n.get("sha256"), n.get("error", "")))'
    ;;
restart)
    [ $# -ge 1 ] || die "usage: restart <vm>"
    say "Test 3: restarting VM $* with its existing data"
    "$DEPLOY_DIR/run_mp" "$@"
    ;;
gfr)
    key="${1:?key}"; vm=$(vmn "${2:?vm}"); name_ok "$key"
    say "Test 3: getfromreplica on VM $vm (its own value and version, no repair)"
    replica_copy "$vm" "$key" "$key.gfr"
    ;;
readrepair)
    key="${1:?key}"; name_ok "$key"
    say "Test 3: read-repair events for $key on each replica's log"
    for n in $(owners "$key"); do "$DEPLOY_DIR/logs_mp" "$n" "read_repair" 3 | cut -c1-400; done
    ;;
verify)
    key="${1:?key}"; name_ok "$key"
    say "Test 3 / Check read repair: getfromreplica on all five replicas of $key"
    versions=""; shas=""
    for n in $(owners "$key"); do
        out=$(replica_copy "$n" "$key" "$key.final") || die "getfromreplica failed on VM $n"
        echo "$out" | head -1; echo "$out" | tail -n 1 | cut -c1-230
        versions+="$(echo "$out" | jfield version)"$'\n'
        shas+="$(sha_of "$n" "\$MP3_RUN/out/$key.final")"$'\n'
    done
    nv=$(printf '%s' "$versions" | sort -u | grep -c .); ns=$(printf '%s' "$shas" | sort -u | grep -c .)
    echo "--- distinct versions across the five replicas: $nv  ($(printf '%s' "$versions" | sort -u | tr '\n' ' '))"
    echo "--- distinct value hashes across the five replicas: $ns"
    sha=$(printf '%s' "$shas" | sort -u | head -1)
    match=""
    for f in $(vm_sh "$(owners "$key" | cut -d' ' -f1)" 'ls "$MP3_DATA" | grep "^business_"'); do
        s=$(sha_of "$(owners "$key" | cut -d' ' -f1)" "\$MP3_DATA/$f")
        [ "$s" = "$sha" ] && match+="$f "
    done
    echo "--- the value equals dataset file(s): ${match:-NONE}"
    if [ "$nv" = 1 ] && [ "$ns" = 1 ] && [ -n "$match" ]; then echo "RESULT: PASS"; else echo "RESULT: FAIL"; exit 1; fi
    ;;
*) sed -n '2,15p' "$0"; exit 2;;
esac
