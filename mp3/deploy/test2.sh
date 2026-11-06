#!/bin/bash
# Demo Test 2: failures + Strong consistency (stay in Strong mode).
#   ./test2.sh kill <key> [--not vm vm] [vmA vmB]   kill -9 two replicas of <key> (random live
#                                                    ones unless named; --not excludes VMs)
#   ./test2.sh put <writer_vm> <key> <file>          put with the TA's file (writer must be up)
#   ./test2.sh restart <vmA> <vmB>                   start the killed processes again (recover)
#   ./test2.sh precheck <key> <vmA> <vmB>            getfromreplica on both restarted replicas
#                                                    + membership shows them ALIVE, same ring ids
#   ./test2.sh get <reader_vm> <key> <file>          get, show, diff against the dataset file
#   ./test2.sh gfr <key> <file> <vm> <vm> <vm>       getfromreplica on the 3 live replicas,
#                                                    diff each with the dataset file, show logs
# <file> is a dataset file name on the VMs, e.g. business_7.
. "$(dirname "$0")/demo_lib.sh"
step="${1:-}"; shift || true
case "$step" in
kill)
    key="${1:?key}"; shift; name_ok "$key"
    excl=()
    if [ "${1:-}" = "--not" ]; then shift; while [ $# -gt 0 ] && vm_num "$1" >/dev/null; do excl+=("$(vm_num "$1")"); shift; done; fi
    if [ $# -ge 2 ]; then victims="$(vmn "$1") $(vmn "$2")"; else victims=$(pick_two "$key" ${excl[@]+"${excl[@]}"}); fi
    say "Test 2: replicas of $key are (ring order): $(owners "$key"); crashing: $victims"
    "$DEPLOY_DIR/kill_mp" $victims
    ;;
put)
    writer=$(vmn "${1:?writer vm}"); key="${2:?key}"; file="${3:?file}"
    [ -n "$(vm_pid "$writer")" ] || die "VM $writer is not running; choose a live writer"
    say "Test 2: put $key from VM $writer with replicas down: $(for n in $(owners "$key"); do [ -z "$(vm_pid "$n")" ] && printf 'vm%s ' "$n"; done)"
    put_file "$writer" "$key" "$file"
    ;;
restart)
    [ $# -ge 1 ] || die "usage: restart <vm> [<vm>]"
    say "Test 2: restarting $* with their existing on-disk data"
    "$DEPLOY_DIR/run_mp" "$@"
    ;;
precheck)
    key="${1:?key}"; shift; name_ok "$key"
    [ $# -ge 1 ] || die "usage: precheck <key> <vm> [<vm>]"
    say "Test 2 / Pre-check: restarted replicas still have their data"
    for n in "$@"; do
        n=$(vmn "$n"); replica_copy "$n" "$key" "$key.replica" || echo "(VM $n has no copy)"
    done
    via=$(vm_num 1); for c in "$@"; do [ "$(vm_num "$c")" = 1 ] && via=$(for n in 2 3 4 5; do [ -n "$(vm_pid "$n")" ] && echo "$n" && break; done); done
    echo "--- membership as seen by VM $via (status and ring ids of the restarted VMs):"
    ring_rows "$via" "$@"
    ;;
get)
    reader=$(vmn "${1:?reader vm}"); key="${2:?key}"; file="${3:?file}"
    say "Test 2 / Check 1: get $key on reader VM $reader and diff with $file"
    fetch_check "$reader" "$key" "$file"
    ;;
gfr)
    key="${1:?key}"; file="${2:?file}"; shift 2; name_ok "$key"; name_ok "$file"
    [ $# -ge 1 ] || die "usage: gfr <key> <file> <vm> <vm> <vm>"
    say "Test 2 / Check 2: getfromreplica of $key on VMs $*"
    for n in "$@"; do
        n=$(vmn "$n")
        replica_copy "$n" "$key" "$key.gfr" || die "getfromreplica failed on VM $n"
        vm_sh "$n" "diff \"\$MP3_RUN/out/$key.gfr\" \"\$MP3_DATA/$file\" >/dev/null && echo 'diff vs $file: IDENTICAL' || { echo 'diff vs $file: DIFFERENT'; exit 1; }" || exit 1
    done
    say "Logs on those VMs (getfromreplica requests for $key)"
    for n in "$@"; do "$DEPLOY_DIR/logs_mp" "$(vmn "$n")" "$key" 3 | cut -c1-200; done
    ;;
*) sed -n '2,14p' "$0"; exit 2;;
esac
