#!/bin/bash
# Demo Test 1: put + get + correct replicas on the ring (all nodes in Strong mode).
#   ./test1.sh put <writer_vm> <key>=<file> [<key>=<file> ...]   five puts, one after the other
#   ./test1.sh get <reader_vm> <key> <dataset_file>               get, show, diff
#   ./test1.sh replicas <key> <liststore_vm>                      ls, list_mem_ids, liststore, logs
# <file> is a dataset file name on the VMs, e.g. business_7. Pass the TA's choices as arguments.
. "$(dirname "$0")/demo_lib.sh"
step="${1:-}"; shift || true
case "$step" in
put)
    writer=$(vmn "${1:?writer vm}"); shift
    [ $# -ge 1 ] || die "give at least one key=file"
    say "Test 1 / Put: from VM $writer"
    for kv in "$@"; do put_file "$writer" "${kv%%=*}" "${kv#*=}"; done
    ;;
get)
    reader=$(vmn "${1:?reader vm}"); key="${2:?key}"; file="${3:?dataset file}"
    say "Test 1 / Check 1: get $key on reader VM $reader and diff with $file"
    fetch_check "$reader" "$key" "$file"
    ;;
replicas)
    key="${1:?key}"; lvm=$(vmn "${2:?VM for liststore}")
    name_ok "$key"
    say "Test 1 / Check 2: replicas of $key on the ring"
    cmd "$lvm" "ls $key"
    echo "--- first 5 successors computed from the manifest: $(owners "$key")"
    echo "--- sorted membership (ring order):"; ring_rows "$lvm"
    echo "--- liststore on VM $lvm:"; cmd "$lvm" "liststore"
    owners "$key" | tr ' ' '\n' | grep -qx "$lvm" \
        && echo "--- VM $lvm IS a replica of $key (its liststore must list it)" \
        || echo "--- VM $lvm is NOT a replica of $key (its liststore must not list it)"
    say "Logs on the five replicas (requests that mention $key)"
    for n in $(owners "$key"); do "$DEPLOY_DIR/logs_mp" "$n" "$key" 4 | cut -c1-220; done
    ;;
*) sed -n '2,7p' "$0"; exit 2;;
esac
