#!/bin/bash
# Full dress rehearsal of demo Tests 1-3 with default choices, timed per test.
# Start from a fresh, all-Strong, ten-node cluster (reset_mp all; run_mp all; check_prep).
#   ./rehearse.sh            all three tests
#   ./rehearse.sh 1 | 2 | 3  one test (2 needs 1 done first; 3 needs 1 and 2)
# Choices (environment): KEY (default k1), WRITER, READER (default VM 2 and VM 6; READER must
# differ from WRITER), FILES (default business_1..5 for the five puts).
. "$(dirname "$0")/demo_lib.sh"
T="$DEPLOY_DIR"
set -e   # stop at the first failing step
KEY="${KEY:-k1}"; WRITER="${WRITER:-2}"; READER="${READER:-6}"
[ "$WRITER" != "$READER" ] || die "READER must differ from WRITER"
which_tests="${1:-all}"
start=$(date +%s); mark() { echo; echo "##### $1 done in $(( $(date +%s) - start ))s"; start=$(date +%s); }

# A live VM that is not in the given list (a client that has not been killed).
live_client() { local n skip c; for n in 1 2 3 4 5 6 7 8 9 10; do skip=0; for c in "$@"; do [ "$c" = "$n" ] && skip=1; done
    [ $skip = 0 ] && [ -n "$(vm_pid "$n")" ] && { echo "$n"; return; }; done; die "no live client"; }

if [ "$which_tests" = all ] || [ "$which_tests" = 1 ]; then
    "$T/test1.sh" put "$WRITER" "$KEY=business_1" k2=business_2 k3=business_3 k4=business_4 k5=business_5
    "$T/test1.sh" get "$READER" "$KEY" business_1
    "$T/test1.sh" replicas "$KEY" 3
    mark "Test 1"
fi

if [ "$which_tests" = all ] || [ "$which_tests" = 2 ]; then
    first=$("$T/test2.sh" kill "$KEY" | tee /dev/stderr | sed -n 's/.*crashing: //p' | head -1)
    set -- $first; a=$1; b=$2
    w=$(live_client $a $b)
    "$T/test2.sh" put "$w" "$KEY" business_6
    "$T/test2.sh" restart "$a" "$b"
    second=$("$T/test2.sh" kill "$KEY" --not "$a" "$b" | tee /dev/stderr | sed -n 's/.*crashing: //p' | head -1)
    set -- $second; c=$1; d=$2
    "$T/test2.sh" precheck "$KEY" "$a" "$b"
    r=$(live_client $c $d)
    "$T/test2.sh" get "$r" "$KEY" business_6
    live3=$(for n in $(owners "$KEY"); do [ "$n" != "$c" ] && [ "$n" != "$d" ] && printf '%s ' "$n"; done)
    "$T/test2.sh" gfr "$KEY" business_6 $live3
    "$T/run_mp" "$c" "$d"          # Test 3 needs all ten processes
    mark "Test 2"
fi

if [ "$which_tests" = all ] || [ "$which_tests" = 3 ]; then
    "$T/test3.sh" mode 1
    "$T/test3.sh" put "$WRITER" "$KEY" business_7
    sleep 3
    "$T/test3.sh" get "$READER" "$KEY" business_7
    victim=$("$T/test3.sh" kill "$KEY" | tee /dev/stderr | sed -n 's/.*crashing VM //p' | head -1)
    clients=$(for n in 1 2 3 4 5 6 7 8 9 10; do [ "$n" != "$victim" ] && printf '%s ' "$n"; done)
    "$T/test3.sh" multiput "$KEY" $clients
    "$T/test3.sh" restart "$victim"
    "$T/test3.sh" gfr "$KEY" "$victim"
    r=$(live_client "$victim")
    cmd "$r" "get $KEY @out/$KEY.afterfail" | tail -n 1 | cut -c1-260
    sleep 5
    "$T/test3.sh" readrepair "$KEY"
    "$T/test3.sh" verify "$KEY"
    mark "Test 3"
fi
