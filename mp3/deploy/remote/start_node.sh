#!/bin/bash
# Runs ON a VM (started by run_mp): start this VM's node.
# First start creates a fresh database; every later start recovers the existing one, so a
# node killed with kill -9 comes back with its on-disk data and its original ring identity.
# Inputs (environment): VMID (vm01..vm10), N (1..10), optional MODE, R, W.
set -u
: "${VMID:?VMID is required}" "${N:?N is required}"
RUN="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$RUN")"
export PYTHONPATH="$ROOT" PYTHONUNBUFFERED=1
cd "$RUN" || exit 2

DB="$RUN/$VMID.db"
if [ -e "$DB" ]; then
    LIFECYCLE=(--recover)
else
    LIFECYCLE=(--bootstrap --bootstrap-mode "${MODE:-strong}")
fi

# This VM's multiput input: its own dataset file business_<N> when present, else the
# node's default (random bytes), so concurrent writers always use distinct values.
EXTRA=()
DATA="${MP3_DATA:-$ROOT/data}"
for f in "$DATA/business_$N" "$DATA/business_$N".*; do
    if [ -f "$f" ]; then EXTRA=(--multiput-file "$f"); break; fi
done

exec python3 -m mp3.node --vm-id "$VMID" --manifest "$RUN/manifest.json" --db "$DB" \
    "${LIFECYCLE[@]}" --read-quorum "${R:-2}" --write-quorum "${W:-2}" \
    --schema2-inventory "$RUN/inventory.json" --log "$RUN/machine.$N.log" ${EXTRA[@]+"${EXTRA[@]}"}
