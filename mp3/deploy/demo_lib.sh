#!/bin/bash
# Helpers shared by test1.sh, test2.sh, test3.sh and rehearse.sh (source; do not run).
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

say()  { printf '\n=== %s\n' "$*"; }
cmd()  { "$DEPLOY_DIR/cmd_mp" "$@"; }                 # ./cmd_mp <vm> "<command>"
name_ok() { [[ "$1" =~ ^[A-Za-z0-9._-]+$ ]] || die "'$1' must contain only letters, digits, . _ -"; }
vmn()  { vm_num "$1" || die "invalid VM '$1'"; }

# jfield NAME  -- read the last JSON line on stdin and print one field.
jfield() { python3 -c 'import sys, json
d = json.loads(sys.stdin.read().strip().splitlines()[-1]); print(d.get(sys.argv[1], ""))' "$1"; }

# put_file VM KEY DATASET_FILE
put_file() {
    name_ok "$2"; name_ok "$3"
    cmd "$1" "put $2 @data/$3" || die "put failed"
}

# fetch_check READER KEY DATASET_FILE  -- get to a local file, show it, diff with the dataset copy
fetch_check() {
    local reader="$1" key="$2" file="$3"
    name_ok "$key"; name_ok "$file"
    cmd "$reader" "get $key @out/$key.fetched" || die "get failed"
    vm_sh "$reader" "echo '--- tail of fetched file on VM $reader:'; tail -n 3 \"\$MP3_RUN/out/$key.fetched\" | cut -c1-100
        if diff \"\$MP3_RUN/out/$key.fetched\" \"\$MP3_DATA/$file\" >/dev/null; then
            echo 'diff: IDENTICAL to dataset file $file'; else echo 'diff: DIFFERENT from dataset file $file'; exit 1; fi"
}

# replica_copy VM KEY OUTNAME  -- VM asks for its own copy of KEY (no repair); prints the version
replica_copy() {
    local vm="$1" key="$2" out="$3"
    cmd "$vm" "getfromreplica vm$(printf '%02d' "$vm") $key @out/$out"
}

# ring_rows VIA_VM [VM...]  -- membership rows (status, ring id) as seen by VIA_VM, ring order
ring_rows() {
    local via="$1"; shift
    cmd "$via" "list_mem_ids" | tail -n 1 | python3 -c '
import sys, json
want = {"vm%02d" % int(v) for v in sys.argv[1:]}
for m in json.loads(sys.stdin.read())["members"]:
    if not want or m["vm"] in want:
        print("  %s ring_id=%s... %s generation=%s" % (m["vm"], m["ring_id"][:12], m["status"], m["generation"]))' "$@"
}

alive_owners() { local n; for n in $(owners "$1"); do [ -n "$(vm_pid "$n")" ] && echo "$n"; done; }

# pick_two KEY [VM...]  -- two random live replicas of KEY, never the VMs listed after KEY
pick_two() {
    local key="$1"; shift
    local c n x pool=()
    for n in $(alive_owners "$key"); do
        x=0; for c in "$@"; do [ "$c" = "$n" ] && x=1; done
        [ $x = 0 ] && pool+=("$n")
    done
    [ "${#pool[@]}" -ge 2 ] || die "fewer than two live replicas to pick from"
    printf '%s\n' "${pool[@]}" | sort -R | head -2 | tr '\n' ' '
}

# sha_of VM PATH  -- sha256 of a file on a VM (PATH may use $MP3_RUN / $MP3_DATA)
sha_of() { vm_sh "$1" "(sha256sum \"$2\" 2>/dev/null || shasum -a 256 \"$2\") | cut -d' ' -f1"; }
