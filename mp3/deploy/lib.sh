#!/bin/bash
# Shared helpers for the MP3 scripts (source this file; do not run it).
#
# MP3_TARGET=remote (default): the ten course VMs over ssh, as in MP2.
# MP3_TARGET=local:            ten processes on this machine, same scripts, for practice.
#
# Settings (environment variables, all optional):
#   MP3_USER         ssh user on the VMs                      (default kbalaji4, as in MP2)
#   MP3_REMOTE_ROOT  install dir on each VM                   (default $HOME/mp3_deploy)
#   MP3_LOCAL_ROOT   parent dir for local practice VMs        (default $HOME/mp3_local)
#   MP3_DATA_DIR     dataset dir on each VM (business_1..20)  (default <root>/data)
#   MP3_R / MP3_W    Eventual read / write quorum             (default 2 / 2)
#   MP3_MODE         mode of a first (bootstrap) start        (default strong)

DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$(dirname "$DEPLOY_DIR")")"

MP3_TARGET="${MP3_TARGET:-remote}"
MP3_USER="${MP3_USER:-kbalaji4}"
MP3_REMOTE_ROOT="${MP3_REMOTE_ROOT:-\$HOME/mp3_deploy}"
MP3_LOCAL_ROOT="${MP3_LOCAL_ROOT:-$HOME/mp3_local}"
MP3_R="${MP3_R:-2}"
MP3_W="${MP3_W:-2}"
MP3_MODE="${MP3_MODE:-strong}"
GEN_DIR="$DEPLOY_DIR/.generated/$MP3_TARGET"

SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=8 -o StrictHostKeyChecking=accept-new
          -o ControlMaster=auto -o ControlPath=/tmp/mp3-ssh-%C -o ControlPersist=300)

die() { echo "ERROR: $*" >&2; exit 1; }

vm_num() {   # 3 | vm3 | VM03 | vm03  ->  3
    local s="${1#[vV][mM]}"
    [[ "$s" =~ ^[0-9]+$ ]] || return 1
    s=$((10#$s))
    [ "$s" -ge 1 ] && [ "$s" -le 10 ] && echo "$s"
}
vm_id()   { printf 'vm%02d' "$1"; }
vm_host() { if [ "$MP3_TARGET" = local ]; then echo 127.0.0.1; else printf 'fa26-cs425-48%02d.cs.illinois.edu' "$1"; fi; }
vm_root() { if [ "$MP3_TARGET" = local ]; then echo "$MP3_LOCAL_ROOT/$(vm_id "$1")"; else echo "$MP3_REMOTE_ROOT"; fi; }
vm_data() { if [ -n "${MP3_DATA_DIR:-}" ]; then echo "$MP3_DATA_DIR"; else echo "$(vm_root "$1")/data"; fi; }

# Resolve the log-query listener from the same manifest that the MP3 node uses.
vm_log_query_port() {
    local n="$1"
    vm_sh "$n" 'python3 - "$MP3_RUN/manifest.json" "$VMID" <<'"'"'PY'"'"'
import json, sys
manifest = json.load(open(sys.argv[1], encoding="utf-8"))
node = next(item for item in manifest["nodes"] if item["vm_id"] == sys.argv[2])
print(node["log_query"]["port"])
PY' 2>/dev/null
}

# parse_vms ARGS...  ->  global array VMS of VM numbers ("all" means 1..10)
parse_vms() {
    VMS=()
    [ $# -gt 0 ] || return 1
    if [ "$1" = all ]; then VMS=(1 2 3 4 5 6 7 8 9 10); return 0; fi
    local a n
    for a in "$@"; do
        n=$(vm_num "$a") || die "invalid VM '$a' (use 1..10, vm03, or all)"
        VMS+=("$n")
    done
}

# vm_sh N 'script'  -- run a script on VM N with MP3_ROOT, MP3_RUN, MP3_DATA, VMID, N set.
vm_sh() {
    local n="$1"; shift
    local pre="export N=$n VMID=$(vm_id "$n") MP3_ROOT=$(vm_root "$n") MP3_DATA=$(vm_data "$n"); "
    pre+='export MP3_RUN="$MP3_ROOT/run" PYTHONPATH="$MP3_ROOT"; '
    if [ "$MP3_TARGET" = local ]; then
        bash -c "$pre$*" </dev/null
    else
        ssh -n "${SSH_OPTS[@]}" "$MP3_USER@$(vm_host "$n")" "$pre$*"
    fi
}

# vm_pipe N 'script'  -- like vm_sh but stdin is passed through (for tar / cat uploads).
vm_pipe() {
    local n="$1"; shift
    local pre="export N=$n VMID=$(vm_id "$n") MP3_ROOT=$(vm_root "$n") MP3_DATA=$(vm_data "$n"); "
    pre+='export MP3_RUN="$MP3_ROOT/run" PYTHONPATH="$MP3_ROOT"; '
    if [ "$MP3_TARGET" = local ]; then
        bash -c "$pre$*"
    else
        ssh "${SSH_OPTS[@]}" "$MP3_USER@$(vm_host "$n")" "$pre$*"
    fi
}

# [m] keeps this very command line (which contains the pattern text) from matching itself.
NODE_PATTERN='-m [m]p3\.node --vm-id $VMID( |\$)'
vm_pid() { vm_sh "$1" "pgrep -f -- \"$NODE_PATTERN\" | head -1" 2>/dev/null; }

log_query_pid() {
    local n="$1" port
    port=$(vm_log_query_port "$n") || return 1
    [ -n "$port" ] || return 1
    # Match the Python process argv rather than the shell command that launched it.
    vm_sh "$n" "python3 - $n $port <<'PY'
import os, shlex, subprocess, sys
number, port = sys.argv[1:]
listing = subprocess.run(['ps', '-Ao', 'pid=,args='], capture_output=True, text=True, check=True)
for line in listing.stdout.splitlines():
    try:
        pid, command = line.strip().split(None, 1)
        argv = shlex.split(command)
    except (ValueError, OSError):
        continue
    if (len(argv) >= 5 and os.path.basename(argv[0]).lower().startswith('python')
            and argv[-3:] == ['server.py', number, port]):
        print(pid)
        break
PY" 2>/dev/null
}

start_log_query() {
    local n="$1" port pid
    LOG_QUERY_STARTED=0
    port=$(vm_log_query_port "$n") || { echo "VM $n: cannot read log_query port from manifest" >&2; return 1; }
    [ -n "$port" ] || { echo "VM $n: manifest has no log_query port" >&2; return 1; }
    pid=$(log_query_pid "$n")
    if [ -n "$pid" ]; then echo "VM $n: MP1 log server already running (pid $pid)"; return 0; fi
    LOG_QUERY_STARTED=1
    vm_sh "$n" "[ -f \"\$MP3_RUN/server.py\" ] && [ -f \"\$MP3_RUN/protocol.py\" ] && cd \"\$MP3_RUN\" && nohup python3 -B server.py $n $port >> log_query.out 2>&1 </dev/null &"
    local i
    for i in 1 2 3 4 5 6 7 8 9 10; do
        if vm_sh "$n" "python3 - $port <<'PY'
import socket, sys
with socket.create_connection(('127.0.0.1', int(sys.argv[1])), timeout=0.3): pass
PY" >/dev/null 2>&1; then
            pid=$(log_query_pid "$n")
            if [ -n "$pid" ]; then
                echo "VM $n: MP1 log server ready on manifest port $port (pid $pid)"
                return 0
            fi
            echo "VM $n: manifest log_query port $port is occupied by an untracked process" >&2
            LOG_QUERY_STARTED=0
            return 1
        fi
        sleep 0.2
    done
    echo "VM $n: MP1 log server failed to listen on $port" >&2
    vm_sh "$n" 'tail -n 20 "$MP3_RUN/log_query.out"' >&2
    pid=$(log_query_pid "$n")
    [ -z "$pid" ] || stop_log_query "$n" >/dev/null 2>&1
    return 1
}

stop_log_query() {
    local n="$1" pid i
    pid=$(log_query_pid "$n")
    [ -n "$pid" ] || { echo "VM $n: MP1 log server is not running"; return 0; }
    vm_sh "$n" "kill -TERM $pid"
    for i in 1 2 3 4 5 6 7 8 9 10; do
        sleep 0.2
        [ -z "$(log_query_pid "$n")" ] && { echo "VM $n: MP1 log server stopped"; return 0; }
    done
    echo "VM $n: MP1 log server did not stop within 2 seconds" >&2
    return 1
}

# for_each_vm FUNCTION VM...  -- run FUNCTION VM in parallel; returns nonzero if any failed.
for_each_vm() {
    local fn="$1"; shift
    local pids=() n failed=0 p
    for n in "$@"; do "$fn" "$n" & pids+=($!); done
    for p in "${pids[@]}"; do wait "$p" || failed=1; done
    return $failed
}

# Manifest and inventory come from gen_manifest.py; the same files go to every VM.
ensure_generated() {
    if [ "$MP3_TARGET" = local ]; then
        python3 "$DEPLOY_DIR/gen_manifest.py" --target local --out "$GEN_DIR" >/dev/null || die "gen_manifest failed"
    else
        python3 "$DEPLOY_DIR/gen_manifest.py" --target remote --out "$GEN_DIR" >/dev/null || die "gen_manifest failed"
    fi
}

# owners KEY -> the five replica VM numbers in ring order (pure computation from the manifest)
owners() {
    ensure_generated
    PYTHONPATH="$REPO_ROOT" python3 - "$GEN_DIR/manifest.json" "$1" <<'PY'
import sys
from mp3.ring import load_manifest
m = load_manifest(sys.argv[1])
print(" ".join(str(int(n.vm_id[2:])) for n in m.owners(sys.argv[2])))
PY
}
