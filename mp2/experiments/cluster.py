import os
import re
import shlex
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Basic settings for the ten-VM cluster.
NETID = os.environ.get("MP2_NETID", "CHANGE_ME")
REMOTE_DIR = "fa26_cs425_.group-mp_.team-48/mp2"
HOSTS = [f"fa26-cs425-48{i:02d}.cs.illinois.edu" for i in range(1, 11)]
PORT = 65433
SRC_FILES = ["node.py", "membership.py", "protocol.py", "machines.txt"]
LOCAL_SRC = os.environ.get("MP2_SRC", ".")
RESULTS_DIR = "results"
ALL_VMS = list(range(1, 11))
INTRODUCER = 1
_CTL_DIR = os.path.expanduser("~/.ssh/mp2-ctl")
_SSH_OPTS = [
    "-o", "BatchMode=yes",
    "-o", "StrictHostKeyChecking=no",
    "-o", "ControlMaster=auto",
    "-o", f"ControlPath={_CTL_DIR}/%r@%h:%p",
    "-o", "ControlPersist=900",
    "-o", "ConnectTimeout=10",
]
os.makedirs(_CTL_DIR, mode=0o700, exist_ok=True)


def host(vm):
    return HOSTS[vm - 1]


def _proc_pattern(vm):
    return f"^python3 node.py {vm}$"


# Run a shell command on one VM over SSH.
def ssh(vm, cmd, timeout=60):
    argv = ["ssh", *_SSH_OPTS, f"{NETID}@{host(vm)}", cmd]
    p = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    if p.returncode != 0 and p.stderr.strip():
        print(f"  [vm{vm}] stderr: {p.stderr.strip()[:200]}")
    return p.stdout.strip()


# Run work on several VMs at the same time.
def parallel(vms, fn, workers=12):
    out = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fn, vm): vm for vm in vms}
        for f in as_completed(futs):
            vm = futs[f]
            try:
                out[vm] = f.result()
            except Exception as e:
                out[vm] = f"ERROR: {e}"
                print(f"  [vm{vm}] {e}")
    return out


def warm_connections(vms=ALL_VMS):
    parallel(vms, lambda vm: ssh(vm, "true", timeout=20))


# Copy the current MP2 files to the selected VMs.
def push(vms=ALL_VMS):
    def _one(vm):
        ssh(vm, f"mkdir -p ~/{REMOTE_DIR}")
        argv = [
            "scp", *_SSH_OPTS,
            *[os.path.join(LOCAL_SRC, f) for f in SRC_FILES],
            f"{NETID}@{host(vm)}:~/{REMOTE_DIR}/",
        ]
        subprocess.run(argv, capture_output=True, text=True, timeout=120)
        return "pushed"
    print(f"push -> {len(vms)} VMs")
    parallel(vms, _one)


def is_running(vm):
    return bool(ssh(vm, f'pgrep -f {shlex.quote(_proc_pattern(vm))} || true'))


# Restart nodes with fresh logs for a new trial.
def restart(vms=ALL_VMS, clear_logs=True):
    clear = "rm -f machine.*.log node.out;" if clear_logs else ""
    def _one(vm):
        pat = shlex.quote(_proc_pattern(vm))
        ssh(vm, f'pkill -9 -f {pat} || true', timeout=30)
        ssh(
            vm,
            f"cd ~/{REMOTE_DIR} && {clear} rm -f mp2_cmd && mkfifo mp2_cmd",
            timeout=30,
        )
        launch = (
            f"cd ~/{REMOTE_DIR} && "
            f"exec bash -c 'exec 3<>mp2_cmd; "
            f"exec env PYTHONUNBUFFERED=1 python3 node.py {vm} <&3 "
            f"> node.out 2>&1'"
        )
        subprocess.run(
            ["ssh", "-f", *_SSH_OPTS, f"{NETID}@{host(vm)}", launch],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=30,
        )
        return "launched"
    if INTRODUCER in vms:
        _one(INTRODUCER)
        time.sleep(1.0)
    others = [v for v in vms if v != INTRODUCER]
    if others:
        parallel(others, _one)
    time.sleep(2.0)
    live = parallel(vms, is_running)
    down = [vm for vm, ok in live.items() if not ok]
    if down:
        print(f"  WARNING: not running after restart: {down}")
        for vm in down[:3]:
            tail = ssh(vm, f"tail -n 15 ~/{REMOTE_DIR}/node.out 2>/dev/null || true")
            if tail:
                print(f"  --- vm{vm} node.out ---\n{tail}")
    return not down


def stop(vms=ALL_VMS):
    parallel(vms, lambda vm: ssh(vm, f'pkill -9 -f {shlex.quote(_proc_pattern(vm))} || true'))


# Send a CLI command to a node through its FIFO.
def send_cmd(vm, command):
    remote = f"cat > ~/{REMOTE_DIR}/mp2_cmd"
    argv = ["ssh", *_SSH_OPTS, f"{NETID}@{host(vm)}", remote]
    subprocess.run(argv, input=command + "\n", capture_output=True,
                   text=True, timeout=30)


def broadcast_cmd(vms, command):
    parallel(vms, lambda vm: send_cmd(vm, command))


# Helpers below change the live experiment settings.
def set_mode(mode, vm=INTRODUCER):
    assert mode in ("suspect", "nosuspect")
    send_cmd(vm, f"switch {mode}")


def set_drop_rate(rate_pct, vm=INTRODUCER):
    send_cmd(vm, f"set_drop_rate {rate_pct}")


def dump_mem(vms):
    broadcast_cmd(vms, "dump_mem")


def mark(vms, text):
    broadcast_cmd(vms, f"mark {text}")


def bwstats(vms):
    broadcast_cmd(vms, "bwstats")


# Schedule crashes using the VMs' own clocks.
def kill_at(vms, at_epoch):
    def _one(vm):
        pat = shlex.quote(_proc_pattern(vm))
        remote = (
            f'pid=$(pgrep -f {pat}); '
            f'python3 -c "import time; t={at_epoch!r}; '
            f'time.sleep(max(0, t - time.time()))"; '
            f'date +%s.%N; '
            f'kill -9 $pid'
        )
        out = ssh(vm, remote, timeout=60)
        line = out.splitlines()[0] if out else ""
        try:
            return float(line)
        except ValueError:
            return None
    return parallel(vms, _one)


def kill_simultaneous(vms, lead=4.0):
    target = time.time() + lead
    stamps = kill_at(vms, target)
    good = [t for t in stamps.values() if t]
    if good:
        spread = (max(good) - min(good)) * 1000
        print(f"  kill spread: {spread:.1f} ms across {len(good)} VMs")
    return stamps


# Pull each VM's log back after a trial.
def collect_logs(vms, dest_dir):
    os.makedirs(dest_dir, exist_ok=True)
    def _one(vm):
        src = f"{NETID}@{host(vm)}:~/{REMOTE_DIR}/machine.{vm}.log"
        dst = os.path.join(dest_dir, f"machine.{vm}.log")
        subprocess.run(["scp", *_SSH_OPTS, src, dst],
                       capture_output=True, text=True, timeout=120)
        return os.path.exists(dst)
    got = parallel(vms, _one)
    missing = [vm for vm, ok in got.items() if not ok]
    if missing:
        print(f"  WARNING: no log retrieved from {missing}")
    return dest_dir


# Wait a little between experiment steps when needed.
def settle(seconds=15, label=""):
    if label:
        print(f"  settling {seconds}s ({label})")
    time.sleep(seconds)
