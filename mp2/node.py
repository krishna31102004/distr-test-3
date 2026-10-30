# Imports used by the membership server and CLI.
import sys
import socket
import threading
import random
import time
import json
import os

from protocol import send_msg, recv_msg, get_byte_counts
from membership import (
    MembershipList,
    ALIVE,
    PLAIN_FAILURE_TIMEOUT,
    SUSPECT_TIMEOUT,
    CONFIRM_TIMEOUT,
)


# Main timing and fanout settings for gossip.
GOSSIP_INTERVAL = 0.5
CHECK_INTERVAL = 0.1
FANOUT = 3
JOIN_TIMEOUT = 2.0
JOIN_RETRIES = 3
DEFAULT_PORT = 65433
LEAVE_SEND_TIMEOUT = 0.5
CONFIG_FILE = "machines.txt"


if len(sys.argv) not in (2, 3):
    print("Usage: python3 node.py <vm_number> [port]")
    sys.exit(1)


VM_NUMBER = int(sys.argv[1])
PORT = int(sys.argv[2]) if len(sys.argv) == 3 else DEFAULT_PORT
LOG_FILE = f"machine.{VM_NUMBER}.log"


# Load the VM hostnames from the shared machines file.
def load_machines():
    """Load hostnames from machines.txt. The port in that file is not used here."""
    hosts = []

    with open(CONFIG_FILE) as f:
        for line in f:
            line = line.strip()
            if line:
                host, _ = line.split(":")
                hosts.append(host)

    return hosts


MACHINES = load_machines()
SELF_HOST = MACHINES[VM_NUMBER - 1]
INTRODUCER_HOST = MACHINES[0]

# Each restart gets a fresh node ID using the current timestamp.
SELF_ID = f"{SELF_HOST}:{PORT}:{time.time_ns()}"


_log_lock = threading.Lock()


# Write the same timestamped event to the terminal and local log.
def log_event(msg):
    now = time.time()
    seconds = int(now)
    micros = int((now - seconds) * 1_000_000)
    line = (
        f"[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now))}"
        f".{micros // 1000:03d} epoch={seconds}.{micros:06d}] {msg}"
    )

    with _log_lock:
        print(line)

        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")


membership = MembershipList(SELF_ID, on_event=log_event)


def send_message(host, port, payload, expect_reply=False, timeout=2.0):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        s.connect((host, port))
        send_msg(s, json.dumps(payload).encode())

        if expect_reply:
            return json.loads(recv_msg(s).decode())

    return None


def node_id_to_host_port(node_id):
    host, port, _version = node_id.split(":")
    return host, int(port)


def send_gossip_to_peer(nid, payload):
    host, port = node_id_to_host_port(nid)

    try:
        send_message(host, port, payload)
    except (socket.timeout, ConnectionRefusedError, OSError):
        pass


# Send the latest membership view to a small random set of peers.
def send_gossip_round():
    membership.tick_heartbeat()

    payload = {
        "type": "GOSSIP",
        "sender_id": SELF_ID,
        "membership_list": membership.snapshot(),
    }

    targets = membership.alive_peers()
    random.shuffle(targets)

    count = 0

    for nid in targets:
        if count >= FANOUT:
            break

        thread = threading.Thread(
            target=send_gossip_to_peer,
            args=(nid, payload),
            daemon=True,
        )

        thread.start()
        count += 1


# Send a self-refutation to every live peer so it spreads quickly.
def send_refutation_round():
    payload = {
        "type": "GOSSIP",
        "sender_id": SELF_ID,
        "membership_list": membership.snapshot(),
    }

    for nid in membership.alive_peers():
        threading.Thread(
            target=send_gossip_to_peer,
            args=(nid, payload),
            daemon=True,
        ).start()


def gossip_loop():
    while True:
        time.sleep(GOSSIP_INTERVAL)
        send_gossip_round()


def timeout_check_loop():
    while True:
        time.sleep(CHECK_INTERVAL)
        membership.run_timeout_check()


def send_config_to_group():
    payload = {
        "type": "CONFIG",
        "sender_id": SELF_ID,
        "suspicion_mode": "suspect" if membership.suspicion_enabled else "nosuspect",
        "drop_rate": membership.drop_rate,
    }

    for nid in membership.alive_peers():
        host, port = node_id_to_host_port(nid)

        try:
            send_message(host, port, payload)
        except (socket.timeout, ConnectionRefusedError, OSError):
            pass


def handle_connection(conn):
    try:
        msg = json.loads(recv_msg(conn).decode())
        mtype = msg["type"]

        # Apply simulated drops before processing an incoming gossip message.
        if mtype == "GOSSIP":
            if random.random() < membership.drop_rate:
                log_event(f"DROPPED gossip message from {msg.get('sender_id')}")
                return

            if membership.merge(msg["membership_list"]):
                send_refutation_round()

        elif mtype == "JOIN":
            new_id = msg["sender_id"]

            membership.merge([
                {
                    "node_id": new_id,
                    "heartbeat": 0,
                    "incarnation": 0,
                    "status": ALIVE,
                }
            ])

            reply = {
                "type": "GOSSIP",
                "sender_id": SELF_ID,
                "membership_list": membership.snapshot(),
                "suspicion_mode": "suspect" if membership.suspicion_enabled else "nosuspect",
                "drop_rate": membership.drop_rate,
            }

            send_msg(conn, json.dumps(reply).encode())

        elif mtype == "LEAVE":
            membership.merge(msg["membership_list"])

        elif mtype == "CONFIG":
            membership.set_suspicion_mode(msg["suspicion_mode"] == "suspect")
            membership.set_drop_rate(msg["drop_rate"])

            log_event(
                f"CONFIG updated from {msg.get('sender_id')}: "
                f"mode={msg['suspicion_mode']}, drop_rate={msg['drop_rate']}"
            )

    except Exception as e:
        log_event(f"error handling connection: {e}")

    finally:
        conn.close()


def server_loop():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            s.bind(("", PORT))
        except OSError as e:
            log_event(f"FATAL: could not bind to port {PORT}: {e}")
            os._exit(1)

        s.listen(16)

        while True:
            conn, _addr = s.accept()

            threading.Thread(
                target=handle_connection,
                args=(conn,),
                daemon=True,
            ).start()


# Non-introducer nodes join through VM1 and copy its current state.
def do_join():
    payload = {"type": "JOIN", "sender_id": SELF_ID}

    for attempt in range(JOIN_RETRIES):
        try:
            reply = send_message(
                INTRODUCER_HOST,
                DEFAULT_PORT,
                payload,
                expect_reply=True,
                timeout=JOIN_TIMEOUT,
            )

            if reply:
                membership.merge(reply["membership_list"])
                membership.set_suspicion_mode(
                    reply.get("suspicion_mode") == "suspect"
                )
                membership.set_drop_rate(reply.get("drop_rate", 0.0))
                log_event("JOIN successful")
                return

        except (socket.timeout, ConnectionRefusedError, OSError):
            log_event(f"JOIN attempt {attempt + 1} failed, retrying")

    log_event("JOIN failed after all retries — introducer may be down")


def do_leave():
    membership.mark_self_left()

    payload = {
        "type": "LEAVE",
        "sender_id": SELF_ID,
        "membership_list": membership.snapshot(),
    }

    targets = membership.alive_peers()
    random.shuffle(targets)

    for nid in targets[:FANOUT]:
        host, port = node_id_to_host_port(nid)

        try:
            send_message(
                host,
                port,
                payload,
                timeout=LEAVE_SEND_TIMEOUT,
            )
        except (socket.timeout, ConnectionRefusedError, OSError):
            pass

    log_event("LEAVE complete, exiting")
    sys.exit(0)


# Handle the required demo commands plus experiment helpers.
def cli_loop():
    while True:
        try:
            cmd = input().strip()
        except EOFError:
            break

        if not cmd:
            continue

        parts = cmd.split()
        name = parts[0]

        if name == "list_mem":
            for row in membership.list_mem():
                print(row)

        elif name == "dump_mem":
            log_event("MEMDUMP " + json.dumps(membership.snapshot()))

        elif name == "list_self":
            print(SELF_ID)

        elif name == "bwstats":
            sent, received = get_byte_counts()
            log_event(f"BWSTATS bytes_sent={sent} bytes_received={received}")

        elif name == "mark" and len(parts) >= 2:
            log_event(f"MARK {cmd.split(maxsplit=1)[1]}")

        elif name == "join":
            do_join()

        elif name == "leave":
            do_leave()

        elif name == "display_suspects":
            for nid, t in membership.get_suspects_log():
                print(
                    f"{nid} suspected at "
                    f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t))}"
                )

        elif name == "switch" and len(parts) == 2:
            membership.set_suspicion_mode(parts[1] == "suspect")
            send_config_to_group()
            print(f"suspicion mode set to {parts[1]}")

        elif name == "display_protocol":
            print("suspect" if membership.suspicion_enabled else "nosuspect")

        elif name == "set_drop_rate" and len(parts) == 2:
            try:
                raw = parts[1].strip()

                if raw.endswith("%"):
                    raw = raw[:-1]

                percent = float(raw)

                if not 0.0 <= percent <= 100.0:
                    raise ValueError

                rate = percent / 100.0

                membership.set_drop_rate(rate)
                send_config_to_group()

                print(f"drop rate set to {percent:g}%")

            except ValueError:
                print("usage: set_drop_rate <0-100%>")

        else:
            print("unknown command")


if __name__ == "__main__":
    log_event(
        f"PARAMS gossip_interval={GOSSIP_INTERVAL} "
        f"fanout={FANOUT} "
        f"check_interval={CHECK_INTERVAL} "
        f"plain_timeout={PLAIN_FAILURE_TIMEOUT} "
        f"suspect_timeout={SUSPECT_TIMEOUT} "
        f"confirm_timeout={CONFIRM_TIMEOUT}"
    )

    threading.Thread(target=server_loop, daemon=True).start()
    threading.Thread(target=gossip_loop, daemon=True).start()
    threading.Thread(target=timeout_check_loop, daemon=True).start()

    if VM_NUMBER != 1:
        do_join()

    cli_loop()
