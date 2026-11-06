"""Assignment constants and an explicit, local inventory format (KD-01/02/07)."""

from dataclasses import dataclass
from pathlib import Path

CLUSTER_SIZE = 10
REPLICATION_FACTOR = 5
DEFAULT_R = 2
DEFAULT_W = 2
MAX_VALUE_BYTES = 1024 * 1024


@dataclass(frozen=True)
class Machine:
    """Inventory label and address; node_id is NOT a computed ring position."""

    node_id: str
    host: str
    port: int


def parse_inventory(text: str) -> tuple[Machine, ...]:
    """Parse `label host port` rows, ignoring blank lines and # comments.

    Return label-sorted rows; reject duplicate labels/endpoints and bad ports.
    This bootstrap format does not assign production ports or ring IDs.
    """
    machines = []
    labels = set()
    endpoints = set()
    for number, raw in enumerate(text.splitlines(), 1):
        line = raw.partition("#")[0].strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) != 3:
            raise ValueError(f"inventory line {number}: expected label host port")
        label, host, raw_port = parts
        if not raw_port.isascii() or not raw_port.isdecimal():
            raise ValueError(f"inventory line {number}: port must be an integer")
        port = int(raw_port)
        if not 1 <= port <= 65535:
            raise ValueError(f"inventory line {number}: port outside 1..65535")
        if label in labels or (host, port) in endpoints:
            raise ValueError(f"inventory line {number}: duplicate label or endpoint")
        labels.add(label)
        endpoints.add((host, port))
        machines.append(Machine(label, host, port))
    if not machines:
        raise ValueError("inventory must contain at least one machine")
    return tuple(sorted(machines, key=lambda machine: machine.node_id))


def load_inventory(path: str | Path) -> tuple[Machine, ...]:
    """Read a UTF-8 inventory without contacting any of its hosts."""
    return parse_inventory(Path(path).read_text(encoding="utf-8"))


def validate_quorums(reads: int, writes: int) -> None:
    """Check eventual R/W independently against N=5; intersection is not required."""
    if any(type(value) is not int or not 1 <= value <= REPLICATION_FACTOR
           for value in (reads, writes)):
        raise ValueError("R and W must each be integers in 1..5")
