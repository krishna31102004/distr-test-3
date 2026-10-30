"""Deterministic ten-node manifest and fixed SHA-256 ownership ring (KD-02/C1)."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
from types import MappingProxyType
from typing import Iterable, Mapping

from .config import CLUSTER_SIZE, REPLICATION_FACTOR


def _strict_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON field: {key}")
        result[key] = value
    return result


def _canonical_json(value: object) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False, allow_nan=False).encode("utf-8", "strict")
    except (TypeError, ValueError, UnicodeError) as error:
        raise ValueError(f"manifest is not canonical UTF-8 JSON: {error}") from error


def node_position(vm_id: str) -> int:
    """SHA-256 of the approved node prefix and stable UTF-8 VM label."""
    _validate_vm_id(vm_id)
    return int.from_bytes(hashlib.sha256(b"mp3/node/" + vm_id.encode("utf-8")).digest(), "big")


def key_position(key: str) -> int:
    """SHA-256 of the approved key prefix and strict UTF-8 key, without normalization."""
    if not isinstance(key, str):
        raise TypeError("key must be a string")
    try:
        encoded = key.encode("utf-8", "strict")
    except UnicodeEncodeError as error:
        raise ValueError("key must be strictly UTF-8 encodable") from error
    return int.from_bytes(hashlib.sha256(b"mp3/key/" + encoded).digest(), "big")


def ring_id_hex(position: int) -> str:
    if type(position) is not int or not 0 <= position < 1 << 256:
        raise ValueError("ring position must be an unsigned 256-bit integer")
    return f"{position:064x}"


def _validate_vm_id(vm_id: str) -> None:
    if not isinstance(vm_id, str) or vm_id not in {f"vm{i:02d}" for i in range(1, 11)}:
        raise ValueError("VM identity must be one of vm01 through vm10")


@dataclass(frozen=True)
class Endpoint:
    host: str
    port: int

    def __post_init__(self) -> None:
        if not isinstance(self.host, str) or not self.host or self.host.strip() != self.host:
            raise ValueError("endpoint host must be a nonempty, trimmed string")
        try:
            self.host.encode("utf-8", "strict")
        except UnicodeEncodeError as error:
            raise ValueError("endpoint host must be UTF-8 encodable") from error
        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise ValueError("endpoint port must be an integer in 1..65535")


@dataclass(frozen=True)
class Node:
    vm_id: str
    ring_id: int
    kv: Endpoint
    membership: Endpoint
    log_query: Endpoint

    def __post_init__(self) -> None:
        _validate_vm_id(self.vm_id)
        if type(self.ring_id) is not int or not 0 <= self.ring_id < 1 << 256:
            raise ValueError("ring_id must be an unsigned 256-bit integer")
        if self.ring_id != node_position(self.vm_id):
            raise ValueError(f"ring_id does not match approved SHA-256 position for {self.vm_id}")
        if not all(isinstance(endpoint, Endpoint)
                   for endpoint in (self.kv, self.membership, self.log_query)):
            raise TypeError("all service endpoints must be Endpoint values")


@dataclass(frozen=True)
class RingNode:
    """Generic already-computed ring point, useful for pure successor tests."""

    node_id: str
    position: int


def select_successors(nodes: Iterable[RingNode], key_id: int, *,
                      count: int = REPLICATION_FACTOR,
                      include_equal: bool) -> tuple[RingNode, ...]:
    """Select distinct clockwise owners with an explicit equality policy."""
    nodes = tuple(nodes)
    if type(key_id) is not int or key_id < 0:
        raise ValueError("key_id must be a nonnegative integer")
    if type(count) is not int or not 1 <= count <= len(nodes):
        raise ValueError("count must be in 1..number of nodes")
    if type(include_equal) is not bool:
        raise ValueError("include_equal must be a bool")
    if any(not isinstance(node.node_id, str) or not node.node_id
           or type(node.position) is not int or node.position < 0 for node in nodes):
        raise ValueError("nodes need nonempty labels and nonnegative integer positions")
    if len({node.node_id for node in nodes}) != len(nodes):
        raise ValueError("node labels must be distinct")
    if len({node.position for node in nodes}) != len(nodes):
        raise ValueError("duplicate ring positions are not allowed")
    ordered = sorted(nodes, key=lambda node: node.position)
    positions = [node.position for node in ordered]
    locate = bisect_left if include_equal else bisect_right
    start = locate(positions, key_id)
    return tuple(ordered[(start + offset) % len(ordered)] for offset in range(count))


def _endpoint_to_dict(endpoint: Endpoint) -> dict[str, object]:
    return {"host": endpoint.host, "port": endpoint.port}


@dataclass(frozen=True)
class Manifest:
    """Immutable complete ten-VM membership and fixed ring configuration."""

    nodes: tuple[Node, ...]
    fingerprint: str = field(init=False)
    _by_id: Mapping[str, Node] = field(init=False, repr=False, compare=False)
    _ring: tuple[RingNode, ...] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        nodes = tuple(self.nodes)
        if len(nodes) != CLUSTER_SIZE or any(not isinstance(node, Node) for node in nodes):
            raise ValueError("manifest must contain exactly ten validated nodes")
        ids = [node.vm_id for node in nodes]
        if set(ids) != {f"vm{i:02d}" for i in range(1, 11)} or len(set(ids)) != 10:
            raise ValueError("manifest must contain each identity vm01 through vm10 exactly once")
        positions = [node.ring_id for node in nodes]
        if len(set(positions)) != CLUSTER_SIZE:
            raise ValueError("two VM identities have the same ring position")
        endpoints = [(endpoint.host, endpoint.port) for node in nodes
                     for endpoint in (node.kv, node.membership, node.log_query)]
        if len(set(endpoints)) != len(endpoints):
            raise ValueError("manifest service endpoints must be unique")
        ordered = tuple(sorted(nodes, key=lambda node: node.vm_id))
        object.__setattr__(self, "nodes", ordered)
        canonical = _canonical_json({"nodes": [self._node_dict(node) for node in ordered]})
        object.__setattr__(self, "fingerprint", hashlib.sha256(canonical).hexdigest())
        object.__setattr__(self, "_by_id", MappingProxyType({node.vm_id: node for node in ordered}))
        object.__setattr__(self, "_ring", tuple(RingNode(node.vm_id, node.ring_id)
                                                  for node in ordered))

    @staticmethod
    def _node_dict(node: Node) -> dict[str, object]:
        return {"vm_id": node.vm_id, "ring_id": node.ring_id,
                "kv": _endpoint_to_dict(node.kv),
                "membership": _endpoint_to_dict(node.membership),
                "log_query": _endpoint_to_dict(node.log_query)}

    @classmethod
    def from_mapping(cls, value: object) -> "Manifest":
        if not isinstance(value, dict) or set(value) != {"nodes"} or not isinstance(value["nodes"], list):
            raise ValueError("manifest JSON must contain exactly one 'nodes' array")
        nodes = []
        for item in value["nodes"]:
            if not isinstance(item, dict) or set(item) != {
                    "vm_id", "ring_id", "kv", "membership", "log_query"}:
                raise ValueError("node must contain exactly vm_id, ring_id, kv, membership, log_query")
            endpoints = {}
            for name in ("kv", "membership", "log_query"):
                raw = item[name]
                if not isinstance(raw, dict) or set(raw) != {"host", "port"}:
                    raise ValueError(f"{name} endpoint must contain exactly host and port")
                endpoints[name] = Endpoint(raw["host"], raw["port"])
            nodes.append(Node(item["vm_id"], item["ring_id"], **endpoints))
        return cls(tuple(nodes))

    @classmethod
    def from_json(cls, text: str) -> "Manifest":
        if not isinstance(text, str):
            raise TypeError("manifest input must be text")
        try:
            value = json.loads(text, object_pairs_hook=_strict_object,
                               parse_constant=lambda token: (_ for _ in ()).throw(
                                   ValueError(f"invalid JSON constant {token}")))
        except (json.JSONDecodeError, UnicodeError) as error:
            raise ValueError(f"invalid manifest JSON: {error}") from error
        return cls.from_mapping(value)

    @classmethod
    def from_endpoints(cls, endpoints: Mapping[str, Mapping[str, Endpoint]]) -> "Manifest":
        """Build the canonical manifest from three explicit service endpoints per VM."""
        if set(endpoints) != {f"vm{i:02d}" for i in range(1, 11)}:
            raise ValueError("endpoints must specify vm01 through vm10 exactly")
        nodes = []
        for vm_id in sorted(endpoints):
            services = endpoints[vm_id]
            if set(services) != {"kv", "membership", "log_query"}:
                raise ValueError(f"{vm_id} must specify kv, membership and log_query endpoints")
            nodes.append(Node(vm_id, node_position(vm_id), services["kv"],
                              services["membership"], services["log_query"]))
        return cls(tuple(nodes))

    def to_json(self) -> str:
        return _canonical_json({"nodes": [self._node_dict(node) for node in self.nodes]}).decode("utf-8")

    def node(self, vm_id: str) -> Node:
        try:
            return self._by_id[vm_id]
        except (KeyError, TypeError) as error:
            raise ValueError(f"unknown VM identity: {vm_id!r}") from error

    def owners_for_id(self, key_id: int) -> tuple[Node, ...]:
        selected = select_successors(self._ring, key_id, count=REPLICATION_FACTOR,
                                     include_equal=True)
        return tuple(self._by_id[node.node_id] for node in selected)

    def owners(self, key: str) -> tuple[Node, ...]:
        """Return exactly five prescribed owners, without consulting liveness."""
        return self.owners_for_id(key_position(key))


def load_manifest(path: str | Path) -> Manifest:
    return Manifest.from_json(Path(path).read_text(encoding="utf-8"))
