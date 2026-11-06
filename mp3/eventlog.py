"""Opt-in JSON-lines logging and local replica events (KD-17).

No handlers are installed at import time. This is not a durability mechanism.
Do not log value bytes. Replicas emit one receipt and one response event for every GET and
PUT, and an Eventual-RR GET adds one structured read_repair event. Written to a file named
machine.<N>.log, these lines can be searched with the unchanged MP1 server and client.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
import sys
import threading


_REPLICA_EVENT_LOCK = threading.Lock()


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        """Encode one event as one JSON line, escaping embedded newlines."""
        event = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "event": record.getMessage(),
            "node_id": getattr(record, "node_id", None),
            "request_id": getattr(record, "request_id", None),
        }
        # Structured events (read repair) add flat fields; they may not replace base keys.
        for name, value in (getattr(record, "fields", None) or {}).items():
            if name in event:
                raise ValueError(f"log field {name!r} clashes with a base field")
            event[name] = value
        return json.dumps(event, ensure_ascii=True, sort_keys=True)


def configure_logging(logger: logging.Logger, path: str | Path, *, stream) -> None:
    """Attach append-only UTF-8 file and console handlers to a fresh owned logger.

    Caller owns handler cleanup. Refuse to replace existing handlers. Parent
    directories must already exist, avoiding implicit data-directory creation.
    """
    if logger.handlers:
        raise ValueError("use a fresh logger without handlers")
    file_handler = logging.FileHandler(path, mode="a", encoding="utf-8")
    console_handler = logging.StreamHandler(stream)
    for handler in (file_handler, console_handler):
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def log_replica_event(logger: logging.Logger | None, phase: str, operation: str,
                      key: str, *, node_id: str, request_id: str,
                      stream=None) -> str:
    """Print the required receipt/response line and append a structured log event.

    `phase` is `received` or `responded`; operation is `get` or `put`. Control
    characters in keys are escaped so one key cannot forge extra terminal lines.
    Pass an append-only logger configured by `configure_logging` for machine logs.
    """
    if phase not in ("received", "responded") or operation not in ("get", "put"):
        raise ValueError("invalid replica event kind")
    if not isinstance(key, str) or not isinstance(request_id, str):
        raise TypeError("key and request_id must be text")
    safe_key = "".join(
        f"\\x{ord(char):02x}" if ord(char) < 32 or ord(char) == 127 else char
        for char in key)
    timestamp = datetime.now().astimezone().isoformat(timespec="milliseconds")
    line = f"{timestamp}: {phase}{operation} {safe_key}"
    with _REPLICA_EVENT_LOCK:
        if logger is not None:
            logger.info(f"{phase}{operation} {safe_key}",
                        extra={"node_id": node_id, "request_id": request_id})
        print(line, file=stream if stream is not None else sys.stdout, flush=True)
    return line


def log_read_repair(logger: logging.Logger | None, *, node_id: str, operation_id: str,
                    fields: dict) -> None:
    """Append one structured Eventual-RR event: returned version, reachable replicas,
    each owner's response and every repair (KD-13/KD-17). `fields` must hold only
    JSON-ready text, numbers, lists and dicts; value bytes are never logged. A missing
    logger is not an error: the node simply runs without a machine log."""
    if logger is None:
        return
    logger.info("read_repair", extra={"node_id": node_id, "request_id": operation_id,
                                      "fields": fields})
