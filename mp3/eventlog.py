import json
import logging
from datetime import datetime, timezone
from pathlib import Path
import sys
import threading


_REPLICA_EVENT_LOCK = threading.Lock()


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        event = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "event": record.getMessage(),
            "node_id": getattr(record, "node_id", None),
            "request_id": getattr(record, "request_id", None),
        }
        for name, value in (getattr(record, "fields", None) or {}).items():
            if name in event:
                raise ValueError(f"log field {name!r} clashes with a base field")
            event[name] = value
        return json.dumps(event, ensure_ascii=True, sort_keys=True)


def configure_logging(logger: logging.Logger, path: str | Path, *, stream) -> None:
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
    if logger is None:
        return
    logger.info("read_repair", extra={"node_id": node_id, "request_id": operation_id,
                                      "fields": fields})
