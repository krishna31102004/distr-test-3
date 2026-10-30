"""Opt-in local JSON-lines formatting prototype (KD-17, final schema pending).

No handlers are installed at import time. This is not a durability mechanism.
Do not log value bytes. Future replicas must emit GET/PUT receipt AND response
events to file and console; no distributed operation runs in Phase 0.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        """Encode one event as one JSON line, escaping embedded newlines."""
        return json.dumps({
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "event": record.getMessage(),
            "node_id": getattr(record, "node_id", None),
            "request_id": getattr(record, "request_id", None),
        }, ensure_ascii=True, sort_keys=True)


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
