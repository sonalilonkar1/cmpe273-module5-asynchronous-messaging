"""Small cross-process append-only event-id store for duplicate protection."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Set


def _read_ids(handle) -> Set[str]:
    handle.seek(0)
    result: Set[str] = set()
    for line in handle:
        try:
            value = json.loads(line)
            if isinstance(value, dict) and isinstance(value.get("event_id"), str):
                result.add(value["event_id"])
        except json.JSONDecodeError:
            continue
    return result


def claim_event(event_id: str, path: str | os.PathLike[str] = "processed_events.jsonl") -> bool:
    """Atomically claim an event ID. False means it was already claimed."""
    if not event_id:
        raise ValueError("event_id must not be empty")
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a+", encoding="utf-8") as handle:
        try:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        except ImportError:  # pragma: no cover
            pass
        try:
            ids = _read_ids(handle)
            if event_id in ids:
                return False
            handle.seek(0, os.SEEK_END)
            handle.write(json.dumps({"event_id": event_id}) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
            return True
        finally:
            try:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except ImportError:  # pragma: no cover
                pass
