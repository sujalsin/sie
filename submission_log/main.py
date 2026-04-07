from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

_LOG_PATH = Path(__file__).resolve().parent.parent / "submission_logs.json"
_ENTRIES: list[dict[str, Any]] = []
_LOADED = False


def _load_if_needed() -> None:
    global _LOADED, _ENTRIES
    if _LOADED:
        return
    _LOADED = True
    if _LOG_PATH.is_file():
        raw = json.loads(_LOG_PATH.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            _ENTRIES = [e for e in raw if isinstance(e, dict)]


def _persist() -> None:
    _LOG_PATH.write_text(json.dumps(_ENTRIES, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def log_result(data: dict[str, Any]) -> None:
    """
    Append one submission record. Required keys: ``submission_id``, ``predicted_fields``, ``decision``.
    Optional: ``blocking_issues`` (list of str), ``ground_truth`` (mapping or None),
    ``metrics`` (mapping or None).
    """
    _load_if_needed()
    blocking = data.get("blocking_issues")
    m = data.get("metrics")
    entry: dict[str, Any] = {
        "submission_id": str(data["submission_id"]),
        "predicted_fields": dict(data["predicted_fields"]),
        "decision": dict(data["decision"]),
        "blocking_issues": list(blocking) if isinstance(blocking, list) else [],
        "ground_truth": None
        if data.get("ground_truth") is None
        else dict(data["ground_truth"]),
        "metrics": None if not isinstance(m, dict) else dict(m),
    }
    _ENTRIES.append(entry)
    _persist()


def get_all_logs() -> list[dict[str, Any]]:
    """Return a deep copy of all stored submission records."""
    _load_if_needed()
    return deepcopy(_ENTRIES)


def reset_logs() -> None:
    """Clear in-memory entries and truncate the JSON log file."""
    global _ENTRIES, _LOADED
    _ENTRIES = []
    _LOADED = True
    _LOG_PATH.write_text("[]\n", encoding="utf-8")
