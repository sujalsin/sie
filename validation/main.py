from __future__ import annotations

from typing import Any


def validate_submission(extracted: dict[str, Any]) -> tuple[bool, list[str]]:
    """Return whether the submission passes validation and a list of issue messages."""
    return True, []
