from __future__ import annotations

from typing import Any


def decide_submission(
    extracted: dict[str, Any],
    validation_ok: bool,
    validation_issues: list[str],
) -> dict[str, Any]:
    """Produce a decision record (e.g. route, risk tier, next action) for a submission."""
    return {}
