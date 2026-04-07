from __future__ import annotations

from typing import Any

from submission_log.main import log_result


def record_pipeline_outcome(
    submission_id: str,
    stages: dict[str, Any],
) -> None:
    """Persist or emit analytics for a completed submission pipeline run."""
    ve = stages.get("validation_engine") or {}
    blocking = ve.get("blocking_issues") if isinstance(ve, dict) else []
    log_result(
        {
            "submission_id": submission_id,
            "predicted_fields": dict(stages.get("extracted") or {}),
            "decision": dict(stages.get("decision") or {}),
            "blocking_issues": list(blocking) if isinstance(blocking, list) else [],
            "ground_truth": stages.get("ground_truth"),
            "metrics": stages.get("metrics"),
        }
    )
