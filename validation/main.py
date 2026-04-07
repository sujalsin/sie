from __future__ import annotations

from typing import Any

from validation.engine import ValidationEngineReport, run_validation_engine


def validate_submission(
    extracted: dict[str, Any],
) -> tuple[bool, list[str], ValidationEngineReport]:
    """Return pass/fail, a flat message list, and structured blocking vs warnings."""
    if not isinstance(extracted, dict):
        report: ValidationEngineReport = {
            "blocking_issues": ["extracted_payload_must_be_a_dict"],
            "warnings": [],
        }
        return False, [*report["blocking_issues"]], report
    report = run_validation_engine(extracted)
    ok = len(report["blocking_issues"]) == 0
    issues = [*report["blocking_issues"], *report["warnings"]]
    return ok, issues, report
