from __future__ import annotations

from typing import Any, Literal, Mapping, TypedDict

from validation.schema import empty_extracted_fields, get_cell_value


class DecisionSignals(TypedDict):
    field_coverage: float
    rule_severity: Literal["LOW", "MEDIUM", "HIGH"]
    warning_count: int
    blocking_count: int


class DecisionResult(TypedDict):
    decision: Literal["READY", "NOT_READY"]
    signals: DecisionSignals


def _field_coverage(extracted: Mapping[str, Any]) -> float:
    template = empty_extracted_fields()
    total = len(template)
    if total == 0:
        return 0.0
    filled = sum(
        1 for key in template if get_cell_value(extracted.get(key)) is not None
    )
    return filled / total


def _rule_severity(
    blocking_count: int,
    warning_count: int,
) -> Literal["LOW", "MEDIUM", "HIGH"]:
    if blocking_count > 0:
        return "HIGH"
    if warning_count > 0:
        return "MEDIUM"
    return "LOW"


def decide_submission(
    extracted: Mapping[str, Any],
    blocking_issues: list[str],
    warnings: list[str],
) -> DecisionResult:
    """Map extraction coverage and validation engine output to a readiness decision."""
    blocking_count = len(blocking_issues)
    warning_count = len(warnings)
    status: Literal["READY", "NOT_READY"] = "NOT_READY" if blocking_issues else "READY"
    signals: DecisionSignals = {
        "field_coverage": _field_coverage(extracted),
        "rule_severity": _rule_severity(blocking_count, warning_count),
        "warning_count": warning_count,
        "blocking_count": blocking_count,
    }
    return {"decision": status, "signals": signals}
