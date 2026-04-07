from __future__ import annotations

from typing import Any, Mapping, TypedDict

from validation.schema import get_cell_value


class ValidationEngineReport(TypedDict):
    blocking_issues: list[str]
    warnings: list[str]


MIN_BUSINESS_DESCRIPTION_LENGTH = 10
MIN_LOSS_RUNS_YEARS = 3


def run_validation_engine(extracted: Mapping[str, Any]) -> ValidationEngineReport:
    """
    Classify business-rule problems on schema-clean extracted fields.

    Blocking issues prevent acceptance; warnings are non-blocking quality flags.
    """
    blocking_issues: list[str] = []
    warnings: list[str] = []

    if get_cell_value(extracted.get("payroll")) is None:
        blocking_issues.append("payroll is missing")
    if get_cell_value(extracted.get("revenue")) is None:
        blocking_issues.append("revenue is missing")

    loss_runs_years = get_cell_value(extracted.get("loss_runs_years"))
    if isinstance(loss_runs_years, int) and loss_runs_years < MIN_LOSS_RUNS_YEARS:
        warnings.append(
            f"loss_runs_years is {loss_runs_years}; minimum recommended is {MIN_LOSS_RUNS_YEARS}"
        )

    description = get_cell_value(extracted.get("business_description"))
    text = description if isinstance(description, str) else ""
    if len(text) < MIN_BUSINESS_DESCRIPTION_LENGTH:
        warnings.append(
            f"business_description is shorter than {MIN_BUSINESS_DESCRIPTION_LENGTH} characters"
        )

    return {
        "blocking_issues": blocking_issues,
        "warnings": warnings,
    }
