from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from statistics import fmean
from typing import Any, TypedDict

from submission_log.main import get_all_logs
from validation.schema import empty_extracted_fields, get_cell_value


class FailureAnalysisReport(TypedDict):
    top_failure_reasons: list[dict[str, Any]]
    error_breakdown: dict[str, Any]
    example_failure_cases: list[dict[str, str]]


_BLOCKING_MISSING_TO_FIELD: dict[str, str] = {
    "payroll is missing": "payroll",
    "revenue is missing": "revenue",
}


def _blocking_issues(entry: Mapping[str, Any]) -> list[str]:
    raw = entry.get("blocking_issues")
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    for item in raw:
        if isinstance(item, str) and item.strip():
            out.append(item.strip())
    return out


def _decision_status(entry: Mapping[str, Any]) -> str:
    dec = entry.get("decision")
    if not isinstance(dec, dict):
        return "unknown"
    status = dec.get("decision")
    if status in ("READY", "NOT_READY"):
        return str(status)
    return "unknown"


def _signals(entry: Mapping[str, Any]) -> Mapping[str, Any] | None:
    dec = entry.get("decision")
    if not isinstance(dec, dict):
        return None
    sig = dec.get("signals")
    return sig if isinstance(sig, dict) else None


def _field_coverage_from_log(entry: Mapping[str, Any]) -> float | None:
    sig = _signals(entry)
    if sig is None:
        return None
    fc = sig.get("field_coverage")
    if isinstance(fc, (int, float)):
        return float(fc)
    return None


def _values_equal(predicted: Any, truth: Any) -> bool:
    if predicted is None and truth is None:
        return True
    if isinstance(predicted, bool) or isinstance(truth, bool):
        return predicted == truth
    if isinstance(predicted, (int, float)) and isinstance(truth, (int, float)):
        return float(predicted) == float(truth)
    return predicted == truth


def _predicted_value(pred: Mapping[str, Any], key: str) -> Any:
    return get_cell_value(pred.get(key))


def _all_predicted_empty(pred: Mapping[str, Any]) -> bool:
    for key in empty_extracted_fields():
        if _predicted_value(pred, key) is not None:
            return False
    return True


def _ground_truth_has_any_value(gt: Mapping[str, Any]) -> bool:
    return any(v is not None for v in gt.values())


def _count_extraction_errors(
    pred: Mapping[str, Any],
    gt: Mapping[str, Any],
) -> float:
    """Labeled field missing or wrong vs ground truth."""
    n = 0.0
    for key, gval in gt.items():
        if gval is None:
            continue
        pval = _predicted_value(pred, str(key))
        if pval is None or not _values_equal(pval, gval):
            n += 1.0
    return n


def _count_rule_errors(pred: Mapping[str, Any], blocking: list[str]) -> float:
    """Blocking says 'missing' but the field is present in extraction."""
    n = 0.0
    for msg in blocking:
        field = _BLOCKING_MISSING_TO_FIELD.get(msg)
        if field is None:
            continue
        if _predicted_value(pred, field) is not None:
            n += 1.0
    return n


def _count_input_issue(
    pred: Mapping[str, Any],
    gt: Any,
) -> float:
    """
    No usable labels and no extracted values — typical of empty / unlabeled input.
    """
    if isinstance(gt, dict) and _ground_truth_has_any_value(gt):
        return 0.0
    if not _all_predicted_empty(pred):
        return 0.0
    return 1.0


def _attribution_scores(entry: Mapping[str, Any]) -> tuple[float, float, float]:
    pred = entry.get("predicted_fields")
    pred = pred if isinstance(pred, dict) else {}
    gt = entry.get("ground_truth")
    gt = gt if isinstance(gt, dict) else {}
    blocking = _blocking_issues(entry)

    ext = _count_extraction_errors(pred, gt) if gt else 0.0
    rule = _count_rule_errors(pred, blocking)
    inp = _count_input_issue(pred, entry.get("ground_truth"))
    return ext, rule, inp


def _describe_example(
    entry: Mapping[str, Any],
    extraction_error: float,
    rule_error: float,
    input_issue: float,
) -> dict[str, str]:
    sid = str(entry.get("submission_id", "unknown"))
    wrong: list[str] = []
    why: list[str] = []

    if extraction_error > 0:
        wrong.append(
            f"{extraction_error:.0f} labeled field(s) missing or mismatched vs ground truth"
        )
        why.append(
            "Extraction did not produce the expected values for fields that were labeled."
        )
    if rule_error > 0:
        wrong.append(
            f"{rule_error:.0f} blocking rule(s) fired despite extracted values being present"
        )
        why.append(
            "Validation rules reported missing payroll/revenue even though extraction returned them."
        )
    if input_issue > 0:
        wrong.append("No fields extracted and no ground-truth labels to compare")
        why.append(
            "Nothing was parsed and there were no labels; often indicates empty or minimal input."
        )

    if not wrong:
        blocking = _blocking_issues(entry)
        if blocking:
            wrong.append("Blocking: " + "; ".join(blocking[:4]))
            why.append(
                "Rules marked the submission as not ready; attribution buckets did not apply "
                "(e.g. no ground truth to score extraction)."
            )
        else:
            wrong.append("No dominant classified failure; review decision and field coverage.")
            why.append("See decision signals and logs for detail.")

    return {
        "submission_id": sid,
        "what_went_wrong": " ".join(wrong),
        "why": " ".join(why),
    }


def _example_failure_cases(
    rows: list[Mapping[str, Any]],
    scores: list[tuple[float, float, float]],
    *,
    limit: int = 3,
) -> list[dict[str, str]]:
    if not rows:
        return []
    order = sorted(
        range(len(rows)),
        key=lambda i: -(scores[i][0] + scores[i][1] + scores[i][2]),
    )
    chosen = order[: min(limit, len(rows))]
    return [_describe_example(rows[i], *scores[i]) for i in chosen]


def _field_comparison_stats(logs: list[Mapping[str, Any]]) -> dict[str, Any]:
    per_field: dict[str, dict[str, int]] = {}
    logs_with_gt = 0

    for entry in logs:
        gt = entry.get("ground_truth")
        if not isinstance(gt, dict) or not gt:
            continue
        logs_with_gt += 1
        pred = entry.get("predicted_fields")
        pred = pred if isinstance(pred, dict) else {}
        for key in gt:
            bucket = per_field.setdefault(
                str(key),
                {"compared": 0, "matches": 0, "mismatches": 0},
            )
            bucket["compared"] += 1
            p_val = pred.get(key)
            if isinstance(p_val, dict) and "value" in p_val:
                p_val = p_val.get("value")
            g_val = gt.get(key)
            if _values_equal(p_val, g_val):
                bucket["matches"] += 1
            else:
                bucket["mismatches"] += 1

    return {
        "logs_with_ground_truth": logs_with_gt,
        "per_field": per_field,
    }


def analyze_failures(
    logs: Iterable[Mapping[str, Any]] | None = None,
    *,
    top_n: int = 25,
) -> FailureAnalysisReport:
    """
    Summarize blocking-issue frequency, ground-truth comparison, and simple error attribution.

    Attribution (counts as floats, explainable heuristics):

    - **extraction_error**: ground-truth field is non-null but prediction is null or unequal.
    - **rule_error**: blocking says payroll/revenue missing but extraction has that field.
    - **input_issue**: no ground-truth labels, all predictions empty (thin / unlabeled input).
    """
    if logs is None:
        logs = get_all_logs()
    rows: list[Mapping[str, Any]] = list(logs)

    issue_counter: Counter[str] = Counter()
    submissions_with_issues = 0
    issues_per_failed: list[int] = []

    decision_counter: Counter[str] = Counter()
    severity_counter: Counter[str] = Counter()
    coverage_when_not_ready: list[float] = []
    coverage_all: list[float] = []

    total_extraction_error = 0.0
    total_rule_error = 0.0
    total_input_issue = 0.0
    attribution_per_row: list[tuple[float, float, float]] = []

    for entry in rows:
        ext, rule, inp = _attribution_scores(entry)
        total_extraction_error += ext
        total_rule_error += rule
        total_input_issue += inp
        attribution_per_row.append((ext, rule, inp))

        decision_counter[_decision_status(entry)] += 1
        issues = _blocking_issues(entry)
        for reason in issues:
            issue_counter[reason] += 1
        if issues:
            submissions_with_issues += 1
            issues_per_failed.append(len(issues))

        sig = _signals(entry)
        if sig is not None:
            sev = sig.get("rule_severity")
            if isinstance(sev, str):
                severity_counter[sev] += 1
        fc = _field_coverage_from_log(entry)
        if fc is not None:
            coverage_all.append(fc)
            if _decision_status(entry) == "NOT_READY":
                coverage_when_not_ready.append(fc)

    top_failure_reasons = [
        {"reason": reason, "count": count}
        for reason, count in issue_counter.most_common(top_n)
    ]

    total = len(rows)
    not_ready = decision_counter.get("NOT_READY", 0)
    avg_issues_when_any = fmean(issues_per_failed) if issues_per_failed else None
    avg_coverage = fmean(coverage_all) if coverage_all else None
    avg_coverage_not_ready = (
        fmean(coverage_when_not_ready) if coverage_when_not_ready else None
    )

    error_breakdown: dict[str, Any] = {
        "total_submissions": total,
        "by_decision": dict(decision_counter),
        "submissions_with_blocking_issues": submissions_with_issues,
        "submissions_without_blocking_issues": total - submissions_with_issues,
        "total_blocking_issue_occurrences": sum(issue_counter.values()),
        "unique_blocking_issue_strings": len(issue_counter),
        "blocking_issue_counts": dict(issue_counter),
        "extraction_error": float(total_extraction_error),
        "rule_error": float(total_rule_error),
        "input_issue": float(total_input_issue),
        "statistics": {
            "share_not_ready": (not_ready / total) if total else 0.0,
            "avg_blocking_issues_per_affected_submission": avg_issues_when_any,
            "avg_field_coverage": avg_coverage,
            "avg_field_coverage_when_not_ready": avg_coverage_not_ready,
            "rule_severity_counts": dict(severity_counter),
        },
        "ground_truth": _field_comparison_stats(rows),
        "error_attribution_notes": (
            "extraction_error counts labeled fields that are null or wrong vs ground_truth; "
            "rule_error counts blocking 'missing' messages when the field is actually present; "
            "input_issue counts submissions with no labels and no extracted values."
        ),
    }

    examples = _example_failure_cases(rows, attribution_per_row, limit=3)

    return {
        "top_failure_reasons": top_failure_reasons,
        "error_breakdown": error_breakdown,
        "example_failure_cases": examples,
    }
