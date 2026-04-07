from __future__ import annotations

from typing import Any

_BLOCKING_TO_REQUIRED_ACTION: dict[str, str] = {
    "payroll is missing": "Provide payroll details",
    "revenue is missing": "Provide revenue information",
    "extracted_payload_must_be_a_dict": "Resubmit with a valid extracted-field payload",
}


def suggest_fixes(blocking_issues: list[Any]) -> list[str]:
    """
    Map blocking issue strings to concrete actions. Order follows ``blocking_issues``;
    duplicate actions are omitted.
    """
    seen: set[str] = set()
    out: list[str] = []
    for item in blocking_issues:
        if not isinstance(item, str):
            continue
        key = item.strip()
        if not key:
            continue
        action = _BLOCKING_TO_REQUIRED_ACTION.get(key, f"Resolve: {key}")
        if action not in seen:
            seen.add(action)
            out.append(action)
    return out


def refine_follow_up_with_llm(draft: str, blocking_issues: list[str]) -> str:
    """
    Placeholder for a future LLM polish step.

    Any implementation must only restate items present in ``blocking_issues`` and must not
    invent new requirements.
    """
    _ = blocking_issues
    return draft


def _normalized_issues(blocking_issues: list[str]) -> list[str]:
    out: list[str] = []
    for item in blocking_issues:
        if not isinstance(item, str):
            continue
        text = item.strip()
        if text:
            out.append(text)
    return out


def _render_follow_up_template(issues: list[str]) -> str:
    header = (
        "Subject: Additional information needed for your submission\n\n"
        "Dear Customer,\n\n"
        "Thank you for your submission. We are unable to proceed until the following "
        "items are addressed:\n\n"
    )
    if not issues:
        return (
            "Subject: Submission received\n\n"
            "Dear Customer,\n\n"
            "Thank you for your submission. We do not require any further information "
            "based on the current review.\n\n"
            "Kind regards,\n"
            "Submissions Team"
        )
    bullets = "\n".join(f"- {issue}" for issue in issues)
    closing = (
        "\n\n"
        "Please reply with the requested details at your earliest convenience.\n\n"
        "Kind regards,\n"
        "Submissions Team"
    )
    return header + bullets + closing


def generate_follow_up(blocking_issues: list[str]) -> str:
    """
    Build a professional follow-up email that mentions only the supplied blocking issues.

    Content is template-driven from the exact issue strings; optional LLM refinement is a no-op stub.
    """
    issues = _normalized_issues(blocking_issues)
    draft = _render_follow_up_template(issues)
    return refine_follow_up_with_llm(draft, issues)
