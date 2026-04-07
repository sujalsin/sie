from __future__ import annotations

from typing import Any, TypedDict

from validation.schema import empty_extracted_fields


class ConflictRecord(TypedDict):
    field: str
    values: list[Any]
    sources: list[str]


def _comparison_key(value: Any) -> tuple[Any, ...]:
    if value is None:
        return ("none",)
    if isinstance(value, str):
        stripped = value.strip()
        return ("none",) if not stripped else ("str", stripped.casefold())
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, (int, float)):
        return ("num", float(value))
    return ("repr", repr(value))


def _source_rows(per_source: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    rows: list[tuple[str, dict[str, Any]]] = []
    email = per_source.get("email")
    if isinstance(email, dict):
        rows.append(("email", email))
    for doc in per_source.get("documents") or []:
        if not isinstance(doc, dict):
            continue
        fields = doc.get("fields")
        if not isinstance(fields, dict):
            continue
        doc_id = str(doc.get("doc_id", ""))
        label = doc_id if doc_id else "document"
        rows.append((label, fields))
    return rows


def detect_conflicts(per_source: dict[str, Any]) -> list[ConflictRecord]:
    """
    Compare the same field across email and documents.

    Emits a record only when at least two sources have a non-null value and those values
    disagree after normalization.
    """
    schema_keys = list(empty_extracted_fields().keys())
    labeled = _source_rows(per_source)
    out: list[ConflictRecord] = []

    for field in schema_keys:
        pairs: list[tuple[str, Any]] = []
        for source_label, row in labeled:
            if field not in row:
                continue
            value = row[field]
            if value is None:
                continue
            pairs.append((source_label, value))
        if len(pairs) < 2:
            continue
        if len({_comparison_key(v) for _, v in pairs}) <= 1:
            continue
        out.append(
            {
                "field": field,
                "values": [v for _, v in pairs],
                "sources": [s for s, _ in pairs],
            }
        )
    return out
