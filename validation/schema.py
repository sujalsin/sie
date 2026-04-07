from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal, TypedDict


class ExtractedFields(TypedDict):
    insured_name: str | None
    business_description: str | None
    revenue: float | None
    payroll: float | None
    employee_count: int | None
    effective_date: str | None
    loss_runs_years: int | None


FieldConfidence = Literal["high", "medium", "low"]


class ProvenanceRecord(TypedDict):
    value: Any
    source: str
    confidence: FieldConfidence


def empty_extracted_fields() -> ExtractedFields:
    return {
        "insured_name": None,
        "business_description": None,
        "revenue": None,
        "payroll": None,
        "employee_count": None,
        "effective_date": None,
        "loss_runs_years": None,
    }


def get_cell_value(cell: Any) -> Any:
    """Return the scalar value from a provenance cell or pass through a legacy scalar."""
    if isinstance(cell, dict) and "value" in cell:
        return cell.get("value")
    return cell


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped else None
    return str(value).strip() or None


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, float):
        return value
    if isinstance(value, int):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value != int(value):
            return None
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            as_float = float(stripped)
        except ValueError:
            return None
        if as_float != int(as_float):
            return None
        return int(as_float)
    try:
        as_float = float(value)
    except (TypeError, ValueError):
        return None
    if as_float != int(as_float):
        return None
    return int(as_float)


def coerce_flat_record(data: Mapping[str, Any]) -> ExtractedFields:
    """Coerce a flat LLM JSON object to ``ExtractedFields`` (no provenance wrapper)."""
    if not isinstance(data, dict):
        return empty_extracted_fields()
    return {
        "insured_name": _optional_str(data.get("insured_name")),
        "business_description": _optional_str(data.get("business_description")),
        "revenue": _optional_float(data.get("revenue")),
        "payroll": _optional_float(data.get("payroll")),
        "employee_count": _optional_int(data.get("employee_count")),
        "effective_date": _optional_str(data.get("effective_date")),
        "loss_runs_years": _optional_int(data.get("loss_runs_years")),
    }


def _coerce_schema_key(key: str, raw: Any) -> Any:
    if key in ("insured_name", "business_description", "effective_date"):
        return _optional_str(raw)
    if key in ("revenue", "payroll"):
        return _optional_float(raw)
    return _optional_int(raw)


def validate_and_sanitize(data: dict[str, Any]) -> dict[str, ProvenanceRecord]:
    """
    Normalize provenance-wrapped fields: coerce ``value``, drop bad types to None.

    Preserves ``source`` and ``confidence`` when coercion succeeds; on failure sets
    ``value`` to None, ``source`` to \"\", ``confidence`` to \"low\".
    """
    template = empty_extracted_fields()
    if not isinstance(data, dict):
        return {k: {"value": None, "source": "", "confidence": "low"} for k in template}

    out: dict[str, ProvenanceRecord] = {}
    for key in template:
        cell = data.get(key)
        if isinstance(cell, dict) and "value" in cell:
            raw_val = cell.get("value")
            source = str(cell.get("source") or "")
            conf_in = cell.get("confidence")
            if conf_in not in ("high", "medium", "low"):
                conf_in = "medium"
        else:
            raw_val = cell
            source = ""
            conf_in = "medium"

        coerced = _coerce_schema_key(key, raw_val)
        if coerced is None:
            out[key] = {"value": None, "source": "", "confidence": "low"}
        else:
            conf: FieldConfidence = conf_in if conf_in in ("high", "medium", "low") else "medium"
            out[key] = {"value": coerced, "source": source, "confidence": conf}
    return out
