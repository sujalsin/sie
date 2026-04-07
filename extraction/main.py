from __future__ import annotations

import json
from typing import Any, TypedDict

from extraction.schema import ExtractedFields, coerce_extracted_fields, empty_extracted_fields
from validation.schema import ProvenanceRecord, validate_and_sanitize


class DocumentExtraction(TypedDict):
    doc_id: str
    fields: ExtractedFields


class PerSourceExtraction(TypedDict):
    email: ExtractedFields
    documents: list[DocumentExtraction]


def call_llm(prompt: str) -> str:
    """
    Call Gemini when ``GEMINI_API_KEY`` (or ``GOOGLE_API_KEY``) is set; otherwise return empty JSON.

    Set ``SIE_DISABLE_GEMINI=1`` to force the stub (tests / offline).
    """
    from extraction.gemini_client import call_gemini_json

    out = call_gemini_json(prompt)
    if out is None:
        return json.dumps(empty_extracted_fields())
    return out


def _build_extraction_prompt(input_text: str) -> str:
    return (
        "You are an insurance submission parser. Extract fields from the following content "
        "and respond with a single JSON object only (no markdown), using these keys exactly:\n"
        "insured_name, business_description, revenue, payroll, employee_count, "
        "effective_date, loss_runs_years.\n"
        "Use null for missing values. revenue and payroll are numbers; employee_count and "
        "loss_runs_years are integers; others are strings.\n\n"
        "---\n"
        f"{input_text}\n"
        "---"
    )


def extract_fields(input_text: str) -> ExtractedFields:
    """Run the LLM stub on ``input_text`` and return a schema-constrained dict."""
    prompt = _build_extraction_prompt(input_text)
    response = call_llm(prompt)
    try:
        parsed = json.loads(response)
    except json.JSONDecodeError:
        return empty_extracted_fields()
    if not isinstance(parsed, dict):
        return empty_extracted_fields()
    return coerce_extracted_fields(parsed)


def _email_block(processed: dict[str, Any]) -> str:
    body = processed.get("email_body")
    if isinstance(body, str) and body.strip():
        return f"### Email\n\n{body.strip()}"
    return "### Email\n\n(no content)"


def _document_block(row: dict[str, Any]) -> str:
    doc_id = row.get("doc_id", "")
    source = row.get("source", "")
    text = row.get("text", "")
    if not isinstance(text, str):
        text = str(text)
    return f"### Document {doc_id} ({source})\n\n{text}"


def extract_per_source(processed: dict[str, Any]) -> PerSourceExtraction:
    """Extract once for the email body and once per attachment document."""
    email_fields = extract_fields(_email_block(processed))
    documents: list[DocumentExtraction] = []
    for row in processed.get("document_extractions") or []:
        if not isinstance(row, dict):
            continue
        doc_id = str(row.get("doc_id", ""))
        fields = extract_fields(_document_block(row))
        documents.append({"doc_id": doc_id, "fields": fields})
    return {"email": email_fields, "documents": documents}


def merge_per_source_extractions(per_source: dict[str, Any]) -> dict[str, ProvenanceRecord]:
    """
    Merge per-source flat fields into provenance records.

    Document values override email; among documents, later attachments win. Email-only
    values use ``medium`` confidence; document-sourced values use ``high``; missing
    values use ``low``.
    """
    template = empty_extracted_fields()
    email = per_source.get("email")
    email = email if isinstance(email, dict) else {}
    merged: dict[str, ProvenanceRecord] = {}

    for key in template:
        chosen: Any = None
        chosen_source = ""
        for doc in per_source.get("documents") or []:
            if not isinstance(doc, dict):
                continue
            fields = doc.get("fields")
            if not isinstance(fields, dict):
                continue
            val = fields.get(key)
            if val is not None:
                chosen = val
                chosen_source = str(doc.get("doc_id", "")) or "document"
        if chosen is None:
            ev = email.get(key)
            if ev is not None:
                chosen = ev
                chosen_source = "email"
        if chosen is None:
            merged[key] = {"value": None, "source": "", "confidence": "low"}
        elif chosen_source == "email":
            merged[key] = {"value": chosen, "source": "email", "confidence": "medium"}
        else:
            merged[key] = {"value": chosen, "source": chosen_source, "confidence": "high"}

    return merged


def compose_extraction_input(processed: dict[str, Any]) -> str:
    """Join email body and processed document texts into one model-facing string."""
    blocks: list[str] = [_email_block(processed)]
    extractions = processed.get("document_extractions") or []
    for row in extractions:
        if isinstance(row, dict):
            blocks.append(_document_block(row))
    return "\n\n".join(blocks) if blocks else "(no content)"


def extract_from_submission(processed: dict[str, Any]) -> dict[str, ProvenanceRecord]:
    """Merged extraction with provenance; schema-sanitized for downstream use."""
    return validate_and_sanitize(merge_per_source_extractions(extract_per_source(processed)))
