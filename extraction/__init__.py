"""Structured field extraction via LLM (stub) with a strict schema."""

from extraction.main import (
    DocumentExtraction,
    PerSourceExtraction,
    call_llm,
    compose_extraction_input,
    extract_fields,
    extract_from_submission,
    extract_per_source,
    merge_per_source_extractions,
)
from extraction.schema import ExtractedFields, coerce_extracted_fields, empty_extracted_fields
from validation.schema import ProvenanceRecord

__all__ = [
    "DocumentExtraction",
    "ExtractedFields",
    "PerSourceExtraction",
    "ProvenanceRecord",
    "call_llm",
    "coerce_extracted_fields",
    "compose_extraction_input",
    "empty_extracted_fields",
    "extract_fields",
    "extract_from_submission",
    "extract_per_source",
    "merge_per_source_extractions",
]
