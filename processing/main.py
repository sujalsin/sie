from __future__ import annotations

from collections.abc import Callable
from typing import Any

from processing.documents import (
    ExtractedDocument,
    OCRProvider,
    process_documents,
)


def process_submission(
    raw: dict[str, Any],
    *,
    ocr_provider: OCRProvider | Callable[[str], str] | None = None,
) -> dict[str, Any]:
    """Transform a raw ingestion record into a canonical internal representation."""
    documents = list(raw.get("documents") or [])
    extractions: list[ExtractedDocument] = process_documents(
        documents,
        ocr_provider=ocr_provider,
    )
    return {
        "submission_id": raw["submission_id"],
        "email_body": raw["email_body"],
        "documents": documents,
        "document_extractions": extractions,
    }
