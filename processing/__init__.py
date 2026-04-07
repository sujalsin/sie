"""Normalize, enrich, and prepare raw submissions."""

from processing.documents import (
    DocumentInput,
    ExtractedDocument,
    OCRProvider,
    extract_document,
    ocr_extract_text,
    process_documents,
    process_file_paths,
)
from processing.main import process_submission

__all__ = [
    "DocumentInput",
    "ExtractedDocument",
    "OCRProvider",
    "extract_document",
    "ocr_extract_text",
    "process_documents",
    "process_file_paths",
    "process_submission",
]
