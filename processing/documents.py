from __future__ import annotations

import os
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Literal, Protocol, TypedDict


class DocumentInput(TypedDict):
    doc_id: str
    file_path: str


class ExtractedDocument(TypedDict):
    doc_id: str
    text: str
    source: str


class OCRProvider(Protocol):
    """Implement this protocol to plug in a real OCR engine."""

    def extract_text(self, file_path: str) -> str:
        """Return plain text for an image or other OCR-backed file."""


_TEXT_EXTENSIONS = frozenset({".txt", ".text", ".md", ".csv", ".tsv", ".log", ".json", ".xml", ".html", ".htm"})


def ocr_extract_text(file_path: str) -> str:
    """Default stub for image-like files when no OCR engine is configured."""
    name = os.path.basename(file_path)
    return f"[OCR stub — image or binary: {name}]"


def _read_pdf_text(path: Path) -> str:
    try:
        from pypdf import PdfReader
    except ImportError:
        return "[pypdf not installed — cannot read PDF text]"
    try:
        reader = PdfReader(str(path))
        parts: list[str] = []
        for page in reader.pages:
            parts.append(page.extract_text() or "")
        out = "\n".join(parts).strip()
        if not out:
            return (
                "[PDF contains no embedded text — likely scanned pages; "
                "add Tesseract/cloud OCR to extract content]"
            )
        return out
    except Exception as exc:
        return f"[PDF read error: {exc}]"


def extract_text_from_path(file_path: str) -> str:
    """
    Read text from a real file when it exists: UTF-8 text types and PDF text layers.

    Missing paths return a clear marker; unknown extensions use the OCR stub hook.
    """
    path = Path(file_path).expanduser()
    if not path.is_file():
        return f"[file not found: {file_path}]"

    ext = path.suffix.lower()
    if ext == ".pdf":
        return _read_pdf_text(path)
    if ext in _TEXT_EXTENSIONS:
        try:
            raw = path.read_text(encoding="utf-8", errors="replace").strip()
            return raw if raw else "[empty text file]"
        except OSError as exc:
            return f"[text read error: {exc}]"

    return ocr_extract_text(file_path)


def _resolve_ocr_callable(
    ocr_provider: OCRProvider | Callable[[str], str] | None,
) -> Callable[[str], str]:
    if ocr_provider is None:
        return ocr_extract_text
    bound = getattr(ocr_provider, "extract_text", None)
    if bound is not None and callable(bound):
        return bound
    if callable(ocr_provider):
        return ocr_provider
    raise TypeError("ocr_provider must be None, a callable(path)->str, or an object with extract_text(path)")


def extract_document(
    doc_id: str,
    file_path: str,
    *,
    ocr_provider: OCRProvider | Callable[[str], str] | None = None,
) -> ExtractedDocument:
    """Load document text from disk (PDF/text) or fall back to OCR stub for images."""
    ocr = _resolve_ocr_callable(ocr_provider)
    text = extract_text_from_path(file_path)
    if text.startswith("[OCR stub"):
        text = ocr(file_path)
    return {
        "doc_id": doc_id,
        "text": text,
        "source": file_path,
    }


def process_documents(
    documents: Iterable[DocumentInput],
    *,
    ocr_provider: OCRProvider | Callable[[str], str] | None = None,
) -> list[ExtractedDocument]:
    """Run extraction for each document; ``file_path`` becomes ``source`` in the output."""
    return [
        extract_document(d["doc_id"], d["file_path"], ocr_provider=ocr_provider)
        for d in documents
    ]


def process_file_paths(file_paths: Sequence[str]) -> list[ExtractedDocument]:
    """Convenience when only paths are available (synthetic ``doc_id`` per index)."""
    return process_documents(
        [{"doc_id": f"path-{i}", "file_path": p} for i, p in enumerate(file_paths)]
    )
