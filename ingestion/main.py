from __future__ import annotations

import hashlib
import os
from typing import Literal, TypedDict


class DocumentRecord(TypedDict):
    doc_id: str
    file_path: str
    doc_type: Literal["unknown"]


class NormalizedSubmission(TypedDict):
    submission_id: str
    email_body: str
    documents: list[DocumentRecord]


def normalize_submission_id(submission_id: str) -> str:
    """Return the submission id with leading and trailing whitespace removed."""
    return submission_id.strip()


def normalize_email_body(email_body: str) -> str:
    """Return the body with CRLF/CR normalized to LF and outer whitespace stripped."""
    text = email_body.replace("\r\n", "\n").replace("\r", "\n")
    return text.strip()


def stable_doc_id(submission_id: str, index: int, file_path: str) -> str:
    """Compute a deterministic doc id from submission context and path."""
    payload = f"{submission_id}\0{index}\0{file_path}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def document_from_attachment(
    submission_id: str,
    index: int,
    file_path: str,
) -> DocumentRecord:
    """Build one document entry from an attachment path at the given index."""
    normalized_path = os.path.normpath(file_path.strip())
    return {
        "doc_id": stable_doc_id(submission_id, index, normalized_path),
        "file_path": normalized_path,
        "doc_type": "unknown",
    }


def ingest_submission(
    submission_id: str,
    email_body: str,
    attachments: list[str],
) -> NormalizedSubmission:
    """Normalize a submission and its attachment paths into the canonical ingestion record."""
    sid = normalize_submission_id(submission_id)
    body = normalize_email_body(email_body)
    documents = [
        document_from_attachment(sid, i, path) for i, path in enumerate(attachments)
    ]
    return {
        "submission_id": sid,
        "email_body": body,
        "documents": documents,
    }
