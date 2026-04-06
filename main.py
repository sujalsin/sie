from __future__ import annotations

from analysis.main import record_pipeline_outcome
from decision.main import decide_submission
from extraction.main import extract_from_submission
from ingestion.main import ingest_submission
from processing.main import process_submission
from utils.main import new_submission_id
from validation.main import validate_submission


def run_pipeline(source: str) -> dict[str, object]:
    """Execute the end-to-end submission intelligence flow for a single source."""

    submission_id = new_submission_id()
    raw = ingest_submission(source)
    processed = process_submission(raw)
    extracted = extract_from_submission(processed)
    ok, issues = validate_submission(extracted)
    decision = decide_submission(extracted, ok, issues)
    stages = {
        "submission_id": submission_id,
        "raw": raw,
        "processed": processed,
        "extracted": extracted,
        "validation_ok": ok,
        "validation_issues": issues,
        "decision": decision,
    }
    record_pipeline_outcome(submission_id, stages)
    return stages


if __name__ == "__main__":
    run_pipeline("default")
