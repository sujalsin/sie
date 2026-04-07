from __future__ import annotations

import json
import time
from contextlib import contextmanager
from pathlib import Path
from pprint import pprint
from typing import Any, Iterator

from analysis.main import record_pipeline_outcome
from conflicts.main import detect_conflicts
from decision.main import DecisionResult, decide_submission
from extraction.main import extract_per_source, merge_per_source_extractions
from followup.main import generate_follow_up, suggest_fixes
from ingestion.main import ingest_submission, normalize_submission_id
from observability.main import build_pipeline_metrics, extraction_call_count
from processing.main import process_submission
from utils.main import new_submission_id
from validation.engine import ValidationEngineReport
from validation.main import validate_submission
from validation.schema import (
    ProvenanceRecord,
    empty_extracted_fields,
    validate_and_sanitize,
)


def _resolve_submission_id(submission_id: str | None) -> str:
    return normalize_submission_id(
        submission_id if submission_id is not None else new_submission_id()
    )


def _normalize_attachments(attachments: list[str] | None) -> list[str]:
    return attachments if attachments is not None else []


def step_ingest_submission(
    submission_id: str,
    email_body: str,
    attachment_paths: list[str],
) -> dict[str, Any]:
    return ingest_submission(submission_id, email_body, attachment_paths)


def step_process_documents(raw: dict[str, Any]) -> dict[str, Any]:
    return process_submission(raw)


def step_extract_per_source(processed: dict[str, Any]) -> dict[str, Any]:
    return extract_per_source(processed)


def step_merge_extractions(per_source: dict[str, Any]) -> dict[str, Any]:
    return merge_per_source_extractions(per_source)


def step_validate_schema(
    extracted: dict[str, Any],
) -> dict[str, ProvenanceRecord]:
    return validate_and_sanitize(extracted)


def step_validation_rules(
    extracted: dict[str, ProvenanceRecord],
) -> tuple[bool, list[str], ValidationEngineReport]:
    return validate_submission(extracted)


def step_detect_conflicts(per_source_extractions: dict[str, Any]) -> list[dict[str, Any]]:
    return detect_conflicts(per_source_extractions)


def step_make_decision(
    extracted: dict[str, ProvenanceRecord],
    validation_engine: ValidationEngineReport,
) -> DecisionResult:
    return decide_submission(
        extracted,
        validation_engine["blocking_issues"],
        validation_engine["warnings"],
    )


def step_generate_follow_up(validation_engine: ValidationEngineReport) -> str:
    return generate_follow_up(validation_engine["blocking_issues"])


def step_log_result(submission_id: str, stages: dict[str, Any]) -> None:
    record_pipeline_outcome(submission_id, stages)


def build_pipeline_output(
    *,
    submission_id: str,
    raw: dict[str, Any],
    processed: dict[str, Any],
    extracted: dict[str, ProvenanceRecord],
    validation_ok: bool,
    validation_issues: list[str],
    validation_engine: ValidationEngineReport,
    per_source_extractions: dict[str, Any],
    conflicts: list[dict[str, Any]],
    decision: DecisionResult,
    follow_up_email: str,
    required_actions: list[str],
    ground_truth: dict[str, object] | None,
    metrics: dict[str, Any],
) -> dict[str, object]:
    return {
        "submission_id": submission_id,
        "raw": raw,
        "processed": processed,
        "extracted": extracted,
        "validation_ok": validation_ok,
        "validation_issues": validation_issues,
        "validation_engine": validation_engine,
        "per_source_extractions": per_source_extractions,
        "conflicts": conflicts,
        "decision": decision,
        "follow_up_email": follow_up_email,
        "required_actions": required_actions,
        "ground_truth": ground_truth,
        "metrics": metrics,
    }


def run_pipeline(
    submission_id: str | None = None,
    email_body: str = "",
    attachments: list[str] | None = None,
    ground_truth: dict[str, object] | None = None,
) -> dict[str, object]:
    """
    Run the submission intelligence pipeline end-to-end and return a structured result.

    1. Ingest submission
    2. Process documents
    3. Extract fields
    4. Validate schema (sanitize to strict types)
    5. Run validation rules (blocking / warnings)
    6. Detect cross-source conflicts
    7. Make readiness decision
    8. Generate follow-up email from blocking issues
    9. Log result
    """
    sid = _resolve_submission_id(submission_id)
    paths = _normalize_attachments(attachments)

    t_total_start = time.time()

    raw = step_ingest_submission(sid, email_body, paths)
    processed = step_process_documents(raw)

    t0 = time.time()
    per_source_extractions = step_extract_per_source(processed)
    extracted_raw = step_merge_extractions(per_source_extractions)
    extracted = step_validate_schema(extracted_raw)
    latency_extraction = time.time() - t0

    t0 = time.time()
    validation_ok, validation_issues, validation_engine = step_validation_rules(extracted)
    latency_validation = time.time() - t0

    conflicts = step_detect_conflicts(per_source_extractions)

    t0 = time.time()
    decision = step_make_decision(extracted, validation_engine)
    latency_decision = time.time() - t0

    follow_up_email = step_generate_follow_up(validation_engine)
    required_actions = suggest_fixes(validation_engine["blocking_issues"])

    latency_total = time.time() - t_total_start
    metrics = build_pipeline_metrics(
        latency_extraction=latency_extraction,
        latency_validation=latency_validation,
        latency_decision=latency_decision,
        latency_total=latency_total,
        extraction_calls=extraction_call_count(processed),
    )

    result = build_pipeline_output(
        submission_id=sid,
        raw=raw,
        processed=processed,
        extracted=extracted,
        validation_ok=validation_ok,
        validation_issues=validation_issues,
        validation_engine=validation_engine,
        per_source_extractions=per_source_extractions,
        conflicts=conflicts,
        decision=decision,
        follow_up_email=follow_up_email,
        required_actions=required_actions,
        ground_truth=ground_truth,
        metrics=metrics,
    )
    step_log_result(sid, result)
    return result


@contextmanager
def _patch_llm_json(payload: dict[str, Any]) -> Iterator[None]:
    import extraction.main as extraction_main

    previous = extraction_main.call_llm

    def stub(_prompt: str) -> str:
        return json.dumps(payload)

    extraction_main.call_llm = stub
    try:
        yield
    finally:
        extraction_main.call_llm = previous


def _llm_payload(**overrides: Any) -> dict[str, Any]:
    base = empty_extracted_fields()
    base.update(overrides)
    return base


def print_pipeline_summary(title: str, result: dict[str, object]) -> None:
    width = 72
    print("\n" + "=" * width)
    print(f" {title}")
    print("=" * width)
    print(f"submission_id:     {result['submission_id']}")
    print("per_source_extractions:")
    pprint(result["per_source_extractions"], indent=4, width=width)
    print("extracted (merged + validated):")
    pprint(result["extracted"], indent=4, width=width)
    ve = result["validation_engine"]
    print("blocking_issues:")
    pprint(ve["blocking_issues"], indent=4, width=width)
    print("warnings:")
    pprint(ve["warnings"], indent=4, width=width)
    print("decision:")
    pprint(result["decision"], indent=4, width=width)
    print("metrics:")
    pprint(result.get("metrics"), indent=4, width=width)
    conflicts = result["conflicts"]
    print(f"conflicts ({len(conflicts)}):")
    pprint(conflicts, indent=4, width=width)
    print("follow_up_email:")
    print(result["follow_up_email"])
    print("required_actions:")
    pprint(result.get("required_actions", []), indent=4, width=width)
    gt = result.get("ground_truth")
    print("ground_truth:", gt if gt is not None else "(none)")


def print_failure_insights() -> None:
    from analysis import analyze_failures

    width = 72
    print("\n" + "=" * width)
    print(" FAILURE ANALYSIS (from submission_log)")
    print("=" * width)
    report = analyze_failures()
    print("top_failure_reasons:")
    pprint(report["top_failure_reasons"], indent=2, width=width)
    bd = report["error_breakdown"]
    print("\nerror_breakdown (summary):")
    print(f"  total_submissions: {bd['total_submissions']}")
    print(f"  by_decision: {bd['by_decision']}")
    print(f"  submissions_with_blocking_issues: {bd['submissions_with_blocking_issues']}")
    print(f"  total_blocking_issue_occurrences: {bd['total_blocking_issue_occurrences']}")
    print(
        f"  error attribution (counts): extraction_error={bd.get('extraction_error')}, "
        f"rule_error={bd.get('rule_error')}, input_issue={bd.get('input_issue')}"
    )
    print("  statistics:")
    pprint(bd["statistics"], indent=4, width=width)
    print("  ground_truth:")
    pprint(bd["ground_truth"], indent=4, width=width)
    print(f"\nfull blocking_issue_counts ({len(bd['blocking_issue_counts'])} keys):")
    pprint(dict(sorted(bd["blocking_issue_counts"].items(), key=lambda x: (-x[1], x[0]))))
    print("\nexample_failure_cases:")
    pprint(report.get("example_failure_cases"), indent=2, width=width)


def _project_root() -> Path:
    return Path(__file__).resolve().parent


def _write_payroll_memo_pdf(path: Path) -> None:
    """Broker-style payroll memo PDF (text layer) aligned with ML Job sample ACORD."""
    from fpdf import FPDF

    lines = [
        "PAYROLL AUTHORIZATION MEMO",
        "To: Commercial Underwriting",
        "Re: ABC Manufacturing - General Liability submission",
        "",
        "The undersigned confirms taxable payroll for the most recent full fiscal year of USD 200000,",
        "supporting the figures stated on the attached ACORD application.",
        "Average full-time-equivalent headcount during that period: 12.",
        "",
        "Prepared by: Jordan Reeves, Hartwell Risk Partners (broker)",
        "On behalf of: ABC Manufacturing - payroll@abcmfg.example",
        "Date: March 18, 2025",
    ]
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Helvetica", size=11)
    inner_w = pdf.w - pdf.l_margin - pdf.r_margin
    for line in lines:
        pdf.multi_cell(inner_w, 7, line or " ")
    pdf.output(str(path))


def _write_demo_pdf(path: Path) -> None:
    """Create a small text-based PDF so pypdf can extract embedded text."""
    from fpdf import FPDF

    lines = [
        "Underwriting supplement (PDF text layer)",
        "Named insured: Summit Tools Inc",
        "Business: wholesale distributor of industrial supplies",
        "Annual revenue (USD): 625000",
        "Payroll (USD): 210000",
        "Employees (FTE): 14",
        "Requested GL effective date: 2025-07-01",
        "Loss runs: 5 prior years on file",
    ]
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Helvetica", size=11)
    # ``w=0`` breaks on some fpdf builds ("not enough horizontal space"); use inner width.
    inner_w = pdf.w - pdf.l_margin - pdf.r_margin
    for line in lines:
        pdf.multi_cell(inner_w, 7, line)
    pdf.output(str(path))


def _preview_text(text: str, max_len: int = 320) -> str:
    collapsed = " ".join(text.split())
    if len(collapsed) <= max_len:
        return collapsed
    return collapsed[: max_len - 3] + "..."


def print_live_demo_walkthrough(result: dict[str, object], *, pack_label: str = "single submission") -> None:
    """Print the ten pipeline stages with concrete artifacts from a single run."""
    raw = result["raw"]
    proc = result["processed"]
    ps = result["per_source_extractions"]
    ext = result["extracted"]
    ve = result["validation_engine"]

    print(f"\n--- Pipeline walkthrough ({pack_label}) ---\n")

    print("1. INGESTION")
    print(f"   submission_id={raw['submission_id']}")
    print(f"   attachments: {len(raw.get('documents') or [])} path(s) recorded")
    for i, d in enumerate(raw.get("documents") or []):
        if isinstance(d, dict):
            print(f"     [{i}] {d.get('file_path')}")

    print("\n2. OCR / PROCESSING (PDF text layer + plain text files)")
    for row in proc.get("document_extractions") or []:
        if not isinstance(row, dict):
            continue
        src = row.get("source", "")
        body = row.get("text") if isinstance(row.get("text"), str) else ""
        ok = not body.startswith("[") and "not found" not in body[:80].lower()
        status = "OK (text extracted)" if ok and len(body) > 40 else "check output"
        print(f"   {src}")
        print(f"   status: {status}; preview: {_preview_text(body)}")

    print("\n3. PER-SOURCE EXTRACTION (Gemini: email + each document)")
    print("   email fields:")
    pprint(ps.get("email"), indent=6, width=68)
    for doc in ps.get("documents") or []:
        if isinstance(doc, dict):
            did = str(doc.get("doc_id", ""))[:10]
            print(f"   document {did}... fields:")
            pprint(doc.get("fields"), indent=6, width=68)

    print("\n4. MERGE + PROVENANCE (documents override email; confidence high/medium/low)")
    pprint(ext, indent=4, width=72)

    from extraction.gemini_client import get_gemini_api_key

    if get_gemini_api_key() and all(
        isinstance(ext.get(k), dict) and ext[k].get("value") is None for k in ext
    ):
        print(
            "\n   Note: API key is set but merged values are all empty. Common causes: "
            "HTTP 429 (quota / free-tier limits for this model), TLS/CA issues, or non-JSON model output. "
            "Run with SIE_GEMINI_DEBUG=1 to print redacted HTTP error details from Gemini."
        )

    print("\n5. VALIDATION (rules engine)")
    print(f"   validation_ok: {result.get('validation_ok')}")
    print("   blocking_issues:")
    pprint(ve.get("blocking_issues"), indent=6, width=68)
    print("   warnings:")
    pprint(ve.get("warnings"), indent=6, width=68)

    print("\n6. CONFLICT DETECTION (cross-source disagreeing non-null values)")
    pprint(result.get("conflicts"), indent=4, width=72)

    print("\n7. DECISION")
    pprint(result.get("decision"), indent=4, width=72)

    print("\n8. FOLLOW-UP GENERATION")
    print(result.get("follow_up_email"))
    print("   required_actions (from suggest_fixes):")
    pprint(result.get("required_actions"), indent=4, width=72)

    print("\n9. LOGGING")
    print("   Appended structured outcome to submission_logs.json (via record_pipeline_outcome).")
    m = result.get("metrics")
    if isinstance(m, dict):
        print("   metrics:")
        pprint(m, indent=6, width=68)


def run_ml_pack_demo() -> None:
    """
    End-to-end demo using ``ML Job Submission PDFs`` (sub_001 ACORD + loss runs) plus a generated PDF.

    Writes ``demo_data/ml_demo_broker_email.txt`` and ``demo_data/payroll_authorization_memo.pdf``.
    """
    from submission_log.main import reset_logs

    from extraction.gemini_client import get_gemini_api_key

    root = _project_root()
    reset_logs()
    print("\n>>> Cleared submission_logs.json for a clean live demo run.\n")

    demo_dir = root / "demo_data"
    demo_dir.mkdir(exist_ok=True)
    ml_dir = root / "ML Job Submission PDFs"

    attachments: list[str] = []
    for name in ("sub_001_acord.pdf", "sub_001_loss_runs.pdf"):
        p = (ml_dir / name).resolve()
        if p.is_file():
            attachments.append(str(p))
        else:
            print(f"WARNING: Missing expected file: {p}\n")

    memo_path = demo_dir / "payroll_authorization_memo.pdf"
    _write_payroll_memo_pdf(memo_path)
    attachments.append(str(memo_path.resolve()))

    email_body = (
        "Subject: New GL submission — ABC Manufacturing\n\n"
        "Hi Underwriting Team,\n\n"
        "Please set up a General Liability renewal for ABC Manufacturing (light manufacturing, "
        "metal fabrication and assembly in Dayton, Ohio). I am attaching the signed ACORD application, "
        "three years of carrier loss runs as requested, and our internal payroll authorization memo "
        "confirming the payroll figure tied to this risk.\n\n"
        "Target effective date is January 1, 2025. Latest financials on the application are "
        "approximately five hundred thousand dollars in revenue, two hundred thousand dollars in payroll, "
        "and about twelve employees.\n\n"
        "Let me know if you need SOV or additional loss detail.\n\n"
        "Thanks,\n"
        "Jordan Reeves\n"
        "Hartwell Risk Partners\n"
        "jordan.reeves@hartwellrisk.example\n"
    )
    (demo_dir / "ml_demo_broker_email.txt").write_text(email_body, encoding="utf-8")

    print("Attachments for this run:")
    for i, a in enumerate(attachments):
        print(f"  [{i}] {a}")
    print(f"Broker email saved to: {demo_dir / 'ml_demo_broker_email.txt'}\n")

    if not get_gemini_api_key():
        print(
            "WARNING: No Gemini API key (set GEMINI_API_KEY in .env) or SIE_DISABLE_GEMINI is on — "
            "extraction will fall back to empty fields.\n"
        )

    result = run_pipeline(
        submission_id="ml-job-sub-001",
        email_body=email_body,
        attachments=attachments,
    )
    print_live_demo_walkthrough(result, pack_label="ML Job PDFs sub_001 + generated payroll memo")

    print("\n10. FAILURE ANALYSIS (aggregate over submission_logs.json)")
    print_failure_insights()


def run_live_demo() -> None:
    """Synthetic Summit Tools demo (no ML Job folder required)."""
    from submission_log.main import reset_logs

    from extraction.gemini_client import get_gemini_api_key

    root = _project_root()
    reset_logs()
    print("\n>>> Cleared submission_logs.json for a clean live demo run.\n")

    demo_dir = root / "demo_data"
    demo_dir.mkdir(exist_ok=True)
    txt_path = demo_dir / "payroll_notes.txt"
    txt_path.write_text(
        "Payroll notes (text attachment)\n"
        "Taxable payroll per auditor (last fiscal year): USD 205000\n"
        "Peak headcount: 12 employees\n"
        "Accounting contact: finance@summittools.example\n",
        encoding="utf-8",
    )
    pdf_path = demo_dir / "underwriting_facts.pdf"
    _write_demo_pdf(pdf_path)

    email_body = (
        "Subject: GL submission — Summit Tools\n\n"
        "Hi Underwriting,\n\n"
        "Please quote General Liability for Summit Tool Company (wholesale industrial supplies). "
        "We have about 11 employees. Target effective date 2025-07-01; we attach payroll notes "
        "and a short underwriting PDF. Revenue last fiscal year was approximately USD 600000 and "
        "payroll around USD 195000. We can provide 5 years of loss history.\n\n"
        "Thanks,\nAlex\n"
    )

    if not get_gemini_api_key():
        print(
            "WARNING: No Gemini API key (set GEMINI_API_KEY in .env) or SIE_DISABLE_GEMINI is on — "
            "extraction will fall back to empty fields.\n"
        )

    result = run_pipeline(
        submission_id="live-demo-1",
        email_body=email_body,
        attachments=[str(txt_path), str(pdf_path)],
    )
    print_live_demo_walkthrough(result, pack_label="synthetic Summit Tools demo")

    print("\n10. FAILURE ANALYSIS (aggregate from submission_logs.json)")
    print_failure_insights()


def run_cli_demo() -> None:
    from submission_log.main import reset_logs

    reset_logs()
    print("\n>>> Cleared submission_logs.json for a clean demo run.\n")

    scenarios: list[tuple[str, dict[str, Any] | None, dict[str, Any]]] = [
        (
            "Mock 1 — default extractor (missing payroll & revenue)",
            None,
            {
                "submission_id": "mock-1-default",
                "email_body": "Please bind GL for our retail store.",
                "attachments": ["/tmp/placeholder_acord.pdf"],
            },
        ),
        (
            "Mock 2 — default extractor, different email",
            None,
            {
                "submission_id": "mock-2-default",
                "email_body": "Urgent: need quote by Friday.",
                "attachments": [],
            },
        ),
        (
            "Mock 3 — full extraction (expect READY)",
            _llm_payload(
                insured_name="Acme Corp",
                business_description="Commercial liability consulting for SMB clients nationwide",
                revenue=250_000.0,
                payroll=120_000.0,
                employee_count=8,
                effective_date="2024-06-01",
                loss_runs_years=5,
            ),
            {
                "submission_id": "mock-3-full",
                "email_body": "See attached SOV and loss runs.",
                "attachments": ["/tmp/sov.csv"],
            },
        ),
        (
            "Mock 4 — revenue only (payroll missing → blocking)",
            _llm_payload(revenue=100_000.0),
            {
                "submission_id": "mock-4-revenue-only",
                "email_body": "Revenue attached; payroll TBD.",
                "attachments": [],
            },
        ),
        (
            "Mock 5 — payroll only + ground_truth for analysis",
            _llm_payload(payroll=75_000.0),
            {
                "submission_id": "mock-5-payroll-only",
                "email_body": "Payroll figures inside PDF.",
                "attachments": ["/tmp/payroll.pdf"],
                "ground_truth": {
                    "revenue": 200_000.0,
                    "payroll": 75_000.0,
                    "employee_count": 12,
                },
            },
        ),
    ]

    for title, llm_payload, kwargs in scenarios:
        if llm_payload is None:
            result = run_pipeline(**kwargs)
        else:
            with _patch_llm_json(llm_payload):
                result = run_pipeline(**kwargs)
        print_pipeline_summary(title, result)

    print_failure_insights()


if __name__ == "__main__":
    import sys

    if "--mock" in sys.argv:
        run_cli_demo()
    elif "--synthetic" in sys.argv:
        run_live_demo()
    else:
        run_ml_pack_demo()
