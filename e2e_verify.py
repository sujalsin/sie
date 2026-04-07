#!/usr/bin/env python3
"""End-to-end verification: every pipeline component + logging + failure analysis."""

from __future__ import annotations

import os

# Deterministic CI / offline runs — never call Gemini from this script.
os.environ["SIE_DISABLE_GEMINI"] = "1"

import json
import sys
from typing import Any

from analysis import analyze_failures
from conflicts import detect_conflicts
from decision import decide_submission
from extraction.main import extract_per_source, merge_per_source_extractions
from extraction.schema import empty_extracted_fields
from followup import generate_follow_up, suggest_fixes
from ingestion import ingest_submission
from main import _patch_llm_json, _llm_payload, run_pipeline
from observability import build_pipeline_metrics, extraction_call_count
from processing import process_submission
from submission_log import get_all_logs, reset_logs
from utils import new_submission_id
from validation import get_cell_value, validate_and_sanitize, validate_submission
from validation.engine import run_validation_engine


def _ok(name: str) -> None:
    print(f"  OK  {name}")


def _fail(name: str, detail: str) -> None:
    print(f"  FAIL {name}: {detail}")
    sys.exit(1)


def main() -> None:
    print("=== SIE end-to-end verification ===\n")

    # --- utils ---
    sid = new_submission_id()
    assert isinstance(sid, str) and len(sid) > 0
    _ok("utils.new_submission_id")

    # --- ingestion ---
    raw = ingest_submission("e2e-1", "Body line one.\n", ["/tmp/x.pdf"])
    assert raw["submission_id"] == "e2e-1"
    assert "documents" in raw and len(raw["documents"]) == 1
    assert raw["documents"][0]["doc_type"] == "unknown"
    _ok("ingestion.ingest_submission")

    # --- processing ---
    proc = process_submission(raw)
    assert "document_extractions" in proc and len(proc["document_extractions"]) == 1
    row = proc["document_extractions"][0]
    assert "doc_id" in row and "text" in row and "source" in row
    _ok("processing.process_submission")

    # --- extraction per-source + merge + schema ---
    ps = extract_per_source(proc)
    assert "email" in ps and "documents" in ps
    assert isinstance(ps["documents"], list) and len(ps["documents"]) == 1
    merged = merge_per_source_extractions(ps)
    assert all(k in merged for k in empty_extracted_fields())
    assert all("value" in merged[k] for k in merged)
    ext = validate_and_sanitize(merged)
    assert get_cell_value(ext.get("revenue")) is None
    _ok("extraction + merge + validate_and_sanitize (provenance)")

    # --- validation engine ---
    ve = run_validation_engine(ext)
    assert "blocking_issues" in ve and "warnings" in ve
    assert "payroll is missing" in ve["blocking_issues"]
    _ok("validation.run_validation_engine")

    ok, issues, ve2 = validate_submission(ext)
    assert ok == (len(ve2["blocking_issues"]) == 0)
    _ok("validation.validate_submission")

    # --- conflicts (flat per-source) ---
    c = detect_conflicts(ps)
    assert isinstance(c, list)
    _ok("conflicts.detect_conflicts")

    # --- decision ---
    dec = decide_submission(ext, ve["blocking_issues"], ve["warnings"])
    assert dec["decision"] in ("READY", "NOT_READY")
    assert "signals" in dec and "field_coverage" in dec["signals"]
    _ok("decision.decide_submission")

    # --- follow-up + suggest_fixes ---
    email = generate_follow_up(ve["blocking_issues"])
    assert "payroll" in email or "revenue" in email or "Additional information" in email
    actions = suggest_fixes(ve["blocking_issues"])
    assert isinstance(actions, list)
    _ok("followup.generate_follow_up + suggest_fixes")

    # --- observability ---
    m = build_pipeline_metrics(
        latency_extraction=0.01,
        latency_validation=0.002,
        latency_decision=0.003,
        latency_total=0.05,
        extraction_calls=extraction_call_count(proc),
    )
    assert m["model"] == "gemini"
    assert set(m["latency"]) >= {"extraction", "validation", "decision", "total"}
    _ok("observability.build_pipeline_metrics")

    # --- full pipeline (default stub LLM) ---
    reset_logs()
    r = run_pipeline(
        submission_id="e2e-pipeline",
        email_body="Short.",
        attachments=["/a.pdf"],
        ground_truth={"revenue": 1.0, "payroll": 1.0},
    )
    required = {
        "submission_id",
        "raw",
        "processed",
        "extracted",
        "validation_ok",
        "validation_issues",
        "validation_engine",
        "per_source_extractions",
        "conflicts",
        "decision",
        "follow_up_email",
        "required_actions",
        "ground_truth",
        "metrics",
    }
    missing = required - set(r.keys())
    if missing:
        _fail("run_pipeline output keys", f"missing {missing}")
    assert r["ground_truth"] == {"revenue": 1.0, "payroll": 1.0}
    assert "latency" in r["metrics"]
    _ok("main.run_pipeline (full output shape + log write)")

    logs = get_all_logs()
    if not logs:
        _fail("submission_log", "no entries after run_pipeline")
    last = logs[-1]
    if last.get("submission_id") != "e2e-pipeline":
        _fail("submission_log", f"last id {last.get('submission_id')}")
    assert "metrics" in last and last["metrics"] is not None
    _ok("submission_log.get_all_logs")

    # --- pipeline with patched LLM (READY path) ---
    full = _llm_payload(
        insured_name="E2E Co",
        business_description="Enough chars for validation minimum threshold here.",
        revenue=500_000.0,
        payroll=200_000.0,
        employee_count=10,
        effective_date="2025-01-01",
        loss_runs_years=5,
    )
    with _patch_llm_json(full):
        r2 = run_pipeline(submission_id="e2e-ready", email_body="See attach", attachments=["/b.pdf"])
    if r2["decision"]["decision"] != "READY":
        _fail("READY path", f"got {r2['decision']}")
    assert r2["validation_ok"] is True
    assert r2["required_actions"] == []
    _ok("run_pipeline with LLM patch → READY")

    # --- failure analysis ---
    rep = analyze_failures()
    assert "top_failure_reasons" in rep and "error_breakdown" in rep
    assert "example_failure_cases" in rep
    eb = rep["error_breakdown"]
    for k in ("extraction_error", "rule_error", "input_issue"):
        assert k in eb, f"missing {k}"
    _ok("analysis.analyze_failures")

    print("\n=== All components verified successfully ===")


if __name__ == "__main__":
    main()
