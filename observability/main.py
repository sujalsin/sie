from __future__ import annotations

from typing import Any

MODEL_NAME = "gemini"

ESTIMATED_INPUT_TOKENS_PER_CALL = 400
ESTIMATED_OUTPUT_TOKENS_PER_CALL = 200
USD_PER_EXTRACTION_CALL = 0.00015


def extraction_call_count(processed: dict[str, Any]) -> int:
    """One LLM-style extraction for email plus one per processed document row."""
    rows = processed.get("document_extractions") or []
    doc_n = sum(1 for row in rows if isinstance(row, dict))
    return 1 + doc_n


def estimate_tokens_and_cost(num_calls: int) -> tuple[int, float]:
    if num_calls <= 0:
        return 0, 0.0
    tokens = num_calls * (ESTIMATED_INPUT_TOKENS_PER_CALL + ESTIMATED_OUTPUT_TOKENS_PER_CALL)
    cost = num_calls * USD_PER_EXTRACTION_CALL
    return tokens, round(cost, 8)


def build_pipeline_metrics(
    *,
    latency_extraction: float,
    latency_validation: float,
    latency_decision: float,
    latency_total: float,
    extraction_calls: int,
) -> dict[str, Any]:
    token_estimate, cost_estimate = estimate_tokens_and_cost(extraction_calls)
    return {
        "latency": {
            "extraction": round(latency_extraction, 6),
            "validation": round(latency_validation, 6),
            "decision": round(latency_decision, 6),
            "total": round(latency_total, 6),
        },
        "model": MODEL_NAME,
        "token_estimate": float(token_estimate),
        "cost_estimate": float(cost_estimate),
    }
