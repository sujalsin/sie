from __future__ import annotations

import json
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import urlencode

_DOTENV_LOADED = False


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_env() -> None:
    global _DOTENV_LOADED
    if _DOTENV_LOADED:
        return
    _DOTENV_LOADED = True
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(_project_root() / ".env")


def get_gemini_api_key() -> str | None:
    if os.environ.get("SIE_DISABLE_GEMINI", "").lower() in ("1", "true", "yes"):
        return None
    _load_env()
    key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if key and str(key).strip():
        return str(key).strip()
    return None


def get_gemini_model_name() -> str:
    """
    Default model id for ``generateContent``.

    ``gemini-2.0-flash`` is deprecated (see Gemini API deprecations); the default is
    ``gemini-2.5-flash``. Override with env ``GEMINI_MODEL`` (e.g. ``gemini-2.5-flash-lite``).
    """
    _load_env()
    return os.environ.get("GEMINI_MODEL", "gemini-2.5-flash").strip() or "gemini-2.5-flash"


def _strip_json_fence(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = re.sub(r"^```(?:json)?\s*", "", t, count=1, flags=re.IGNORECASE)
        if "```" in t:
            t = t.split("```", 1)[0]
    return t.strip()


def _ssl_context() -> ssl.SSLContext | None:
    """Prefer certifi so HTTPS works on Python builds missing system CA bundles."""
    try:
        import certifi

        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        return None


def _redact_url_for_log(url: str) -> str:
    """Strip API key query param from URLs before logging."""
    return re.sub(r"(key=)[^&]+", r"\1REDACTED", url, count=1, flags=re.IGNORECASE)


def _gemini_debug(msg: str) -> None:
    if os.environ.get("SIE_GEMINI_DEBUG", "").lower() not in ("1", "true", "yes"):
        return
    sys.stderr.write(f"[SIE_GEMINI_DEBUG] {msg}\n")
    sys.stderr.flush()


def _collect_part_texts(parts: list[object]) -> list[str]:
    out: list[str] = []
    for p in parts:
        if not isinstance(p, dict):
            continue
        t = p.get("text")
        if isinstance(t, str) and t.strip():
            out.append(t.strip())
    return out


def _pick_json_text_from_parts(texts: list[str]) -> str | None:
    """Prefer a segment that looks like a JSON object (models may emit multiple parts)."""
    if not texts:
        return None
    for t in reversed(texts):
        if t.lstrip().startswith("{"):
            return t
    return texts[-1]


def _extract_text_from_response(data: object) -> str | None:
    if not isinstance(data, dict):
        return None
    cands = data.get("candidates")
    if not isinstance(cands, list) or not cands:
        pf = data.get("promptFeedback")
        if isinstance(pf, dict):
            _gemini_debug(f"no candidates; promptFeedback={pf!r}")
        else:
            _gemini_debug("no candidates and no promptFeedback")
        return None
    first = cands[0]
    if not isinstance(first, dict):
        return None
    content = first.get("content")
    if not isinstance(content, dict):
        fr = first.get("finishReason")
        _gemini_debug(f"missing content; finishReason={fr!r}")
        return None
    parts = content.get("parts")
    if not isinstance(parts, list) or not parts:
        fr = first.get("finishReason")
        _gemini_debug(f"empty parts; finishReason={fr!r}")
        return None
    texts = _collect_part_texts(parts)
    picked = _pick_json_text_from_parts(texts)
    if not picked:
        _gemini_debug(f"no text in parts; part keys={[list(p.keys()) if isinstance(p, dict) else type(p) for p in parts[:3]]}")
    return picked


def call_gemini_json(prompt: str) -> str | None:
    """
    Call Gemini (Google AI) ``generateContent`` and return a JSON object string.

    Uses the public REST API so no ``google-generativeai`` wheel is required (e.g. Python 3.13).

    Returns None if disabled, misconfigured, or on failure (caller falls back to stub).
    """
    key = get_gemini_api_key()
    if not key:
        return None

    model = get_gemini_model_name()
    base = "https://generativelanguage.googleapis.com/v1beta"
    q = urlencode({"key": key})
    url = f"{base}/models/{model}:generateContent?{q}"

    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "maxOutputTokens": 2048,
            "temperature": 0.2,
        },
    }

    payload = json.dumps(body).encode("utf-8")
    ctx = _ssl_context()

    raw: dict[str, object] | None = None
    for attempt in range(2):
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            kw = {"timeout": 120}
            if ctx is not None:
                kw["context"] = ctx
            with urllib.request.urlopen(req, **kw) as resp:
                raw = json.load(resp)
                break
        except urllib.error.HTTPError as e:
            err_body = ""
            try:
                err_body = e.read().decode("utf-8", errors="replace")[:1200]
            except Exception:
                pass
            err_url = getattr(e, "filename", None) or getattr(e, "url", None) or ""
            _gemini_debug(
                f"HTTPError {e.code} url={_redact_url_for_log(str(err_url))!r} "
                f"body[:1200]={err_body!r}"
            )
            if e.code == 429 and attempt == 0:
                time.sleep(2.5)
                continue
            return None
        except (urllib.error.URLError, json.JSONDecodeError, TimeoutError):
            return None
        except OSError:
            return None

    if raw is None:
        return None

    if isinstance(raw, dict) and raw.get("error"):
        _gemini_debug(f"top-level error: {raw.get('error')!r}")
        return None

    text = _extract_text_from_response(raw)
    if not text:
        if isinstance(raw, dict):
            um = raw.get("usageMetadata")
            _gemini_debug(f"no extractable text; usageMetadata={um!r}")
        return None
    out = _strip_json_fence(text)
    try:
        json.loads(out)
    except json.JSONDecodeError as e:
        _gemini_debug(f"model text is not valid JSON after fence strip: {e!r} text[:400]={out[:400]!r}")
        return None
    return out
