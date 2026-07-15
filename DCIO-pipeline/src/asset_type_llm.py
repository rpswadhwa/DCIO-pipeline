"""LLM fallback for asset-type labels the regex/list can't map.

Safety net for the *unknown tail* only: the deterministic patterns
(asset_type_patterns / SECTION_HEADING_MAP) stay the first line of defense.
This module is consulted ONLY when a string that sits in a *type position*
(a section heading or a per-row type column) fails to match any known
pattern. It never sees fund names — typing stays driven by document
structure, not by guessing from a security's name.

Design:
  * Constrained output: the model may return exactly one CANONICAL category
    or "UNKNOWN" -- it can never invent a category.
  * Persistent write-through cache keyed on the normalized label, so any
    given label hits the LLM at most once (cheap, deterministic across runs,
    human-reviewable/overridable by editing the cache file).
  * Every resolution is appended to an audit CSV for review, and resolved
    labels are meant to be folded back into the regex over time.
  * Dormant unless configure(use_llm=True, model=...) is called.
"""
import os
import csv
import json
import time
import threading
from typing import Optional

# Canonical vehicle categories the LLM may return (user-curated 2026-07-14).
# NOTE: "Index Fund" and "Target Date Fund" are deliberately absent -- those
# are strategies, not vehicles. A strategy-only label with no vehicle named
# resolves to UNKNOWN (left blank), typed instead by an actual vehicle signal.
CANONICAL_ASSET_TYPES = [
    "Commingled Fund",
    "Common Stock",
    "Common/Collective Trust Fund",
    "Currency",
    "ETF",
    "Employer Stock",
    "Group Annuity Contract",
    "Insurance General Account",
    "Money Market Fund",
    "Mutual Fund",
    "Mutual Fund/Collective Trust",
    "Participant Loan",
    "Partnership Interest",
    "Preferred Stock",
    "Self-Directed Brokerage Account",
    "Separately Managed Account",
    "Short-Term Investment Fund",
    "Stable Value Fund",
]
_CANON_LOWER = {c.lower(): c for c in CANONICAL_ASSET_TYPES}

_state = {
    "enabled": False,
    "model": "",
    "client": None,
    "cache": None,          # dict: normalized_label -> canonical ("" == UNKNOWN)
    "cache_path": "",
    "audit_path": "",
}
_lock = threading.Lock()


def _normalize(label: str) -> str:
    return " ".join(str(label or "").lower().replace("*", " ").strip(" :;.,-*").split())


def _default_cache_path() -> str:
    p = os.environ.get("DCIO_ASSET_TYPE_CACHE")
    if p:
        return p
    return os.path.join(os.path.dirname(__file__), "..", "data", "asset_type_llm_cache.json")


def _load_cache(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def configure(use_llm: bool, model: str = "") -> None:
    """Enable/disable the LLM fallback. Call once per run before extraction."""
    _state["enabled"] = bool(use_llm)
    _state["model"] = model or os.environ.get("ASSET_TYPE_LLM_MODEL", "gpt-4o-mini")
    _state["cache_path"] = _default_cache_path()
    _state["audit_path"] = os.path.join(
        os.path.dirname(_state["cache_path"]) or ".", "asset_type_llm_audit.csv"
    )
    _state["cache"] = _load_cache(_state["cache_path"])
    if use_llm and _state["client"] is None:
        try:
            from openai import OpenAI
            _state["client"] = OpenAI()
        except Exception:
            _state["client"] = None
            _state["enabled"] = False


def _save_cache() -> None:
    path = _state["cache_path"]
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_state["cache"], f, indent=0, sort_keys=True)
        os.replace(tmp, path)
    except Exception:
        pass


def _audit(label: str, norm: str, category: str, source: str) -> None:
    try:
        path = _state["audit_path"]
        new = not os.path.exists(path)
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["ts", "raw_label", "normalized", "category", "source"])
            w.writerow([time.strftime("%Y-%m-%d %H:%M:%S"), label, norm, category or "UNKNOWN", source])
    except Exception:
        pass


def _ask_llm(label: str) -> str:
    """Return a canonical category or '' (UNKNOWN). Never raises."""
    client = _state["client"]
    if client is None:
        return ""
    cats = "\n".join("- " + c for c in CANONICAL_ASSET_TYPES)
    system = (
        "You classify the investment-vehicle TYPE of a label taken from a "
        "US Form 5500 Schedule H Line 4i schedule of assets. You are given a "
        "label that appeared where an asset TYPE belongs (a section heading or "
        "a type column) -- not a fund's proper name. Map it to exactly one of "
        "the allowed categories based on the VEHICLE it names. Return UNKNOWN "
        "if it is not an investment vehicle type (a total line, date, or note) "
        "OR if it only names an investment STRATEGY/objective with no vehicle "
        "(e.g. 'index fund', 'target date', 'growth', 'large cap') -- do NOT "
        "guess a vehicle from a strategy word. Return JSON only: "
        "{\"category\": \"<one of the list, or UNKNOWN>\"}."
    )
    user = "Allowed categories:\n" + cats + "\n\nLabel: " + str(label)
    try:
        resp = client.chat.completions.create(
            model=_state["model"],
            messages=[{"role": "system", "content": system},
                      {"role": "user", "content": user}],
            temperature=0,
            response_format={"type": "json_object"},
        )
        text = resp.choices[0].message.content
        cat = (json.loads(text) or {}).get("category", "")
    except Exception:
        return ""
    return _CANON_LOWER.get(str(cat or "").strip().lower(), "")


def resolve(label: str, source: str = "") -> str:
    """Map a type-position label to a canonical category, or '' if unknown.

    No-op ('' ) when the fallback is disabled. Cached write-through so each
    distinct label costs at most one LLM call ever.
    """
    if not _state["enabled"]:
        return ""
    norm = _normalize(label)
    if not norm or len(norm) > 120:
        return ""
    # exact canonical already? nothing to do
    if norm in _CANON_LOWER:
        return _CANON_LOWER[norm]
    cache = _state["cache"]
    if norm in cache:
        return cache[norm]
    with _lock:
        if norm in cache:            # double-checked after acquiring lock
            return cache[norm]
        category = _ask_llm(label)
        cache[norm] = category
        _save_cache()
        _audit(label, norm, category, source)
    return category
