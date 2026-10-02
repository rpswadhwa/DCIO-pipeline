"""Full-row LLM extraction path: skip Camelot/text-regex row parsing entirely and
ask the LLM to read a Schedule H, Line 4i page's raw text and return structured
investment rows directly.

This is a SEPARATE path from the existing `use_llm` flag in text_extract.py,
which only uses the LLM to normalize Camelot's column HEADERS -- the row values
there still come from Camelot's parsed cells or the text-regex parser. Here the
LLM produces the row values themselves, for plans where per-filer parser bugs
(column misalignment, garbled text, missing section headings, non-standard
layouts) make the normal extraction path too slow to fix one plan at a time.

Returns data in the exact same shape as `extract_tables_and_map()` -- a list of
per-page dicts with `pdf`, `pdf_stem`, `page_number`, `mapped_rows`, `ocr_cells`,
`normalized_path` -- so it drops into run_pipeline.py in place of the normal
extraction call and the rest of the pipeline (dedup, cleanup, validation, load)
runs unmodified.
"""
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor

import pdfplumber

from .asset_type_patterns import detect_asset_type
from .llm_provider import call_llm_json

_MAX_WORKERS = 6
_MAX_ATTEMPTS = 3
_RETRY_BACKOFF_SEC = [3, 8]

_SCHEMA_FIELDS = [
    "issuer_name", "investment_description", "asset_type", "asset_type_source",
    "par_value", "cost", "current_value", "units_or_shares",
]

_KNOWN_ASSET_TYPES = [
    "Mutual Fund", "Common/Collective Trust Fund", "Money Market Fund",
    "Stable Value Fund", "Separate Account", "Group Annuity Contract",
    "Variable Annuity Contract", "Self-Directed Brokerage Account",
    "Stock", "Bond", "Participant Loan",
]

_KNOWN_ASSET_TYPE_SOURCES = ["row", "heading", "total", "inferred", "none"]

_PROMPT_INSTRUCTIONS = (
    "You are reading one page of a Form 5500 Schedule H, Line 4i "
    "\"Schedule of Assets (Held at End of Year)\" from a retirement plan's "
    "annual filing. Extract every individual investment holding row as a JSON "
    "array. For each row return an object with these fields: "
    "issuer_name (the base sponsoring organization only, e.g. 'Vanguard', "
    "'Fidelity', 'PIMCO' -- NOT the full fund name), "
    "investment_description (the COMPLETE fund/security name and any share "
    "class or type label as printed, verbatim, word for word -- if the "
    "printed text has a form like 'Vanguard Inflation-Protected Securities "
    "Fund: Inv Shares Mutual Funds', investment_description must be the "
    "ENTIRE string 'Inflation-Protected Securities Fund: Inv Shares Mutual "
    "Funds', not just the trailing share-class words. Never drop or "
    "summarize any words from the middle of the printed name), "
    "asset_type (classify using ONLY one of these labels: " +
    ", ".join(_KNOWN_ASSET_TYPES) + " -- otherwise empty string, do not "
    "invent a label outside this list). This page's rows are often grouped "
    "under a SECTION HEADING (e.g. a line reading 'Registered Investment "
    "Company', 'Common/Collective Trust', 'Separate Account') that applies "
    "to every row printed below it, up to the next heading or the end of "
    "the page -- 'Registered Investment Company' means Mutual Fund. Rows "
    "may also be followed by a SUBTOTAL/TOTAL line (e.g. 'Total - "
    "Registered Investment Companies  $12,345') that retroactively labels "
    "every row above it back to the prior heading or subtotal. Use these "
    "rules, in priority order, for every row: "
    "(1) if the row's own printed text names its type explicitly, use that; "
    "(2) otherwise, if a section heading above the row (before the next "
    "heading/subtotal) names a type, use that; "
    "(3) otherwise, if a subtotal/total line below the row (before the next "
    "heading) names a type, use that; "
    "(4) otherwise, if you can confidently infer the type from general "
    "knowledge of the named fund/manager (e.g. a well-known mutual fund "
    "family's share class), use that; "
    "(5) otherwise, empty string -- never guess at random. "
    "asset_type_source (which rule above produced asset_type: 'row', "
    "'heading', 'total', 'inferred', or 'none' if asset_type is empty), "
    "section_heading (copy the EXACT, VERBATIM text of the section heading "
    "line that applies to this row, per rule (2) above -- the nearest heading "
    "line above the row, before any other heading/subtotal -- or empty string "
    "if no such heading exists. Just copy the printed text, do not interpret "
    "or classify it), "
    "current_value (the row's current/fair value in dollars, digits only, no "
    "$ sign or commas, as a plain number), "
    "units_or_shares (share/unit count if printed, else empty string), "
    "par_value (if printed, else empty string), "
    "cost (historical cost if printed, else empty string). "
    "Do NOT include subtotal rows, section-heading-only rows (e.g. a line that "
    "just says \"Mutual Funds:\"), grand-total rows, or blank/filler rows -- "
    "use their text only to classify the holding rows as described above. "
    "If the page has no real holding rows, return an empty array. "
    "Return ONLY the JSON array, no other text."
)


def _get_page_text(pdf_path: str, page_num: int) -> str:
    with pdfplumber.open(pdf_path) as pdf:
        if page_num < 1 or page_num > len(pdf.pages):
            return ""
        return pdf.pages[page_num - 1].extract_text() or ""


def _parse_llm_rows(raw_text: str) -> list:
    text = raw_text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\[.*\]", text, re.DOTALL)
        if not match:
            return []
        try:
            data = json.loads(match.group(0))
        except json.JSONDecodeError:
            return []
    if not isinstance(data, list):
        return []
    return [r for r in data if isinstance(r, dict)]


def _process_page(pdf_path: str, pdf_stem: str, page_num: int, provider: str, model: str) -> dict:
    empty = {
        "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
        "mapped_rows": [], "ocr_cells": [], "normalized_path": pdf_path,
    }

    page_text = _get_page_text(pdf_path, page_num)
    if not page_text.strip():
        return empty

    prompt = {"instructions": _PROMPT_INSTRUCTIONS, "page_text": page_text}
    raw = None
    last_exc = None
    for attempt in range(_MAX_ATTEMPTS):
        try:
            raw = call_llm_json(prompt, provider=provider, model=model)
            break
        except Exception as exc:
            last_exc = exc
            if attempt < _MAX_ATTEMPTS - 1:
                wait_sec = _RETRY_BACKOFF_SEC[attempt]
                print(f"    [llm_row_extract] page {page_num}: attempt {attempt + 1} failed "
                      f"({exc}), retrying in {wait_sec}s...")
                time.sleep(wait_sec)
    if raw is None:
        print(f"    [llm_row_extract] page {page_num}: LLM call failed after "
              f"{_MAX_ATTEMPTS} attempts: {last_exc}")
        return empty

    llm_rows = _parse_llm_rows(raw)
    mapped_rows = []
    for row_idx, llm_row in enumerate(llm_rows, start=1):
        row = {f: "" for f in _SCHEMA_FIELDS}
        for field in _SCHEMA_FIELDS:
            val = llm_row.get(field, "")
            row[field] = "" if val is None else str(val).strip()

        if not row["asset_type"]:
            # Deterministic backfill: don't trust the LLM to apply the heading
            # -> type inheritance rule itself (observed failing silently on
            # most rows under a shared heading, e.g. J&J entity 1 p.225,
            # 2026-10-02). Instead have it just copy the heading text
            # (section_heading, not part of _SCHEMA_FIELDS) and run it through
            # the same regex table enhance_asset_types.py/text_extract.py
            # already rely on elsewhere in the pipeline.
            section_heading = str(llm_row.get("section_heading") or "").strip()
            detected = detect_asset_type(section_heading) if section_heading else ""
            if not detected:
                detected = detect_asset_type(f"{row['investment_description']} {row['issuer_name']}")
            if detected and detected in _KNOWN_ASSET_TYPES:
                row["asset_type"] = detected
                row["asset_type_source"] = "heading" if section_heading else "row"

        if row["asset_type"] and row["asset_type"] not in _KNOWN_ASSET_TYPES:
            # Log instead of silently discarding -- a near-miss label (wrong
            # case, a synonym) is a prompt/model issue worth seeing, not a
            # value to lose without a trace.
            print(f"    [llm_row_extract] page {page_num} row {row_idx}: "
                  f"dropping out-of-vocabulary asset_type {row['asset_type']!r}")
            row["asset_type"] = ""
            row["asset_type_source"] = "none"
        elif row["asset_type_source"] not in _KNOWN_ASSET_TYPE_SOURCES:
            row["asset_type_source"] = "inferred" if row["asset_type"] else "none"
        row["page_number"] = page_num
        row["row_id"] = row_idx
        mapped_rows.append(row)

    print(f"    [llm_row_extract] page {page_num}: {len(mapped_rows)} row(s) via {provider}/{model}")
    return {
        "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
        "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
    }


def extract_investments_via_llm(
    pdf_path: str,
    page_nums: list,
    provider: str = "gemini",
    model: str = "gemini-2.5-flash",
) -> list:
    """Returns a page_data list matching extract_tables_and_map()'s contract.

    Pages are sent to the LLM concurrently (each page is an independent prompt/
    response, no shared state) since a page-by-page sequential loop made even a
    3-PDF smoke test run past a 30-minute SSM command timeout -- each call is a
    real network round trip, and a multi-page schedule has many of them.
    """
    pdf_stem = pdf_path.split("/")[-1].rsplit(".", 1)[0].replace("\\", "/").split("/")[-1]

    if len(page_nums) <= 1:
        return [_process_page(pdf_path, pdf_stem, p, provider, model) for p in page_nums]

    with ThreadPoolExecutor(max_workers=min(_MAX_WORKERS, len(page_nums))) as pool:
        futures = {
            pool.submit(_process_page, pdf_path, pdf_stem, p, provider, model): p
            for p in page_nums
        }
        by_page = {futures[fut]: fut.result() for fut in futures}

    return [by_page[p] for p in page_nums]
