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
from concurrent.futures import ThreadPoolExecutor

import pdfplumber

from .llm_provider import call_llm_json

_MAX_WORKERS = 6

_SCHEMA_FIELDS = [
    "issuer_name", "investment_description", "asset_type",
    "par_value", "cost", "current_value", "units_or_shares",
]

_KNOWN_ASSET_TYPES = [
    "Mutual Fund", "Common/Collective Trust Fund", "Money Market Fund",
    "Stable Value Fund", "Separate Account", "Group Annuity Contract",
    "Variable Annuity Contract", "Self-Directed Brokerage Account",
    "Stock", "Bond", "Participant Loan",
]

_PROMPT_INSTRUCTIONS = (
    "You are reading one page of a Form 5500 Schedule H, Line 4i "
    "\"Schedule of Assets (Held at End of Year)\" from a retirement plan's "
    "annual filing. Extract every individual investment holding row as a JSON "
    "array. For each row return an object with these fields: "
    "issuer_name (the fund/security name as printed), "
    "investment_description (share class, fund type label, or other "
    "descriptive text next to the name -- empty string if none), "
    "asset_type (your best classification, using ONLY one of these labels if "
    "it clearly applies: " + ", ".join(_KNOWN_ASSET_TYPES) + " -- otherwise "
    "empty string, do not invent a label outside this list), "
    "current_value (the row's current/fair value in dollars, digits only, no "
    "$ sign or commas, as a plain number), "
    "units_or_shares (share/unit count if printed, else empty string), "
    "par_value (if printed, else empty string), "
    "cost (historical cost if printed, else empty string). "
    "Do NOT include subtotal rows, section-heading-only rows (e.g. a line that "
    "just says \"Mutual Funds:\"), grand-total rows, or blank/filler rows. "
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
    try:
        raw = call_llm_json(prompt, provider=provider, model=model)
    except Exception as exc:
        print(f"    [llm_row_extract] page {page_num}: LLM call failed: {exc}")
        return empty

    llm_rows = _parse_llm_rows(raw)
    mapped_rows = []
    for row_idx, llm_row in enumerate(llm_rows, start=1):
        row = {f: "" for f in _SCHEMA_FIELDS}
        for field in _SCHEMA_FIELDS:
            val = llm_row.get(field, "")
            row[field] = "" if val is None else str(val).strip()
        if row["asset_type"] not in _KNOWN_ASSET_TYPES:
            row["asset_type"] = ""
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
