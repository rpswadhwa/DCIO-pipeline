"""Vision-based LLM extraction for scanned (no-text-layer) Schedule H, Line 4i
pages: rasterizes the target page to an image and sends it directly to the
model's vision input, instead of routing through pytesseract/classify_pages +
PaddleOCR (ocr_passes.py) -- removes the missing-tesseract-binary dependency
from the OCR-fallback escalation path entirely for plans with a known manual
page override.

Reuses the same prompt/schema contract as llm_row_extract.py (the primary,
text-based full-row LLM extraction path) so a page produces identical output
whether its text came from pdfplumber or from the rasterized image -- only
`_get_page_text()` is swapped for `_render_page_image()`.

Returns data in the same page_data shape as extract_tables_and_map() /
extract_investments_via_llm() -- a list of per-page dicts with `pdf`,
`pdf_stem`, `page_number`, `mapped_rows`, `ocr_cells`, `normalized_path` --
so it drops into run_pipeline.py's OCR escalation step in place of
_ocr_pdf_worker's tesseract-based chain.
"""
import base64
import time
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO

from pdf2image import convert_from_path

from .asset_type_patterns import detect_asset_type
from .llm_provider import call_llm_json
from .llm_row_extract import (
    _KNOWN_ASSET_TYPES,
    _KNOWN_ASSET_TYPE_SOURCES,
    _MAX_ATTEMPTS,
    _PROMPT_INSTRUCTIONS,
    _RETRY_BACKOFF_SEC,
    _SCHEMA_FIELDS,
    _parse_llm_rows,
)

_MAX_WORKERS = 4


def _render_page_image_b64(pdf_path: str, page_num: int, dpi: int = 300) -> str:
    images = convert_from_path(pdf_path, dpi=dpi, first_page=page_num, last_page=page_num)
    if not images:
        return ""
    buf = BytesIO()
    images[0].save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode("ascii")


def _process_page_vision(pdf_path: str, pdf_stem: str, page_num: int, provider: str, model: str) -> dict:
    empty = {
        "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
        "mapped_rows": [], "ocr_cells": [], "normalized_path": pdf_path,
    }

    try:
        image_b64 = _render_page_image_b64(pdf_path, page_num)
    except Exception as exc:
        print(f"    [llm_vision_extract] page {page_num}: render failed: {exc}")
        return empty
    if not image_b64:
        return empty

    prompt = {"instructions": _PROMPT_INSTRUCTIONS}
    raw = None
    last_exc = None
    for attempt in range(_MAX_ATTEMPTS):
        try:
            raw = call_llm_json(prompt, provider=provider, model=model, image_b64=image_b64)
            break
        except Exception as exc:
            last_exc = exc
            if attempt < _MAX_ATTEMPTS - 1:
                wait_sec = _RETRY_BACKOFF_SEC[attempt]
                print(f"    [llm_vision_extract] page {page_num}: attempt {attempt + 1} failed "
                      f"({exc}), retrying in {wait_sec}s...")
                time.sleep(wait_sec)
    if raw is None:
        print(f"    [llm_vision_extract] page {page_num}: LLM call failed after "
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
            section_heading = str(llm_row.get("section_heading") or "").strip()
            detected = detect_asset_type(section_heading) if section_heading else ""
            if not detected:
                detected = detect_asset_type(f"{row['investment_description']} {row['issuer_name']}")
            if detected and detected in _KNOWN_ASSET_TYPES:
                row["asset_type"] = detected
                row["asset_type_source"] = "heading" if section_heading else "row"

        if row["asset_type"] and row["asset_type"] not in _KNOWN_ASSET_TYPES:
            print(f"    [llm_vision_extract] page {page_num} row {row_idx}: "
                  f"dropping out-of-vocabulary asset_type {row['asset_type']!r}")
            row["asset_type"] = ""
            row["asset_type_source"] = "none"
        elif row["asset_type_source"] not in _KNOWN_ASSET_TYPE_SOURCES:
            row["asset_type_source"] = "inferred" if row["asset_type"] else "none"
        row["page_number"] = page_num
        row["row_id"] = row_idx
        mapped_rows.append(row)

    print(f"    [llm_vision_extract] page {page_num}: {len(mapped_rows)} row(s) via {provider}/{model}")
    return {
        "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
        "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
    }


def extract_investments_via_llm_vision(
    pdf_path: str,
    page_nums: list,
    provider: str = "gemini",
    model: str = "gemini-2.5-flash",
) -> list:
    """Vision-based twin of extract_investments_via_llm() -- same output shape,
    same concurrency pattern, but sends a rasterized page image instead of the
    PDF's text layer. For scanned pages (no text layer at all), this is the
    only path that can extract anything; it also sidesteps the tesseract/
    PaddleOCR chain in _ocr_pdf_worker entirely.
    """
    pdf_stem = pdf_path.split("/")[-1].rsplit(".", 1)[0].replace("\\", "/").split("/")[-1]

    if len(page_nums) <= 1:
        return [_process_page_vision(pdf_path, pdf_stem, p, provider, model) for p in page_nums]

    with ThreadPoolExecutor(max_workers=min(_MAX_WORKERS, len(page_nums))) as pool:
        futures = {
            pool.submit(_process_page_vision, pdf_path, pdf_stem, p, provider, model): p
            for p in page_nums
        }
        by_page = {futures[fut]: fut.result() for fut in futures}

    return [by_page[p] for p in page_nums]
