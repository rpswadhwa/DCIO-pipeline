import json
import os
import re
from typing import Dict, List, Tuple, Optional

import camelot
import pdfplumber
from openai import OpenAI
from rapidfuzz import process, fuzz

import pandas as pd

from .asset_type_patterns import ASSET_TYPE_PATTERNS, detect_asset_type
from .data_cleaner import handle_split_rows, parse_investment_row
from .utils import load_yaml, normalize_whitespace

# Strips leading total/subtotal words, or trailing total/subtotal/(Continued) words,
# before section-heading pattern matching -- e.g. "MUTUAL FUNDS (Continued)" on a
# multi-page Schedule H section must still match the "MUTUAL FUNDS" heading pattern.
_TOTAL_AFFIX_RE = re.compile(
    r'^(?:total|subtotal|grand\s+total)\s+'
    r'|\s+(?:total|subtotal|grand\s+total)$'
    r'|\s*\(\s*continued\s*\)\s*$'
    r'|\s+continued$',
    re.IGNORECASE,
)



def _page_values_are_in_thousands(text: str) -> bool:
    """Return True when page text declares dollar amounts in thousands."""
    return bool(re.search(
        r'\b(?:in\s+thousands|amounts?\s+(?:are\s+)?in\s+thousands|'
        r'dollars?\s+in\s+thousands|\$\s*000s?)\b',
        text or '',
        re.IGNORECASE,
    ))


def _page_values_are_in_millions(text: str) -> bool:
    """Return True when page text declares dollar amounts in millions."""
    return bool(re.search(
        r'\b(?:in\s+millions|amounts?\s+(?:are\s+)?in\s+millions|'
        r'dollars?\s+in\s+millions)\b',
        text or '',
        re.IGNORECASE,
    ))


def _page_value_scale_factor(text: str) -> int:
    """Return the dollar-value scale factor (1, 1_000, or 1_000_000) declared by page text."""
    if _page_values_are_in_millions(text):
        return 1_000_000
    if _page_values_are_in_thousands(text):
        return 1_000
    return 1


def _scale_currency_string(raw: str, factor: int) -> str:
    """Scale a parsed currency value while preserving a plain numeric string."""
    if factor == 1 or raw is None:
        return raw
    text = str(raw).strip()
    if not text:
        return raw
    negative = text.startswith('-') or (text.startswith('(') and text.endswith(')'))
    cleaned = re.sub(r'[^0-9.]', '', text)
    if not cleaned:
        return raw
    try:
        value = float(cleaned) * factor
    except ValueError:
        return raw
    if negative:
        value *= -1
    if value.is_integer():
        return str(int(value))
    return f"{value:.2f}"


def _pdf_has_gm_hh1c_schedule_4i_layout(pdf_path: str, pages: List[int]) -> bool:
    """Detect the HH1C composite Schedule H Line 4i layout used by GM filings."""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            pages_to_check = [p for p in pages if 1 <= p <= len(pdf.pages)] or range(1, len(pdf.pages) + 1)
            checked = 0
            for page_num in pages_to_check:
                text = pdf.pages[page_num - 1].extract_text() or ""
                upper = text.upper()
                if (
                    "HH1C" in upper
                    and "COMPOSITE PLAN YEAR ENDING" in upper
                    and "SCHEDULE H, LINE 4I - SCHEDULE OF ASSETS" in upper
                    and "GENERAL MOTORS" in upper
                ):
                    return True
                checked += 1
                if checked >= 12:
                    break
    except Exception:
        return False
    return False


def _build_text_result(pdf_path: str, rows: List[Dict]) -> List[Dict]:
    """Wrap already-normalized text parser rows in the extractor result shape."""
    pdf_stem = pdf_path.split("/")[-1].rsplit(".", 1)[0]
    by_page: Dict[int, List[Dict]] = {}
    for row in rows:
        by_page.setdefault(int(row.get("page_number", 0) or 0), []).append(row)
    return [
        {
            "pdf": pdf_path,
            "pdf_stem": pdf_stem,
            "page_number": page_num,
            "mapped_rows": page_rows,
            "ocr_cells": [],
            "normalized_path": pdf_path,
        }
        for page_num, page_rows in sorted(by_page.items())
        if page_num
    ]


def _is_new_exhibit_or_schedule_page(text: str) -> bool:
    lines = [line.strip().upper() for line in (text or "").splitlines() if line.strip()]
    first_lines = lines[:8]
    return any(
        line.startswith("EXHIBIT ")
        or line.startswith("SCHEDULE ")
        or line.startswith("FORM 5500")
        for line in first_lines
    )


def _infer_continuation_family(text: str) -> str:
    """Infer the asset/table family for conservative continuation-page expansion."""
    upper = (text or "").upper()
    if "REPORTABLE TRANSACTIONS" in upper or "SERVICE PROVIDER INFORMATION" in upper:
        return ""
    # Be deliberately narrow. Generic pages with "MUTUAL FUNDS" or
    # "COMMON STOCK" can be standalone schedules; auto-expanding them pulled
    # unrelated stock continuation pages into the IBM file. The known missing
    # case is Exhibit A expanded choice mutual funds continuing onto pages
    # without repeated headers.
    if "EXHIBIT A - EXPANDED CHOICE MUTUAL FUNDS" in upper:
        return "mutual_fund"
    return ""


def _looks_like_investment_continuation_page(text: str, family: str = "") -> bool:
    """Return True for headerless pages that continue the same investment listing family."""
    if not text:
        return False
    upper = text.upper()
    if _is_new_exhibit_or_schedule_page(text):
        return False
    if any(marker in upper for marker in ["REPORTABLE TRANSACTIONS", "SERVICE PROVIDER INFORMATION"]):
        return False

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if family == "mutual_fund":
        mutual_value_lines = sum(
            1 for line in lines[:80]
            if re.search(r'\bMUTUAL FUNDS?\b', line, re.IGNORECASE)
            and re.search(r'\b\d{1,3}(?:,\d{3})+(?:\.\d+)?\b\s*$', line)
        )
        structural_profile = _infer_structural_row_profile(text)
        structural_profile_lines = 0
        if structural_profile:
            prefix = structural_profile.get('issuer_prefix', '')
            prefix_re = re.compile(r'^\*?\s*' + re.escape(prefix), re.IGNORECASE)
            structural_profile_lines = sum(
                1 for line in lines[:100]
                if prefix_re.search(normalize_whitespace(line))
                and re.search(r'\b\d{1,3}(?:,\d{3})+(?:\.\d+)?\b\s*$', line)
            )
        conflicting_stock_lines = sum(
            1 for line in lines[:40]
            if re.search(r'\bCOMMON STOCK\b|\bREIT\b', line, re.IGNORECASE)
        )
        return (mutual_value_lines >= 5 or structural_profile_lines >= 5) and conflicting_stock_lines == 0


    if family == "common_stock":
        stock_value_lines = sum(
            1 for line in lines[:80]
            if re.search(r'\bCOMMON STOCK\b|\bREIT\b', line, re.IGNORECASE)
            and re.search(r'\b\d{1,3}(?:,\d{3})+(?:\.\d+)?\b\s*$', line)
        )
        return stock_value_lines >= 5

    return False


def _infer_inline_text_parser_profile(text: str) -> str:
    """Infer a row-pattern parser profile from text that was accepted as a base page."""
    if not text:
        return ""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    mutual_lines = sum(
        1 for line in lines[:100]
        if re.search(r'\bMUTUAL FUNDS?\b', line, re.IGNORECASE)
        and re.search(r'\b\d{1,3}(?:,\d{3})+(?:\.\d+)?\b\s*$', line)
    )
    if mutual_lines >= 5:
        return "inline_mutual_fund_units_value"
    if re.search(r'\bMUTUAL\s+FUNDS?\b', text, re.IGNORECASE) and _infer_structural_row_profile(text):
        return "inline_mutual_fund_units_value"
    return ""


def _profile_family(profile: str) -> str:
    if profile == "inline_mutual_fund_units_value":
        return "mutual_fund"
    return ""


def expand_continuation_pages(pdf_path: str, supplemental_pages: List[int], max_extra_pages: int = 5) -> List[int]:
    """Add headerless continuation pages after detected Schedule 4i pages.

    Expansion is family-scoped: a mutual-fund page can only pull mutual-fund
    continuation pages, and a stock page can only pull stock continuation pages.
    """
    if not supplemental_pages:
        return supplemental_pages
    expanded = set(supplemental_pages)
    try:
        with pdfplumber.open(pdf_path) as pdf:
            page_count = len(pdf.pages)
            for start_page in sorted(supplemental_pages):
                start_text = pdf.pages[start_page - 1].extract_text() or ""
                family = _infer_continuation_family(start_text)
                if not family:
                    continue
                added = 0
                page_num = start_page + 1
                while page_num <= page_count and page_num not in expanded and added < max_extra_pages:
                    page_text = pdf.pages[page_num - 1].extract_text() or ""
                    if not _looks_like_investment_continuation_page(page_text, family):
                        break
                    expanded.add(page_num)
                    added += 1
                    page_num += 1
    except Exception:
        return supplemental_pages
    return sorted(expanded)

def _detect_section_heading(row_data: Dict, fields: List[str]) -> Optional[str]:
    """
    Returns canonical asset type string if this row is a section heading,
    otherwise None.

    A row with a current_value is always a data row, not a section heading.
    Section headings are label-only rows (no dollar amount).
    """
    # If the row has a dollar value it is a data row, not a section heading.
    cv = str(row_data.get('current_value', '')).strip()
    if cv and cv not in ('', 'nan', '-', '**'):
        return None

    for field in ('issuer_name', 'investment_description', 'asset_type', 'par_value'):
        text = str(row_data.get(field, '')).strip()
        if not text or text == 'nan':
            continue
        # Strip trailing colon, then try both the raw text and the de-totalled form
        text_clean = text.rstrip(':').strip()
        text_stripped = _TOTAL_AFFIX_RE.sub('', text_clean).strip()
        for candidate in {text_clean, text_stripped}:
            for pattern, canonical in ASSET_TYPE_PATTERNS:
                if re.search(pattern, candidate, re.IGNORECASE):
                    return canonical

    return None


_VALUE_LIKE_RE = re.compile(r"\$?\s*\(?\s*[0-9][0-9,]*(?:\.[0-9]+)?\)?")


def _detect_section_heading_text(text: str) -> Optional[str]:
    """Return canonical asset type when a text line is a label-only section heading."""
    text_clean = normalize_whitespace(text or "").rstrip(":").strip()
    if not text_clean:
        return None

    # A line with numeric value content is data/subtotal, not a heading.
    if _VALUE_LIKE_RE.search(text_clean):
        return None

    text_stripped = _TOTAL_AFFIX_RE.sub("", text_clean).strip()
    for candidate in {text_clean, text_stripped}:
        for pattern, canonical in ASSET_TYPE_PATTERNS:
            if re.search(pattern, candidate, re.IGNORECASE):
                return canonical
    return None


def _find_section_table_areas(page) -> List[Tuple[str, str]]:
    """Find table areas for pages with multiple asset-type section headings."""
    words = page.extract_words(
        x_tolerance=1,
        y_tolerance=3,
        keep_blank_chars=False,
        use_text_flow=True,
    )
    # Cluster words into visual lines by proximity, not by rounding each word's
    # "top" to the nearest integer pixel independently. Words in the same table
    # row can differ by a fraction of a pixel across columns (font metrics,
    # baseline offsets), and a hard round() can land them on opposite sides of
    # an integer boundary (e.g. 235.45 -> 235 vs 235.69 -> 236), silently
    # splitting one row into two "lines." When that isolates a row's type-label
    # text (e.g. "Common/Collective Trust") away from its own numeric value,
    # the value-guard in _detect_section_heading_text no longer sees the value
    # and misclassifies an ordinary data row as a section heading.
    line_tolerance = 3.0
    sorted_words = sorted(words, key=lambda w: float(w["top"]))
    line_clusters: List[List[Dict]] = []
    for word in sorted_words:
        word_top = float(word["top"])
        if line_clusters and word_top - line_clusters[-1][-1]["_top"] <= line_tolerance:
            line_clusters[-1].append({**word, "_top": word_top})
        else:
            line_clusters.append([{**word, "_top": word_top}])

    headings = []
    header_bottom = None
    for line_words in line_clusters:
        line_words = sorted(line_words, key=lambda w: float(w["x0"]))
        line_text = normalize_whitespace(" ".join(w["text"] for w in line_words))
        if re.search(
            r'(security\s+description|asset\s+id|shares?/par|current\s+value|cost)',
            line_text,
            re.IGNORECASE,
        ):
            header_bottom = max(float(w["bottom"]) for w in line_words)
        asset_type = _detect_section_heading_text(line_text)
        if not asset_type:
            continue
        headings.append({
            "top": min(float(w["top"]) for w in line_words),
            "bottom": max(float(w["bottom"]) for w in line_words),
            "asset_type": asset_type,
            "text": line_text,
        })

    if len(headings) < 2:
        return []

    areas: List[Tuple[str, str]] = []
    page_height = float(page.height)
    page_width = float(page.width)
    for idx, heading in enumerate(headings):
        next_top = headings[idx + 1]["top"] if idx + 1 < len(headings) else page_height - 30
        if next_top <= heading["top"]:
            continue
        # Camelot table_areas use "x1,y1,x2,y2" with origin at bottom-left.
        area_top = heading["top"] - 8
        if idx == 0 and header_bottom is not None and header_bottom < heading["top"]:
            area_top = max(0, header_bottom - 25)
        y_top = page_height - max(0, area_top)
        y_bottom = page_height - min(page_height, next_top - 4)
        area = f"0,{y_top:.2f},{page_width:.2f},{y_bottom:.2f}"
        areas.append((area, heading["asset_type"]))

    return areas


_TOTAL_ONLY_RE = re.compile(
    r'^\s*(?:total|subtotal|sub-total|grand\s+total)\s*$',
    re.IGNORECASE,
)

_TOTAL_CATEGORY_RE = re.compile(
    r'^\s*'
    r'(?:total|subtotal|sub-total|grand\s+total)'
    r'\s+'
    r'(?:'
    r'investments?|assets?|plan\s+assets?|mutual\s+funds?'
    r'|registered\s+investment\s+compan(?:y|ies)'
    r'|collective\s+investment\s+funds?'
    r'|common\s+collective\s+funds?'
    r'|pooled\s+separate\s+accounts?'
    r'|participant\s+loans?'
    r'|common\s+stocks?'
    r'|employer\s+securit(?:y|ies)'
    r'|insurance\s+company\s+general\s+accounts?'
    r'|general\s+accounts?'
    r'|stable\s+value\s+funds?'
    r'|money\s+market\s+funds?'
    r')'
    r'(?:\s*[:\-]?\s*[\d,$().-]+)?\s*$',
    re.IGNORECASE,
)


def _is_total_summary_label(text: str) -> bool:
    text = normalize_whitespace(text or "")
    if not text:
        return False
    return bool(_TOTAL_ONLY_RE.match(text) or _TOTAL_CATEGORY_RE.match(text))


def _is_blank_asset_type(value: str) -> bool:
    return not value or str(value).strip().lower() in ('', 'nan', '-', '*', '**')


def _looks_like_headerless_continuation(df, previous_column_map: Dict[int, str]) -> bool:
    """Return True when a Camelot table likely continues the previous table."""
    if not previous_column_map or 'current_value' not in previous_column_map.values():
        return False
    if df.shape[0] < 2 or df.shape[1] < 3:
        return False

    numeric_like_rows = 0
    rows_to_check = min(df.shape[0], 8)
    money_re = re.compile(r"^\$?\s*\(?\s*[0-9][0-9,]*(?:\.[0-9]+)?\)?$")
    for idx in range(rows_to_check):
        row = [normalize_whitespace(str(c)) for c in df.iloc[idx].tolist()]
        non_empty = [c for c in row if c]
        if len(non_empty) < 3:
            continue
        if any(money_re.match(c) for c in non_empty[1:]):
            numeric_like_rows += 1

    return numeric_like_rows >= 2


def _best_header_match(header: str, synonyms: Dict[str, List[str]]) -> Tuple[str, int]:
    header = header.lower()
    best_field = ""
    best_score = 0
    for field, terms in synonyms.items():
        match, score, _ = process.extractOne(
            header,
            terms,
            scorer=fuzz.partial_ratio,
        ) or ("", 0, None)
        if score > best_score:
            best_field = field
            best_score = score
    return best_field, best_score



# ---------------------------------------------------------------------------
# See-attachment detection and attachment page finder
# ---------------------------------------------------------------------------

_SEE_ATTACHMENT_RE = re.compile(r"see\s+attachment", re.IGNORECASE)
_DETAIL_REFERENCE_RE = re.compile(
    r"see\s+attachment|refer\s+to\s+exhibit\s+[A-Z]\s*-\s*investments"
    r"|exhibit\s+[A-Z]\s*-\s*investments",
    re.IGNORECASE,
)


def _has_see_attachment(page_data: List[Dict]) -> bool:
    """Return True if any extracted row references see attachment."""
    for page in page_data:
        for row in page.get("mapped_rows", []):
            for field in ("issuer_name", "investment_description"):
                val = str(row.get(field, "") or "")
                if _SEE_ATTACHMENT_RE.search(val):
                    return True
    return False


def find_attachment_pages(
    pdf_path: str,
    last_sup_page: int,
    keywords_yml: str,
    max_pages: int = 100,
    max_consecutive_empty: int = 3,
    ignore_negatives: bool = False,
) -> List[int]:
    """Return page numbers of attachment pages following last_sup_page.

    Scans forward looking for pages with investment table content (3+ dollar
    amounts).  Stops on negative keywords or max_consecutive_empty dry pages.
    Set ignore_negatives=True when scanning for see-attachment pages where
    negative keywords should not block the scan.
    """
    import pdfplumber as _plumber

    cfg = load_yaml(keywords_yml)
    negatives = [k.upper() for k in cfg.get("negative_keywords", [])]
    _DOLLAR_RE = re.compile(r"\$[\d,]+|(?<![\d])\d{1,3}(?:,\d{3}){2,}(?![\d])")

    attachment_pages: List[int] = []
    consecutive_empty = 0

    try:
        with _plumber.open(pdf_path) as pdf:
            total = len(pdf.pages)
            start = last_sup_page + 1
            end = min(start + max_pages, total + 1)

            for page_num in range(start, end):
                page = pdf.pages[page_num - 1]
                raw_text = page.extract_text() or ""
                upper_text = raw_text.upper()

                if not ignore_negatives and any(neg in upper_text for neg in negatives):
                    print(f"    [attachment] Stopped at page {page_num} (negative keyword)")
                    break

                dollar_matches = _DOLLAR_RE.findall(raw_text)
                if len(dollar_matches) >= 3:
                    attachment_pages.append(page_num)
                    consecutive_empty = 0
                else:
                    consecutive_empty += 1
                    if consecutive_empty >= max_consecutive_empty:
                        break
    except Exception as exc:
        print(f"    [attachment] Error scanning {pdf_path}: {exc}")

    return attachment_pages

def classify_pages_text(pdf_path: str, keywords_yml: str) -> List[Dict]:
    cfg = load_yaml(keywords_yml)
    keywords = [k.upper() for k in cfg.get("supplemental_schedule_keywords", [])]
    negatives = [k.upper() for k in cfg.get("negative_keywords", [])]
    min_hits = int(cfg.get("min_keyword_hits", 1))
    max_lines = int(cfg.get("header_scan_max_lines", 12))

    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            lines = [normalize_whitespace(l) for l in text.splitlines() if l.strip()]
            header_lines = lines[:max_lines]
            header_text = " ".join(header_lines).upper()
            hits = sum(1 for k in keywords if k in header_text)
            neg_hits = sum(1 for k in negatives if k in header_text)
            pages.append(
                {
                    "pdf": pdf_path,
                    "pdf_stem": pdf_path.split("/")[-1].rsplit(".", 1)[0],
                    "page_number": i,
                    "header_text": header_text,
                    "is_supplemental": 1 if hits >= min_hits and neg_hits == 0 else 0,
                }
            )
    return pages


def extract_ein_from_pdf(pdf_path: str, schedule_h_pages: List[int]) -> Optional[Dict[str, str]]:
    """
    Extract EIN, plan name, and sponsor from PDF.
    
    Priority:
    1. Extract from Schedule H pages (Line 4(i) - most reliable, has actual EIN)
    2. Validate/fallback to Part II (pages 1-3) if needed
    
    Returns dict with: ein, plan_number, plan_name, sponsor
    """
    # Patterns for EIN - handle different dash characters (hyphen -, en-dash ‐, etc.)
    ein_patterns = [
        re.compile(r'Employer\s+Identification\s+Number[:\s]*([0-9]{2}[\-\u2010-\u2015\s]?[0-9]{7})', re.IGNORECASE),
        re.compile(r'EIN[\s#:]*([0-9]{2}[\-\u2010-\u2015\s]?[0-9]{7})', re.IGNORECASE),
        re.compile(r'2b[:\s]+Employer[^0-9]*([0-9]{2}[\-\u2010-\u2015][0-9]{7})', re.IGNORECASE | re.DOTALL),  # Part II field 2b
    ]
    
    # Patterns for plan number
    plan_patterns = [
        re.compile(r'Plan[\s#:]*([0-9]{1,6})', re.IGNORECASE),
        re.compile(r'PN[:\s#]*([0-9]{1,6})', re.IGNORECASE),
        re.compile(r'\(PN\)[\s:]*([0-9]{1,6})', re.IGNORECASE),
    ]
    
    ein_schedule_h = None
    ein_part_ii = None
    plan_number = None
    plan_name_schedule_h = None
    plan_name_part_ii = None
    sponsor = None
    
    with pdfplumber.open(pdf_path) as pdf:
        # STEP 1: Extract from Schedule H pages (PRIORITY - has actual EIN)
        if schedule_h_pages:
            for page_num in schedule_h_pages:
                if page_num < 1 or page_num > len(pdf.pages):
                    continue
                
                page = pdf.pages[page_num - 1]
                text = page.extract_text() or ""
                
                # Only process if this is actually a Schedule H page
                if 'SCHEDULE H' not in text.upper():
                    continue
                
                lines = text.split('\n')
                
                # Extract EIN from Schedule H
                if not ein_schedule_h:
                    for line in lines[:20]:  # Check first 20 lines of Schedule H
                        for pattern in ein_patterns:
                            match = pattern.search(line)
                            if match:
                                ein_raw = match.group(1)
                                # Normalize: remove all dash variants and spaces, then reformat
                                ein_clean = re.sub(r'[\-\u2010-\u2015\s]', '', ein_raw)
                                if len(ein_clean) == 9 and ein_clean.isdigit():
                                    ein_schedule_h = f"{ein_clean[:2]}-{ein_clean[2:]}"
                                    break
                        if ein_schedule_h:
                            break
                
                # Extract plan number from Schedule H
                if not plan_number:
                    for line in lines[:20]:
                        for pattern in plan_patterns:
                            match = pattern.search(line)
                            if match:
                                pn = match.group(1)
                                # Filter out obviously wrong numbers
                                if pn not in ['2024', '2025', '2023'] and len(pn) <= 6:
                                    plan_number = pn.lstrip('0') or '1'
                                    break
                        if plan_number:
                            break
                
                # Extract sponsor and plan name from Schedule H header
                if not sponsor or not plan_name_schedule_h:
                    for i, line in enumerate(lines[:15]):
                        line_stripped = line.strip()
                        # Look for plan name (Schedule H header / helpful text)
                        if not plan_name_schedule_h:
                            if re.search(r'name of plan', line, re.IGNORECASE):
                                plan_name_schedule_h = line_stripped
                            elif re.search(r'plan name', line, re.IGNORECASE) and not re.search(r'\b1[ab]\b', line, re.IGNORECASE):
                                plan_name_schedule_h = line_stripped
                            elif '401' in line or 'savings' in line.lower():
                                if len(line_stripped) > 10 and 'SCHEDULE' not in line_stripped:
                                    plan_name_schedule_h = line_stripped
                        
                        # Look for sponsor (typically says "Plan Sponsor:")
                        if not sponsor and 'PLAN SPONSOR' in line.upper():
                            sponsor_match = re.search(r'Plan\s+Sponsor[:\s]+(.+)', line, re.IGNORECASE)
                            if sponsor_match:
                                sponsor = sponsor_match.group(1).strip()
                
                # If we found EIN on this Schedule H page, we're done
                if ein_schedule_h:
                    break
        
        # STEP 2: Check Part II (pages 1-3) for validation or backup
        for page_num in range(1, min(4, len(pdf.pages) + 1)):
            page = pdf.pages[page_num - 1]
            text = page.extract_text() or ""
            lines = text.split('\n')
            
            # Look for Part II EIN (may be redacted, so we use Schedule H as priority)
            if not ein_part_ii:
                for line in lines:
                    for pattern in ein_patterns:
                        match = pattern.search(line)
                        if match:
                            ein_raw = match.group(1)
                            ein_clean = re.sub(r'[\-\u2010-\u2015\s]', '', ein_raw)
                            if len(ein_clean) == 9 and ein_clean.isdigit():
                                # Check if it's not a placeholder (00-0000000, 12-3456789, etc.)
                                if not re.match(r'^(00-?0000000|12-?3456789|01-?2345678)$', ein_raw.replace(' ', '')):
                                    ein_part_ii = f"{ein_clean[:2]}-{ein_clean[2:]}"
                                    break
                    if ein_part_ii:
                        break
            
            # Extract plan name and sponsor from Part II (preferred for plan_name because it's field 1a)
            for line in lines:
                if not plan_name_part_ii:
                    m = re.search(r'1a[\.\s]*name of plan[:\s]*(.+)', line, re.IGNORECASE)
                    if m:
                        plan_name_part_ii = m.group(1).strip()
                        continue

                    m2 = re.search(r'name of plan[:\s]*(.+)', line, re.IGNORECASE)
                    if m2:
                        plan_name_part_ii = m2.group(1).strip()
                        continue

                if not sponsor:
                    m = re.search(r'1b[\.\s]*name of plan sponsor[:\s]*(.+)', line, re.IGNORECASE)
                    if m:
                        sponsor = m.group(1).strip()
                        continue

                    m2 = re.search(r'plan sponsor[:\s]*(.+)', line, re.IGNORECASE)
                    if m2:
                        sponsor_candidate = m2.group(1).strip()
                        # Avoid capturing lines that are data headings
                        if len(sponsor_candidate) > 3:
                            sponsor = sponsor_candidate
                            continue

            # Extract plan number from Part II if not found in Schedule H
            if not plan_number:
                for line in lines:
                    # Look for plan number near form field identifiers
                    if 'PN' in line or 'plan number' in line.lower():
                        for pattern in plan_patterns:
                            match = pattern.search(line)
                            if match:
                                pn = match.group(1)
                                if pn not in ['2024', '2025', '2023'] and len(pn) <= 6:
                                    plan_number = pn.lstrip('0') or '1'
                                    break
                    if plan_number:
                        break
    
    # Choose the EIN: prefer Schedule H (actual EIN), fallback to Part II if valid
    final_ein = ein_schedule_h or ein_part_ii
    
    # Validate: if both exist and differ, log warning but use Schedule H
    if ein_schedule_h and ein_part_ii and ein_schedule_h != ein_part_ii:
        print(f"    [!] EIN mismatch - Schedule H: {ein_schedule_h}, Part II: {ein_part_ii} (using Schedule H)")
    
    # Prefer Part II plan name (1a), fallback to Schedule H plan name
    final_plan_name = plan_name_part_ii or plan_name_schedule_h

    # Avoid capturing label lines accidentally (e.g. field captions like "1b Three-digit plan")
    if final_plan_name and re.search(r'\b(?:1a|1b|2b|three-digit plan|employer identification|name of plan sponsor|plan sponsor)\b', final_plan_name, re.IGNORECASE):
        final_plan_name = plan_name_part_ii or None

    # Final normalize spacing
    if final_plan_name:
        final_plan_name = re.sub(r'\s+', ' ', final_plan_name).strip()

    if final_ein:
        return {
            'ein': final_ein,
            'plan_number': plan_number or '001',
            'plan_name': final_plan_name,
            'sponsor': sponsor
        }
    return None


def _llm_normalize_headers(client: OpenAI, model: str, headers: List[str], schema_fields: List[str]) -> Dict[int, str]:
    prompt = {
        "headers": headers,
        "schema_fields": schema_fields,
        "instruction": "Map each header to the best matching schema field or null. Return JSON with keys as header index and value as schema field or null.",
    }
    
    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": "You are a data mapping assistant. Return valid JSON only, no extra text."
            },
            {
                "role": "user",
                "content": json.dumps(prompt)
            }
        ],
        temperature=0.3,
    )
    
    text = response.choices[0].message.content
    try:
        data = json.loads(text)
        return {int(k): v for k, v in data.items() if v}
    except Exception:
        return {}


def _extract_gm_column_format(text: str, page_num: int) -> List[Dict]:
    """
    Parse GM/HH1C composite Schedule H Line 4i pages.

    Layout is one-column text, not a true table:
      issuer/fund name line
      units_or_shares cost current_value
      optional fund-code/CUSIP detail line
    """
    lines = [re.sub(r'\s+', ' ', l).strip() for l in text.split('\n')]
    combined_upper = '\n'.join(lines).upper()

    if not (
        'HH1C' in combined_upper
        and 'GENERAL MOTORS' in combined_upper
        and 'SCHEDULE H, LINE 4I - SCHEDULE OF ASSETS' in combined_upper
    ):
        return []

    section_map = {
        'INTEREST BEARING CASH': 'Money Market Fund',
        'COMMON/COLLECTIVE TRUSTS': 'Common/Collective Trust Fund',
        'COMMON/COLLECTIVE TRUST': 'Common/Collective Trust Fund',
        'REGISTERED INVESTMENT COMPANY': 'Mutual Fund',
        'REGISTERED INVESTMENT COMPANIES': 'Mutual Fund',
        'INSURANCE CO. GENERAL ACCOUNT': 'Insurance General Account',
        'INSURANCE COMPANY GENERAL ACCOUNT': 'Insurance General Account',
    }

    value_line_re = re.compile(
        r'^\s*([0-9][0-9,]*\.\d+)\s+([0-9][0-9,]*\.\d+)\s+([0-9][0-9,]*\.\d+)\s*$'
    )
    fund_code_re = re.compile(r'^[A-Z]{2}[A-Z0-9]{2}\s+[A-Z0-9]{8,10}\s')
    dash_re = re.compile(r'^[-=]{3,}')
    noise_re = re.compile(
        r'^(HH1C|COMPOSITE PLAN|SCHEDULE H|\(HELD AT END|THIS IS A COMPOSITE|\(A\)|FUND SHARES|RUN DATE|GRAND TOTALS)',
        re.IGNORECASE,
    )

    def clean_name(raw: str) -> str:
        cleaned = re.sub(r'\s+', ' ', raw or '').strip()
        cleaned = re.sub(r'\bMUTUAL FUND\s+NPV\b', '', cleaned, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r'\bMUTUAL FUND\b$', '', cleaned, flags=re.IGNORECASE).strip()
        return cleaned

    investments = []
    current_asset_type = ''
    row_num = 0
    i = 0

    while i < len(lines):
        line = lines[i]
        if not line or noise_re.search(line) or dash_re.match(line):
            i += 1
            continue

        mapped_section = section_map.get(line.upper().strip())
        if mapped_section:
            current_asset_type = mapped_section
            i += 1
            continue

        # Skip value/detail/summary rows. Detail rows duplicate the issuer-level values.
        if fund_code_re.match(line) or value_line_re.match(line):
            i += 1
            continue

        value_match = None
        value_line_idx = None
        for j in range(i + 1, min(i + 3, len(lines))):
            m = value_line_re.match(lines[j])
            if m:
                value_match = m
                value_line_idx = j
                break

        if not value_match:
            i += 1
            continue

        issuer_name = clean_name(line)
        if not issuer_name or issuer_name.upper() in section_map:
            i += 1
            continue

        units_or_shares, cost, current_value = value_match.groups()
        row_num += 1
        investments.append({
            'issuer_name': issuer_name,
            'investment_description': '',
            'asset_type': current_asset_type,
            'par_value': '',
            'cost': cost.replace(',', ''),
            'current_value': current_value.replace(',', ''),
            'units_or_shares': units_or_shares.replace(',', ''),
            'page_number': page_num,
            'row_id': row_num,
        })

        i = value_line_idx + 1
        if i < len(lines) and fund_code_re.match(lines[i]):
            i += 1

    return investments


def _extract_gm_column_format_for_pdf(pdf_path: str) -> List[Dict]:
    """Parse all GM/HH1C 4i pages so asset section context carries across pages."""
    all_rows: List[Dict] = []
    current_asset_type = ''

    section_map = {
        'INTEREST BEARING CASH': 'Money Market Fund',
        'COMMON/COLLECTIVE TRUSTS': 'Common/Collective Trust Fund',
        'COMMON/COLLECTIVE TRUST': 'Common/Collective Trust Fund',
        'REGISTERED INVESTMENT COMPANY': 'Mutual Fund',
        'REGISTERED INVESTMENT COMPANIES': 'Mutual Fund',
        'INSURANCE CO. GENERAL ACCOUNT': 'Insurance General Account',
        'INSURANCE COMPANY GENERAL ACCOUNT': 'Insurance General Account',
    }
    value_line_re = re.compile(
        r'^\s*([0-9][0-9,]*\.\d+)\s+([0-9][0-9,]*\.\d+)\s+([0-9][0-9,]*\.\d+)\s*$'
    )
    fund_code_re = re.compile(r'^[A-Z]{2}[A-Z0-9]{2}\s+[A-Z0-9]{8,10}\s')
    dash_re = re.compile(r'^[-=]{3,}')
    noise_re = re.compile(
        r'^(HH1C|COMPOSITE PLAN|SCHEDULE H|\(HELD AT END|THIS IS A COMPOSITE|\(A\)|FUND SHARES|RUN DATE|GRAND TOTALS)',
        re.IGNORECASE,
    )

    def clean_name(raw: str) -> str:
        cleaned = re.sub(r'\s+', ' ', raw or '').strip()
        cleaned = re.sub(r'\bMUTUAL FUND\s+NPV\b', '', cleaned, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r'\bMUTUAL FUND\b$', '', cleaned, flags=re.IGNORECASE).strip()
        return cleaned

    with pdfplumber.open(pdf_path) as pdf:
        for page_idx, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ''
            upper = text.upper()
            if not (
                'HH1C' in upper
                and 'GENERAL MOTORS' in upper
                and 'SCHEDULE H, LINE 4I - SCHEDULE OF ASSETS' in upper
            ):
                continue

            lines = [re.sub(r'\s+', ' ', l).strip() for l in text.split('\n')]
            row_num = 0
            i = 0
            while i < len(lines):
                line = lines[i]
                if not line or noise_re.search(line) or dash_re.match(line):
                    i += 1
                    continue

                mapped_section = section_map.get(line.upper().strip())
                if mapped_section:
                    current_asset_type = mapped_section
                    i += 1
                    continue

                if fund_code_re.match(line) or value_line_re.match(line):
                    i += 1
                    continue

                value_match = None
                value_line_idx = None
                for j in range(i + 1, min(i + 3, len(lines))):
                    m = value_line_re.match(lines[j])
                    if m:
                        value_match = m
                        value_line_idx = j
                        break

                if not value_match:
                    i += 1
                    continue

                issuer_name = clean_name(line)
                if not issuer_name or issuer_name.upper() in section_map:
                    i += 1
                    continue

                units_or_shares, cost, current_value = value_match.groups()
                row_num += 1
                all_rows.append({
                    'issuer_name': issuer_name,
                    'investment_description': '',
                    'asset_type': current_asset_type,
                    'par_value': '',
                    'cost': cost.replace(',', ''),
                    'current_value': current_value.replace(',', ''),
                    'units_or_shares': units_or_shares.replace(',', ''),
                    'page_number': page_idx,
                    'row_id': row_num,
                })

                i = value_line_idx + 1
                if i < len(lines) and fund_code_re.match(lines[i]):
                    i += 1

    return all_rows






def _strip_trailing_asset_label(text: str, asset_type_patterns: Dict[str, str]) -> Tuple[str, str]:
    """Strip a generic asset label from the right side without damaging fund names."""
    cleaned = normalize_whitespace(str(text or '')).strip()
    best = None
    upper = cleaned.upper().rstrip(' *:;.,')
    for pattern, asset_name in asset_type_patterns.items():
        match = re.search(r'(?:^|\s)' + pattern + r'\s*\**\s*$', upper, flags=re.IGNORECASE)
        if match:
            if best is None or match.start() > best[0].start():
                best = (match, asset_name)
    if not best:
        return cleaned, ''
    match, asset_name = best
    return cleaned[:match.start()].rstrip(' *:;.,'), asset_name

def _is_category_only_investment_label(desc: str, asset_type: str) -> bool:
    """Return True when Camelot captured only a generic investment category, not a name."""
    labels = {
        'mutual fund',
        'money market fund',
        'variable annuity contract',
        'guaranteed insurance contract',
        'guaranteed investment contract',
        'pooled separate account',
        'commingled fund',
        'self-directed account',
        'self directed account',
        'self-directed brokerage account',
        'common collective trust fund',
        'common/collective trust fund',
    }
    desc_text = normalize_whitespace(str(desc or '')).lower().strip(' *:;.,')
    if desc_text:
        return desc_text in labels

    asset_text = normalize_whitespace(str(asset_type or '')).lower().strip(' *:;.,')
    return asset_text in labels

def extract_text_based_investments(pdf_path: str, page_num: int, parser_profile: str = "", inherited_asset_type: str = "") -> List[Dict]:
    """
    Extract investment data from text-based format (non-table).
    Used as fallback when camelot can't detect tables.
    
    Expected format:
    ISSUER NAME    DESCRIPTION    TYPE    ** $VALUE
    """
    investments = []
    
    with pdfplumber.open(pdf_path) as pdf:
        if page_num < 1 or page_num > len(pdf.pages):
            return investments
        
        page = pdf.pages[page_num - 1]
        text = page.extract_text() or ""
        
        # Check if this is a Schedule H Line 4(i) investment page or a
        # headerless continuation of one.
        has_schedule_marker = bool(re.search(
            r'Schedule\s+[HI][,.]?\s+Line\s+4\s*\(?\s*[ij]\s*\)?'
            r'|LINE\s+4\s*\(?\s*[IJ]\s*\)?'
            r'|SCHEDULE\s+OF\s+(ASSETS|INVESTMENTS)'
            r'|ASSETS\s+HELD\s+(FOR\s+INVESTMENT|AT\s+END)',
            text, re.IGNORECASE
        ))
        profile_family = _profile_family(parser_profile)
        has_inherited_continuation_profile = bool(profile_family) and _looks_like_investment_continuation_page(
            text, profile_family
        )
        if not has_schedule_marker and not has_inherited_continuation_profile:
            return investments
        
        lines = text.split('\n')
        
        if (
            'HH1C' in text.upper()
            and 'GENERAL MOTORS' in text.upper()
            and 'SCHEDULE H, LINE 4I - SCHEDULE OF ASSETS' in text.upper()
        ):
            gm_rows = [
                row for row in _extract_gm_column_format_for_pdf(pdf_path)
                if row.get('page_number') == page_num
            ]
            if gm_rows:
                return gm_rows

        # Find where investment data starts (after headers). Prefer the complete
        # Schedule H table header when present so narrative/certification text above
        # the table is not glued onto the first investment row.
        data_start_idx = 0
        for i, line in enumerate(lines):
            upper_line = line.upper()
            if (
                'IDENTITY OF ISSUE' in upper_line
                and 'CLASSIFICATION' in upper_line
                and 'CURRENT VALUE' in upper_line
            ):
                data_start_idx = i + 1
                break
            if ('CURRENT VALUE' in upper_line or 'MATURITY VALUE' in upper_line
                    or 'DESCRIPTION OF INVESTMENT' in upper_line
                    or 'IDENTITY OF ISSUE' in upper_line):
                data_start_idx = i + 1
                break

        page_scale_factor = _page_value_scale_factor(text)

        # Two value patterns:
        # 1. "** $VALUE" or "** VALUE"  (classic Form 5500 format)
        # 2. "Fund Name $ 225,122,092" or "Fund Name 225,122,092" (simple two-column format)
        # 3. "Fund Name $ 698" — explicit $ with small value (no minimum digit count)
        star_value_pattern   = re.compile(r'\*\*\s*\$?\s*([\d,]+)')
        dollar_value_pattern = re.compile(r'\$\s*([\d,]+)\s*$')          # explicit $
        simple_value_pattern = re.compile(r'([\d,]{4,})\s*$')             # no $, 4+ chars

        # Section heading detection for simple two-column format
        # Keys are matched both exactly and as substrings of the line
        SECTION_HEADING_MAP = {
            'mutual fund': 'Mutual Fund',
            'registered investment compan': 'Mutual Fund',
            'registered investment fund': 'Mutual Fund',
            'variable annuity': 'Variable Annuity Contract',
            'money market fund': 'Money Market Fund',
            'money market funds': 'Money Market Fund',
            'interest-bearing cash': 'Money Market Fund',
            'interest bearing cash': 'Money Market Fund',
            'mmrk': 'Money Market Fund',
            'common stock': 'Employer Stock',
            'commingled fund': 'Commingled Fund',
            'collective fund': 'Commingled Fund',
            'commingled and other funds': 'Commingled Fund',
            'pooled separate account': 'Separate Account',
            'pooled separate investment account': 'Separate Account',
            'separate accounts': 'Separate Account',
            'separate account': 'Separate Account',
            'self-directed brokerage': 'Self-Directed Brokerage Account',
            'self directed brokerage': 'Self-Directed Brokerage Account',
            'guaranteed investment contract': 'Guaranteed Investment Contract',
            'common/collective trust': 'Common/Collective Trust Fund',
            'collective/common trust': 'Common/Collective Trust Fund',
            'common collective trust': 'Common/Collective Trust Fund',
            'collective investment trust': 'Common/Collective Trust Fund',
            'collective investment fund': 'Common/Collective Trust Fund',
            'collective trust': 'Common/Collective Trust Fund',
            'self-managed fund': 'Separately Managed Account',
            'self managed fund': 'Separately Managed Account',
            'managed custom fund': 'Separately Managed Account',
            'insurance company general account': 'Insurance General Account',
            'general account contract': 'Insurance General Account',
            'stable value': 'Stable Value Fund',
            # additional non-MF categories (specific keys only -- substring-matched on
            # value-less heading lines, so no bare 'bond'/'real estate'/'government').
            'joint venture': 'Joint Venture',
            'real property': 'Real Estate',
            'real estate investment trust': 'Real Estate',
            'hedge fund': 'Hedge Fund',
            'corporate debt': 'Bond',
            'corporate bond': 'Bond',
            'government bond': 'Bond',
            'municipal bond': 'Bond',
            'u.s. government securities': 'Bond',
            'government obligations': 'Bond',
            'debenture': 'Bond',
            '103-12': '103-12 Investment Entity',
            'derivative': 'Derivative',
        }
        current_section_type = inherited_asset_type or ''

        _footnote_re = re.compile(r'(?:\s*(?:\([A-Za-z0-9]{1,3}\)|\*+)\s*,?)+\s*$')
        # Rejoin a space-split leading number group into the value (e.g. "6 1,962,451" ->
        # "61,962,451"). The lookbehind excludes a LETTER (so "R6 2,072,867" is left alone)
        # and a HYPHEN (so a hyphenated share class "R-6 1,962,451" is NOT merged -- the "6"
        # is a share class, not a millions digit; without this it leaked into the value and
        # truncated the name to "...R-").
        _split_value_re = re.compile(r'(?<![A-Za-z-])(\d{1,3})\s+([\d,]*\d)')
        def _rejoin_split_number(_text):
            def _repl(m):
                joined = m.group(1) + m.group(2)
                return joined if re.fullmatch(r'\d{1,3}(?:,\d{3})+', joined) else m.group(0)
            return _split_value_re.sub(_repl, _text)

        row_num = 0
        for i in range(data_start_idx, len(lines)):
            line = lines[i].strip()

            if not line or len(line) < 5:
                continue
            if any(skip in line.upper() for skip in ['PAGE', 'EIN #', 'PLAN #', 'DECEMBER', 'CONTINUED']):
                continue

            # Check value FIRST — a line with a dollar amount is always a data row,
            # even if it contains an asset-type keyword like "Registered Investment
            # Company".  Section heading detection only fires when no value is found.
            line = _footnote_re.sub('', _rejoin_split_number(line)).rstrip()
            value_match = star_value_pattern.search(line)
            if value_match:
                current_value = value_match.group(1).replace(',', '')
                issuer_description = line[:value_match.start()].strip()
            else:
                # Try explicit "$ VALUE" pattern (any digit count)
                value_match = dollar_value_pattern.search(line)
                if value_match:
                    current_value = value_match.group(1).replace(',', '')
                    issuer_description = line[:value_match.start()].strip().rstrip('$').strip()
                else:
                    # Try plain trailing number (4+ chars to avoid false positives)
                    value_match = simple_value_pattern.search(line)
                    if not value_match:
                        # No value on this line — check if it is a section heading
                        line_lower_full = line.lower().strip()
                        for key, val in SECTION_HEADING_MAP.items():
                            if key in line_lower_full:
                                current_section_type = val
                                break
                        continue
                    current_value = value_match.group(1).replace(',', '')
                    issuer_description = line[:value_match.start()].strip()

            if page_scale_factor != 1:
                current_value = _scale_currency_string(current_value, page_scale_factor)

            # Skip actual total/summary labels, while preserving fund names such as
            # "PIMCO Total Return" or "Vanguard Total Bond Market". Also skip note
            # continuations that can look like value rows after a notes-receivable block.
            issuer_lower = issuer_description.lower()
            if issuer_lower.strip().startswith((
                'ranging from',
                'maturity dates ranging from',
                'total investments and notes receivable',
            )):
                continue
            if _is_total_summary_label(issuer_description):
                continue

            # Skip section-label subtotal rows: lines whose pre-value text is
            # purely a category heading with no fund name before or after it.
            # e.g. "Registered Investment Companies $5,404,934,804" is a section
            # total, not an individual investment.
            _is_section_label = False
            for _key, _val in SECTION_HEADING_MAP.items():
                if _key in issuer_lower:
                    _idx = issuer_lower.index(_key)
                    _before = issuer_lower[:_idx].strip(' *')
                    _after  = issuer_lower[_idx + len(_key):].strip(' *:.,s')
                    # allow up to 1 char before and 3 after (handles plural endings like 'ie' from 'ies')
                    if len(_before) <= 1 and len(_after) <= 3:
                        current_section_type = _val
                        _is_section_label = True
                        break
            if _is_section_label:
                continue

            # Determine asset type from section heading or embedded keyword
            asset_type = current_section_type
            asset_type_patterns = {
                'COMMON/COLLECTIVE TRUST FUND': 'Common/Collective Trust Fund',
                'COMMON COLLECTIVE TRUST FUND': 'Common/Collective Trust Fund',
                'COLLECTIVE INVESTMENT TRUST': 'Common/Collective Trust Fund',
                'COLLECTIVE INVESTMENT FUND': 'Common/Collective Trust Fund',
                'COLLECTIVE TRUST FUND': 'Common/Collective Trust Fund',
                'COMMON/COLLECTIVE TRUST': 'Common/Collective Trust Fund',
                'COMMON COLLECTIVE TRUST': 'Common/Collective Trust Fund',
                'COLLECTIVE TRUST': 'Common/Collective Trust Fund',
                'SEPARATELY MANAGED ACCOUNT': 'Separately Managed Account',
                'SELF DIRECTED BROKERAGE': 'Self-Directed Brokerage Account',
                'REGISTERED INVESTMENT COMPANY': 'Mutual Fund',
                'REGISTERED INVESTMENT FUND': 'Mutual Fund',
                'MUTUAL FUND': 'Mutual Fund',
                'MONEY MARKET FUND': 'Money Market Fund',
                'VARIABLE ANNUITY CONTRACT': 'Variable Annuity Contract',
                'COMMON STOCK': 'Employer Stock',
                'GUARANTEED INSURANCE CONTRACT': 'Guaranteed Insurance Contract',
                'POOLED SEPARATE INVESTMENT ACCOUNT': 'Separate Account',
                'POOLED SEPARATE ACCOUNT': 'Separate Account',
                'SEPARATE ACCOUNTS': 'Separate Account',
                'SEPARATE ACCOUNT': 'Separate Account',
                'COLLECTIVE FUND': 'Commingled Fund',
                'SELF-DIRECTED ACCOUNT': 'Self-Directed Brokerage Account',
                'SELF DIRECTED ACCOUNT': 'Self-Directed Brokerage Account',
                'GUARANTEED INTEREST ACCOUNT': 'Stable Value Fund',
                'GUARANTEED INCOME ACCOUNT': 'Stable Value Fund',
                'GUARANTEED INVESTMENT CONTRACT': 'Stable Value Fund',
                'INTEREST-BEARING CASH': 'Money Market Fund',
                'MONEY MARKET': 'Money Market Fund',
                'MMRK': 'Money Market Fund',
                # additional non-MF categories -- SAFE trailing labels only (anchored to end of
                # the description). Bond/corporate keys are deliberately OMITTED here: a bond
                # MUTUAL FUND description can end in 'Corporate Bond', which must stay MF.
                'JOINT VENTURE': 'Joint Venture',
                'REAL PROPERTY': 'Real Estate',
                'HEDGE FUND': 'Hedge Fund',
                'DEBENTURE': 'Bond',
                '103-12 INVESTMENT ENTITY': '103-12 Investment Entity',
                'DERIVATIVE': 'Derivative',
            }
            # A row's OWN explicit trailing type label (e.g. "... Collective Trust
            # Fund") wins over a propagated section/inherited type — the section
            # heading is only a fallback for rows that carry no type of their own.
            stripped_description, trailing_asset_type = _strip_trailing_asset_label(
                issuer_description, asset_type_patterns
            )
            if trailing_asset_type:
                asset_type = trailing_asset_type
                issuer_description = stripped_description

            if not asset_type and re.search(r'[\d,]+\s+(?:units|shares)\b', issuer_description, re.IGNORECASE):
                _ud = issuer_description.upper()
                for _k, _v in asset_type_patterns.items():
                    _pos = _ud.find(_k)
                    if _pos != -1:
                        asset_type = _v
                        issuer_description = issuer_description[:_pos].strip().rstrip(',').strip()
                        break

            issuer_name = issuer_description.lstrip('*').rstrip('*').strip()
            if not issuer_name:
                continue

            row_num += 1
            investments.append({
                'issuer_name': issuer_name,
                'investment_description': '',
                'asset_type': asset_type,
                'par_value': '',
                'cost': '',
                'current_value': current_value,
                'units_or_shares': '',
                'page_number': page_num,
                'row_id': row_num,
            })

        # Fallback: GM composite plan format (fund-code column A, values on next line)
        if not investments:
            investments = _extract_gm_column_format(text, page_num)

    return investments






def _looks_like_structural_investment_schedule(text: str) -> bool:
    """Detect asset schedules that have table structure but no explicit Schedule H/4i title."""
    text = text or ''
    header_text = " ".join(text.splitlines()[:15]).lower()
    has_identity = bool(re.search(r'identity\s+of\s+issue|borrower,?\s+lessor', header_text))
    has_description = bool(re.search(r'description\s+of\s+investments?', header_text))
    has_current_value = 'current' in header_text and 'value' in header_text
    if not (has_identity and has_description and has_current_value):
        return False
    if re.search(
        r'SCHEDULE\s+C\s+SUPPLEMENTAL\s+REPORT|INFORMATION\s+ON\s+SERVICE\s+PROVIDERS|'
        r'INDIRECT\s+COMPENSATION|REPORTABLE\s+TRANSACTIONS',
        text,
        re.IGNORECASE,
    ):
        return False
    if not re.search(
        r'mutual\s+funds?|common/?collective\s+trusts?|collective\s+investment\s+funds?|'
        r'guaranteed\s+investment\s+contracts?|self[- ]directed\s+brokerage\s+accounts?|'
        r'pooled\s+separate\s+accounts?',
        text,
        re.IGNORECASE,
    ):
        return False
    return len(re.findall(r'\$?\s*\d{1,3}(?:,\d{3})+(?:\.\d+)?', text)) >= 3



def _looks_like_structural_investment_continuation(text: str) -> bool:
    text = text or ''
    if re.search(
        r'SCHEDULE\s+C\s+SUPPLEMENTAL\s+REPORT|INFORMATION\s+ON\s+SERVICE\s+PROVIDERS|'
        r'INDIRECT\s+COMPENSATION|REPORTABLE\s+TRANSACTIONS|PARTY-IN-INTEREST\s+AS\s+DEFINED',
        text,
        re.IGNORECASE,
    ):
        return False
    issuer_like = len(re.findall(r'Fidelity\s+Management\s+Trust\s+Company|\*\s+Fidelity\s+Management\s+Trust\s+Company', text, re.IGNORECASE))
    values = len(re.findall(r'\$?\s*\d{1,3}(?:,\d{3})+(?:\.\d+)?', text))
    return issuer_like >= 3 and values >= 3



def _infer_first_section_asset_type(text: str) -> str:
    """Infer the first asset section heading on a structural investment schedule page."""
    section_map = [
        (r'\bmutual\s+funds?\b', 'Mutual Fund'),
        (r'\bcommon/?collective\s+trusts?\b|\bcollective\s+investment\s+funds?\b', 'Common/Collective Trust Fund'),
        (r'\bguaranteed\s+investment\s+contracts?\b', 'Guaranteed Investment Contract'),
        (r'\bself[- ]directed\s+brokerage\s+accounts?\b', 'Self-Directed Brokerage Account'),
        (r'\bpooled\s+separate\s+(?:investment\s+)?accounts?\b', 'Separate Account'),
        (r'\bcollective\s+funds?\b', 'Commingled Fund'),
        (r'\bmoney\s+market\s+funds?\b|\bmmrk\b', 'Money Market Fund'),
        (r'\bvariable\s+annuit(?:y|ies)\b', 'Variable Annuity Contract'),
        (r'\bcommon\s+stocks?\b', 'Employer Stock'),
    ]
    for line in (text or '').splitlines()[:40]:
        clean = normalize_whitespace(line)
        for pattern, asset_type in section_map:
            if re.search(pattern, clean, re.IGNORECASE):
                return asset_type
    return ''

def _infer_structural_row_profile(text: str) -> Dict[str, str]:
    """Infer a repeated issuer-prefix row shape from a structural investment page."""
    candidates = []
    value_at_end = re.compile(r'\$?\s*\d{1,3}(?:,\d{3})+(?:\.\d+)?\s*$')
    for line in (text or '').splitlines():
        clean = normalize_whitespace(line).lstrip('* ').strip()
        if not value_at_end.search(clean):
            continue
        words = re.findall(r'[A-Za-z][A-Za-z&.-]*', clean)
        if len(words) < 4:
            continue
        # Try prefixes from specific to broad; keep only prefixes repeated enough later.
        for n in (5, 4, 3):
            if len(words) >= n:
                candidates.append(' '.join(words[:n]))
    if not candidates:
        return {}
    counts = {}
    for prefix in candidates:
        counts[prefix] = counts.get(prefix, 0) + 1
    prefix, count = max(counts.items(), key=lambda kv: (kv[1], len(kv[0])))
    if count < 5:
        return {}
    return {'issuer_prefix': prefix}


def _matches_structural_row_profile(text: str, profile: Dict[str, str], min_rows: int = 5) -> bool:
    """Return True when a page continues the repeated row shape inferred from a base page."""
    prefix = (profile or {}).get('issuer_prefix', '')
    if not prefix:
        return False
    if re.search(
        r'SCHEDULE\s+C\s+SUPPLEMENTAL\s+REPORT|INFORMATION\s+ON\s+SERVICE\s+PROVIDERS|'
        r'INDIRECT\s+COMPENSATION|REPORTABLE\s+TRANSACTIONS',
        text or '',
        re.IGNORECASE,
    ):
        return False
    prefix_re = re.compile(r'^\*?\s*' + re.escape(prefix), re.IGNORECASE)
    value_at_end = re.compile(r'\$?\s*\d{1,3}(?:,\d{3})+(?:\.\d+)?\s*$')
    matches = 0
    for line in (text or '').splitlines()[:120]:
        clean = normalize_whitespace(line).strip()
        if prefix_re.search(clean) and value_at_end.search(clean):
            matches += 1
    return matches >= min_rows

def find_structural_investment_pages(pdf_path: str, max_pages: int = 1000) -> List[int]:
    """Fallback page finder for asset schedules that lack explicit Schedule H/4i keywords."""
    pages: List[int] = []

    section_re = re.compile(
        r'mutual\s+funds?|common/?collective\s+trusts?|collective\s+investment\s+funds?|'
        r'guaranteed\s+investment\s+contracts?|self[- ]directed\s+brokerage\s+accounts?|'
        r'pooled\s+separate\s+accounts?',
        re.IGNORECASE,
    )
    value_re = re.compile(r'\$?\s*\d{1,3}(?:,\d{3})+(?:\.\d+)?')
    negative_re = re.compile(
        r'SCHEDULE\s+C\s+SUPPLEMENTAL\s+REPORT|INFORMATION\s+ON\s+SERVICE\s+PROVIDERS|'
        r'INDIRECT\s+COMPENSATION|REPORTABLE\s+TRANSACTIONS',
        re.IGNORECASE,
    )
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for idx, page in enumerate(pdf.pages[:max_pages], start=1):
                text = page.extract_text() or ''
                if _looks_like_structural_investment_schedule(text):
                    pages.append(idx)
                    structural_profile = _infer_structural_row_profile(text)
                    next_idx = idx + 1
                    while next_idx <= min(len(pdf.pages), max_pages):
                        next_text = pdf.pages[next_idx - 1].extract_text() or ''
                        if not (
                            _looks_like_structural_investment_continuation(next_text)
                            or _matches_structural_row_profile(next_text, structural_profile)
                        ):
                            break
                        pages.append(next_idx)
                        next_idx += 1
    except Exception as exc:
        print(f"    [fallback] Error scanning structural investment pages: {exc}")
    return pages


def _looks_like_simple_investment_schedule(text: str) -> bool:
    """Last-resort, looser detector for simplified 2-column Schedule-of-Assets formats
    (e.g. a bare "INVESTMENT" / "CURRENT VALUE" header) that lack the "identity of issue" /
    "description of investments" column headers required by
    _looks_like_structural_investment_schedule. Only ever called from
    find_simple_investment_pages, which the pipeline invokes as a third-tier fallback after
    both the keyword classifier and the structural fallback have already found nothing for a
    given PDF -- so the looser match here only ever fires on documents already confirmed to be
    total extraction failures, not on the general population.
    """
    text = text or ''
    header_text = " ".join(text.splitlines()[:15]).lower()
    has_schedule_title = bool(re.search(r'schedule\s+of\s+assets|statement\s+of\s+assets', header_text))
    has_current_value = 'current' in header_text and 'value' in header_text
    if not (has_schedule_title or has_current_value):
        return False
    # Broad negative keywords only rule out the page by its own header/title -- checking them
    # against the full page text caused false negatives on legitimate schedule pages that
    # happen to also mention e.g. "independent auditor" elsewhere in dense page text.
    if re.search(
        r'SIGNATURE|INDEPENDENT\s+AUDITOR|ACCOUNTANT|SUMMARY|INCOME\s+STATEMENT|BALANCE\s+SHEET|'
        r'NET\s+ASSETS\s+AVAILABLE\s+FOR\s+BENEFITS|STATEMENT\s+OF\s+CHANGES',
        header_text,
        re.IGNORECASE,
    ):
        return False
    # These are specific enough multi-word phrases that a match anywhere on the page reliably
    # signals a different schedule type, so they're still checked against the full text.
    if re.search(
        r'SCHEDULE\s+C\s+SUPPLEMENTAL\s+REPORT|INFORMATION\s+ON\s+SERVICE\s+PROVIDERS|'
        r'INDIRECT\s+COMPENSATION|REPORTABLE\s+TRANSACTIONS',
        text,
        re.IGNORECASE,
    ):
        return False
    return len(re.findall(r'\$?\s*\d{1,3}(?:,\d{3})+(?:\.\d+)?', text)) >= 3


def find_simple_investment_pages(pdf_path: str, max_pages: int = 1000) -> List[int]:
    """Third-tier fallback page finder, only ever invoked when a PDF has already produced zero
    usable rows through both the keyword classifier and find_structural_investment_pages. Catches
    simplified Schedule-of-Assets formats (bare "INVESTMENT" / "CURRENT VALUE" columns) that the
    stricter tiers miss.
    """
    pages: List[int] = []
    consumed = set()
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for idx, page in enumerate(pdf.pages[:max_pages], start=1):
                if idx in consumed:
                    continue
                text = page.extract_text() or ''
                if _looks_like_simple_investment_schedule(text):
                    pages.append(idx)
                    consumed.add(idx)
                    profile = _infer_structural_row_profile(text)
                    next_idx = idx + 1
                    while next_idx <= min(len(pdf.pages), max_pages):
                        next_text = pdf.pages[next_idx - 1].extract_text() or ''
                        if not (
                            _looks_like_simple_investment_schedule(next_text)
                            or _matches_structural_row_profile(next_text, profile)
                        ):
                            break
                        pages.append(next_idx)
                        consumed.add(next_idx)
                        next_idx += 1
    except Exception as exc:
        print(f"    [fallback] Error scanning simple investment pages: {exc}")
    return pages


_SCHEDULE_OF_TITLE_TYPE_MAP = [
    (r'REGISTERED\s+INVESTMENT\s+COMPAN', 'Mutual Fund'),
    (r'MUTUAL\s+FUND', 'Mutual Fund'),
    (r'MONEY\s+MARKET', 'Money Market Fund'),
    (r'COMMON\s*/?\s*COLLECTIVE\s+TRUST|COLLECTIVE\s+INVESTMENT', 'Common/Collective Trust Fund'),
    (r'POOLED\s+SEPARATE\s+ACCOUNT', 'Commingled Fund'),
    (r'SELF[\s-]*DIRECTED\s+BROKERAGE', 'Self-Directed Brokerage Account'),
    (r'GUARANTEED\s+INVESTMENT\s+CONTRACT|GUARANTEED\s+INSURANCE', 'Guaranteed Investment Contract'),
    (r'U\.?\s*S\.?\s+GOVERNMENT|GOVERNMENT\s+(SECURITIES|OBLIGATIONS|AGENC)', 'Government Securities'),
    (r'MUNICIPAL', 'Government Securities'),
    (r'CORPORATE\s+DEBT|CORPORATE\s+BOND|DEBT\s+INSTRUMENT', 'Corporate Debt'),
    (r'PREFERRED\s+STOCK', 'Preferred Stock'),
    (r'CORPORATE\s+STOCK|COMMON\s+STOCK', 'Common Stock'),
    (r'INTEREST[\s-]*BEARING\s+CASH|CASH\s+EQUIVALENT', 'Cash'),
    (r'PARTICIPANT\s+LOAN', 'Participant Loan'),
    (r'REAL\s+ESTATE', 'Real Estate'),
    (r'PARTNERSHIP', 'Partnership Interest'),
    (r'EMPLOYER\s+(STOCK|SECURITIES)', 'Employer Stock'),
]


def _infer_schedule_of_title_asset_type(text: str) -> str:
    """Map an audited sub-schedule page title ('SCHEDULE OF <asset type>') to a
    canonical asset_type. 5500 audited 4i schedules split holdings into per-asset-type
    sub-schedules (e.g. 'SCHEDULE OF U.S. GOVERNMENT SECURITIES', 'SCHEDULE OF CORPORATE
    STOCK - COMMON', 'SCHEDULE OF REGISTERED INVESTMENT COMPANIES'). Tagging rows with
    the sub-schedule type lets the MF load gate keep only mutual-fund (RIC) sub-schedules
    and drop bond/stock/cash sub-schedules that otherwise leak through as blank-type
    numeric-name junk. Uses a dedicated title map (NOT row-level ASSET_TYPE_PATTERNS) so
    fund NAMES are never mis-tagged.
    """
    for line in (text or "").splitlines()[:8]:
        u = normalize_whitespace(line).upper()
        m = re.match(r'SCHEDULE\s+OF\s+(.+)$', u)
        if not m:
            continue
        title = m.group(1)
        if 'ASSETS HELD' in title or 'INVESTMENT PURPOSES' in title:
            continue
        for pattern, atype in _SCHEDULE_OF_TITLE_TYPE_MAP:
            if re.search(pattern, title):
                return atype
        return ''
    return ''


def extract_tables_and_map(
    pdf_path: str,
    supplemental_pages: List[int],
    schema_yml: str,
    model: str,
    use_llm: bool = True,
) -> Tuple[Optional[Dict[str, str]], List[Dict]]:
    cfg = load_yaml(schema_yml)
    fields = cfg["schema"]["fields"]
    synonyms = cfg["schema"]["header_synonyms"]

    if not supplemental_pages:
        return None, []
    
    # EIN extraction disabled — not required
    # plan_info = extract_ein_from_pdf(pdf_path, supplemental_pages)
    plan_info = None

    # The HH1C composite Schedule 4i layout is text-structured, not table-structured.
    # Route it before Camelot, otherwise pseudo-tables can suppress the GM parser fallback.
    if _pdf_has_gm_hh1c_schedule_4i_layout(pdf_path, supplemental_pages):
        gm_rows = _extract_gm_column_format_for_pdf(pdf_path)
        if gm_rows:
            print(f"    GM/HH1C Schedule 4i parser extracted {len(gm_rows)} investments")
            return plan_info, _build_text_result(pdf_path, gm_rows)

    # Pre-filter: prefer the actual Schedule H/I Line 4i/4j asset table.
    # Generic mentions like "Schedule I" or "Schedule of Assets" can appear in audit
    # notes and fair-value summaries, so those pages need real table-header evidence.
    _line_4ij_re = re.compile(
        r'Schedule\s+[HI][,.]?\s+Line\s+4\s*\(?\s*[ij]\s*\)?'
        r'|LINE\s+4\s*\(?\s*[IJ]\s*\)?',
        re.IGNORECASE
    )
    _asset_table_header_re = re.compile(
        r'IDENTITY\s+OF\s+ISSUE|BORROWER,?\s+LESSOR|CURRENT\s+VALUE|COST\s+VALUE',
        re.IGNORECASE
    )
    _generic_schedule_re = re.compile(
        r'SCHEDULE\s+OF\s+(ASSETS|INVESTMENTS)'
        r'|ASSETS\s+HELD\s+(FOR\s+INVESTMENT|AT\s+END)',
        re.IGNORECASE
    )
    _note_summary_re = re.compile(
        r'CERTIFIED\s+INVESTMENT\s+INFORMATION|FAIR\s+VALUE\s+OF\s+FINANCIAL\s+INSTRUMENTS'
        r'|FAIR\s+VALUE\s+HIERARCHY|LEVEL\s+1\s+LEVEL\s+2\s+LEVEL\s+3\s+TOTAL',
        re.IGNORECASE
    )
    page_value_scale: Dict[int, int] = {}
    continuation_parser_profiles: Dict[int, str] = {}
    section_table_areas_by_page: Dict[int, List[Tuple[str, str]]] = {}
    with pdfplumber.open(pdf_path) as _doc:
        filtered_pages = []
        active_parser_profile = ""
        active_continuation_family = ""
        active_structural_profile: Dict[str, str] = {}
        active_structural_asset_type = ""
        continuation_asset_types: Dict[int, str] = {}
        for p in sorted(supplemental_pages):
            page_text = _doc.pages[p - 1].extract_text() or ''

            has_line_4ij = bool(_line_4ij_re.search(page_text))
            has_asset_table_header = bool(_asset_table_header_re.search(page_text))
            has_generic_schedule = bool(_generic_schedule_re.search(page_text))
            is_note_summary = bool(_note_summary_re.search(page_text))
            is_structural_schedule = _looks_like_structural_investment_schedule(page_text)
            is_target_schedule = has_line_4ij or (has_generic_schedule and has_asset_table_header and not is_note_summary) or is_structural_schedule

            if is_target_schedule:
                active_parser_profile = _infer_inline_text_parser_profile(page_text)
                active_continuation_family = _profile_family(active_parser_profile)
                active_structural_profile = _infer_structural_row_profile(page_text) if is_structural_schedule else {}
                active_structural_asset_type = _infer_first_section_asset_type(page_text) if is_structural_schedule else ""
            elif _is_new_exhibit_or_schedule_page(page_text):
                active_parser_profile = ""
                active_continuation_family = ""
                active_structural_profile = {}
                active_structural_asset_type = ""

            is_profile_continuation = _matches_structural_row_profile(page_text, active_structural_profile)
            is_continuation = (
                bool(active_continuation_family) and _looks_like_investment_continuation_page(
                    page_text, active_continuation_family
                )
            ) or is_profile_continuation
            if is_target_schedule or is_continuation:
                filtered_pages.append(p)
                section_table_areas = _find_section_table_areas(_doc.pages[p - 1])
                if section_table_areas:
                    section_table_areas_by_page[p] = section_table_areas
                if is_continuation and active_parser_profile:
                    continuation_parser_profiles[p] = active_parser_profile
                if is_profile_continuation and active_structural_asset_type:
                    continuation_asset_types[p] = active_structural_asset_type
                page_value_scale[p] = _page_value_scale_factor(page_text)
        supplemental_pages = filtered_pages
    if not supplemental_pages:
        return plan_info, []

    pages_arg = ",".join(str(p) for p in supplemental_pages)
    default_pages = [p for p in supplemental_pages if p not in section_table_areas_by_page]
    tables = []
    if default_pages:
        try:
            tables.extend(camelot.read_pdf(
                pdf_path,
                pages=",".join(str(p) for p in default_pages),
                flavor="stream",
            ))
        except Exception as _exc:
            print(f"    Camelot failed on default pages {default_pages}: {_exc}")
    section_asset_type_by_table: Dict[int, str] = {}
    for page_num in supplemental_pages:
        section_table_areas = section_table_areas_by_page.get(page_num, [])
        if not section_table_areas:
            continue
        print(f"    Splitting page {page_num} into {len(section_table_areas)} section table areas")
        for table_area, section_asset_type in section_table_areas:
            try:
                section_tables = list(camelot.read_pdf(
                    pdf_path,
                    pages=str(page_num),
                    flavor="stream",
                    table_areas=[table_area],
                ))
            except Exception as _exc:
                print(f"    Skipping section area on page {page_num}: {_exc}")
                section_tables = []
            for section_table in section_tables:
                tables.append(section_table)
                section_asset_type_by_table[id(section_table)] = section_asset_type

    if use_llm:
        api_key = os.getenv("OPENAI_API_KEY")
        client = OpenAI(api_key=api_key) if api_key else None
    else:
        client = None
    mapped_pages: Dict[int, List[Dict]] = {}
    
    # Track which pages had tables extracted
    pages_with_tables = set()
    for table in tables:
        pages_with_tables.add(int(table.page))
    
    # Separate storage for text-extracted pages (no DataFrame processing needed)
    text_extracted_pages: Dict[int, List[Dict]] = {}

    def _correct_maturing_value_description_column(header: List[str], column_map: Dict[int, str]) -> None:
        """Disambiguate Form 5500 description column text from actual current value.

        The description header can end with "collateral, par, or maturing value".
        If a separate column is already mapped to current_value, this maturing-value
        header is column C / investment_description, not the value column.
        """
        current_value_cols = {idx for idx, field in column_map.items() if field == 'current_value'}
        if not current_value_cols:
            return
        for idx, h in enumerate(header):
            if idx in current_value_cols and len(current_value_cols) == 1:
                continue
            if re.search(r'collateral.*par.*matur(?:ing|ity)\s+value', h, re.IGNORECASE):
                column_map[idx] = 'investment_description'

    def _verify_or_remap_value_column(df, data_start_row: int, column_map: Dict[int, str]) -> Dict[int, str]:
        """A reused column map assumes the same column layout as the table it
        was captured from. Camelot's stream flavor infers columns
        independently per extraction, so a section-area-restricted
        re-extraction (see _find_section_table_areas) can land on a
        different column count than the original table -- e.g. a wrapped
        fund name spills into its own near-empty column, shifting the real
        value column one to the right. Reusing the old map then silently
        points current_value at a blank column while the real values sit
        unread. Verify the mapped value column actually looks like values
        here; if not, retarget it to whichever column (preferring the
        rightmost, since the value column is always last) is mostly numeric.
        """
        value_col = next((idx for idx, f in column_map.items() if f == 'current_value'), None)
        if value_col is None or df.shape[0] <= data_start_row:
            return column_map
        sample = df.iloc[data_start_row:data_start_row + 15]

        def _numeric_ratio(col_idx):
            if col_idx >= df.shape[1]:
                return 0.0
            cells = [normalize_whitespace(str(v)) for v in sample.iloc[:, col_idx].tolist()]
            non_empty = [c for c in cells if c]
            if not non_empty:
                return 0.0
            matches = sum(1 for c in non_empty if re.fullmatch(r"\$?\s*\(?\s*[0-9][0-9,]*(?:\.[0-9]+)?\)?", c))
            return matches / len(non_empty)

        if _numeric_ratio(value_col) >= 0.5:
            return column_map
        best_col, best_ratio = None, 0.0
        for col_idx in range(df.shape[1] - 1, -1, -1):
            ratio = _numeric_ratio(col_idx)
            if ratio > best_ratio:
                best_col, best_ratio = col_idx, ratio
        if best_col is not None and best_ratio >= 0.5:
            column_map = {idx: f for idx, f in column_map.items() if f != 'current_value'}
            column_map[best_col] = 'current_value'
            print(f"    Remapped current_value column {value_col} -> {best_col} (reused map didn't match this table's layout)")
        return column_map

    # Persists across pages: once a section heading is seen, all following rows
    # inherit its type until a new heading overrides it
    current_section_type = ""
    previous_column_map: Dict[int, str] = {}
    previous_column_map_page: Optional[int] = None
    pending_single_cell_fragments: Dict[int, str] = {}

    for table in tables:
        pending_single_cell_fragments.clear()
        df = table.df
        if df.shape[0] < 2:
            continue
        
        # Find the actual header rows - headers may span multiple rows
        # Look for rows with high schema matches and also check for partial keywords
        # BUT: Only consider first 4 rows as potential headers to avoid false positives
        header_rows = []
        for idx in range(min(4, df.shape[0])):  # Changed from 8 to 4
            potential_header = [normalize_whitespace(h) for h in df.iloc[idx].tolist()]
            # A one-cell row is usually a section heading, but Form 5500 headers can
            # wrap across rows with only one visible cell, e.g.
            # "Description of investment, including" / "maturity date, rate of interest".
            non_empty_header_cells = [h for h in potential_header if h]
            if len(non_empty_header_cells) == 1:
                one_cell_header = non_empty_header_cells[0].lower()
                if not any(
                    kw in one_cell_header
                    for kw in ['description of investment', 'maturity date', 'rate of interest']
                ):
                    continue
            # A real header row never carries an actual dollar figure. Short fund-name
            # cells like "Mid Cap Value Fund" can fuzzy-match header synonyms (e.g.
            # "current_value") at very high scores, which was misclassifying the first
            # DATA row as a header row and dropping both it and the preceding section
            # heading. A value-bearing cell is decisive proof this is a data row.
            if any(
                re.fullmatch(r"\$?\s*\(?\s*[0-9][0-9,]*(?:\.[0-9]+)?\)?", h)
                for h in non_empty_header_cells
            ):
                continue
            match_count = 0
            partial_match = False
            for h in potential_header:
                field, score = _best_header_match(h, synonyms)
                if score >= 70:
                    match_count += 1
                elif any(kw in h.lower() for kw in ['current', 'value', 'cost', 'par', 'date', 'rate', 'lessor', 'issue', 'issuer', 'identity']):
                    partial_match = True
            # Only mark as header if we have strong matches OR partial matches in the first 3 rows
            if (match_count >= 2) or (idx < 3 and (match_count > 0 or partial_match)):
                header_rows.append(idx)
        
        page_num = int(table.page)
        table_section_asset_type = section_asset_type_by_table.get(id(table), "")
        reused_previous_column_map = False

        # If we found header rows, use the first and last to determine the header span.
        # If not, a continuation page may start directly with data rows; in that case,
        # reuse the previous compatible column map instead of treating row 0 as a header.
        if header_rows:
            best_header_row = header_rows[0]
            last_header_row = header_rows[-1]
            data_start_row = last_header_row + 1

            # Combine headers from all header rows
            combined_header = [""] * df.shape[1]
            for hrow in range(best_header_row, last_header_row + 1):
                row_vals = [normalize_whitespace(h) for h in df.iloc[hrow].tolist()]
                for col_idx, val in enumerate(row_vals):
                    if val and not combined_header[col_idx]:
                        combined_header[col_idx] = val
                    elif val and combined_header[col_idx]:
                        # Append multi-row headers with a space
                        combined_header[col_idx] = combined_header[col_idx] + " " + val

            header = combined_header
            column_map = {}
            for i, h in enumerate(header):
                field, score = _best_header_match(h, synonyms)
                if field and score >= 70:
                    column_map[i] = field

            if use_llm and client is not None:
                llm_map = _llm_normalize_headers(client, model, header, fields)
                for k, v in llm_map.items():
                    column_map[k] = v

            _correct_maturing_value_description_column(header, column_map)

            if column_map:
                previous_column_map = dict(column_map)
                previous_column_map_page = page_num
        elif table_section_asset_type and previous_column_map_page == page_num and previous_column_map:
            column_map = _verify_or_remap_value_column(df, 0, dict(previous_column_map))
            data_start_row = 0
            reused_previous_column_map = True
            print(f"    Reusing same-page column map for section table on page {page_num}")
        elif _looks_like_headerless_continuation(df, previous_column_map):
            column_map = _verify_or_remap_value_column(df, 0, dict(previous_column_map))
            data_start_row = 0
            reused_previous_column_map = True
            print(f"    Reusing previous column map for headerless continuation page {page_num}")
        else:
            # Fallback to row 0 if no matches found and this does not look like a continuation.
            best_header_row = 0
            last_header_row = 0
            data_start_row = last_header_row + 1
            combined_header = [""] * df.shape[1]
            row_vals = [normalize_whitespace(h) for h in df.iloc[0].tolist()]
            for col_idx, val in enumerate(row_vals):
                if val:
                    combined_header[col_idx] = val

            header = combined_header
            column_map = {}
            for i, h in enumerate(header):
                field, score = _best_header_match(h, synonyms)
                if field and score >= 70:
                    column_map[i] = field

            if use_llm and client is not None:
                llm_map = _llm_normalize_headers(client, model, header, fields)
                for k, v in llm_map.items():
                    column_map[k] = v

            _correct_maturing_value_description_column(header, column_map)

            if column_map:
                previous_column_map = dict(column_map)
                previous_column_map_page = page_num
        for row_idx in range(data_start_row, df.shape[0]):
            row_data = {f: "" for f in fields}
            row_data["page_number"] = page_num
            row_data["row_id"] = row_idx - data_start_row + 1
            row = df.iloc[row_idx].tolist()

            # If the entire row has only one non-empty cell it is a section heading label,
            # not investment data. Match against known asset type patterns and update
            # current_section_type; reset to '' for unrecognised headings so the previous
            # type does not bleed into a new section.
            non_empty_cells = [
                (col_idx, normalize_whitespace(str(cell)))
                for col_idx, cell in enumerate(row)
                if normalize_whitespace(str(cell))
            ]
            non_empty = [text for _, text in non_empty_cells]
            if len(non_empty_cells) == 1:
                fragment_col_idx, candidate_text = non_empty_cells[0]
                candidate = candidate_text.rstrip(':').strip()
                candidate_stripped = _TOTAL_AFFIX_RE.sub('', candidate).strip()
                matched = None
                for cand in (candidate, candidate_stripped):
                    for pattern, canonical in ASSET_TYPE_PATTERNS:
                        if re.fullmatch(pattern, cand, re.IGNORECASE):
                            matched = canonical
                            break
                    if matched:
                        break
                if matched:
                    current_section_type = matched
                    pending_single_cell_fragments.clear()
                    print(f"    Section heading: '{matched}' (row {row_idx})")
                elif re.fullmatch(r"\$?\s*\(?\s*[0-9][0-9,]*(?:\.[0-9]+)?\)?", candidate_text):
                    # A one-cell numeric row is usually a subtotal/duplicate value line,
                    # not a split name. Do not attach it to the next investment row.
                    pending_single_cell_fragments.clear()
                else:
                    # Preserve split investment names that Camelot emits as a single-cell
                    # row. Merge them into the same column on the next value-bearing row.
                    existing_fragment = pending_single_cell_fragments.get(fragment_col_idx, '')
                    pending_single_cell_fragments[fragment_col_idx] = normalize_whitespace(
                        f"{existing_fragment} {candidate_text}" if existing_fragment else candidate_text
                    )
                continue

            has_value_like_cell = any(
                re.fullmatch(r"\$?\s*\(?\s*[0-9][0-9,]*(?:\.[0-9]+)?\)?", text)
                for _, text in non_empty_cells
            )
            if pending_single_cell_fragments and has_value_like_cell:
                row = list(row)
                for pending_col_idx, fragment in list(pending_single_cell_fragments.items()):
                    if pending_col_idx < len(row):
                        current_text = normalize_whitespace(str(row[pending_col_idx]))
                        row[pending_col_idx] = normalize_whitespace(
                            f"{fragment} {current_text}" if current_text else fragment
                        )
                pending_single_cell_fragments.clear()

            for col_idx, cell in enumerate(row):
                text = normalize_whitespace(str(cell))
                if not text:
                    continue
                field = column_map.get(col_idx)
                if field:
                    if row_data.get(field):
                        row_data[field] = normalize_whitespace(str(row_data[field]) + " " + text)
                    else:
                        row_data[field] = text

            # Strip party-in-interest marker (*) from issuer name — column (a) in Form 5500
            if row_data.get('issuer_name'):
                row_data['issuer_name'] = row_data['issuer_name'].lstrip('* ').strip()

            value_scale = page_value_scale.get(page_num, 1)
            if value_scale != 1 and row_data.get('current_value'):
                row_data['current_value'] = _scale_currency_string(row_data['current_value'], value_scale)

            # If this row is just an asset-type section heading, record the type and skip it
            section_type = _detect_section_heading(row_data, fields)
            if section_type is not None:
                current_section_type = section_type
                print(f"    Section heading detected: '{section_type}' (row {row_idx})")
                continue

            # Propagate the current section type to rows with blank asset_type
            if _is_blank_asset_type(row_data.get('asset_type', '')):
                if table_section_asset_type:
                    row_data['asset_type'] = table_section_asset_type
                elif current_section_type:
                    row_data['asset_type'] = current_section_type

            mapped_pages.setdefault(page_num, []).append(row_data)

    # FALLBACK: Check if table extraction produced mostly empty data
    # If so, try text-based extraction instead
    pages_to_retry = []
    for page_num, rows in mapped_pages.items():
        if not rows:
            pages_to_retry.append(page_num)
            continue
        
        # Count how many rows have meaningful data (non-empty issuer or description with value).
        # Also catch pages where Camelot found amounts but lost the investment-name column,
        # leaving only generic category labels such as "Mutual Fund" in description/type.
        meaningful_rows = 0
        valued_rows = 0
        category_only_rows = 0
        for row in rows:
            issuer = normalize_whitespace(row.get('issuer_name', '')).strip()
            desc = normalize_whitespace(row.get('investment_description', '')).strip()
            asset_type = normalize_whitespace(row.get('asset_type', '')).strip()
            value = normalize_whitespace(row.get('current_value', '')).strip()
            has_value = bool(value and value not in ['', '**', '-', 'nan'])
            if (issuer or desc) and has_value:
                meaningful_rows += 1
            if has_value:
                valued_rows += 1
                if not issuer and _is_category_only_investment_label(desc, asset_type):
                    category_only_rows += 1
        
        # If less than 10% of rows have data, consider it a failed extraction.
        # If most valued rows have blank issuers and only category labels, Camelot likely
        # dropped the investment names; retry with text extraction for that page.
        category_only_ratio = category_only_rows / valued_rows if valued_rows else 0
        if len(rows) > 0 and meaningful_rows / len(rows) < 0.1:
            print(f"    Table extraction on page {page_num} yielded poor results ({meaningful_rows}/{len(rows)} meaningful rows)")
            pages_to_retry.append(page_num)
        elif valued_rows >= 5 and category_only_ratio >= 0.5:
            print(f"    Table extraction on page {page_num} lost issuer names ({category_only_rows}/{valued_rows} valued rows are category-only); retrying text extraction")
            pages_to_retry.append(page_num)
    
    # Remove poor quality pages from mapped_pages so we can retry with text extraction
    for page_num in pages_to_retry:
        if page_num in mapped_pages:
            del mapped_pages[page_num]
    
    # RETRY with text-based extraction for pages that had no tables or poor table extraction
    for page_num in supplemental_pages:
        if page_num not in pages_with_tables or page_num in pages_to_retry:
            print(f"    No tables found on page {page_num}, trying text-based extraction...")
            text_investments = extract_text_based_investments(
                pdf_path,
                page_num,
                parser_profile=continuation_parser_profiles.get(page_num, ""),
                inherited_asset_type=continuation_asset_types.get(page_num, ""),
            )
            if text_investments:
                print(f"      [OK] Extracted {len(text_investments)} investments from text")
                # Store separately - these are already properly formatted
                text_extracted_pages[page_num] = text_investments
            else:
                print(f"      [!] No investments found in text format either")

    # Build results: Process table data and text data separately
    result = []
    
    # Process TABLE-EXTRACTED pages with DataFrame operations
    for page_num, rows in mapped_pages.items():
        df = pd.DataFrame(rows)
        df = handle_split_rows(df)
        cleaned_rows = []
        for row in df.to_dict(orient='records'):
            parsed = parse_investment_row(row)
            # Merge parsed fields back
            row['issuer_name'] = parsed['issuer_name']
            row['asset_type'] = parsed['asset_type']
            row['investment_description'] = parsed['investment_description']
            cleaned_rows.append(row)
        
        result.append(
            {
                "pdf": pdf_path,
                "pdf_stem": pdf_path.split("/")[-1].rsplit(".", 1)[0],
                "page_number": page_num,
                "mapped_rows": cleaned_rows,
                "ocr_cells": [],
                "normalized_path": pdf_path,
            }
        )
    
    # Process TEXT-EXTRACTED pages WITHOUT DataFrame operations (already clean)
    for page_num, rows in text_extracted_pages.items():
        # Text-extracted data is already properly formatted, use as-is
        result.append(
            {
                "pdf": pdf_path,
                "pdf_stem": pdf_path.split("/")[-1].rsplit(".", 1)[0],
                "page_number": page_num,
                "mapped_rows": rows,  # Use directly without processing
                "ocr_cells": [],
                "normalized_path": pdf_path,
            }
        )
    
    # --- Sub-schedule title asset-type tagging (audited 4i "SCHEDULE OF <type>" pages) ---
    # Fill blank asset_type from each page's sub-schedule title so non-MF sub-schedules
    # (govt/corporate bonds, stocks, cash) are dropped by the MF load gate instead of
    # leaking through as numeric-name junk; "registered investment companies" -> Mutual Fund.
    try:
        _title_type_by_page: Dict[int, str] = {}
        with pdfplumber.open(pdf_path) as _ttl_doc:
            _npages = len(_ttl_doc.pages)
            for _entry in result:
                _pn = _entry.get("page_number")
                if isinstance(_pn, int) and _pn not in _title_type_by_page and 1 <= _pn <= _npages:
                    _htxt = _ttl_doc.pages[_pn - 1].extract_text() or ""
                    _title_type_by_page[_pn] = _infer_schedule_of_title_asset_type(_htxt)
        for _entry in result:
            _ptype = _title_type_by_page.get(_entry.get("page_number"), "")
            if not _ptype:
                continue
            for _row in _entry.get("mapped_rows", []):
                if not normalize_whitespace(str(_row.get("asset_type", "") or "")).strip():
                    _row["asset_type"] = _ptype
    except Exception as _ttl_exc:
        print(f"    [sub-schedule tag] skipped: {_ttl_exc}")

    return plan_info, result
