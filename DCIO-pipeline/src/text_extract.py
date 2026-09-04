import json
import os
import re
from typing import Dict, List, Tuple, Optional

import camelot
import pdfplumber
from openai import OpenAI
from rapidfuzz import process, fuzz

import pandas as pd

from .asset_type_patterns import ASSET_TYPE_PATTERNS, detect_asset_type, detect_asset_type_strict
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
    if re.search(
        r'\b(?:in\s+thousands|amounts?\s+(?:are\s+)?in\s+thousands|'
        r'dollars?\s+in\s+thousands|thousands\s+of\s+dollars|\$\s*000s?|'
        # Some filers use "(amounts in 000's)" instead of the word "thousands";
        # the apostrophe often extracts as a stray/garbled character (curly
        # quote, PDF font mojibake) rather than a plain ' , so match any
        # short run of non-word characters between "000" and the trailing "s".
        r'\bin\s+000\W{0,2}s\b)\b',
        text or '',
        re.IGNORECASE,
    ):
        return True
    # Some filers (e.g. ALLETE) print a bare standalone "Thousands" label as its
    # own header line instead of an "in thousands" phrase. Anchored to a whole
    # line (not just a substring) so it doesn't fire on unrelated prose that
    # happens to contain the word "thousands" (e.g. "thousands of participants").
    return bool(re.search(r'(?m)^\s*thousands\s*$', text or '', re.IGNORECASE))


def _page_values_are_in_millions(text: str) -> bool:
    """Return True when page text declares dollar amounts in millions."""
    return bool(re.search(
        r'\b(?:in\s+millions|amounts?\s+(?:are\s+)?in\s+millions|'
        r'dollars?\s+in\s+millions|millions\s+of\s+dollars)\b',
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

def _page_has_ruling_lines(pdf_path: str, page_num: int, min_edges: int = 50) -> bool:
    """Check whether a page has real vector table gridlines (rects/lines/edges).

    Camelot's stream flavor infers columns from whitespace gaps, which breaks
    on pages with wrapped multi-line cells. lattice locks onto ruling lines
    instead and is far more reliable when a page actually has them.
    """
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if page_num - 1 >= len(pdf.pages):
                return False
            page = pdf.pages[page_num - 1]
            return len(page.edges) >= min_edges
    except Exception:
        return False

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

# Heading-only patterns: phrasing that is a clear section-heading label but is
# deliberately NOT in the shared ASSET_TYPE_PATTERNS vocabulary, because that
# list is reused for loose substring matching against fund NAMES elsewhere
# (data_cleaner.py, post_extract_validator.py). A bare "Government Securities"
# there would mistype real mutual funds whose own name contains that phrase
# (e.g. "Vanguard Government Securities Fund") as a Bond. _detect_section_heading_text
# is only ever called on value-free candidate lines (never fund/row text with a
# dollar value), so these patterns are safe here without loosening name matching.
_HEADING_ONLY_PATTERNS = [
    (r'Cash\s+Equivalents?',        'Cash Equivalent'),
    (r'Government\s+Securities',    'Bond'),
]

# A section heading naming the vehicle plus its distributor/provider, e.g.
# "Mutual funds offered by Teachers Insurance and Annuity Association" (Brown
# University). The trailing "offered by <provider>" clause means it never
# fullmatches an ASSET_TYPE_PATTERNS entry, so without this it fell through
# to the split-fund-name-fragment path and got fused onto the next row's
# issuer_name instead of being recognized as a heading. Anchored to the
# start of the cell and requires "offered by" so it can never match a real
# one-cell fund name.
_HEADING_OFFERED_BY_RE = re.compile(
    r'^(mutual\s+funds?|registered\s+investment\s+compan(?:y|ies))\s+offered\s+by\b',
    re.IGNORECASE,
)

# Strips just the "<vehicle> offered by <provider>:" clause (through its
# trailing colon) so any real fund name/value Camelot fused onto the same
# row as this heading survives instead of being dropped with it.
_HEADING_OFFERED_BY_STRIP_RE = re.compile(
    r'^(?:mutual\s+funds?|registered\s+investment\s+compan(?:y|ies))\s+offered\s+by\s+[^:]*:\s*',
    re.IGNORECASE,
)

# "Total <Provider>" subtotal rows (e.g. Brown University's "Total Fidelity",
# "Total Transamerica") group holdings by distributor rather than by a known
# ASSET_TYPE_PATTERNS category, so _TOTAL_CATEGORY_RE never matches them and
# they leak into the data as if they were real holdings. Require the exact
# "Total <1-4 words>" shape and exclude any word that could plausibly be part
# of a real fund's own name (Fund, Index, Bond, ...) so a legitimately-named
# holding like "PIMCO Total Return Fund" is never dropped.
_TOTAL_PROVIDER_RE = re.compile(
    r'^(?:total|subtotal|sub-total|grand\s+total)\s+'
    r'([A-Za-z][A-Za-z&.\-]*(?:\s+[A-Za-z][A-Za-z&.\-]*){0,3})$',
    re.IGNORECASE,
)
_TOTAL_PROVIDER_EXCLUDE_WORDS = {
    'fund', 'funds', 'trust', 'account', 'accounts', 'index', 'class',
    'shares', 'share', 'portfolio', 'bond', 'bonds', 'equity', 'equities',
    'stock', 'stocks', 'annuity', 'annuities', 'series', 'cap', 'growth',
    'value', 'income', 'securities', 'market', 'markets', 'investment',
    'investments', 'asset', 'assets', 'loan', 'loans', 'return', 'returns',
}


# Some layouts (e.g. Cleveland Clinic) don't emit the section heading as its
# own row at all -- Camelot fuses it directly onto the first data row's own
# cell, e.g. investment_description = "Mutual Funds and Variable Annuity
# Contracts BLKRK LP IDX RTMT K" where "BLKRK LP IDX RTMT K" is the real fund
# name. Strip the known heading prefix and use the remainder as the real
# field value, same as any other data row.
_HEADING_PREFIX_RE = re.compile(
    r'^mutual\s+funds?\s+and\s+variable\s+annuit(?:y|ies)\s+contracts?\s+',
    re.IGNORECASE,
)


def _is_total_provider_label(text: str) -> bool:
    text = normalize_whitespace(text or '').rstrip(':').strip()
    if not text:
        return False
    m = _TOTAL_PROVIDER_RE.match(text)
    if not m:
        return False
    words = re.findall(r"[A-Za-z]+", m.group(1))
    if any(w.lower() in _TOTAL_PROVIDER_EXCLUDE_WORDS for w in words):
        return False
    return True


# Some filers put "Total"/"Subtotal" at the FRONT of a category subtotal line
# ("Total Mutual Funds"), others put it at the END ("Mutual Funds Total" --
# e.g. Lee Health System's Schedule H). Recognize both shapes so a trailing-
# total line is still treated as a total/backfill trigger rather than leaking
# into the data as a fake row (and, worse, leaving every real row above it
# with no asset_type at all since the backfill never fires). Only used to
# decide whether to ATTEMPT resolving a canonical type via
# _detect_section_heading_text -- a line that merely happens to end in the
# word "Total" but isn't a real category (e.g. "PIMCO Income Total") still
# won't resolve there, so this stays safe to widen.
def _is_total_line_shape(text: str) -> bool:
    text = (text or '').strip()
    if not text:
        return False
    if re.match(r'^(?:total|subtotal|sub-total|grand\s+total)\b', text, re.IGNORECASE):
        return True
    return bool(re.search(r'\b(?:total|subtotal|sub-total|grand\s+total)\s*$', text, re.IGNORECASE))


def _detect_section_heading_text(text: str) -> Optional[str]:
    """Return canonical asset type when a text line is a label-only section heading."""
    text_clean = normalize_whitespace(text or "").rstrip(":").strip()
    if not text_clean:
        return None

    # A line with numeric value content is data/subtotal, not a heading.
    if _VALUE_LIKE_RE.search(text_clean):
        return None

    text_stripped = _TOTAL_AFFIX_RE.sub("", text_clean).strip()
    # Fullmatch, not search: this function is documented to detect "label-only"
    # heading lines. A substring search here lets a short pattern (e.g. "Interest
    # Bearing Cash") false-match inside an unrelated longer sentence, such as a
    # parenthetical description line that merely mentions that phrase in passing
    # (e.g. a Self-Directed Brokerage Account's own multi-line heading text
    # "...MUTUAL FUNDS, INTEREST BEARING CASH, NONINTEREST-BEARING CASH AND
    # OTHER LIABILITIES)"), incorrectly splitting a false new section area.
    for candidate in {text_clean, text_stripped}:
        for pattern, canonical in ASSET_TYPE_PATTERNS:
            if re.fullmatch(pattern, candidate, re.IGNORECASE):
                return canonical
        for pattern, canonical in _HEADING_ONLY_PATTERNS:
            if re.fullmatch(pattern, candidate, re.IGNORECASE):
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
    r'|(?:other\s+)?common\s+stocks?'
    r'|government\s+securit(?:y|ies)'
    r'|employer\s+securit(?:y|ies)'
    r'|insurance\s+company\s+general\s+accounts?'
    r'|general\s+accounts?'
    r'|stable\s+value\s+funds?'
    r'|money\s+market\s+funds?'
    r')'
    # Grand-total lines often qualify the category with a trailing "(held at
    # end of year)" / "held for investment at end of year" phrase before the
    # value (e.g. Schedule H's "TOTAL ASSETS (HELD AT END OF YEAR)") -- allow
    # it here so the category still matches instead of falling through and
    # leaking the grand-total line into the data as a fake row.
    r'(?:\s*\(?\s*held\s+(?:at|for\s+investment\s+at)\s+end\s+of\s+year\s*\)?)?'
    r'(?:\s*[:\-]?\s*[\d,$().-]+)?\s*$',
    re.IGNORECASE,
)


def _is_total_summary_label(text: str) -> bool:
    text = normalize_whitespace(text or "")
    if not text:
        return False
    return bool(_TOTAL_ONLY_RE.match(text) or _TOTAL_CATEGORY_RE.match(text))


# A one-cell "Total <arbitrary section name>: <amount>" subtotal line (e.g.
# BASF's "Total BASF Stable Value Fund: 916,262,534" / "Total Common/Collective
# Trust: 896,769,005"). _TOTAL_CATEGORY_RE only matches a FIXED enum of known
# category names directly after "Total ", so a plan-specific section name like
# "BASF Stable Value Fund" -- or any name containing a word from
# _TOTAL_PROVIDER_EXCLUDE_WORDS, which deliberately blocks _is_total_provider_label
# on real fund names like "PIMCO Total Return Fund" -- never matches either
# helper. Left unrecognized, this text falls into the split-fund-name-fragment
# path below and gets merged onto the NEXT section's first data row's value
# cell (e.g. "644,262,835 Total BASF Stable Value Fund: 916,262,534"), which
# then fails parse_currency_value() and silently drops that row's real value.
# Requiring a trailing "colon + amount" is what makes this safe to match on
# ANY label without an enum: a genuine wrapped/split fund-name fragment (what
# this one-cell branch exists to preserve) is bare text and never ends in a
# colon followed by a number.
_TOTAL_LABELED_SUBTOTAL_RE = re.compile(
    r'^(?:total|subtotal|sub-total|grand\s+total)\b.*:\s*\$?\s*\(?[\d,]+(?:\.\d+)?\)?\s*$',
    re.IGNORECASE,
)


def _is_total_labeled_subtotal(text: str) -> bool:
    text = normalize_whitespace(text or "")
    if not text:
        return False
    return bool(_TOTAL_LABELED_SUBTOTAL_RE.match(text))


# BASF's actual corruption mechanism (confirmed against the raw Camelot cell
# dump, not just the flattened CSV): a section subtotal like "Total BASF
# Stable Value Fund: 644,262,835" is emitted by Camelot as its OWN one-cell
# row, but with the value line and the label line in the opposite order from
# a normal row -- one cell containing "644,262,835\nTotal BASF Stable Value
# Fund:" (amount FIRST, label second, no trailing amount). That one-cell row
# doesn't match _is_total_labeled_subtotal (which requires the label to come
# first and a trailing colon+amount), so it falls into the generic
# split-fund-name-fragment path below and gets PREPENDED onto the next row's
# real value cell (e.g. Vanguard Institutional 500 Index's own
# "916,262,534" becomes "644,262,835 Total BASF Stable Value Fund:
# 916,262,534"), corrupting that row's real value. Recognize this
# amount-then-label shape at the source and drop it like any other subtotal
# marker, instead of queuing it for merge.
_VALUE_THEN_TOTAL_LABEL_RE = re.compile(
    r'^\$?\s*\(?[\d,]+(?:\.\d+)?\)?\s+(?:total|subtotal|sub-total|grand\s+total)\b.*:\s*$',
    re.IGNORECASE,
)


def _is_value_then_total_label(text: str) -> bool:
    text = normalize_whitespace(text or "")
    if not text:
        return False
    return bool(_VALUE_THEN_TOTAL_LABEL_RE.match(text))


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
    # An exact synonym match (e.g. a bare "value" header cell) is unambiguous
    # and must win outright. Without this, fuzz.partial_ratio scores "value"
    # as a 100% substring match against BOTH "par value" and the literal
    # "value" synonym under current_value, and since ties keep the
    # first-seen field (strict `>`), whichever field is iterated first in
    # schema.yml wins even when a later field has the true exact match.
    for field, terms in synonyms.items():
        if header in (t.lower() for t in terms):
            return field, 100
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

# Kelley Drye & Warren LLP Retirement Savings Plan (ack_id
# 20251015111048NAL0002192867001) embeds its Schedule H, Line 4i table with
# characters flagged upright=False whose transform matrices are actually
# near-identity (only ~1e-8/1e-9 floating-point noise off the diagonal, not
# real rotation/skew). pdfplumber's extract_text() mis-groups these chars
# into garbled, unreadable lines. Scoped to this one filing only -- other
# filers are unaffected and this has not been checked for regressions
# elsewhere.
_GARBLED_UPRIGHT_ACK_IDS = {"20251015111048NAL0002192867001"}


def _pdf_stem_from_path(pdf_path: str) -> str:
    return pdf_path.replace("\\", "/").split("/")[-1].rsplit(".", 1)[0]


def _extract_text_robust(page) -> str:
    """
    Fallback to a page's raw text when pdfplumber's extract_text() garbles it
    due to chars incorrectly flagged upright=False (see
    _GARBLED_UPRIGHT_ACK_IDS above). Groups chars by rounded top position and
    sorts each line by x0, inserting a space wherever the x-gap between
    consecutive chars suggests a word boundary.
    """
    rows: Dict[int, list] = {}
    for c in page.chars:
        rows.setdefault(round(c["top"]), []).append(c)

    out_lines = []
    for top in sorted(rows.keys()):
        row_chars = sorted(rows[top], key=lambda c: c["x0"])
        parts = []
        prev_x1 = None
        for c in row_chars:
            if prev_x1 is not None and c["x0"] - prev_x1 > 2.0:
                parts.append(" ")
            parts.append(c["text"])
            prev_x1 = c["x1"]
        out_lines.append("".join(parts))
    return "\n".join(out_lines)


def classify_pages_text(pdf_path: str, keywords_yml: str) -> List[Dict]:
    cfg = load_yaml(keywords_yml)
    keywords = [k.upper() for k in cfg.get("supplemental_schedule_keywords", [])]
    negatives = [k.upper() for k in cfg.get("negative_keywords", [])]
    min_hits = int(cfg.get("min_keyword_hits", 1))
    max_lines = int(cfg.get("header_scan_max_lines", 12))
    money_token_re = re.compile(r"\$?\s*\(?\s*[0-9][0-9,]*(?:\.[0-9]+)?\)?")

    # "Registered Investment Companies" (the formal 5500 term for mutual funds) is common
    # boilerplate that can appear in narrative text or unrelated schedules, so it's not a
    # keywords.yml entry -- it only counts as a match here when the page ALSO carries the
    # actual asset-schedule column headers, mirroring the stricter structural-page check in
    # _looks_like_structural_investment_schedule. This catches filers (e.g. Oracle) who
    # split their Schedule of Assets so an early page opens under "Notes to Financial
    # Statements" with this heading instead of the usual "Schedule H, Line 4(i)" title.
    ric_re = re.compile(r'registered\s+investment\s+compan(?:y|ies)', re.IGNORECASE)
    identity_re = re.compile(r'identity\s+of\s+issue|borrower,?\s+lessor', re.IGNORECASE)
    description_re = re.compile(r'description\s+of\s+investments?', re.IGNORECASE)

    use_robust_extraction = _pdf_stem_from_path(pdf_path) in _GARBLED_UPRIGHT_ACK_IDS

    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = _extract_text_robust(page) if use_robust_extraction else (page.extract_text() or "")
            lines = [normalize_whitespace(l) for l in text.splitlines() if l.strip()]
            header_lines = lines[:max_lines]
            header_text = " ".join(header_lines).upper()
            hits = sum(1 for k in keywords if k in header_text)
            neg_hits = sum(1 for k in negatives if k in header_text)
            money_line_count = sum(1 for l in lines if money_token_re.search(l))
            money_line_density = (money_line_count / len(lines)) if lines else 0.0
            has_ric_schedule = (
                bool(ric_re.search(header_text))
                and bool(identity_re.search(header_text))
                and bool(description_re.search(header_text))
                and "CURRENT" in header_text
                and "VALUE" in header_text
            )
            # Some filers print a routine citation like "(See Independent Auditors'
            # Report)" directly in the real schedule's own header, which trips
            # negative_keywords entries meant to reject the auditor's narrative
            # opinion pages (e.g. "INDEPENDENT AUDITOR") and wrongly zeroes out the
            # genuine Schedule H, Line 4i page (seen on Wilbur-Ellis 401(k) Plan).
            # A negative-keyword hit shouldn't veto a page that also carries the
            # actual schedule's column structure -- the identity-of-issue column
            # header plus a current-value column -- since that combination is
            # specific to the real asset table, not narrative auditor prose.
            looks_like_schedule_page = (
                bool(identity_re.search(header_text))
                and "CURRENT" in header_text
                and "VALUE" in header_text
            )
            # A page that matches a schedule keyword only because it's narrating/
            # citing the schedule in prose (e.g. an auditor's "Other Matter --
            # Supplemental Schedules" boilerplate paragraph) shouldn't be able to
            # kick off the continuation run below and sweep in the real narrative
            # pages that follow it (seen on Wilbur-Ellis: the auditor's own
            # sign-off paragraph pulled in 13 pages of "Notes to the Financial
            # Statements" as a false continuation). Genuine schedule pages are
            # dense with dollar-value-shaped lines; prose pages that merely
            # mention a schedule are not, even though they can contain plenty of
            # scattered dollar figures/dates/percentages of their own.
            MIN_START_DENSITY = 0.4
            is_narrative_keyword_match = (
                not has_ric_schedule and not looks_like_schedule_page and money_line_density < MIN_START_DENSITY
            )
            pages.append(
                {
                    "pdf": pdf_path,
                    "pdf_stem": pdf_path.split("/")[-1].rsplit(".", 1)[0],
                    "page_number": i,
                    "header_text": header_text,
                    "is_supplemental": 1
                    if (hits >= min_hits or has_ric_schedule) and (neg_hits == 0 or looks_like_schedule_page)
                    else 0,
                    "_neg_hits": neg_hits,
                    "_money_line_count": money_line_count,
                    "_is_narrative_keyword_match": is_narrative_keyword_match,
                }
            )

    # A multi-page investment schedule's own keyword text (e.g. "Schedule H,
    # Line 4i") can appear only on the schedule's first page or two --
    # continuation pages that follow are sometimes nothing but bare
    # issuer/value rows with no repeated header at all (seen on Quad/Graphics
    # and Walmart: ~90% of a schedule's rows were dropped because only the
    # first page(s) matched a keyword). Forward-propagate supplemental status
    # from a matched page across a contiguous run of pages that still look
    # like the same table -- dense with dollar-value-like tokens -- and stop
    # the run the moment a page hits a negative keyword or stops looking
    # tabular (prose, a new section, a signature page, etc.), so this never
    # over-runs into unrelated content.
    MIN_MONEY_LINES_FOR_CONTINUATION = 3
    in_run = False
    for p in pages:
        if p["is_supplemental"] == 1:
            in_run = not p["_is_narrative_keyword_match"]
            continue
        if in_run:
            if p["_neg_hits"] > 0:
                in_run = False
            elif p["_money_line_count"] >= MIN_MONEY_LINES_FOR_CONTINUATION:
                p["is_supplemental"] = 1
            else:
                in_run = False

    # Kelley Drye & Warren LLP Retirement Savings Plan (ack_id
    # 20251015111048NAL0002192867001) has page 16 as a byte-for-byte duplicate
    # of page 15's Schedule H, Line 4i table (confirmed: identical char stream,
    # same positions and text). Without this override, both pages would end
    # up supplemental (page 16 would also re-qualify via the continuation-run
    # money-line check above, since it's literally the same dense table) and
    # the pipeline would double-count every holding. Applied after the
    # continuation run so it can't be re-flipped back to 1. Scoped to this
    # one filing/page only.
    if _pdf_stem_from_path(pdf_path) in _GARBLED_UPRIGHT_ACK_IDS:
        for p in pages:
            if p["page_number"] == 16:
                p["is_supplemental"] = 0

    for p in pages:
        del p["_neg_hits"]
        del p["_money_line_count"]
        del p["_is_narrative_keyword_match"]

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


_NUMERIC_TOKEN_RE = re.compile(r'^\(?-?\$?[0-9][0-9,]*\.[0-9]+\)?$')
_NUMERIC_TAIL_RE = re.compile(r'\(?-?\$?\s*[0-9][0-9,]*(?:\.[0-9]+)?\)?')
_ID_TOKEN_RE = re.compile(r'^(?=[A-Z0-9]{5,}$)(?=.*[0-9])[A-Z0-9]+$')

_COLUMN_SEMANTIC_PATTERNS = [
    (re.compile(r'shares?\s*/?\s*par|shares?\b|par\s+value', re.IGNORECASE), 'shares'),
    (re.compile(r'\bcost\b', re.IGNORECASE), 'cost'),
    (re.compile(r'market\s+value|current\s+value|fair\s+value', re.IGNORECASE), 'value'),
    (re.compile(r'unrealized\s+gain|gain\s*/\s*loss', re.IGNORECASE), 'gain_loss'),
]


def _match_asset_category_text(text: str) -> Optional[str]:
    """Match a heading OR a 'TOTAL <category>' subtotal line's text against the
    canonical ASSET_TYPE_PATTERNS vocabulary, regardless of whether the line
    also carries trailing dollar values (unlike _detect_section_heading_text,
    which requires a value-free line)."""
    text_clean = normalize_whitespace(text or "").rstrip(":").strip()
    if not text_clean:
        return None
    text_no_values = _NUMERIC_TAIL_RE.sub('', text_clean).strip()
    text_stripped = _TOTAL_AFFIX_RE.sub('', text_no_values).strip()
    for candidate in {text_no_values, text_stripped}:
        if not candidate:
            continue
        for pattern, canonical in ASSET_TYPE_PATTERNS:
            if re.search(pattern, candidate, re.IGNORECASE):
                return canonical
    return None


def _split_trailing_numeric_tokens(line: str) -> Tuple[str, List[str]]:
    """Split a text line into (leading description, [trailing numeric tokens]),
    walking back from the end and stopping at the first whitespace-delimited
    token that isn't purely numeric. This tolerates embedded dates/rates in
    the description (e.g. '01/01/2049 DD 03/01/24') because a token with
    slashes never matches the numeric pattern."""
    tokens = line.split()
    idx = len(tokens)
    tail: List[str] = []
    while idx > 0 and _NUMERIC_TOKEN_RE.match(tokens[idx - 1]):
        tail.insert(0, tokens[idx - 1])
        idx -= 1
    return ' '.join(tokens[:idx]), tail


def _infer_participation_column_order(header_line: str) -> List[str]:
    """Infer the left-to-right semantic order of numeric columns from a
    schedule's own header wording, tolerant of vendor-specific phrasing
    (e.g. 'Market Value' vs 'Current Value' vs 'Fair Value')."""
    matches = []
    for pattern, semantic in _COLUMN_SEMANTIC_PATTERNS:
        m = pattern.search(header_line)
        if m:
            matches.append((m.start(), semantic))
    matches.sort(key=lambda x: x[0])
    order: List[str] = []
    seen = set()
    for _, semantic in matches:
        if semantic not in seen:
            order.append(semantic)
            seen.add(semantic)
    return order


def _composite_participation_schedule_pages(pdf_path: str, pages: Optional[List[int]] = None) -> List[int]:
    """Return the page numbers that individually match the composite
    Master-Trust-style participation schedule signature -- not any
    filer-specific wording: a flat, unruled text page carrying (a) at least
    one recognized bare section-heading line, (b) at least two recognized
    'TOTAL <category>' subtotal lines, and (c) a meaningful density of
    holding rows ending in multiple comma-formatted numeric values.
    Deliberately filer-agnostic so the same check applies to Howmet, J&J, or
    any future composite participation report. Scoped per-page (rather than
    per-PDF) so unrelated pages elsewhere in the same filing -- e.g. Form
    5500 or a transactions schedule -- are never swept in as data rows.
    """
    matched: List[int] = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            page_range = pages if pages else range(1, len(pdf.pages) + 1)
            for page_num in page_range:
                if not (1 <= page_num <= len(pdf.pages)):
                    continue
                if _page_has_ruling_lines(pdf_path, page_num):
                    continue
                text = pdf.pages[page_num - 1].extract_text() or ""
                if not text.strip():
                    continue
                lines = [normalize_whitespace(l) for l in text.split("\n") if l.strip()]
                heading_categories = set()
                subtotal_categories = set()
                numeric_row_count = 0
                for line in lines:
                    if re.match(r'^(?:total|grand\s+total)\b', line, re.IGNORECASE):
                        cat = _match_asset_category_text(line)
                        if cat:
                            subtotal_categories.add(cat)
                        continue
                    if not _VALUE_LIKE_RE.search(line):
                        cat = _detect_section_heading_text(line)
                        if cat:
                            heading_categories.add(cat)
                        continue
                    if len(re.findall(r'\b\d{1,3}(?:,\d{3})+(?:\.\d+)?\b', line)) >= 2:
                        numeric_row_count += 1
                if heading_categories and len(subtotal_categories) >= 2 and numeric_row_count >= 5:
                    matched.append(page_num)
    except Exception:
        return []
    return matched


def _pdf_has_composite_participation_schedule(pdf_path: str, pages: Optional[List[int]] = None) -> bool:
    return bool(_composite_participation_schedule_pages(pdf_path, pages))


def _parse_composite_participation_row(lead_text: str, tail_tokens: List[str], column_order: List[str]) -> Optional[Dict]:
    if not tail_tokens or not lead_text:
        return None
    if re.match(r'^(?:total|grand\s+total)\b', lead_text, re.IGNORECASE):
        return None

    order = column_order[-len(tail_tokens):] if len(column_order) >= len(tail_tokens) else []
    semantics = dict(zip(order, tail_tokens)) if len(order) == len(tail_tokens) else {}
    if not semantics:
        if len(tail_tokens) == 4:
            semantics = dict(zip(['shares', 'cost', 'value', 'gain_loss'], tail_tokens))
        elif len(tail_tokens) == 3:
            semantics = dict(zip(['cost', 'value', 'gain_loss'], tail_tokens))
        elif len(tail_tokens) == 1:
            semantics = {'value': tail_tokens[0]}
        else:
            return None

    current_value = semantics.get('value')
    if not current_value:
        return None

    # Strip leading security/fund-code tokens (mixed letters+digits, no
    # vowel-only words) so the human-readable description remains.
    lead_tokens = lead_text.split()
    desc_start = 0
    for tok in lead_tokens[:3]:
        if _ID_TOKEN_RE.match(tok):
            desc_start += 1
        else:
            break
    description = ' '.join(lead_tokens[desc_start:]).strip() or lead_text

    return {
        'issuer_name': description,
        'investment_description': description,
        'par_value': (semantics.get('shares', '') or '').replace(',', ''),
        'cost': (semantics.get('cost', '') or '').replace(',', '').strip('()'),
        'current_value': current_value.replace(',', '').strip('()'),
        'units_or_shares': (semantics.get('shares', '') or '').replace(',', ''),
    }


def _extract_composite_participation_rows_for_pdf(pdf_path: str, pages: List[int]) -> List[Dict]:
    """Generic parser for composite Master-Trust-style participation schedules:
    flat, unruled text pages with recognized section headings, 'TOTAL <category>'
    subtotals, and ID-code-prefixed holding rows ending in several numeric
    columns. Column semantics (shares/cost/value/gain-loss) are inferred per
    page from the header row's own wording rather than a fixed assumed order,
    so this generalizes across filers with different column layouts. Only
    processes the given page numbers -- callers should scope this to the
    pages that actually matched the composite-schedule signature, since a
    single filing can also contain unrelated pages (Form 5500, transaction
    schedules) that would otherwise be swept in as noise rows."""
    all_rows: List[Dict] = []
    page_set = set(pages)
    with pdfplumber.open(pdf_path) as pdf:
        for page_idx, page in enumerate(pdf.pages, start=1):
            if page_idx not in page_set:
                continue
            text = page.extract_text() or ''
            if not text.strip():
                continue
            lines = [normalize_whitespace(l) for l in text.split('\n') if l.strip()]
            column_order: List[str] = []
            current_asset_type = ''
            row_num = 0
            for line in lines:
                lead, tail = _split_trailing_numeric_tokens(line)

                if not tail:
                    inferred = _infer_participation_column_order(line)
                    if len(inferred) >= 3:
                        column_order = inferred
                    heading = _detect_section_heading_text(line)
                    if heading:
                        current_asset_type = heading
                    continue

                row = _parse_composite_participation_row(lead, tail, column_order)
                if not row:
                    continue
                row_num += 1
                row['asset_type'] = current_asset_type
                row['page_number'] = page_idx
                row['row_id'] = row_num
                all_rows.append(row)
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
        text = (
            _extract_text_robust(page)
            if _pdf_stem_from_path(pdf_path) in _GARBLED_UPRIGHT_ACK_IDS
            else (page.extract_text() or "")
        )

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
        # Oracle Corporation's 401(k) filing omits the "Schedule H, Line 4(i)"
        # title on the schedule's own first page (it only says "Notes to
        # Financial Statements", same as the narrative pages before it) --
        # the standard schedule-marker regex above can't tell this page apart
        # from a real notes page. Narrow, filer-specific exception rather than
        # widening the general marker, since a bare "Notes to Financial
        # Statements" heading is not on its own a reliable signal for any
        # other filer.
        is_oracle_untitled_schedule_page = (
            'ORACLE' in text.upper() and 'NOTES TO FINANCIAL STATEMENTS' in text.upper()
        )
        if not has_schedule_marker and not has_inherited_continuation_profile and not is_oracle_untitled_schedule_page:
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

        # Value patterns:
        # 1. Line contains a footnote asterisk ("*"/"**") anywhere -- strip it and
        #    take the trailing number as the value (no minimum digit count, since
        #    the asterisk already confirms a value is present). See note below on
        #    why position is no longer used to anchor this.
        # 2. "Fund Name $ 225,122,092" or "Fund Name 225,122,092" (simple two-column format)
        # 3. "Fund Name $ 698" — explicit $ with small value (no minimum digit count)
        _asterisk_re = re.compile(r'\*+')
        trailing_value_pattern = re.compile(r'\$?\s*([\d,]+)\s*$')
        dollar_value_pattern = re.compile(r'\$\s*([\d,]+)\s*$')          # explicit $
        simple_value_pattern = re.compile(r'([\d,]{4,})\s*$')             # no $, 4+ chars

        # Section heading detection for simple two-column format
        # Keys are matched both exactly and as substrings of the line
        SECTION_HEADING_MAP = {
            'mutual fund': 'Mutual Fund',
            'mutual and exchange-traded fund': 'Mutual Fund',
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
        # Rows appended to `investments` with no asset_type resolved yet (no
        # leading heading seen, no keyword match). Some layouts (e.g. American
        # Cancer Society's 403(b) plan) never announce a section with a leading
        # heading at all -- the only type signal is a TRAILING "Total <category>"
        # line after the block. Hold these rows here until such a line resolves
        # a type, then back-fill it onto all of them. Cleared on every section
        # boundary (a new leading heading, or ANY trailing Total-row, resolved
        # or not) so one group's rows can never leak into a later, unrelated
        # group's backfill. Mirrors the same mechanism in the camelot
        # table-based extraction loop above.
        pending_untyped_rows: List[Dict] = []
        # A fund's own name+description line with NO trailing value (e.g.
        # "Dodge & Cox Stock X Registered investment company", value wraps to
        # a later, separate line) is otherwise indistinguishable from a bare
        # category heading like "Mutual Funds:" once it falls into the
        # no-value branch below. Stash the name text here so the next line
        # that resolves to a value with no name of its own can reclaim it,
        # instead of losing it to the section-heading match. Mirrors
        # `pending_single_cell_fragments` in the camelot table-based loop
        # above, which solves the identical problem for single-cell table
        # rows. Cleared on every section boundary alongside
        # pending_untyped_rows so a stale name can never leak onto an
        # unrelated later row.
        pending_issuer_name: str = ""

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

        # Some filers print two trailing dollar columns per row -- "Fair Value"
        # then "Cost" -- reversed from the IRS's standard Cost/Current-Value
        # order that the trailing-number logic below otherwise assumes (last
        # number on the line = current value). Detected via this filer's own
        # header wording ("... Value Cost"); only fires on lines that actually
        # have two distinct trailing numbers, so single-value lines and
        # standard-order filers are unaffected.
        _value_before_cost = bool(re.search(r'\bvalue\s+cost\b', text, re.IGNORECASE))
        # Same shape of bug, different column: some filers add a supplemental
        # "Shares Held" column AFTER the standard Current Value column (e.g.
        # "... CREF Stock R2  33,385,095  36,632" = value then share count).
        # Header wording wraps unpredictably across the fixed-width columns
        # (pdfplumber often emits "...Current Shares\n...value value held"),
        # so match loosely across the gap rather than anchoring on word order.
        # Same guard as _value_before_cost: only changes behavior on lines that
        # actually have two distinct trailing numbers.
        _value_before_shares = bool(re.search(r'shares\b[\s\S]{0,120}\bheld\b', text, re.IGNORECASE))
        _dual_trailing_pattern = re.compile(r'\$?\s*([\d,]+)\s+\$?\s*([\d,]+)\s*$')

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

            # Asterisks are Form 5500 footnote markers ("**Party-in-interest",
            # "*Participant-directed", etc.). Their POSITION in the line varies by
            # filer -- sometimes right before the value (classic "TYPE ** $VALUE"
            # format), sometimes as a suffix on the fund name itself (e.g.
            # "FID 500 INDEX** 1,943,507.37shares * 396,844,770", where the fund's
            # own footnote sits well before the real value). Anchoring on the
            # marker's position grabbed whatever number happened to follow the
            # FIRST asterisk -- the share count, not the value, whenever a filer
            # put the footnote on the fund name instead of on the value. Strip all
            # asterisks and take the line's TRAILING number instead, regardless of
            # where any asterisk sat.
            dual_match = _dual_trailing_pattern.search(line) if (_value_before_cost or _value_before_shares) else None
            if dual_match:
                current_value = dual_match.group(1).replace(',', '')
                issuer_description = line[:dual_match.start()].strip()
            elif '*' in line:
                stripped = _asterisk_re.sub(' ', line)
                value_match = trailing_value_pattern.search(stripped)
            else:
                stripped = line
                value_match = None

            if dual_match:
                pass
            elif value_match:
                current_value = value_match.group(1).replace(',', '')
                issuer_description = stripped[:value_match.start()].strip()
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
                        # A trailing "Total <category>" subtotal whose amount
                        # wraps to the NEXT line (e.g. "TOTAL MUTUAL FUNDS" /
                        # "408,217,661" on separate lines) lands here too, since
                        # this line itself has no value. Check that shape FIRST:
                        # SECTION_HEADING_MAP's substring match below is a
                        # LEADING-heading heuristic and would otherwise treat
                        # "TOTAL MUTUAL FUNDS" as if "Mutual Funds" were being
                        # announced going forward, discarding pending_untyped_rows
                        # without ever backfilling them, and leaking the type
                        # onto whatever unrelated row comes next instead.
                        _is_trailing_total_heading = _is_total_line_shape(line.strip())
                        _vl_trailing_type = (
                            _detect_section_heading_text(line.strip()) if _is_trailing_total_heading else None
                        )
                        if _vl_trailing_type:
                            if pending_untyped_rows:
                                for _pending_row in pending_untyped_rows:
                                    _pending_row['asset_type'] = _vl_trailing_type
                                print(f"    Back-filled asset_type '{_vl_trailing_type}' onto "
                                      f"{len(pending_untyped_rows)} row(s) from trailing total "
                                      f"'{line.strip()}' (text-based, value on next line, page {page_num})")
                            pending_untyped_rows.clear()
                            pending_issuer_name = ""
                        else:
                            for key, val in SECTION_HEADING_MAP.items():
                                if key not in line_lower_full:
                                    continue
                                _idx = line_lower_full.index(key)
                                _before = line_lower_full[:_idx].strip(' *')
                                # No length check on what follows the matched key --
                                # a bare heading can carry innocuous trailing words
                                # (e.g. "Registered Investment Companies Shares")
                                # and must still match unconditionally, exactly as
                                # the original code did. Only _before distinguishes
                                # a real bare heading from a fund's own name+
                                # description line.
                                if len(_before) <= 1:
                                    # Bare category heading (e.g. "Mutual Funds:") --
                                    # unchanged existing behavior.
                                    current_section_type = val
                                    pending_untyped_rows.clear()
                                    pending_issuer_name = ""
                                else:
                                    # Real fund-name text precedes the description
                                    # phrase (e.g. "Dodge & Cox Stock X Registered
                                    # investment company") -- this filer wraps the
                                    # row's value onto a LATER, separate line
                                    # instead of trailing it on this one. Stash the
                                    # name instead of discarding it as a false
                                    # section-heading match.
                                    pending_issuer_name = line[:_idx].strip(' *')
                                    current_section_type = val
                                break
                        continue
                    current_value = value_match.group(1).replace(',', '')
                    issuer_description = line[:value_match.start()].strip()

            if pending_issuer_name:
                if not issuer_description.strip():
                    # This line is a bare value with no name of its own --
                    # reclaim the name stashed from the preceding name+
                    # description line (e.g. "$ 461,376,276" following
                    # "Dodge & Cox Stock X Registered investment company").
                    issuer_description = pending_issuer_name
                else:
                    # This value line already carries its own text, so the
                    # pending fragment wasn't actually followed by a bare
                    # value line as expected. Drop it rather than risk it
                    # leaking onto a later, unrelated row.
                    pass
                pending_issuer_name = ""

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
            # As in the camelot table-based loop above, _is_total_summary_label's
            # fixed category enum doesn't cover every real "Total <category>"
            # shape (e.g. "TOTAL FIXED ANNUITY CONTRACTS", "TOTAL VARIABLE
            # ANNUITY ACCOUNTS" -- American Cancer Society's 403(b) plan). Widen
            # the drop to also catch any literal "Total ..."/"Subtotal ..."/
            # "Grand total ..." line whose text resolves to a real canonical
            # asset type via _detect_section_heading_text, and use that
            # resolved type to back-fill any rows collected in
            # pending_untyped_rows since the last section boundary.
            _is_trailing_total_line = _is_total_line_shape(issuer_description)
            _trailing_type = _detect_section_heading_text(issuer_description) if _is_trailing_total_line else None
            if _is_total_summary_label(issuer_description) or _trailing_type:
                if _trailing_type and pending_untyped_rows:
                    for _pending_row in pending_untyped_rows:
                        _pending_row['asset_type'] = _trailing_type
                    print(f"    Back-filled asset_type '{_trailing_type}' onto "
                          f"{len(pending_untyped_rows)} row(s) from trailing total "
                          f"'{issuer_description}' (text-based, page {page_num})")
                pending_untyped_rows.clear()
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
                        pending_untyped_rows.clear()
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
                # Charles Schwab's own product name for its SDBA offering, seen both spelled out
                # and abbreviated ("Retment") in filer text -- kept in sync with the same rule in
                # src/asset_type_patterns.py's ROW_TYPE_PATTERNS (which the camelot table-based
                # extraction path uses but this text-fallback path did not).
                'PERSONAL CHOICE RETIREMENT ACCOUNT': 'Self-Directed Brokerage Account',
                'PERSONAL CHOICE RETMENT ACCOUNT': 'Self-Directed Brokerage Account',
                'GUARANTEED INTEREST ACCOUNT': 'Stable Value Fund',
                'GUARANTEED INCOME ACCOUNT': 'Stable Value Fund',
                'GUARANTEED INVESTMENT CONTRACT': 'Stable Value Fund',
                'INTEREST-BEARING CASH': 'Money Market Fund',
                'MONEY MARKET': 'Money Market Fund',
                'MMRK': 'Money Market Fund',
                # Vanguard's own DC-plan terminology for its Target Retirement Trusts (a CIT
                # product line, reported in "shares" like a mutual fund would be) -- kept in sync
                # with the same rule in src/asset_type_patterns.py's ROW_TYPE_PATTERNS, which the
                # camelot table-based extraction path uses but this text-fallback path did not.
                'LIFECYCLE INVESTMENT OPTION': 'Common/Collective Trust Fund',
                'LIFECYCLE INVESTMENT OPTIONS': 'Common/Collective Trust Fund',
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

            # Some filers (e.g. plans reporting "N/A" instead of a share count for
            # participant-directed investments -- no "shares"/"units" text for the
            # check above to key off) put the category label at the FRONT of the
            # identity-of-issue field on every row instead of as a section heading
            # ("Registered Investment Company Nuveen International Equity Index
            # N/A ..." -- the real fund name follows the label on the same row).
            # Anchored to the start of the line so it only fires on this literal
            # per-row layout and can't misfire on a fund name that merely mentions
            # one of these phrases elsewhere in its text.
            if not asset_type:
                _ud_stripped = issuer_description.lstrip('*').strip().upper()
                for _k, _v in asset_type_patterns.items():
                    if _ud_stripped.startswith(_k):
                        asset_type = _v
                        issuer_description = issuer_description.lstrip('*').strip()[len(_k):].strip()
                        break

            issuer_name = issuer_description.lstrip('*').rstrip('*').strip()
            if not issuer_name:
                continue

            row_num += 1
            _investment_row = {
                'issuer_name': issuer_name,
                'investment_description': '',
                'asset_type': asset_type,
                'par_value': '',
                'cost': '',
                'current_value': current_value,
                'units_or_shares': '',
                'page_number': page_num,
                'row_id': row_num,
            }
            investments.append(_investment_row)
            # Still no type after all the above? Hold onto this row so a later
            # trailing "Total <category>" line can back-fill it (see the
            # _is_total_summary_label branch above). Holds a reference to the
            # same dict just appended, so mutating it later still reaches
            # this row.
            if not _investment_row['asset_type']:
                pending_untyped_rows.append(_investment_row)

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
        r'pooled\s+separate\s+accounts?|registered\s+investment\s+compan(?:y|ies)',
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
        (r'\bregistered\s+investment\s+compan(?:y|ies)\b', 'Mutual Fund'),
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
        r'pooled\s+separate\s+accounts?|registered\s+investment\s+compan(?:y|ies)',
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


def _dedupe_duplicate_tables(tables: list) -> list:
    """Camelot's stream flavor can detect multiple overlapping table regions on a
    single page that all resolve to identical content -- seen on Trane
    Technologies' Schedule of Assets, where a single continuous, unruled table
    on page 1 was returned as three separate 40x8 tables with identical rows,
    silently tripling every extracted value when concatenated. Drop later
    tables whose full cell content exactly matches an already-kept table from
    the same page."""
    seen_by_page: Dict[int, set] = {}
    deduped = []
    for t in tables:
        page_num = int(t.page)
        signature = tuple(
            tuple(normalize_whitespace(str(cell)) for cell in row)
            for row in t.df.values.tolist()
        )
        page_signatures = seen_by_page.setdefault(page_num, set())
        if signature in page_signatures:
            print(f"    Dropping duplicate table on page {page_num} (identical to an already-extracted table)")
            continue
        page_signatures.add(signature)
        deduped.append(t)
    return deduped


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

    # Composite Master-Trust-style participation schedules: flat, unruled text
    # reports with recognized section headings + 'TOTAL <category>' subtotals.
    # Filer-agnostic detector (e.g. covers Howmet's Master Trust report).
    composite_pages = _composite_participation_schedule_pages(pdf_path, supplemental_pages)
    if composite_pages:
        participation_rows = _extract_composite_participation_rows_for_pdf(pdf_path, composite_pages)
        if participation_rows:
            print(f"    Composite participation schedule parser extracted {len(participation_rows)} investments")
            return plan_info, _build_text_result(pdf_path, participation_rows)

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
    _reportable_or_service_page_re = re.compile(
        r'REPORTABLE\s+TRANSACTIONS|SERVICE\s+PROVIDER\s+INFORMATION', re.IGNORECASE
    )
    with pdfplumber.open(pdf_path) as _doc:
        filtered_pages = []
        active_parser_profile = ""
        active_schedule_run = False
        active_structural_profile: Dict[str, str] = {}
        active_structural_asset_type = ""
        continuation_asset_types: Dict[int, str] = {}
        # Running "last section heading actually seen so far" (top-to-bottom,
        # across pages), distinct from active_structural_asset_type which is
        # frozen at whatever heading appeared FIRST on the page that started
        # this schedule run. A multi-section filer whose schedule opens with
        # "Mutual Funds" but later moves into "Common Stocks" (e.g. Chubb)
        # would otherwise have every continuation page mistyped as Mutual
        # Fund for the rest of the schedule, however many section changes
        # happen in between.
        last_seen_section_asset_type = ""
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
                active_schedule_run = True
                active_structural_profile = _infer_structural_row_profile(page_text) if is_structural_schedule else {}
                active_structural_asset_type = _infer_first_section_asset_type(page_text) if is_structural_schedule else ""
            elif _is_new_exhibit_or_schedule_page(page_text) or _reportable_or_service_page_re.search(page_text):
                active_parser_profile = ""
                active_schedule_run = False
                active_structural_profile = {}
                active_structural_asset_type = ""

            is_profile_continuation = _matches_structural_row_profile(page_text, active_structural_profile)
            # classify_pages_text already forward-fills which pages belong to the
            # active schedule using a structural signal (dollar-value-line
            # density), not an asset-type keyword -- trust that here instead of
            # re-deriving a narrower per-asset-type text pattern (the old
            # approach only ever recognized "mutual fund" and "common stock"
            # row shapes by name, silently dropping every other format's
            # continuation pages). This is a candidate filter only: the
            # per-table Camelot column-layout check
            # (_looks_like_headerless_continuation, in the main row loop below)
            # is still the real gate before any row from a candidate page gets used.
            is_continuation = (active_schedule_run and not is_target_schedule) or is_profile_continuation
            if is_target_schedule or is_continuation:
                filtered_pages.append(p)
                section_table_areas = _find_section_table_areas(_doc.pages[p - 1])
                if section_table_areas:
                    section_table_areas_by_page[p] = section_table_areas
                    last_seen_section_asset_type = section_table_areas[-1][1]
                if is_continuation and active_parser_profile:
                    continuation_parser_profiles[p] = active_parser_profile
                if is_profile_continuation and (last_seen_section_asset_type or active_structural_asset_type):
                    continuation_asset_types[p] = last_seen_section_asset_type or active_structural_asset_type
                page_value_scale[p] = _page_value_scale_factor(page_text)
        supplemental_pages = filtered_pages
    if not supplemental_pages:
        return plan_info, []

    pages_arg = ",".join(str(p) for p in supplemental_pages)
    default_pages = [p for p in supplemental_pages if p not in section_table_areas_by_page]
    tables = []
    if default_pages:
        # Stream mode infers columns from whitespace gaps, which breaks on pages
        # that have a wrapped multi-line cell (e.g. a long "Description of
        # investment" column): each wrapped line gets read as its own stray row
        # and columns can merge. Pages with real vector table lines (rects/edges
        # from ruled Schedule H layouts) parse far more reliably with lattice
        # mode, which locks onto those lines instead of guessing from
        # whitespace. Route each page to whichever flavor its own structure
        # supports; unruled pages keep using stream exactly as before.
        lattice_pages = [p for p in default_pages if _page_has_ruling_lines(pdf_path, p)]
        stream_pages = [p for p in default_pages if p not in lattice_pages]
        if lattice_pages:
            try:
                tables.extend(camelot.read_pdf(
                    pdf_path,
                    pages=",".join(str(p) for p in lattice_pages),
                    flavor="lattice",
                ))
            except Exception as _exc:
                print(f"    Camelot lattice failed on pages {lattice_pages}: {_exc}")
                stream_pages = stream_pages + lattice_pages
        if stream_pages:
            try:
                tables.extend(camelot.read_pdf(
                    pdf_path,
                    pages=",".join(str(p) for p in stream_pages),
                    flavor="stream",
                ))
            except Exception as _exc:
                print(f"    Camelot failed on default pages {stream_pages}: {_exc}")
    section_asset_type_by_table: Dict[int, str] = {}
    for page_num in supplemental_pages:
        section_table_areas = section_table_areas_by_page.get(page_num, [])
        if not section_table_areas:
            continue
        print(f"    Splitting page {page_num} into {len(section_table_areas)} section table areas")
        section_flavor = "lattice" if _page_has_ruling_lines(pdf_path, page_num) else "stream"
        for table_area, section_asset_type in section_table_areas:
            try:
                section_tables = list(camelot.read_pdf(
                    pdf_path,
                    pages=str(page_num),
                    flavor=section_flavor,
                    table_areas=[table_area],
                ))
            except Exception as _exc:
                if section_flavor == "lattice":
                    try:
                        section_tables = list(camelot.read_pdf(
                            pdf_path,
                            pages=str(page_num),
                            flavor="stream",
                            table_areas=[table_area],
                        ))
                    except Exception as _exc2:
                        print(f"    Skipping section area on page {page_num}: {_exc2}")
                        section_tables = []
                else:
                    print(f"    Skipping section area on page {page_num}: {_exc}")
                    section_tables = []
            for section_table in section_tables:
                tables.append(section_table)
                section_asset_type_by_table[id(section_table)] = section_asset_type

    # Process tables in page order (not "default-pages batch, then split-section
    # batch"). The per-row loop below tracks current_section_type/table_section_type
    # as a running state that persists across tables so continuation pages with no
    # heading of their own can inherit the type from the page before them. Since
    # split-section tables (pages with 2+ headings) are appended after the default
    # batch above, an out-of-page-order list would process a continuation page's
    # default table BEFORE the heading-bearing page's split tables ever run,
    # leaving the continuation page's rows with a blank asset_type. Sort is stable,
    # so same-page tables (e.g. multiple section areas) keep their relative order.
    tables = _dedupe_duplicate_tables(tables)
    tables = sorted(tables, key=lambda t: int(t.page))

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
            # Some filers' fonts extract with a stray space inside "maturity"
            # (e.g. "m aturity value") -- normalize it away before matching so
            # this guard still recognizes the boilerplate description-column
            # header instead of leaving it mismapped to current_value.
            h_norm = re.sub(r'\bm\s+aturity\b', 'maturity', h, flags=re.IGNORECASE)
            if re.search(r'(?:collateral.*)?par.*matur(?:ing|ity)\s+value', h_norm, re.IGNORECASE):
                column_map[idx] = 'investment_description'

    # Plan-specific column-mapping fix, scoped by plan name (not ack_id, since the
    # ack_id changes every filing year but this filer's PDF layout persists across
    # years) -- not a general rule. On this filer's Schedule H, line 4i pages,
    # Camelot's stream parser detects the "Identity of issuer, borrower, lessor, or
    # similar party" header text one column to the right of where the real issuer
    # data (e.g. "Fidelity", "TIAA Trust, N.A.") actually sits. issuer_name then
    # gets bound to the boilerplate category text ("Registered Investment Company")
    # instead, and the true issuer column is left unmapped and silently dropped --
    # which then trips the "issuer is a pure category label" heuristic in
    # remove_total_rows and deletes the row entirely. Shifts issuer_name one column
    # left only when the plan name is detected on a supplemental page; every other
    # plan's column_map is untouched.
    _PLAN_SPECIFIC_ISSUER_COLUMN_SHIFT_BY_NAME = {
        re.compile(r'SAINT\s+LOUIS\s+UNIVERSITY\s+403\(B\)\s+ANNUITY\s+PLAN', re.IGNORECASE): -1,
        # Same filer/vendor, same layout quirk, different plan at the same university.
        re.compile(r'SAINT\s+LOUIS\s+UNIVERSITY\s+RETIREMENT\s+PLAN', re.IGNORECASE): -1,
    }

    def _detect_plan_specific_column_shift(pdf_path: str, pages: List[int]) -> int:
        try:
            with pdfplumber.open(pdf_path) as _doc:
                for p in pages:
                    page_text = _doc.pages[p - 1].extract_text() or ''
                    for name_re, shift in _PLAN_SPECIFIC_ISSUER_COLUMN_SHIFT_BY_NAME.items():
                        if name_re.search(page_text):
                            return shift
        except Exception:
            return 0
        return 0

    def _apply_plan_specific_column_overrides(shift: int, column_map: Dict[int, str]) -> None:
        if not shift:
            return
        for idx, field in list(column_map.items()):
            if field == 'issuer_name':
                new_idx = idx + shift
                if new_idx >= 0 and new_idx not in column_map:
                    del column_map[idx]
                    column_map[new_idx] = 'issuer_name'
                    # The vacated column held the boilerplate asset-category text
                    # (e.g. "Registered Investment Company") that the mis-detected
                    # issuer_name header had been bound to. Fold it into
                    # investment_description (it will concatenate with the real
                    # fund-name column already mapped there) instead of dropping
                    # it, so asset-type detection still has the category keyword.
                    column_map[idx] = 'investment_description'
                break

    _plan_specific_column_shift = _detect_plan_specific_column_shift(pdf_path, supplemental_pages)

    # Plan-specific value-column bootstrap, scoped by plan name (same
    # rationale as the issuer-column-shift table above: the ack_id changes
    # every filing year but this filer's PDF layout persists). On Nouryon
    # Chemicals LLC's Schedule H, line 4i page, column (E)'s header text is
    # corrupted in the source PDF -- it literally reads "18" instead of
    # "Current Value" -- and the real header band sits above the area
    # _find_section_table_areas scanned (its header_bottom regex doesn't
    # recognize this filer's header wording), so Camelot never sees a header
    # row at all. The "Fallback to row 0" path then scores a data row
    # against header synonyms, nothing clears the 70-point threshold, and
    # column_map ends up completely empty -- every row on the page loses
    # all data. Text-based header matching can never recover this column
    # since "18" has zero overlap with any current_value synonym, so this
    # falls back to positional detection: the mostly-numeric column is
    # current_value, the column with the most non-numeric text is
    # issuer_name. Only engages when this plan is detected on a
    # supplemental page AND header-text matching found neither column, so
    # it can never override a table that already mapped correctly.
    _PLAN_SPECIFIC_VALUE_COLUMN_BOOTSTRAP_BY_NAME = (
        re.compile(r'NOURYON\s+CHEMICALS', re.IGNORECASE),
    )

    def _detect_plan_specific_value_bootstrap(pdf_path: str, pages: List[int]) -> bool:
        try:
            with pdfplumber.open(pdf_path) as _doc:
                for p in pages:
                    page_text = _doc.pages[p - 1].extract_text() or ''
                    for name_re in _PLAN_SPECIFIC_VALUE_COLUMN_BOOTSTRAP_BY_NAME:
                        if name_re.search(page_text):
                            return True
        except Exception:
            return False
        return False

    _plan_specific_value_bootstrap = _detect_plan_specific_value_bootstrap(pdf_path, supplemental_pages)

    def _bootstrap_missing_value_and_issuer_columns(enabled: bool, df, data_start_row: int, column_map: Dict[int, str]) -> Dict[int, str]:
        if not enabled or df.shape[0] <= data_start_row:
            return column_map
        if 'current_value' in column_map.values() or 'issuer_name' in column_map.values():
            return column_map
        sample = df.iloc[data_start_row:]
        numeric_pattern = r"\$?\s*\(?\s*[0-9][0-9,]*(?:\.[0-9]+)?\)?"

        def _ratio(col_idx, matches_numeric):
            cells = [normalize_whitespace(str(v)) for v in sample.iloc[:, col_idx].tolist()]
            non_empty = [c for c in cells if c]
            if not non_empty:
                return 0.0, non_empty
            is_numeric = [bool(re.fullmatch(numeric_pattern, c)) for c in non_empty]
            matches = sum(is_numeric) if matches_numeric else sum(not m for m in is_numeric)
            return matches / len(non_empty), non_empty

        best_value_col, best_value_ratio = None, 0.0
        for col_idx in range(df.shape[1] - 1, -1, -1):
            ratio, _ = _ratio(col_idx, matches_numeric=True)
            if ratio > best_value_ratio:
                best_value_col, best_value_ratio = col_idx, ratio
        if best_value_col is None or best_value_ratio < 0.5:
            return column_map

        # Restrict the issuer search to the rows where the value column is
        # actually populated -- on this layout, pure asset-type-category
        # label rows (e.g. "COMMON/COLLECTIVE TRUST") share the table with
        # real data rows but land on alternating physical rows with an
        # empty value cell, so counting non-empty text across ALL rows
        # picks the category-label column instead of the true issuer
        # column (which is blank on those same label rows).
        value_cells = [normalize_whitespace(str(v)) for v in sample.iloc[:, best_value_col].tolist()]
        data_row_mask = [bool(re.fullmatch(numeric_pattern, c)) for c in value_cells]
        best_issuer_col, best_issuer_nonempty = None, 0
        for col_idx in range(df.shape[1]):
            if col_idx == best_value_col:
                continue
            cells = [normalize_whitespace(str(v)) for v in sample.iloc[:, col_idx].tolist()]
            non_empty = sum(1 for c, is_data_row in zip(cells, data_row_mask) if is_data_row and c)
            if non_empty > best_issuer_nonempty:
                best_issuer_col, best_issuer_nonempty = col_idx, non_empty
        if best_issuer_col is None or best_issuer_nonempty == 0:
            return column_map

        column_map = dict(column_map)
        column_map[best_value_col] = 'current_value'
        column_map[best_issuer_col] = 'issuer_name'
        print(f"    Plan-specific bootstrap: mapped column {best_issuer_col} -> issuer_name, column {best_value_col} -> current_value (header text unrecoverable)")
        return column_map

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

    def _verify_or_remap_description_column(df, data_start_row: int, column_map: Dict[int, str]) -> Tuple[Dict[int, str], Optional[int]]:
        """A wrapped multi-line header cell (e.g. Form 5500 column (c)'s
        "Description of investment including maturity date, rate of
        interest, collateral, par, or maturity value") can lead Camelot's
        stream flavor to infer a wider column boundary for the header than
        the data rows underneath actually use, so the header text lands one
        column over from where the row-level description text sits (seen on
        Syracuse University's Schedule H, line 4i table: header cell at
        column 3, but every data row's description text is in column 2,
        leaving column 3 -- and therefore investment_description -- blank
        for every row). Verify the mapped description column actually has
        text in the sampled data rows; if it's empty, retarget to whichever
        immediately adjacent unmapped column has description-like
        (non-numeric) text in most sampled rows.

        Also returns a secondary "sibling" column index, if one is found.
        Camelot's stream flavor can split the description text across TWO
        adjacent columns inconsistently row-to-row -- not just wrong for the
        whole table -- when a longer issuer name pushes the following text
        past the inferred column boundary (seen on Penn's Supplemental
        Retirement Annuity Plan: "Mutual Fund" / "Pooled Separate Account"
        lands in one column for most rows but the column next to it for
        rows with longer issuer names, e.g. "TIAA Traditional Benefit
        Responsive Annuity"). The caller uses this sibling column as a
        per-row fallback when the primary description column is blank for
        that specific row, rather than silently losing the value.
        """
        desc_col = next((idx for idx, f in column_map.items() if f == 'investment_description'), None)
        if desc_col is None or df.shape[0] <= data_start_row:
            return column_map, None
        sample = df.iloc[data_start_row:data_start_row + 15]

        def _text_ratio(col_idx):
            if col_idx < 0 or col_idx >= df.shape[1]:
                return 0.0
            cells = [normalize_whitespace(str(v)) for v in sample.iloc[:, col_idx].tolist()]
            non_empty = [c for c in cells if c]
            if not non_empty:
                return 0.0
            matches = sum(
                1 for c in non_empty
                if not re.fullmatch(r"\$?\s*\(?\s*[0-9][0-9,]*(?:\.[0-9]+)?\)?", c)
            )
            return matches / len(non_empty)

        if _text_ratio(desc_col) < 0.3:
            for candidate in (desc_col - 1, desc_col + 1):
                if candidate in column_map:
                    continue
                if _text_ratio(candidate) >= 0.5:
                    column_map = dict(column_map)
                    del column_map[desc_col]
                    column_map[candidate] = 'investment_description'
                    print(f"    Remapped investment_description column {desc_col} -> {candidate} (header cell empty in data rows)")
                    desc_col = candidate
                    break

        sibling_col = None
        for candidate in (desc_col - 1, desc_col + 1):
            if candidate < 0 or candidate >= df.shape[1] or candidate in column_map:
                continue
            if _text_ratio(candidate) >= 0.3:
                sibling_col = candidate
                break

        return column_map, sibling_col

    # Persists across pages: once a section heading is seen, all following rows
    # inherit its type until a new heading overrides it
    current_section_type = ""
    previous_column_map: Dict[int, str] = {}
    previous_column_map_page: Optional[int] = None
    pending_single_cell_fragments: Dict[int, str] = {}
    # Rows appended to mapped_pages with no asset_type resolved yet (no leading
    # heading, no Type column value). Some layouts (e.g. American Cancer Society's
    # 403(b) plan) never announce a section with a leading heading at all -- the
    # only type signal is a TRAILING "Total <category>" line after the block. Hold
    # these rows here until such a line resolves a type, then back-fill it onto all
    # of them. Cleared on every section boundary (a new leading heading, or ANY
    # trailing Total-row, resolved or not) so one group's rows can never leak into
    # a later, unrelated group's backfill.
    pending_untyped_rows: List[Dict] = []

    for table in tables:
        pending_single_cell_fragments.clear()
        # Tracks whether a row-level section-heading label (e.g. "REGISTERED
        # INVESTMENT COMPANY") has been seen within THIS table, as opposed to
        # inherited from a previous table's running current_section_type.
        # Reset per table so a fresh, more specific in-row signal can outrank
        # this table's own coarse table_section_asset_type default (see the
        # priority check below), without letting current_section_type's
        # cross-table persistence (needed for headerless continuation pages)
        # wrongly override a genuinely different table_section_asset_type on
        # a later table that hasn't announced its own heading yet.
        section_type_seen_in_table = False
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
                    for kw in ['description of investment', 'maturity date', 'rate of interest', 'current']
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
        desc_fallback_col: Optional[int] = None

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
            _apply_plan_specific_column_overrides(_plan_specific_column_shift, column_map)
            column_map, desc_fallback_col = _verify_or_remap_description_column(df, data_start_row, column_map)

            if column_map:
                previous_column_map = dict(column_map)
                previous_column_map_page = page_num
        elif table_section_asset_type and previous_column_map:
            # This table's own row 0 is a known section-heading label (that's how
            # table_section_asset_type got set), not a real header row, even though
            # the generic header-detection above found nothing. Falling through to
            # the "no header_rows" fallback below would treat that heading text as
            # a header and fuzzy-match it against column-header synonyms -- e.g.
            # "Cash Equivalent" scores 80 against the current_value synonym "value",
            # producing a garbage single-column map that then corrupts every row in
            # this split section. Reuse the running column map instead (not
            # restricted to same-page: this is often the first section table on a
            # freshly-split page, so no page-page match exists yet).
            column_map = _verify_or_remap_value_column(df, 0, dict(previous_column_map))
            data_start_row = 0
            reused_previous_column_map = True
            print(f"    Reusing running column map for section table on page {page_num}")
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
            _apply_plan_specific_column_overrides(_plan_specific_column_shift, column_map)
            column_map, desc_fallback_col = _verify_or_remap_description_column(df, data_start_row, column_map)
            column_map = _bootstrap_missing_value_and_issuer_columns(_plan_specific_value_bootstrap, df, data_start_row, column_map)

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
                if not matched and _HEADING_OFFERED_BY_RE.match(candidate):
                    matched = 'Mutual Fund'
                if matched:
                    current_section_type = matched
                    section_type_seen_in_table = True
                    pending_single_cell_fragments.clear()
                    pending_untyped_rows.clear()
                    print(f"    Section heading: '{matched}' (row {row_idx})")
                elif re.fullmatch(r"\$?\s*\(?\s*[0-9][0-9,]*(?:\.[0-9]+)?\)?", candidate_text):
                    # A one-cell numeric row is usually a subtotal/duplicate value line,
                    # not a split name. Do not attach it to the next investment row.
                    pending_single_cell_fragments.clear()
                elif _is_total_labeled_subtotal(candidate_text) or _is_value_then_total_label(candidate_text):
                    # A "Total <section name>: <amount>" subtotal line for a
                    # section name not covered by _TOTAL_CATEGORY_RE's fixed
                    # enum (see _TOTAL_LABELED_SUBTOTAL_RE above), in either
                    # label-then-amount or amount-then-label order (the latter
                    # is how Camelot emits BASF's per-section subtotals -- see
                    # _VALUE_THEN_TOTAL_LABEL_RE above). Same treatment as the
                    # bare-numeric case: drop it, do not merge it onto the
                    # next row's value cell. This is still a section boundary --
                    # clear pending_untyped_rows so an unresolved label here can't
                    # let an earlier group's rows get backfilled by a later one.
                    pending_single_cell_fragments.clear()
                    pending_untyped_rows.clear()
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
                    if pending_col_idx >= len(row):
                        continue
                    current_text = normalize_whitespace(str(row[pending_col_idx]))
                    if current_text:
                        row[pending_col_idx] = normalize_whitespace(f"{fragment} {current_text}")
                        continue
                    shifted_col = pending_col_idx + 1
                    shifted_text = (
                        normalize_whitespace(str(row[shifted_col])) if shifted_col < len(row) else ""
                    )
                    if shifted_text and column_map.get(shifted_col) is None:
                        # Camelot shifted this row's data one column right of where the
                        # fragment's own column sits (e.g. individual fund names indented
                        # under a manager sub-heading like "TIAA-CREF:"). The stale label
                        # doesn't belong here -- drop it and let the shifted-column
                        # recovery below pick up the real value instead of overwriting it.
                        continue
                    row[pending_col_idx] = fragment
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

            # Camelot's stream flavor can split description text across two adjacent
            # columns inconsistently row-to-row (see _verify_or_remap_description_column).
            # If the primary description column came up blank for THIS row, check the
            # sibling column before giving up on it.
            if not row_data.get('investment_description') and desc_fallback_col is not None and desc_fallback_col < len(row):
                fallback_text = normalize_whitespace(str(row[desc_fallback_col]))
                if fallback_text and not re.fullmatch(r"\$?\s*\(?\s*[0-9][0-9,]*(?:\.[0-9]+)?\)?", fallback_text):
                    row_data['investment_description'] = fallback_text

            # Recover a fund name Camelot shifted one column right of where the header
            # says issuer_name lives -- happens when a row is visually indented under a
            # manager sub-heading (e.g. individual funds listed under "TIAA-CREF:"), so
            # its column boundaries don't line up with the header row's. Only fires when
            # the mapped column truly has nothing AND the very next column is completely
            # unmapped, so this never overwrites or steals a value from another field.
            if not row_data.get('issuer_name'):
                issuer_col = next((c for c, f in column_map.items() if f == 'issuer_name'), None)
                if issuer_col is not None:
                    shifted_col = issuer_col + 1
                    if shifted_col < len(row) and column_map.get(shifted_col) is None:
                        shifted_text = normalize_whitespace(str(row[shifted_col]))
                        if shifted_text and not re.fullmatch(
                            r"\$?\s*\(?\s*[0-9][0-9,]*(?:\.[0-9]+)?\)?", shifted_text
                        ):
                            row_data['issuer_name'] = shifted_text

            # Strip party-in-interest marker (*) from issuer name — column (a) in Form 5500
            if row_data.get('issuer_name'):
                row_data['issuer_name'] = row_data['issuer_name'].lstrip('* ').strip()

            for _field in ('investment_description', 'issuer_name'):
                _val = row_data.get(_field, '')
                if _val and _HEADING_PREFIX_RE.match(_val):
                    row_data[_field] = _HEADING_PREFIX_RE.sub('', _val).strip()
                    current_section_type = 'Mutual Fund'
                    section_type_seen_in_table = True
                    pending_untyped_rows.clear()
                    print(f"    Section heading prefix stripped from {_field} (row {row_idx})")

            value_scale = page_value_scale.get(page_num, 1)
            if value_scale != 1 and row_data.get('current_value'):
                row_data['current_value'] = _scale_currency_string(row_data['current_value'], value_scale)

            # If this row is just an asset-type section heading, record the type and skip it
            section_type = _detect_section_heading(row_data, fields)
            if section_type is not None:
                current_section_type = section_type
                section_type_seen_in_table = True
                pending_untyped_rows.clear()
                print(f"    Section heading detected: '{section_type}' (row {row_idx})")
                continue

            # "X offered by <provider>" heading rows can carry a stray value in
            # some layouts (see Brown University's "Mutual funds offered by
            # Fidelity: BrokerageLink ..." row), so _detect_section_heading's
            # value-free check above misses them. Catch them here by text
            # shape regardless of whether a value landed on the row.
            _issuer_or_desc_field = 'issuer_name' if row_data.get('issuer_name') else 'investment_description'
            _issuer_or_desc = row_data.get(_issuer_or_desc_field) or ''
            if _HEADING_OFFERED_BY_RE.match(normalize_whitespace(str(_issuer_or_desc)).rstrip(':').strip()):
                current_section_type = 'Mutual Fund'
                section_type_seen_in_table = True
                pending_untyped_rows.clear()
                # Camelot can fuse this heading directly onto the FIRST real data
                # row of its section rather than emitting it as its own row (see
                # Brown University's "Mutual funds offered by Fidelity:
                # BrokerageLink Fidelity Fund" and "...Teachers Insurance and
                # Annuity Association: John Hancock Funds III..." rows) -- in
                # that shape the row also carries a real fund name and dollar
                # value that would be silently lost by unconditionally dropping
                # the row. Strip just the heading clause (through its trailing
                # colon) and keep processing the row if anything real remains.
                _stripped = _HEADING_OFFERED_BY_STRIP_RE.sub('', normalize_whitespace(str(_issuer_or_desc))).strip()
                if _stripped:
                    row_data[_issuer_or_desc_field] = _stripped
                    print(f"    Section heading (offered-by) prefix stripped, kept fused data row (row {row_idx})")
                else:
                    print(f"    Section heading (offered-by, heading-only row): 'Mutual Fund' (row {row_idx})")
                    continue

            # "Total <Provider>" / "Total <Category>" subtotal rows are not
            # individual holdings -- drop them rather than let them leak into
            # the data as a fake row (Brown's "Total Fidelity" $270,880,520,
            # "Total Transamerica" $723,006).
            # _is_total_summary_label's category enum and _is_total_provider_label's
            # provider-shape check both have deliberately narrow coverage (the
            # latter excludes "annuity" outright, to avoid dropping a real fund
            # named e.g. "XYZ Variable Annuity Fund"), so neither one recognizes
            # a line like "TOTAL FIXED ANNUITY CONTRACTS" or "TOTAL VARIABLE
            # ANNUITY ACCOUNTS" (American Cancer Society's 403(b) plan). Catch
            # those here: any literal "Total ..."/"Subtotal ..."/"Grand total ..."
            # line whose text (after stripping that prefix) resolves to a real
            # canonical asset type via _detect_section_heading_text is safe to
            # treat as a subtotal too -- a genuine fund name only starting with
            # "Total" (e.g. "Total Return Fund") strips down to something that
            # does NOT resolve to a canonical type, so it is left alone.
            _is_trailing_total_line = _is_total_line_shape(_issuer_or_desc)
            _trailing_type = _detect_section_heading_text(_issuer_or_desc) if _is_trailing_total_line else None
            if _is_total_summary_label(_issuer_or_desc) or _is_total_provider_label(_issuer_or_desc) or _trailing_type:
                # Some layouts (e.g. American Cancer Society's 403(b) plan)
                # never announce a section with a leading heading -- the only
                # type signal is this trailing "Total <category>" line. If it
                # resolves to a real canonical type, back-fill it onto every
                # row collected in pending_untyped_rows since the last section
                # boundary. Either way (resolved or not), this line marks a
                # boundary, so the pending list is cleared here.
                if _trailing_type and pending_untyped_rows:
                    for _pending_row in pending_untyped_rows:
                        _pending_row['asset_type'] = _trailing_type
                    print(f"    Back-filled asset_type '{_trailing_type}' onto "
                          f"{len(pending_untyped_rows)} row(s) from trailing total "
                          f"'{_issuer_or_desc}' (row {row_idx})")
                pending_untyped_rows.clear()
                print(f"    Subtotal row dropped: '{_issuer_or_desc}' (row {row_idx})")
                continue

            # Propagate the current section type to rows with blank asset_type, or to
            # rows whose own 'asset_type' cell holds a non-canonical investment-style
            # label (e.g. "Domestic equities", "Multi-strategy funds") rather than a
            # real DOL vehicle-type declaration -- happens when a PDF's per-row column
            # in this position is actually a style/category column, not a Type column
            # (e.g. Brown University's "Mutual funds offered by Teachers Insurance and
            # Annuity Association" section). The section heading is the authoritative
            # vehicle-type signal; an in-row value only overrides it when that value
            # itself resolves to a real canonical type.
            _row_asset_type = row_data.get('asset_type', '')
            if _is_blank_asset_type(_row_asset_type) or not detect_asset_type_strict(_row_asset_type):
                # A row-level heading seen WITHIN this table (e.g. "REGISTERED
                # INVESTMENT COMPANY" between individual fund rows) is more
                # specific than this table's own table_section_asset_type default
                # (set once for the whole Camelot-detected area, which can span
                # multiple real asset-type sections when area-splitting only
                # found the page's coarse headings -- see Nouryon Chemicals
                # LLC's Schedule H, 4i page, where one merged "Mutual Fund" area
                # actually contains Common/Collective Trust, Registered
                # Investment Company, Managed Separate Account, and Interest
                # Bearing Cash rows). Prefer it when present; otherwise fall
                # back to table_section_asset_type as before (needed for
                # continuation pages/tables with no heading of their own yet).
                if section_type_seen_in_table and current_section_type:
                    row_data['asset_type'] = current_section_type
                elif table_section_asset_type:
                    row_data['asset_type'] = table_section_asset_type
                elif current_section_type:
                    row_data['asset_type'] = current_section_type

            # Still no type after the forward-stamp? Hold onto this row so a
            # later trailing "Total <category>" line can back-fill it (see
            # the _is_total_summary_label/_is_total_provider_label branch
            # above). Holds a reference to the same dict that's about to be
            # appended below, so mutating it later still reaches this row.
            if _is_blank_asset_type(row_data.get('asset_type', '')):
                pending_untyped_rows.append(row_data)

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
        # Names/descriptions are already properly formatted, but asset_type still needs
        # the same per-row override the table path gets below (a row's own explicit
        # vehicle-type declaration, e.g. "Registered Investment Company", must win over
        # a wrongly-propagated section type) -- pages with no standalone section-heading
        # line (type stated inline per row instead) never get typed correctly otherwise.
        cleaned_text_rows = []
        for row in rows:
            parsed = parse_investment_row(row)
            row['asset_type'] = parsed['asset_type']
            cleaned_text_rows.append(row)
        result.append(
            {
                "pdf": pdf_path,
                "pdf_stem": pdf_path.split("/")[-1].rsplit(".", 1)[0],
                "page_number": page_num,
                "mapped_rows": cleaned_text_rows,
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

    # --- Diversified-holdings demotion for 'Employer Stock' ---
    # A "Common Stock" heading or a per-row single-security regex match (name + share
    # count) assumes the row IS the plan's own employer stock -- true for ESOPs/stock
    # funds, which show exactly one company name repeated. But self-directed brokerage
    # schedules holding many DIFFERENT companies' individual stocks (sometimes even fund
    # names) trip the same per-row match and get every holding mistyped as Employer
    # Stock. Distinct-issuer count is a cheap, reliable signal: a real employer-stock
    # fund never shows more than a couple of distinct names in one filing; a diversified
    # brokerage sleeve routinely shows dozens.
    _emp_stock_rows = [
        _r for _entry in result for _r in _entry.get("mapped_rows", [])
        if normalize_whitespace(str(_r.get("asset_type", "") or "")).strip() == "Employer Stock"
    ]
    _distinct_issuers = {
        normalize_whitespace(str(_r.get("issuer_name") or _r.get("investment_description") or "")).strip().upper()
        for _r in _emp_stock_rows
    }
    _distinct_issuers.discard("")
    if len(_distinct_issuers) > 3:
        for _r in _emp_stock_rows:
            _r["asset_type"] = "Common Stock"
        print(f"    Demoted {len(_emp_stock_rows)} 'Employer Stock' rows ({len(_distinct_issuers)} distinct issuers) to 'Common Stock' -- diversified holdings, not a single employer-stock fund")

    return plan_info, result
