import json
import re
from typing import Dict, List, Tuple

from dotenv import load_dotenv
from rapidfuzz import process, fuzz

from .llm_provider import call_llm_json
from .utils import load_yaml, normalize_whitespace, sort_cells_to_rows
from .text_extract import _page_value_scale_factor, _scale_currency_string

_HEADER_FRAGMENT_KEYWORDS = [
    "issue", "issuer", "borrower", "lessor", "similar party", "identity",
    "description", "investment", "maturity date",
    "number of units", "units", "shares", "par",
    "cost", "current value", "value", "rate of interest",
]
_HEADER_LABEL_RE = re.compile(r"^\(?[a-e]\)?$", re.IGNORECASE)


def _is_header_fragment_row(row: List[Dict]) -> bool:
    row_text = normalize_whitespace(" ".join(c.get("text", "") for c in row)).strip()
    if not row_text:
        return False
    lowered = row_text.lower()
    if _HEADER_LABEL_RE.match(lowered):
        return True
    if any(k in lowered for k in _HEADER_FRAGMENT_KEYWORDS):
        # A header fragment names a column, not a specific holding -- bail
        # out if the row also carries a dollar amount or a long digit run,
        # which marks it as an actual data row that merely mentions a
        # header word in passing.
        if re.search(r"\$\s?\d", row_text) or re.search(r"\d{4,}", row_text):
            return False
        return True
    return False


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


def _detect_header_row(rows: List[List[Dict]]) -> int:
    for i, row in enumerate(rows):
        row_text = " ".join([c.get("text", "") for c in row]).lower()
        if any(k in row_text for k in ["issuer", "description", "current value", "value", "cost"]):
            return i
    return 0


def _collect_header_row_range(rows: List[List[Dict]], anchor_idx: int) -> Tuple[int, int]:
    # Wrapped column headers often OCR as several separate word-clustered
    # rows above and below the row _detect_header_row() happens to anchor on
    # (e.g. "(a)", "(b) Identity of issue, borrower, lessor or similar
    # party", "(c) Description of investment...", "Number of units", "(d)(e)
    # Current", "Cost**", "value"). Expand outward from the anchor in both
    # directions while neighboring rows still look like header fragments, so
    # every physical line of a multi-line header gets merged into one
    # logical header block instead of only the single anchor row.
    start = anchor_idx
    while start - 1 >= 0 and _is_header_fragment_row(rows[start - 1]):
        start -= 1
    end = anchor_idx
    while end + 1 < len(rows) and _is_header_fragment_row(rows[end + 1]):
        end += 1
    return start, end


def _llm_normalize_headers(provider: str, model: str, headers: List[str], schema_fields: List[str]) -> Dict[int, str]:
    prompt = {
        "headers": headers,
        "schema_fields": schema_fields,
        "instruction": "Map each header to the best matching schema field or null. Return JSON with keys as the header's 0-based index (as a string, e.g. \"0\") in the headers list, and value as schema field or null."
    }
    try:
        text = call_llm_json(prompt, provider, model)
        data = json.loads(text)
    except Exception:
        return {}

    # Not every provider reliably returns integer-index keys as instructed
    # (Gemini has been observed echoing the header text itself as the key
    # instead) -- fall back to matching the key against the headers list by
    # text so a differently-shaped-but-still-valid response isn't silently
    # discarded.
    result = {}
    for k, v in data.items():
        if not v or v not in schema_fields:
            continue
        idx = None
        if isinstance(k, str) and k.isdigit():
            idx = int(k)
        elif isinstance(k, int):
            idx = k
        elif k in headers:
            idx = headers.index(k)
        if idx is not None and 0 <= idx < len(headers):
            result[idx] = v
    return result


def map_rows_with_llm(pages: List[Dict], schema_yml: str, model: str, use_llm: bool = True, provider: str = "openai") -> List[Dict]:
    cfg = load_yaml(schema_yml)
    fields = cfg["schema"]["fields"]
    synonyms = cfg["schema"]["header_synonyms"]

    out = []
    for page in pages:
        rows = sort_cells_to_rows(page.get("ocr_cells", []))
        if not rows:
            page["mapped_rows"] = []
            out.append(page)
            continue

        anchor_idx = _detect_header_row(rows)
        header_start, header_end = _collect_header_row_range(rows, anchor_idx)
        first_data_idx = header_end + 1

        # Word-clustering pages (no ruled grid) tag each cell with the true
        # column band it was bucketed into via col_idx; a column whose cell
        # is blank for a given row is simply absent from that row's cell
        # list, so list position alone doesn't tell you which column a cell
        # belongs to (it drifts left with every earlier blank column). Cells
        # from the ruled-grid path have no col_idx, so fall back to list
        # position there, matching the previous (still-correct) behavior.
        # A multi-line header can spread a single column's label across
        # several physical rows (e.g. "(c) Description of investment" then
        # "including maturity date" on the next line) -- merge every
        # fragment that shares a col_idx into one combined label, in
        # top-to-bottom reading order, so fuzzy-matching sees the whole
        # label instead of whichever fragment happened to land in a given
        # row.
        merged_by_col: Dict[int, List[str]] = {}
        for row in rows[header_start:header_end + 1]:
            for i, cell in enumerate(row):
                text = normalize_whitespace(cell.get("text", ""))
                if not text:
                    continue
                col_idx = cell.get("col_idx", i)
                merged_by_col.setdefault(col_idx, []).append(text)

        header_col_idx = sorted(merged_by_col.keys())
        header_text = [" ".join(merged_by_col[c]) for c in header_col_idx]

        column_map = {}
        for i, h in enumerate(header_text):
            field, score = _best_header_match(h, synonyms)
            if field and score >= 70:
                column_map[header_col_idx[i]] = field

        if use_llm:
            llm_map = _llm_normalize_headers(provider, model, header_text, fields)
            for k, v in llm_map.items():
                column_map[header_col_idx[k]] = v

        # OCR pages have no single full-page text blob the way the primary
        # text-extraction path does, so build one from what's already been
        # OCR'd for other purposes: classify_pages' top-of-page keyword scan
        # (header_text) plus every word run_ocr already pulled off this page
        # (ocr_cells). Reuses text_extract.py's own "in thousands"/"in
        # millions" detection rather than duplicating that regex.
        scale_probe_text = " ".join([
            page.get("header_text", ""),
            " ".join(c.get("text", "") for c in page.get("ocr_cells", [])),
        ])
        scale_factor = _page_value_scale_factor(scale_probe_text)

        mapped_rows = []
        for row_idx, row in enumerate(rows[first_data_idx:], start=1):
            row_data = {f: "" for f in fields}
            row_data["page_number"] = page["page_number"]
            row_data["row_id"] = row_idx

            for i, cell in enumerate(row):
                text = normalize_whitespace(cell.get("text", ""))
                if not text:
                    continue
                col_idx = cell.get("col_idx", i)
                field = column_map.get(col_idx)
                if field:
                    if row_data[field]:
                        row_data[field] = normalize_whitespace(row_data[field] + " " + text)
                    else:
                        row_data[field] = text

            if scale_factor != 1 and row_data.get("current_value"):
                row_data["current_value"] = _scale_currency_string(row_data["current_value"], scale_factor)

            mapped_rows.append(row_data)

        # A value that visually sits on the same printed line as the fund
        # name above it can still land in its own word-cluster row (e.g. a
        # lone "-" whose glyph height/position throws off the row-clustering
        # tolerance in _cluster_words_to_rows). If a row has no issuer,
        # description or cost of its own -- just a value -- and the row
        # before it is still missing a current value, treat it as that
        # row's value instead of leaving it as a standalone phantom row.
        merged_rows: List[Dict] = []
        for row_data in mapped_rows:
            is_orphan_value = (
                not row_data.get("issuer_name")
                and not row_data.get("investment_description")
                and not row_data.get("cost")
                and row_data.get("current_value")
            )
            if is_orphan_value and merged_rows and not merged_rows[-1].get("current_value"):
                merged_rows[-1]["current_value"] = row_data["current_value"]
                continue
            merged_rows.append(row_data)

        for i, row_data in enumerate(merged_rows, start=1):
            row_data["row_id"] = i

        page["mapped_rows"] = merged_rows
        out.append(page)
    return out
