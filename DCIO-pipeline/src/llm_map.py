import json
from typing import Dict, List, Tuple

from dotenv import load_dotenv
from openai import OpenAI
from rapidfuzz import process, fuzz

from .utils import load_yaml, normalize_whitespace, sort_cells_to_rows
from .text_extract import _page_value_scale_factor, _scale_currency_string


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


def _llm_normalize_headers(client: OpenAI, model: str, headers: List[str], schema_fields: List[str]) -> Dict[int, str]:
    prompt = {
        "headers": headers,
        "schema_fields": schema_fields,
        "instruction": "Map each header to the best matching schema field or null. Return JSON with keys as header index and value as schema field or null."
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
    )
    text = response.choices[0].message.content
    try:
        data = json.loads(text)
        return {
            int(k): v for k, v in data.items()
            if v and v in schema_fields
        }
    except Exception:
        return {}


def map_rows_with_llm(pages: List[Dict], schema_yml: str, model: str, use_llm: bool = True) -> List[Dict]:
    cfg = load_yaml(schema_yml)
    fields = cfg["schema"]["fields"]
    synonyms = cfg["schema"]["header_synonyms"]

    client = OpenAI() if use_llm else None

    out = []
    for page in pages:
        rows = sort_cells_to_rows(page.get("ocr_cells", []))
        if not rows:
            page["mapped_rows"] = []
            out.append(page)
            continue

        header_idx = _detect_header_row(rows)
        header = rows[header_idx]
        header_text = [normalize_whitespace(c.get("text", "")) for c in header]
        # Word-clustering pages (no ruled grid) tag each cell with the true
        # column band it was bucketed into via col_idx; a column whose cell
        # is blank for a given row is simply absent from that row's cell
        # list, so list position alone doesn't tell you which column a cell
        # belongs to (it drifts left with every earlier blank column). Cells
        # from the ruled-grid path have no col_idx, so fall back to list
        # position there, matching the previous (still-correct) behavior.
        header_col_idx = [c.get("col_idx", i) for i, c in enumerate(header)]

        column_map = {}
        for i, h in enumerate(header_text):
            field, score = _best_header_match(h, synonyms)
            if field and score >= 70:
                column_map[header_col_idx[i]] = field

        if use_llm and client is not None:
            llm_map = _llm_normalize_headers(client, model, header_text, fields)
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
        for row_idx, row in enumerate(rows[header_idx + 1 :], start=1):
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

        page["mapped_rows"] = mapped_rows
        out.append(page)
    return out
