import json
from typing import Dict, List, Tuple

import camelot
import pdfplumber
from openai import OpenAI
from rapidfuzz import process, fuzz

from .utils import load_yaml, normalize_whitespace


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


def _llm_normalize_headers(client: OpenAI, model: str, headers: List[str], schema_fields: List[str]) -> Dict[int, str]:
    prompt = {
        "headers": headers,
        "schema_fields": schema_fields,
        "instruction": "Map each header to the best matching schema field or null. Return JSON with keys as header index and value as schema field or null.",
    }
    response = client.responses.create(
        model=model,
        input=json.dumps(prompt),
    )
    text = response.output_text
    try:
        data = json.loads(text)
        return {int(k): v for k, v in data.items() if v}
    except Exception:
        return {}


def extract_tables_and_map(
    pdf_path: str,
    supplemental_pages: List[int],
    schema_yml: str,
    model: str,
    use_llm: bool = True,
) -> List[Dict]:
    cfg = load_yaml(schema_yml)
    fields = cfg["schema"]["fields"]
    synonyms = cfg["schema"]["header_synonyms"]

    if not supplemental_pages:
        return []

    pages_arg = ",".join(str(p) for p in supplemental_pages)
    tables = camelot.read_pdf(pdf_path, pages=pages_arg, flavor="stream")

    client = OpenAI() if use_llm else None
    mapped_pages: Dict[int, List[Dict]] = {}

    for table in tables:
        df = table.df
        if df.shape[0] < 2:
            continue
        header = [normalize_whitespace(h) for h in df.iloc[0].tolist()]

        column_map = {}
        for i, h in enumerate(header):
            field, score = _best_header_match(h, synonyms)
            if field and score >= 70:
                column_map[i] = field

        if use_llm and client is not None:
            llm_map = _llm_normalize_headers(client, model, header, fields)
            for k, v in llm_map.items():
                column_map[k] = v

        page_num = int(table.page)
        for row_idx in range(1, df.shape[0]):
            row_data = {f: "" for f in fields}
            row_data["page_number"] = page_num
            row_data["row_id"] = row_idx
            row = df.iloc[row_idx].tolist()
            for col_idx, cell in enumerate(row):
                text = normalize_whitespace(str(cell))
                if not text:
                    continue
                field = column_map.get(col_idx)
                if field:
                    if row_data[field]:
                        row_data[field] = normalize_whitespace(row_data[field] + " " + text)
                    else:
                        row_data[field] = text
            mapped_pages.setdefault(page_num, []).append(row_data)

    result = []
    for page_num, rows in mapped_pages.items():
        result.append(
            {
                "pdf": pdf_path,
                "pdf_stem": pdf_path.split("/")[-1].rsplit(".", 1)[0],
                "page_number": page_num,
                "mapped_rows": rows,
                "ocr_cells": [],
            }
        )
    return result
