"""Manual page-selection overrides for the LLM extraction path.

A human reviewer can specify exactly which pages of a plan's PDF contain the
Schedule H, Line 4i investment schedule, bypassing automatic page detection
(classify_pages_text/expand_continuation_pages) entirely for that plan. Keyed
by ack_id (the PDF filename stem), same identifier used everywhere else in
the pipeline.

File format (JSON):
    {"<ack_id>": [23, 24, 25, 26, 27, 28], ...}
"""
import json
import os


def load_page_overrides(path: str) -> dict:
    if not path or not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        return {}
    overrides = {}
    for ack_id, pages in data.items():
        if isinstance(pages, list):
            overrides[str(ack_id).strip()] = sorted({int(p) for p in pages})
    return overrides
