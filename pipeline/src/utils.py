import json
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import yaml


def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def read_env(name: str, default: str | None = None) -> str:
    val = os.environ.get(name, default)
    if val is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def to_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=True)


def parse_currency(text: str) -> Tuple[str, bool]:
    if text is None:
        return "", False
    raw = text.strip()
    if not raw:
        return "", False
    cleaned = raw.replace(",", "")
    cleaned = cleaned.replace("$", "").replace("(", "-").replace(")", "")
    cleaned = cleaned.strip()
    is_number = bool(re.fullmatch(r"-?\d+(\.\d+)?", cleaned))
    return cleaned, is_number


def iou(a: List[int], b: List[int]) -> float:
    ax1, ay1, ax2, ay2 = a
    bx1, by1, bx2, by2 = b
    inter_x1 = max(ax1, bx1)
    inter_y1 = max(ay1, by1)
    inter_x2 = min(ax2, bx2)
    inter_y2 = min(ay2, by2)
    inter_w = max(0, inter_x2 - inter_x1)
    inter_h = max(0, inter_y2 - inter_y1)
    inter = inter_w * inter_h
    area_a = max(0, ax2 - ax1) * max(0, ay2 - ay1)
    area_b = max(0, bx2 - bx1) * max(0, by2 - by1)
    denom = area_a + area_b - inter
    return inter / denom if denom > 0 else 0.0


def sort_cells_to_rows(cells: List[Dict[str, Any]], y_threshold: int = 10) -> List[List[Dict[str, Any]]]:
    cells_sorted = sorted(cells, key=lambda c: (c["bbox"][1], c["bbox"][0]))
    rows: List[List[Dict[str, Any]]] = []
    for cell in cells_sorted:
        if not rows:
            rows.append([cell])
            continue
        last_row = rows[-1]
        last_y = last_row[0]["bbox"][1]
        if abs(cell["bbox"][1] - last_y) <= y_threshold:
            last_row.append(cell)
        else:
            rows.append([cell])
    for row in rows:
        row.sort(key=lambda c: c["bbox"][0])
    return rows


def flatten(list_of_lists: Iterable[Iterable[Any]]) -> List[Any]:
    return [item for sub in list_of_lists for item in sub]
