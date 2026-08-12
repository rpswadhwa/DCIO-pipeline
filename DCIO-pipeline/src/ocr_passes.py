from typing import Dict, List

import cv2
import pytesseract
from pytesseract import Output

from .utils import normalize_whitespace

_HEADER_KEYWORDS = ("identity", "description", "current", "value", "issuer")
_OCR_TIMEOUT_S = 30


def _words_from_image(img) -> List[Dict]:
    try:
        data = pytesseract.image_to_data(img, output_type=Output.DICT, timeout=_OCR_TIMEOUT_S)
    except RuntimeError:
        # Tesseract hung/timed out on a pathological cell image -- treat as no words
        # rather than blocking the whole pipeline run on one bad image.
        return []
    words = []
    for i in range(len(data["text"])):
        text = data["text"][i].strip()
        if not text:
            continue
        try:
            conf = float(data["conf"][i])
        except (TypeError, ValueError):
            conf = -1.0
        words.append({
            "text": text,
            "x": data["left"][i],
            "y": data["top"][i],
            "w": data["width"][i],
            "h": data["height"][i],
            "conf": conf,
        })
    return words


def _cluster_words_to_rows(words: List[Dict]) -> List[List[Dict]]:
    if not words:
        return []
    heights = sorted(w["h"] for w in words)
    median_h = heights[len(heights) // 2]
    row_tol = max(6, median_h * 0.6)

    ordered = sorted(words, key=lambda w: (w["y"], w["x"]))
    rows: List[List[Dict]] = []
    for w in ordered:
        cy = w["y"] + w["h"] / 2
        placed = False
        for row in rows:
            row_cy = sum(r["y"] + r["h"] / 2 for r in row) / len(row)
            if abs(cy - row_cy) <= row_tol:
                row.append(w)
                placed = True
                break
        if not placed:
            rows.append([w])
    rows.sort(key=lambda row: sum(r["y"] for r in row) / len(row))
    for row in rows:
        row.sort(key=lambda w: w["x"])
    return rows


def _find_header_row(rows: List[List[Dict]]) -> List[Dict]:
    for row in rows:
        row_text = " ".join(w["text"] for w in row).lower()
        if sum(1 for k in _HEADER_KEYWORDS if k in row_text) >= 2:
            return row
    return rows[0] if rows else []


def _column_bounds(header_row: List[Dict]) -> List[int]:
    # A generous gap between consecutive header words marks a new column start.
    bounds = []
    prev_end = None
    for w in header_row:
        if prev_end is None or w["x"] - prev_end > 60:
            bounds.append(w["x"])
        prev_end = w["x"] + w["w"]
    return bounds or [0]


def _bucket_row_into_cells(row: List[Dict], bounds: List[int]) -> List[Dict]:
    buckets: Dict[int, List[Dict]] = {i: [] for i in range(len(bounds))}
    for w in row:
        col_idx = max(i for i, b in enumerate(bounds) if w["x"] >= b) if any(w["x"] >= b for b in bounds) else 0
        buckets[col_idx].append(w)

    cells = []
    for col_idx in sorted(buckets):
        cell_words = buckets[col_idx]
        if not cell_words:
            continue
        text = normalize_whitespace(" ".join(w["text"] for w in cell_words))
        confs = [w["conf"] for w in cell_words if w["conf"] >= 0]
        conf = (sum(confs) / len(confs) / 100.0) if confs else 0.0
        x1 = min(w["x"] for w in cell_words)
        y1 = min(w["y"] for w in cell_words)
        x2 = max(w["x"] + w["w"] for w in cell_words)
        y2 = max(w["y"] + w["h"] for w in cell_words)
        cells.append({"bbox": [x1, y1, x2, y2], "text": text, "confidence": conf})
    return cells


def _cluster_words_to_cells(img) -> List[Dict]:
    """Last-resort fallback for pages with no OpenCV-detected grid cells (e.g. scanned
    schedules with no ruled table borders). Groups Tesseract's word boxes into rows by
    y-position, derives column bands from gaps in the header row's x-positions, and
    buckets every row's words into those bands -- producing the same bbox/text cell
    shape the OpenCV cell-grid path already produces, so downstream mapping is unchanged.
    """
    words = _words_from_image(img)
    rows = _cluster_words_to_rows(words)
    if not rows:
        return []
    header_row = _find_header_row(rows)
    bounds = _column_bounds(header_row)

    all_cells = []
    for row in rows:
        all_cells.extend(_bucket_row_into_cells(row, bounds))
    return all_cells


def _ocr_cell(img):
    try:
        data = pytesseract.image_to_data(img, output_type=Output.DICT, timeout=_OCR_TIMEOUT_S)
    except RuntimeError:
        return "", 0.0
    words = []
    confs = []
    for text, conf in zip(data["text"], data["conf"]):
        text = text.strip()
        if not text:
            continue
        words.append(text)
        try:
            c = float(conf)
        except (TypeError, ValueError):
            continue
        if c >= 0:
            confs.append(c)
    text = normalize_whitespace(" ".join(words))
    conf = (sum(confs) / len(confs) / 100.0) if confs else 0.0
    return text, conf


def run_ocr(pages: List[Dict[str, str]]) -> List[Dict[str, str]]:
    out = []
    for page in pages:
        img = cv2.imread(page["normalized_path"])
        if img is None:
            page["ocr_cells"] = []
            out.append(page)
            continue

        cells = page.get("cells", [])
        if not cells:
            # No ruled table borders were detected for this page (common on scanned
            # filings without grid lines). Only in that case, fall back to clustering
            # Tesseract's word boxes directly into rows/columns.
            page["ocr_cells"] = _cluster_words_to_cells(img)
            out.append(page)
            continue

        ocr_cells = []
        for idx, cell in enumerate(cells):
            x1, y1, x2, y2 = cell["bbox"]
            crop = img[y1:y2, x1:x2]
            text, conf = _ocr_cell(crop)
            ocr_cells.append({
                "cell_id": idx,
                "bbox": cell["bbox"],
                "text": text,
                "confidence": conf,
            })
        page["ocr_cells"] = ocr_cells
        out.append(page)
    return out
