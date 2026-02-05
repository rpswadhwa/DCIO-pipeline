from typing import Dict, List

import cv2
import numpy as np
from paddleocr import PPStructure


def _detect_table_regions(img):
    pp = PPStructure(layout=False, table=True, ocr=False)
    result = pp(img)
    regions = []
    for block in result:
        if block.get("type") == "table" and "bbox" in block:
            x1, y1, x2, y2 = block["bbox"]
            regions.append([x1, y1, x2, y2])
    if not regions:
        h, w = img.shape[:2]
        regions = [[0, 0, w, h]]
    return regions


def _find_cells(crop):
    gray = crop if len(crop.shape) == 2 else cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
    thresh = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY_INV, 35, 15)

    kernel_len = max(10, crop.shape[1] // 80)
    vert_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, kernel_len))
    hori_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (kernel_len, 1))

    vert_lines = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, vert_kernel, iterations=2)
    hori_lines = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, hori_kernel, iterations=2)
    table_mask = cv2.addWeighted(vert_lines, 0.5, hori_lines, 0.5, 0.0)
    table_mask = cv2.dilate(table_mask, cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3)), iterations=1)

    contours, _ = cv2.findContours(table_mask, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
    cells = []
    for cnt in contours:
        x, y, w, h = cv2.boundingRect(cnt)
        if w < 20 or h < 15:
            continue
        cells.append([x, y, x + w, y + h])
    cells = sorted(cells, key=lambda b: (b[1], b[0]))
    return cells


def detect_tables(pages: List[Dict[str, str]]) -> List[Dict[str, str]]:
    out = []
    for page in pages:
        img = cv2.imread(page["normalized_path"])
        if img is None:
            page["cells"] = []
            out.append(page)
            continue
        regions = _detect_table_regions(img)
        all_cells = []
        for region in regions:
            x1, y1, x2, y2 = region
            crop = img[y1:y2, x1:x2]
            cells = _find_cells(crop)
            for c in cells:
                all_cells.append({
                    "bbox": [c[0] + x1, c[1] + y1, c[2] + x1, c[3] + y1]
                })
        page["cells"] = all_cells
        out.append(page)
    return out
