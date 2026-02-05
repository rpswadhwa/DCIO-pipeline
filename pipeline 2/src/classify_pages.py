from typing import Dict, List

import cv2
from paddleocr import PaddleOCR

from .utils import load_yaml, normalize_whitespace


def _ocr_lines(ocr: PaddleOCR, image_path: str) -> List[str]:
    img = cv2.imread(image_path)
    if img is None:
        return []
    result = ocr.ocr(img, cls=True)
    lines = []
    for line in result or []:
        text = line[1][0] if line and len(line) > 1 else ""
        if text:
            lines.append(normalize_whitespace(text))
    return lines


def classify_pages(pages: List[Dict[str, str]], keywords_yml: str) -> List[Dict[str, str]]:
    cfg = load_yaml(keywords_yml)
    keywords = [k.upper() for k in cfg.get("supplemental_schedule_keywords", [])]
    negatives = [k.upper() for k in cfg.get("negative_keywords", [])]
    min_hits = int(cfg.get("min_keyword_hits", 1))
    max_lines = int(cfg.get("header_scan_max_lines", 12))

    ocr = PaddleOCR(use_angle_cls=True, lang="en")

    out = []
    for page in pages:
        lines = _ocr_lines(ocr, page["image_path"])
        header_lines = lines[:max_lines]
        header_text = " ".join(header_lines).upper()
        hits = sum(1 for k in keywords if k in header_text)
        neg_hits = sum(1 for k in negatives if k in header_text)
        page["is_supplemental"] = 1 if hits >= min_hits and neg_hits == 0 else 0
        page["header_text"] = header_text
        out.append(page)
    return out
