from typing import Dict, List

import cv2
import pytesseract

from .utils import load_yaml, normalize_whitespace


_OCR_TIMEOUT_S = 30
_HEADER_CROP_FRACTION = 0.3  # classification only needs the page's title/header region


def _ocr_lines(image_path: str) -> List[str]:
    img = cv2.imread(image_path)
    if img is None:
        return []
    header_h = max(1, int(img.shape[0] * _HEADER_CROP_FRACTION))
    header_img = img[:header_h, :]
    try:
        text = pytesseract.image_to_string(header_img, timeout=_OCR_TIMEOUT_S)
    except RuntimeError:
        # Tesseract hung/timed out on a pathological page image -- treat as blank
        # rather than blocking the whole pipeline run on one bad page.
        return []
    lines = [normalize_whitespace(line) for line in text.splitlines() if line.strip()]
    return lines


def classify_pages(pages: List[Dict[str, str]], keywords_yml: str) -> List[Dict[str, str]]:
    cfg = load_yaml(keywords_yml)
    keywords = [k.upper() for k in cfg.get("supplemental_schedule_keywords", [])]
    negatives = [k.upper() for k in cfg.get("negative_keywords", [])]
    min_hits = int(cfg.get("min_keyword_hits", 1))
    max_lines = int(cfg.get("header_scan_max_lines", 12))

    out = []
    for page in pages:
        lines = _ocr_lines(page["image_path"])
        header_lines = lines[:max_lines]
        header_text = " ".join(header_lines).upper()
        hits = sum(1 for k in keywords if k in header_text)
        neg_hits = sum(1 for k in negatives if k in header_text)
        page["is_supplemental"] = 1 if hits >= min_hits and neg_hits == 0 else 0
        page["header_text"] = header_text
        out.append(page)
    return out
