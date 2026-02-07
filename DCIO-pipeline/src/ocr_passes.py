from typing import Dict, List

import cv2
from paddleocr import PaddleOCR

from .utils import normalize_whitespace


def _ocr_cell(ocr: PaddleOCR, img):
    result = ocr.ocr(img, cls=True, det=False)
    if not result:
        return "", 0.0
    text = result[0][0]
    conf = result[0][1]
    return normalize_whitespace(text), float(conf)


def run_ocr(pages: List[Dict[str, str]]) -> List[Dict[str, str]]:
    ocr_tab = PaddleOCR(use_angle_cls=True, lang="en")
    ocr_text = PaddleOCR(use_angle_cls=True, lang="en")

    out = []
    for page in pages:
        img = cv2.imread(page["normalized_path"])
        if img is None:
            page["ocr_cells"] = []
            out.append(page)
            continue
        ocr_cells = []
        for idx, cell in enumerate(page.get("cells", [])):
            x1, y1, x2, y2 = cell["bbox"]
            crop = img[y1:y2, x1:x2]
            text1, conf1 = _ocr_cell(ocr_tab, crop)
            text2, conf2 = _ocr_cell(ocr_text, crop)
            text = text1 if len(text1) >= len(text2) else text2
            conf = max(conf1, conf2)
            ocr_cells.append({
                "cell_id": idx,
                "bbox": cell["bbox"],
                "text": text,
                "confidence": conf,
            })
        page["ocr_cells"] = ocr_cells
        out.append(page)
    return out
