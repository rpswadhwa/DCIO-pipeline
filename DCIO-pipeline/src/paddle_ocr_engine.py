"""
paddle_ocr_engine.py
~~~~~~~~~~~~~~~~~~~~
PaddleOCR wrapper standing in for pytesseract. tesseract is not installable
on the EC2 runner (Amazon Linux 2023: no package in the base repo or in
EPEL9), so this project uses PaddleOCR -- already present in the venv --
as its OCR engine instead.

Two PaddleOCR behaviors this file works around, both found empirically
against this instance's CPU (no GPU, no MKLDNN-friendly acceleration path):
  - The default detector (PP-OCRv5_server_det) crashes with a PIR/oneDNN
    incompatibility when MKLDNN is on (`enable_mkldnn` defaults to True),
    and separately fails with a runaway ~59GB allocation in the plain-CPU
    path when MKLDNN is off -- the server-size conv kernels are not viable
    on this box either way.
  - The lightweight detector (PP-OCRv5_mobile_det) has neither problem with
    MKLDNN off, and is what this module always requests.

A single module-level PaddleOCR instance is reused across calls within a
process -- initialization loads real model weights (~1-3s) -- since each
PDF already gets its own subprocess in run_pipeline.py's OCR worker, this
does not defeat that per-PDF isolation.
"""

from typing import Dict, List

_ocr = None


def _get_ocr():
    global _ocr
    if _ocr is None:
        from paddleocr import PaddleOCR
        _ocr = PaddleOCR(
            lang="en",
            enable_mkldnn=False,
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=False,
            text_detection_model_name="PP-OCRv5_mobile_det",
            text_det_limit_side_len=960,
        )
    return _ocr


def image_to_words(img) -> List[Dict]:
    """Detect text in an already-loaded (cv2/BGR numpy) image.

    Returns one dict per detected text region -- {"text", "x", "y", "w", "h",
    "conf"} with conf on pytesseract's familiar 0-100 scale -- matching the
    shape ocr_passes.py's row/column clustering expects. PaddleOCR groups at
    line/phrase granularity rather than tesseract's per-word granularity;
    the clustering logic only needs *a* bounding region and text per
    detected unit, so this is a drop-in.
    """
    if img is None or img.size == 0:
        return []
    results = list(_get_ocr().predict(img))
    if not results:
        return []
    r0 = results[0]
    words = []
    for text, score, box in zip(r0["rec_texts"], r0["rec_scores"], r0["rec_boxes"]):
        text = text.strip()
        if not text:
            continue
        x1, y1, x2, y2 = (int(v) for v in box)
        words.append({
            "text": text,
            "x": x1,
            "y": y1,
            "w": max(0, x2 - x1),
            "h": max(0, y2 - y1),
            "conf": float(score) * 100.0,
        })
    return words


def image_to_string(img) -> str:
    """Full-image text, newline-joined in reading order (top-to-bottom, then
    left-to-right within a line's own y-band). Stand-in for pytesseract's
    image_to_string, used by classify_pages.py's header-keyword scan.
    """
    words = image_to_words(img)
    words.sort(key=lambda w: (w["y"], w["x"]))
    return "\n".join(w["text"] for w in words)


def image_to_cell_text(img):
    """Text + 0-1 confidence for one small cropped region (a table cell).
    Stand-in for ocr_passes.py's per-cell pytesseract call: joins every
    detected text region in the crop (usually just one) and averages
    confidence.
    """
    words = image_to_words(img)
    if not words:
        return "", 0.0
    text = " ".join(w["text"] for w in words)
    conf = sum(w["conf"] for w in words) / len(words) / 100.0
    return text, conf
