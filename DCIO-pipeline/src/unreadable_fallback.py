"""(2026-11-16) Unreadable-PDF fallback: find the Schedule of Assets on pages the text layer
cannot read, then extract them through the vision extractor.

Why: in the gap-ordered run 20% of filings landed nothing. Two causes share one fix:
  * image-only scans -- no text layer at all (Cisco, Fred Hutch, T. Rowe Price US Retirement);
  * scrambled text layers -- the page LOOKS normal, but the font has no usable character map,
    so the text a machine reads is a cipher ("+74-0=6D0A3" for "The Vanguard") and digits come
    out as tabs/newlines (Vanguard, CIBC, Federated Hermes, CACI, Norfolk Southern, Cargill).
classify_pages_text() finds no keywords on either kind, so no page is picked and nothing is
extracted. The rendered page image is fine in both cases, so this module:
  1. marks pages whose text layer is empty or scrambled (common-word density test);
  2. OCRs only those pages (tesseract, header strip first) and applies the same keyword /
     column-header logic classify_pages_text() uses, plus a continuation run;
  3. returns the page numbers; run_pipeline sends them to extract_investments_via_llm_vision().

Runs only for a filing whose normal extraction produced no usable rows, so it never changes a
filing that already extracts. Toggle: UNREADABLE_FALLBACK=0 disables it.
"""
import os
import re
from typing import Dict, List, Optional

_COMMON_WORDS = re.compile(
    r"\b(the|and|of|plan|to|in|for|fund|total|investment|value|trust|assets?)\b", re.I)
_MONEY = re.compile(r"\$?\s*\(?\s*\d{1,3}(?:[,.]\d{3})+(?:\.\d+)?\)?|\$\s*\d+")
_TITLE = re.compile(
    r"schedule\s+h\W{0,3}\s*(?:part\s+iv\W{0,3}\s*)?line\s*4\s*\(?\s*i\b"
    r"|line\s*4\s*\(?i\)?"
    r"|schedule\s+of\s+assets"
    r"|assets\s*\(?\s*held\s+at\s+end"
    r"|schedule\s+of\s+investments"
    r"|held\s+for\s+investment\s+purposes", re.I)
_COLUMNS = re.compile(r"identity\s+of\s+issue|borrower,?\s+lessor|description\s+of\s+investment", re.I)
_CURRENT_VALUE = re.compile(r"current\s+value|fair\s+value|market\s+value", re.I)
# Pages that carry a "Schedule H, line 4" title but are not the asset schedule.
_OTHER_SCHEDULE = re.compile(
    r"line\s*4\s*\(?\s*[aj]\b|reportable\s+transactions?|delinquent|transferred\s+late"
    r"|nonexempt|non-exempt|loans?\s+or\s+fixed\s+income\s+obligations\s+in\s+default", re.I)
_NARRATIVE = re.compile(
    r"independent\s+auditor|report\s+of\s+independent|in\s+our\s+opinion|table\s+of\s+contents"
    r"|signature", re.I)

MAX_OCR_PAGES = int(os.environ.get("UNREADABLE_MAX_OCR_PAGES", "250"))
MAX_SCHEDULE_PAGES = int(os.environ.get("UNREADABLE_MAX_SCHEDULE_PAGES", "40"))


def _page_texts(pdf_path: str) -> List[str]:
    import pypdfium2 as pdfium
    doc = pdfium.PdfDocument(pdf_path)
    try:
        return [doc[i].get_textpage().get_text_range() or "" for i in range(len(doc))]
    finally:
        doc.close()


def is_unreadable_text(text: str) -> bool:
    """True when a page's text layer is empty or scrambled (real prose/tables always carry
    common English words; a cipher layer carries almost none)."""
    s = (text or "").strip()
    if len(s) < 80:
        return True
    per_1000 = len(_COMMON_WORDS.findall(s)) / (len(s) / 1000.0)
    return per_1000 < 3.0


def unreadable_pages(pdf_path: str) -> List[int]:
    return [i + 1 for i, t in enumerate(_page_texts(pdf_path)) if is_unreadable_text(t)]


def _render(pdf_path: str, page_num: int, dpi: int, rotation: int = 0):
    import pypdfium2 as pdfium
    doc = pdfium.PdfDocument(pdf_path)
    try:
        img = doc[page_num - 1].render(scale=dpi / 72.0).to_pil()
    finally:
        doc.close()
    if rotation:
        img = img.rotate(-rotation, expand=True)  # clockwise degrees
    return img


def _ocr_score(text: str) -> int:
    # Real words only: OCR of a sideways page is digit-heavy noise that fakes "amount" hits.
    return len(_COMMON_WORDS.findall(text))


def _detect_rotation(pdf_path: str, page_num: int) -> int:
    """Scanned schedules are often fed sideways or upside down; OCR of a page in the wrong
    orientation is noise. Keep 0 when it already reads; otherwise take the best of 90/270/180."""
    base = _ocr_score(_ocr(pdf_path, page_num, dpi=100))
    if base >= 10:
        return 0
    best, best_rot = base, 0
    for rot in (90, 270, 180):
        s = _ocr_score(_ocr(pdf_path, page_num, dpi=100, rotation=rot))
        if s > best:
            best, best_rot = s, rot
    return best_rot if best >= max(8, 3 * base) else 0


def _ocr(pdf_path: str, page_num: int, dpi: int, top_fraction: float = 1.0,
         rotation: int = 0) -> str:
    import subprocess
    import tempfile
    cmd = os.environ.get("TESSERACT_CMD") or "tesseract"
    img = _render(pdf_path, page_num, dpi, rotation)
    w, h = img.size
    if w > h * 1.15:  # landscape page: header strip is shorter relative to width
        top_fraction = min(1.0, top_fraction * 1.3)
    if top_fraction < 1.0:
        img = img.crop((0, 0, w, int(h * top_fraction)))
    img = img.convert("L")
    # tesseract binary directly (pytesseract drags in pandas; not needed for plain text)
    fd, png = tempfile.mkstemp(suffix=".png")
    os.close(fd)
    try:
        img.save(png)
        out = subprocess.run([cmd, png, "stdout", "--psm", "6"], capture_output=True,
                             timeout=120)
        return out.stdout.decode("utf-8", "replace")
    except Exception:
        return ""
    finally:
        try:
            os.remove(png)
        except OSError:
            pass


def _header(text: str, max_lines: int = 14) -> str:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    return " ".join(lines[:max_lines])


def _money_lines(text: str) -> int:
    return sum(1 for ln in text.splitlines() if _MONEY.search(ln))


def _is_schedule_start(header: str, full: Optional[str]) -> bool:
    if _OTHER_SCHEDULE.search(header) and not _COLUMNS.search(header):
        return False
    titled = bool(_TITLE.search(header))
    columns = bool(_COLUMNS.search(header)) and bool(_CURRENT_VALUE.search(header))
    if not (titled or columns):
        return False
    if _NARRATIVE.search(header) and not columns:
        return False
    # A title alone on a divider/TOC page is not the schedule: need table-like body.
    return full is None or _money_lines(full) >= 3


def find_schedule_pages(pdf_path: str, log=print) -> List[int]:
    return find_schedule(pdf_path, log=log)[0]


def find_schedule(pdf_path: str, log=print):
    """Schedule of Assets pages among the PDF's unreadable pages, found by OCR.
    Returns (pages, {page: clockwise rotation}) -- rotation only for pages scanned sideways."""
    bad = unreadable_pages(pdf_path)
    if not bad:
        return [], {}
    if len(bad) > MAX_OCR_PAGES:
        log(f"    [unreadable] {len(bad)} unreadable pages; OCR capped at last {MAX_OCR_PAGES}")
        bad = bad[-MAX_OCR_PAGES:]  # schedules sit after the notes, toward the back
    bad_set = set(bad)
    full_cache: Dict[int, str] = {}
    rot: Dict[int, int] = {}

    def rotation(p: int) -> int:
        if p not in rot:
            rot[p] = _detect_rotation(pdf_path, p)
        return rot[p]

    def full(p: int) -> str:
        if p not in full_cache:
            full_cache[p] = _ocr(pdf_path, p, dpi=150, rotation=rotation(p))
        return full_cache[p]

    starts = []
    for p in bad:
        head = _header(_ocr(pdf_path, p, dpi=150, top_fraction=0.3, rotation=rotation(p)))
        if _TITLE.search(head) or (_COLUMNS.search(head) and _CURRENT_VALUE.search(head)):
            if _is_schedule_start(head, full(p)):
                starts.append(p)
    picked: List[int] = []
    for s in starts:
        if s in picked:
            continue
        picked.append(s)
        nxt = s + 1
        while nxt in bad_set and len(picked) < MAX_SCHEDULE_PAGES:
            t = full(nxt)
            head = _header(t)
            if _OTHER_SCHEDULE.search(head) or _NARRATIVE.search(head):
                break
            if _money_lines(t) < 3:
                break
            if nxt not in picked:
                picked.append(nxt)
            nxt += 1
        if len(picked) >= MAX_SCHEDULE_PAGES:
            break
    picked = sorted(set(picked))
    # Filers often attach the same schedule twice (Vanguard pp18-19 = pp21-22; CIBC p18 = p19;
    # Cisco pp16-17 = pp18-19); reading both doubles every holding. A page whose dollar amounts
    # mostly repeat an earlier picked page's is a copy: drop it.
    kept: List[int] = []
    kept_amounts: List[set] = []
    for p in picked:
        amts = set(m.group(0).replace(" ", "").replace("$", "") for m in _MONEY.finditer(full(p)))
        dup = len(amts) >= 3 and any(
            len(amts & prev) / len(amts | prev) >= 0.7 for prev in kept_amounts if prev)
        if dup:
            log(f"    [unreadable] page {p} repeats an earlier schedule page; skipped")
            continue
        kept.append(p)
        kept_amounts.append(amts)
    picked = kept
    rotated = {p: rot[p] for p in picked if rot.get(p)}
    log(f"    [unreadable] {len(bad)} unreadable page(s); schedule pages by OCR: {picked or 'none'}"
        + (f" (rotated: {rotated})" if rotated else ""))
    return picked, rotated
