"""(2026-11-27) Filing-level "in thousands" scale for an asset schedule that prints no scale label.

Auditors often print "(In Thousands)" once, on the Statements of Net Assets Available for Benefits,
and leave it off the Schedule H line 4i further back (Amazon 401(k), Endeavor Legacy 403(b)).
Every page-level reader keys on the schedule page's own text, so those rows load at 1/1000.

Rule, per filing, run on the cleaned rows (totals already removed):
  1. a statements page in the SAME PDF says "in thousands" (text layer, or one vision read of a
     scanned page), and
  2. the schedule rows' sum, as extracted, is within STATEMENT_MATCH_TOL of an "investments" figure
     printed on that statements page.
Then every row of the filing is scaled x1000.

Check 2 is what makes this safe. Many filers print statements in thousands but a whole-dollar
schedule; their row sum is ~1000x the statement figure and never matches. A filing that a reader
already scaled (page label, registered parser, vision scale_multiplier) is likewise ~1000x off and
is left alone. A filing with dropped rows simply doesn't match and is left as is (safe-fail).
Compares two pages of the same document -- never the certified 5500 amounts.
"""
import json
import re
from typing import Dict, List, Optional

import pdfplumber

from .text_extract import _page_values_are_in_millions, _page_values_are_in_thousands, _scale_currency_string

STATEMENT_MATCH_TOL = 0.02          # Endeavor: rows 2,153,683 vs fair value 2,184,663 (1.4%)
MAX_VISION_PROBE_PAGES = 10         # scanned filings: statements sit in the first pages
_STATEMENT_TITLE = re.compile(r'statements?\s*of\s*net\s*assets\s*available', re.I)
_INVESTMENT_LINE = re.compile(r'investments?', re.I)
# Comma-grouped or 5+ digits: a bare 4-digit run is a year ("December 31, 2025" on the auditor's
# report, which Amazon's p3 otherwise passed off as a statements page with figures).
_NUMBER = re.compile(r'\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d{5,}(?:\.\d+)?')
_LOAN = re.compile(r'\bloans?\b|notes?\s*receivable', re.I)

_VISION_PROMPT = (
    "You are looking at one page of a retirement plan's audited financial statements. "
    "Return ONLY a JSON object with: is_statement_of_net_assets (true if this page is the "
    "'Statement(s) of Net Assets Available for Benefits'), in_thousands (true only if the page "
    "explicitly says amounts are in thousands, e.g. '(In Thousands)', '(Dollars in Thousands)', "
    "'($000s)'), and investment_figures (every dollar figure printed on a line whose label mentions "
    "investments, e.g. 'Investments, at fair value', 'Total investments', digits only as printed, "
    "never rescaled)."
)


def _num(text) -> Optional[float]:
    s = re.sub(r'[^0-9.]', '', str(text or ''))
    try:
        return float(s) if s else None
    except ValueError:
        return None


def _figures_from_text(text: str) -> List[float]:
    out = []
    for line in (text or '').splitlines():
        if _INVESTMENT_LINE.search(line):
            out.extend(v for v in (_num(m) for m in _NUMBER.findall(line)) if v)
    return out


def _text_statement(pdf_path: str) -> Optional[Dict]:
    """Text-layer statements page: {'page', 'in_thousands', 'figures'}; None if none carries figures.

    The auditor's report also names "the statements of net assets available for benefits" but has
    no investment figures, so only a titled page WITH figures counts (Amazon's text pages are all
    auditor's report -> None -> vision probe of the scanned pages)."""
    found = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ''
            if _STATEMENT_TITLE.search(text):
                figures = _figures_from_text(text)
                if figures:
                    found.append({"page": i, "figures": figures,
                                  "in_thousands": _page_values_are_in_thousands(text)
                                  and not _page_values_are_in_millions(text)})
    return next((s for s in found if s["in_thousands"]), found[0] if found else None)


def _scanned_pages(pdf_path: str, limit: int) -> List[int]:
    with pdfplumber.open(pdf_path) as pdf:
        return [i for i, p in enumerate(pdf.pages[:limit], start=1) if len((p.extract_text() or '').strip()) < 20]


def _vision_statement(pdf_path: str, provider: str, model: str, before_page: int) -> Optional[Dict]:
    """Scanned filing: read the image pages before the schedule, stop at the statements page."""
    from .llm_provider import call_llm_json
    from .llm_vision_extract import _render_page_image_b64
    limit = min(MAX_VISION_PROBE_PAGES, max(before_page - 1, 0))
    for p in _scanned_pages(pdf_path, limit):
        try:
            raw = call_llm_json({"instructions": _VISION_PROMPT}, provider=provider, model=model,
                                image_b64=_render_page_image_b64(pdf_path, p, dpi=150))
            data = json.loads(raw) if isinstance(raw, str) else raw
        except Exception as exc:
            print(f"    [statement_scale] page {p}: vision read failed: {exc}")
            continue
        if isinstance(data, dict) and data.get("is_statement_of_net_assets"):
            figures = [v for v in (_num(x) for x in data.get("investment_figures") or []) if v]
            return {"page": p, "in_thousands": bool(data.get("in_thousands")), "figures": figures}
    return None


def _schedule_sum(rows: List[Dict]) -> float:
    total = 0.0
    for r in rows:
        if str(r.get("asset_type") or "").lower() == "participant loan":
            continue
        if _LOAN.search(f"{r.get('issuer_name', '')} {r.get('investment_description', '')}"):
            continue
        total += _num(r.get("current_value")) or 0.0
    return total


def decide_statement_scale(rows: List[Dict], statement: Optional[Dict]) -> Optional[float]:
    """Pure rule: the matched statements figure when rows should be scaled x1000, else None."""
    if not statement or not statement.get("in_thousands") or len(rows) < 3:
        return None
    row_sum = _schedule_sum(rows)
    if row_sum <= 0:
        return None
    for fig in statement.get("figures") or []:
        if fig and abs(row_sum - fig) / fig <= STATEMENT_MATCH_TOL:
            return fig
    return None


def apply_statement_scale(clean_rows: List[Dict], supplemental_pages: List[Dict],
                          provider: str = "gemini", model: str = "gemini-2.5-flash") -> int:
    """Scale x1000 every row of each filing that meets the rule. Returns filings scaled."""
    by_stem: Dict[str, List[Dict]] = {}
    for r in clean_rows:
        if r.get("pdf_stem"):
            by_stem.setdefault(r["pdf_stem"], []).append(r)
    pdf_by_stem = {p.get("pdf_stem"): p.get("pdf") for p in supplemental_pages if p.get("pdf")}

    scaled = 0
    for stem, rows in by_stem.items():
        pdf_path = pdf_by_stem.get(stem)
        if not pdf_path:
            continue
        try:
            statement = _text_statement(pdf_path)
            # Vision probe costs LLM calls: only when no schedule page of this filing was
            # already read at a scale (vision scale_multiplier), so a labelled scan pays nothing.
            already_scaled = any((p.get("scale_multiplier") or 1) != 1
                                 for p in supplemental_pages if p.get("pdf_stem") == stem)
            if statement is None and not already_scaled:
                first_sched = min((int(r.get("page_number") or 0) for r in rows), default=0) or 99
                statement = _vision_statement(pdf_path, provider, model, first_sched)
        except Exception as exc:
            print(f"    [statement_scale] {stem}: skipped ({exc})")
            continue
        fig = decide_statement_scale(rows, statement)
        if fig is None:
            continue
        for r in rows:
            r["current_value"] = _scale_currency_string(r.get("current_value"), 1000)
        for p in supplemental_pages:
            if p.get("pdf_stem") == stem:
                p["scale_multiplier"] = 1000
        scaled += 1
        print(f"    [statement_scale] {stem}: schedule has no scale label; statements page "
              f"{statement['page']} says thousands and its investments figure {fig:,.0f} matches "
              f"the rows -> {len(rows)} rows x1000")
    return scaled
