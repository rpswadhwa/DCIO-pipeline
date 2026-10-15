"""Deterministic text parser for Schedule H, Line 4i "Schedule of Assets
(Held at End of Year)" pages laid out as fixed columns:

    Asset Identifier | Security Title | Interest Rate | Maturity Date |
    Face Amount or Number of Shares | Cost | Market Value

with holdings grouped under section-heading lines ("Mutual funds:",
"Absolute return:", "Private Investments:", ...) and each section closed by a
numbers-only subtotal line. First seen on Mayo Pension Plan (ack
20251006115620NAL0003598321001, pp.45-52), where LLM_FULL_EXTRACT kept losing
whole pages to Gemini read timeouts on these dense pages (2026-10-12).

No LLM involved: asset_type comes from the section heading the row sits
under, every section that both starts and ends inside the parsed pages is
reconciled against its printed subtotal, and the row total is checked
against the schedule's printed grand total.

A second layout, "issue_par_rate" (2026-10-14), covers filers that print

    Identity of Issue | Par Value | Rate (%) | Maturity Date | Cost | Current Value

with ALL-CAPS section headings ("SHORT-TERM INVESTMENTS", "PARTNERSHIPS")
closed by "TOTAL <HEADING> <cost> <value>" lines and no asset identifier
column. First seen on Shell Pension Plan (ack 20251009145059NAL0006755009001,
pp.90-128), where the generic extractor dropped ~$3.0B of $9.49B in whole
blocks (all commingled funds, 30 partnerships, entire bond pages). Those
pages pad right-aligned numbers with space glyphs that overlap the first
digit, so pdfplumber reads "739,223" as "7 39,223"; _drop_overlapping_spaces
removes them before text extraction.

Registered per plan, not per filing: PLAN_PARSERS is keyed by plan_master
plan_id (EIN-PN), which stays fixed across years while ack_id and page
numbers change. run_pipeline.py resolves each PDF's ack_id to a plan_id via
default.plan_master and, for registered plans, lets find_schedule_4i_pages
locate the schedule itself -- no PAGES_CSV needed. Returns None when the
pages don't carry this column header, so the caller falls through to the
existing path.
"""

import re
from typing import Dict, List, Optional, Set

import pdfplumber

from .asset_type_patterns import detect_asset_type

# plan_master plan_id -> filer whose 4i schedule uses this layout (2026-10-12).
# start_heading: parse from the page carrying this section heading to the end
# of the schedule. Mayo: only the Mutual funds / Collective trust / Absolute
# return / Private Investments / Other block (pp.45-52 in the 2025 filing) is
# wanted, not the cash/government/bond/equity pages before it.
PLAN_PARSERS = {
    "416011702-001": {"name": "Mayo Pension Plan", "start_heading": r"^mutual\s+funds?$"},
    # Shell: the whole schedule, every section (2026-10-14).
    "131299890-001": {"name": "Shell Pension Plan", "layout": "issue_par_rate",
                      "start_heading": r"^SHORT-TERM\s+INVESTMENTS$"},
}

# Every requested page must carry this column-header line, else we don't own the layout.
_HEADER_RE = re.compile(r"Identifier\s+Security\s+Title\b.*\bValue\b", re.I)
_IPR_HEADER_RE = re.compile(r"Identity\s+of\s+Issue\b.*\bPar\s+Value\b.*\bCurrent\s+Value\b", re.I)
_IPR_HEADING_RE = re.compile(r"^[A-Z][A-Z ,&/()'\-]*[A-Z)]$")
_BOILERPLATE_RE = re.compile(
    r"^(EIN\s|Schedule\s+H,|December\s+\d|Face\s+Amount|Asset\s+Interest|Identifier\s+Security)", re.I)
_NUM_RE = re.compile(r"^\(?-?\$?\d[\d,]*(\.\d+)?\)?$|^[-—–]$")
_ID_RE = re.compile(r"^(?=[A-Z0-9]*\d)[A-Z0-9]{7,10}$")
_DATE_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}$")
_RATE_RE = re.compile(r"^\d{1,2}\.\d+%?$|^\d{1,2}%$")
_HEADING_RE = re.compile(r"^[A-Za-z][A-Za-z0-9 ,&/()'.\-]*:$")

# Section heading -> asset_type, first match wins. Labels must be in
# llm_row_extract._KNOWN_ASSET_TYPES so downstream vocab checks agree.
_HEADING_ASSET_TYPES = [
    (r"mutual\s+fund|registered\s+investment", "Mutual Fund"),
    (r"collective\s+trust|common/collective", "Common/Collective Trust Fund"),
    (r"absolute\s+return|hedge\s+fund", "Hedge Fund"),
    (r"commingled", "Commingled Fund"),
    (r"private\s+invest|private\s+equity|partnership", "Partnership Interest"),
    (r"insurance\s+contract", "Insurance General Account"),
    (r"real\s+estate", "Real Estate"),
    (r"preferred\s+stock", "Preferred Stock"),
    (r"common\s+stock|corporate\s+stock|equit", "Common Stock"),
    (r"bond|government|fixed\s+income|debt", "Bond"),
    (r"money\s+market", "Money Market Fund"),
    (r"short[\s-]term", "Short-Term Investment Fund"),
    (r"^cash", "Cash"),
]

# Applied to the row title when its heading maps to nothing (e.g. "Other:").
_TITLE_ASSET_TYPES = [
    (r"\bcash\b", "Cash"),
]

# Rows sitting above the first heading on the first requested page belong to
# a section opened on an earlier page; look back this far for its heading.
_LOOKBACK_PAGES = 15


def _num(tok: str) -> Optional[float]:
    if tok in ("-", "—", "–"):
        return 0.0
    neg = tok.startswith("(") or tok.startswith("-")
    digits = tok.strip("()-$").replace(",", "")
    try:
        val = float(digits)
    except ValueError:
        return None
    return -val if neg else val


def _fmt(val: Optional[float]) -> str:
    if val is None:
        return ""
    return str(int(val)) if val == int(val) else str(val)


def heading_asset_type(heading: str) -> str:
    for pattern, label in _HEADING_ASSET_TYPES:
        if re.search(pattern, heading, re.I):
            return label
    return ""


def _is_page_footer(tokens: List[str]) -> bool:
    return len(tokens) == 1 and tokens[0].isdigit()


def _heading_of(line: str) -> Optional[str]:
    return line[:-1].strip() if _HEADING_RE.match(line) else None


def _classify_line(line: str) -> tuple:
    """Return (kind, payload) for one text line.

    kind is one of: skip, heading, subtotal, row, unparsed.
    """
    line = line.strip()
    if not line or _BOILERPLATE_RE.match(line):
        return "skip", None
    heading = _heading_of(line)
    if heading:
        return "heading", heading
    tokens = line.split()
    if _is_page_footer(tokens):
        return "skip", None
    if all(_NUM_RE.match(t) for t in tokens):
        return "subtotal", [_num(t) for t in tokens]

    trailing = []
    while tokens and _NUM_RE.match(tokens[-1]) and len(trailing) < 3:
        trailing.insert(0, tokens.pop())
    if not trailing or not tokens:
        return "unparsed", line

    identifier = tokens.pop(0) if _ID_RE.match(tokens[0]) else ""
    maturity = tokens.pop() if tokens and _DATE_RE.match(tokens[-1]) else ""
    rate = tokens.pop() if maturity and tokens and _RATE_RE.match(tokens[-1]) else ""
    title = " ".join(tokens)
    if not title:
        return "unparsed", line

    nums = [_num(t) for t in trailing]
    shares, cost, value = ([None] * (3 - len(nums)) + nums)
    return "row", {
        "identifier": identifier, "title": title, "rate": rate, "maturity": maturity,
        "shares": shares, "cost": cost, "value": value,
    }


def _ipr_classify_line(line: str, heading: str) -> tuple:
    """_classify_line for the issue_par_rate layout (title, par, rate, maturity, cost, value)."""
    line = line.strip()
    if not line:
        return "skip", None
    tokens = line.split()
    if heading and line.upper().startswith(f"TOTAL {heading.upper()} "):
        rest = line[len(heading) + 7:].split()
        if rest and all(_NUM_RE.match(t) for t in rest):
            return "subtotal", [_num(t) for t in rest]
    if _IPR_HEADING_RE.match(line):
        return "heading", line
    if _is_page_footer(tokens):
        return "skip", None

    trailing = []
    while tokens and _NUM_RE.match(tokens[-1]) and len(trailing) < 2:
        trailing.insert(0, tokens.pop())
    maturity = tokens.pop() if len(trailing) == 2 and tokens and _DATE_RE.match(tokens[-1]) else ""
    rate = tokens.pop() if maturity and tokens and _RATE_RE.match(tokens[-1]) else ""
    # Par value (bonds) or shares/units (no maturity) sits left of cost.
    shares = _num(tokens.pop()) if len(trailing) == 2 and tokens and _NUM_RE.match(tokens[-1]) else None
    title = " ".join(tokens)
    if not trailing or not re.search(r"[A-Za-z]", title):
        return "unparsed", line

    cost, value = ([None] * (2 - len(trailing)) + [_num(t) for t in trailing])
    return "row", {
        "identifier": "", "title": title, "rate": rate, "maturity": maturity,
        "shares": shares, "cost": cost, "value": value,
    }


def _drop_overlapping_spaces(page):
    """Remove space glyphs drawn on top of a real character (right-aligned
    padding), which otherwise split one number into two tokens."""
    rows: Dict[int, list] = {}
    for c in page.chars:
        if c["text"] != " ":
            rows.setdefault(round(c["top"]), []).append(c)

    def overlaps(sp) -> bool:
        top = round(sp["top"])
        return any(min(sp["x1"], c["x1"]) - max(sp["x0"], c["x0"]) > 0.5
                   for k in (top - 1, top, top + 1) for c in rows.get(k, ()))

    bad = {id(c) for c in page.chars if c["text"] == " " and overlaps(c)}
    return page.filter(lambda obj: id(obj) not in bad) if bad else page


def _page_text(page, layout: str) -> str:
    if layout == "issue_par_rate":
        page = _drop_overlapping_spaces(page)
    return page.extract_text() or ""


def _header_re(layout: str):
    return _IPR_HEADER_RE if layout == "issue_par_rate" else _HEADER_RE


def _headings(text: str, layout: str) -> List[str]:
    if layout == "issue_par_rate":
        lines = text.splitlines()
        header_idx = next((i for i, l in enumerate(lines) if _IPR_HEADER_RE.search(l)), len(lines))
        return [l.strip() for l in lines[header_idx + 1:] if _IPR_HEADING_RE.match(l.strip())]
    return [h for h in (_heading_of(l.strip()) for l in text.splitlines()) if h]


def registered_ack_ids(workgroup: str, s3_output: str) -> Dict[str, str]:
    """ack_id -> plan_id for every plan_master filing of a registered plan."""
    import awswrangler as wr

    plan_ids = ", ".join(f"'{p}'" for p in PLAN_PARSERS)
    df = wr.athena.read_sql_query(
        sql=f"SELECT ack_id, plan_id FROM plan_master WHERE plan_id IN ({plan_ids})",
        database="default", workgroup=workgroup, s3_output=s3_output, ctas_approach=False,
    )
    return dict(zip(df["ack_id"], df["plan_id"]))


def find_schedule_4i_pages(pdf_path: str, start_heading: str, layout: str = "default") -> List[int]:
    """Pages of the first 4i schedule copy, from start_heading's page to its end.

    The schedule is the first contiguous run of pages carrying the 4i column
    header; some filers print it twice (Mayo 2025: pp.28-52 and again
    pp.103-127), and only the first copy is taken. Returns [] if the run or
    the heading isn't found.
    """
    run: List[int] = []
    start = None
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            text = _page_text(page, layout)
            if not _header_re(layout).search(text):
                if run:
                    break
                continue
            run.append(page_num)
            if start is None and any(re.match(start_heading, h, re.I) for h in _headings(text, layout)):
                start = page_num
    return [p for p in run if p >= start] if start else []


def _inherited_heading(pdf, first_page: int, layout: str = "default") -> str:
    """Last section heading printed before first_page, scanning backwards."""
    for page_num in range(first_page - 1, max(0, first_page - 1 - _LOOKBACK_PAGES), -1):
        text = _page_text(pdf.pages[page_num - 1], layout)
        if layout == "issue_par_rate" and not _IPR_HEADER_RE.search(text):
            continue
        headings = _headings(text, layout)
        if headings:
            return headings[-1]
    return ""


def parse_schedule_4i_pages(pdf_path: str, page_nums: List[int], layout: str = "default") -> Optional[Dict]:
    """Parse the requested pages. Returns None if any page lacks the 4i header.

    Result: {"pages": [<run_pipeline page_data dicts>], "sections": [...],
             "unparsed": [...], "grand_total": [cost, value] or None}
    """
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        texts = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            texts[page_num] = _page_text(pdf.pages[page_num - 1], layout)
            if not _header_re(layout).search(texts[page_num]):
                return None
        inherited = _inherited_heading(pdf, page_nums[0], layout)

    sections = []
    current = {"heading": inherited, "asset_type": heading_asset_type(inherited),
               "partial": True, "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None}
    sections.append(current)
    unparsed = []
    grand_total = None
    last_kind = None
    pages = []

    for page_num in page_nums:
        mapped_rows = []
        lines = texts[page_num].splitlines()
        # Everything above the column header is the page's title block (plan name, EIN, ...).
        header_idx = next(i for i, l in enumerate(lines) if _header_re(layout).search(l))
        for line in lines[header_idx + 1:]:
            if layout == "issue_par_rate":
                kind, payload = _ipr_classify_line(line, current["heading"])
            else:
                kind, payload = _classify_line(line)
            if kind == "skip":
                continue
            if kind == "heading":
                current = {"heading": payload, "asset_type": heading_asset_type(payload),
                           "partial": False, "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None}
                sections.append(current)
            elif kind == "subtotal":
                # A second totals line straight after a subtotal is the schedule grand total.
                if last_kind == "subtotal":
                    grand_total = payload
                else:
                    current["subtotal"] = payload
            elif kind == "unparsed":
                unparsed.append({"page_number": page_num, "line": payload})
            else:
                asset_type = current["asset_type"]
                source = "heading" if asset_type else "none"
                if not asset_type:
                    asset_type = detect_asset_type(f"{current['heading']} {payload['title']}")
                    if not asset_type:
                        asset_type = next((label for pattern, label in _TITLE_ASSET_TYPES
                                           if re.search(pattern, payload["title"], re.I)), "")
                    source = "row" if asset_type else "none"
                current["rows"] += 1
                current["cost"] += payload["cost"] or 0.0
                current["value"] += payload["value"] or 0.0
                mapped_rows.append({
                    "issuer_name": "",
                    "investment_description": payload["title"],
                    "asset_type": asset_type,
                    "asset_type_source": source,
                    "par_value": "",
                    "cost": _fmt(payload["cost"]),
                    "current_value": _fmt(payload["value"]),
                    "units_or_shares": _fmt(payload["shares"]),
                    "asset_identifier": payload["identifier"],
                    "interest_rate": payload["rate"],
                    "maturity_date": payload["maturity"],
                    "section_heading": current["heading"],
                    "extraction_method": "sch4i_text",
                    "page_number": page_num,
                    "row_id": len(mapped_rows) + 1,
                })
            last_kind = kind
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    if not sections[0]["rows"]:
        sections.pop(0)
    for section in sections:
        sub = section["subtotal"]
        sub_cost, sub_value = (sub[-2], sub[-1]) if sub and len(sub) >= 2 else (None, sub[-1] if sub else None)
        section["cost_diff"] = None if sub_cost is None else section["cost"] - sub_cost
        section["value_diff"] = None if sub_value is None else section["value"] - sub_value
        if section["partial"] or sub_value is None:
            section["reconciled"] = None
        else:
            # Filers print whole dollars but total the unrounded figures, so allow
            # up to $1 of rounding drift per row before calling it a mismatch.
            tol = max(1.0, float(section["rows"]))
            # issue_par_rate: cost is advisory. Shell's own printed cost subtotals
            # don't foot (stocks +$831, insurance +$5,343, partnerships +$21,864
            # over the printed rows, checked by hand) while every value subtotal
            # does to the dollar, and value is the only figure loaded.
            cost_ok = (layout == "issue_par_rate" or section["cost_diff"] is None
                       or abs(section["cost_diff"]) <= tol)
            section["reconciled"] = abs(section["value_diff"]) <= tol and cost_ok

    # When the printed grand total is in range and no section was cut off at
    # the start, every row of the schedule was parsed: the rows must sum to it.
    grand_reconciled = None
    if grand_total and not any(s["partial"] for s in sections):
        total_rows = sum(s["rows"] for s in sections)
        grand_diff = sum(s["value"] for s in sections) - grand_total[-1]
        grand_reconciled = abs(grand_diff) <= max(1.0, float(total_rows))
    return {"pages": pages, "sections": sections, "unparsed": unparsed,
            "grand_total": grand_total, "grand_reconciled": grand_reconciled}


def is_trustworthy(result: Optional[Dict]) -> bool:
    """Every reconcilable section and the grand total (if checkable) tie out."""
    if result is None or not any(p["mapped_rows"] for p in result["pages"]):
        return False
    if any(s["reconciled"] is False for s in result["sections"]):
        return False
    return result["grand_reconciled"] is not False


def print_parse_report(result: Dict) -> None:
    rows = sum(len(p["mapped_rows"]) for p in result["pages"])
    value = sum(s["value"] for s in result["sections"])
    print(f"    [sch4i_text_parser] {rows} row(s), market value {value:,.0f}")
    for s in result["sections"]:
        status = {True: "OK", False: "MISMATCH", None: "n/a (section starts before requested pages)"
                  if s["partial"] else "n/a (no subtotal in range)"}[s["reconciled"]]
        diffs = "" if s["partial"] else (
            f" value_diff={s['value_diff']:+,.0f}" if s["value_diff"] is not None else "") + (
            f" cost_diff={s['cost_diff']:+,.0f}" if s["cost_diff"] is not None and not s["partial"] else "")
        sub = s["subtotal"][-1] if s["subtotal"] else None
        print(f"      {s['heading'] or '<no heading>'!r:28} -> {s['asset_type'] or '<blank>':30} "
              f"rows={s['rows']:4d} value={s['value']:>16,.0f} subtotal="
              f"{'-' if sub is None else format(sub, ',.0f'):>16} {status}{diffs}")
    for u in result["unparsed"]:
        print(f"      UNPARSED p.{u['page_number']}: {u['line']}")
    if result["grand_total"]:
        status = {True: "OK", False: "MISMATCH", None: "n/a (partial schedule)"}[result["grand_reconciled"]]
        print(f"      schedule grand total printed: {result['grand_total']} {status}")
