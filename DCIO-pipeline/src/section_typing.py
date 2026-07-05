"""Stage-2 over-capture fix: section-aware asset-type re-typing (ISOLATED).

This module is used ONLY by the Stage-2 over-capture re-extraction path (branch
`overcapture-reextract`, flag OVERCAPTURE_MODE=1). It does NOT modify the core
extractor. It runs as a POST-PASS over the output of the unchanged
`text_extract.extract_tables_and_map`, and can only DOWNGRADE rows (assign a
correct non-MF asset_type so the load gate drops them). It never adds rows and
never turns a non-MF row into MF unless the row sits under an explicit MF section.

Root cause it fixes (Mode 3): audited 4i schedules group holdings under section
headers ("Common Stocks", "Corporate Debt Instruments", "Mutual Funds", ...). The
core text parser's SECTION_HEADING_MAP lists only MF-ish labels, so entering a
non-MF section does not reset the running section type; the stale MF type bleeds
onto bonds/stocks/treasuries, which then pass the MF load gate. Here we re-derive
each page's section boundaries from a FULL map (MF + non-MF) using word geometry,
then assign every row the asset_type of the section it physically falls under.
"""

import re
from typing import Dict, List, Optional

# ---------------------------------------------------------------------------
# Full section-header map (MF-ish AND non-MF). Order matters: first match wins.
# Non-MF types are chosen to match the load gate's exclusion set / to be types
# NOT in MF_ASSET_TYPES so build_mf_rows_df drops them.
# ---------------------------------------------------------------------------
_SECTION_MAP = [
    # --- MF / MF-adjacent (KEPT by the load gate) ---
    (r"registered\s+investment\s+compan", "Mutual Fund"),
    (r"\bmutual\s+funds?\b", "Mutual Fund"),
    (r"money\s+market\s+funds?\b", "Money Market Fund"),
    # --- non-MF (DROPPED by the load gate) ---
    (r"common\s*/\s*collective\s+trust|collective\s+investment\s+trust|collective\s+trust", "Common/Collective Trust Fund"),
    (r"commingled|pooled\s+separate\s+account", "Commingled Fund"),
    (r"self[\s-]*directed\s+brokerage", "Self-Directed Brokerage Account"),
    (r"preferred\s+stock", "Preferred Stock"),
    (r"employer\s+(stock|securities)|company\s+stock", "Employer Stock"),
    (r"common\s+stocks?\b|corporate\s+stock", "Common Stock"),
    (r"corporate\s+debt|corporate\s+bond|debt\s+instrument", "Corporate Debt"),
    (r"asset[\s-]*backed|mortgage[\s-]*backed", "Asset-Backed Security"),
    (r"u\.?\s*s\.?\s+government|government\s+(securit|obligation|agenc)|treasur", "Government Securities"),
    (r"municipal", "Government Securities"),
    (r"certificates?\s+of\s+deposit", "Cash"),
    (r"interest[\s-]*bearing\s+cash|cash\s+equivalent|short[\s-]*term\s+invest", "Cash"),
    (r"guaranteed\s+(investment|insurance)\s+contract", "Guaranteed Investment Contract"),
    (r"stable\s+value", "Stable Value Fund"),
    (r"insurance\s+(company\s+)?general\s+account|general\s+account\s+contract", "Insurance General Account"),
    (r"group\s+annuity", "Group Annuity Contract"),
    (r"partnership|limited\s+partnership|private\s+equity", "Partnership Interest"),
    (r"real\s+estate", "Real Estate"),
    (r"participant\s+loans?|notes?\s+receivable", "Participant Loan"),
    (r"other\s+investments?\b", "Other Investment"),
]
_SECTION_MAP = [(re.compile(p, re.IGNORECASE), t) for p, t in _SECTION_MAP]

# MF-ish types that must NOT be overwritten and that pass the gate.
MF_SECTION_TYPES = frozenset({"Mutual Fund", "Money Market Fund"})

# A line is only treated as a section header if it is short and label-like:
# no trailing dollar value, few words, mostly letters. Prevents a fund NAME that
# contains "common stock" etc. from being read as a header.
_HAS_VALUE_RE = re.compile(r"[\$]?\s*[\d,]{2,}(?:\.\d+)?\s*$")
_DIGIT_RE = re.compile(r"\d")


def classify_section_line(line: str) -> Optional[str]:
    """Return the asset_type if `line` is a standalone section header, else None."""
    s = (line or "").strip()
    if not s or len(s) > 60:
        return None
    # headers have no trailing money value and are short (<= 6 words)
    if _HAS_VALUE_RE.search(s):
        return None
    if len(s.split()) > 6:
        return None
    for rx, atype in _SECTION_MAP:
        m = rx.search(s)
        if m:
            # the matched label must DOMINATE the line (a header, not a fund name that
            # merely contains the words). Strip the label + common section-noun
            # decoration; if a real brand/fund name remains, it is not a header.
            residue = rx.sub("", s)
            residue = re.sub(
                r"(?i)\b(total|sub|schedule|of|held|investments?|instruments?|"
                r"securit(?:y|ies)|accounts?|obligations?|compan(?:y|ies)|funds?|"
                r"trusts?|stocks?|contracts?|deposits?|cash|equivalents?|at|fair|"
                r"value|and|other|the|for|purposes|u|s)\b",
                "", residue)
            residue = re.sub(r"[^A-Za-z]", "", residue)
            if len(residue) <= 3:
                return atype
    return None


def _cluster_lines(words: List[Dict], y_tol: float = 3.0):
    """Group pdfplumber words into text lines by their top-y. Returns list of
    (top_y, line_text, [words]) sorted top-to-bottom."""
    rows: List[List[Dict]] = []
    for w in sorted(words, key=lambda w: (round(w["top"] / y_tol), w["x0"])):
        placed = False
        for r in rows:
            if abs(r[0]["top"] - w["top"]) <= y_tol:
                r.append(w)
                placed = True
                break
        if not placed:
            rows.append([w])
    out = []
    for r in rows:
        r.sort(key=lambda w: w["x0"])
        top = min(w["top"] for w in r)
        text = " ".join(w["text"] for w in r)
        out.append((top, text, r))
    out.sort(key=lambda t: t[0])
    return out


def _value_variants(current_value) -> set:
    """Numeric string forms a row's value might take on the page (raw vs de-scaled),
    all comma-stripped for comparison against a line's trailing number."""
    out = set()
    try:
        v = float(str(current_value).replace(",", ""))
    except (TypeError, ValueError):
        return out
    iv = int(round(v))
    out.add(str(iv))
    if iv % 1000 == 0:            # page may show "(in thousands)" -> row scaled x1000
        out.add(str(iv // 1000))
    return out


_NUM_TOKEN_RE = re.compile(r"\d[\d,]*(?:\.\d+)?")


def _line_trailing_number(text: str) -> Optional[str]:
    """The last numeric token on a line (holdings put current value at line end),
    comma-stripped and truncated to the integer part."""
    nums = _NUM_TOKEN_RE.findall(text or "")
    if not nums:
        return None
    return nums[-1].replace(",", "").split(".")[0]


def retype_page(page_words: List[Dict], rows: List[Dict], carry_type: Optional[str]) -> Optional[str]:
    """Re-type one page's rows in place using word geometry. Returns the section
    type in effect at the bottom of the page (to carry into the next page)."""
    lines = _cluster_lines(page_words)
    # ordered section boundaries on this page: (top_y, type)
    headers = []
    for top, text, _ in lines:
        atype = classify_section_line(text)
        if atype:
            headers.append((top, atype))

    def section_at(y: Optional[float]) -> Optional[str]:
        cur = carry_type
        if y is None:
            return cur
        for htop, atype in headers:
            if htop <= y + 1.0:
                cur = atype
            else:
                break
        return cur

    # locate each row's y: (1) by its value == a line's trailing number, else
    # (2) by its name appearing on a line. If unlocatable, section_at(None)
    # falls back to the carried section -> stragglers still get the right type.
    for row in rows:
        y = None
        variants = _value_variants(row.get("current_value"))
        if variants:
            for top, text, _ in lines:
                if _line_trailing_number(text) in variants:
                    y = top  # last matching line (values sit at the row's baseline)
        if y is None:
            nm = (row.get("issuer_name") or row.get("investment_description") or "").strip()
            key = nm[:18].upper()
            if len(key) >= 4:
                for top, text, _ in lines:
                    if key in text.upper():
                        y = top
                        break
        sect = section_at(y)
        if sect and sect not in MF_SECTION_TYPES:
            row["asset_type"] = sect            # non-MF -> gate will drop it
        elif sect in MF_SECTION_TYPES and not str(row.get("asset_type") or "").strip():
            row["asset_type"] = sect            # only fill blanks with MF type

    # section in effect at page bottom
    if headers:
        return headers[-1][1]
    return carry_type


def retype_result(result: List[Dict], pdf_path: str) -> Dict[str, int]:
    """Post-pass: re-type asset_type on every row of an extraction `result`
    (list of page entries with 'page_number' and 'mapped_rows'), using each
    page's section headers. Threads the running section type across pages so
    multi-page sections (continuation pages with no header) are handled.
    Returns a small stats dict. Mutates `result` in place.
    """
    import pdfplumber

    stats = {"pages": 0, "rows": 0, "retyped_non_mf": 0}
    carry: Optional[str] = None
    with pdfplumber.open(pdf_path) as doc:
        npages = len(doc.pages)
        # process entries in page order so `carry` flows correctly
        for entry in sorted([e for e in result if isinstance(e.get("page_number"), int)],
                            key=lambda e: e["page_number"]):
            pn = entry["page_number"]
            rows = entry.get("mapped_rows", [])
            if not (1 <= pn <= npages) or not rows:
                continue
            words = doc.pages[pn - 1].extract_words() or []
            before = [str(r.get("asset_type") or "").strip() for r in rows]
            carry = retype_page(words, rows, carry)
            after = [str(r.get("asset_type") or "").strip() for r in rows]
            stats["pages"] += 1
            stats["rows"] += len(rows)
            for b, a in zip(before, after):
                if a and a != b and a not in MF_SECTION_TYPES:
                    stats["retyped_non_mf"] += 1
    return stats


# ---------------------------------------------------------------------------
# Row-level cleanup (subtotal drop + dedup) -- also isolated to the Stage-2 path.
# ---------------------------------------------------------------------------
_TOTAL_PREFIX_RE = re.compile(r"(?i)^\s*(sub[\s-]?)?totals?\b")

# Name-based CIT catch (defense-in-depth). CITs must be EXCLUDED from the MF table
# (captured elsewhere). Section-typing removes CITs where the "Common/Collective
# Trusts" section is detected; this catches CIT holdings on plans/pages where the
# section header was missed and the row arrived blank- or MF-typed. Patterns are
# distinctive to collective vehicles so real mutual funds (which may say "Trust",
# e.g. "iShares Trust") are NOT hit.
_CIT_NAME_RE = re.compile(
    r"(?i)(collective\s+(trust|investment|fund)|commingled|common\s*/\s*collective"
    r"|\bcoll(?:ective)?\s+(trust|tr|inv)\b|\bC\s*/\s*C\s+trust\b|\bCCT\b)")


def looks_like_cit(name: str) -> bool:
    return bool(_CIT_NAME_RE.search(name or ""))


def is_section_subtotal(name: str) -> bool:
    """True if `name` is a section subtotal line ("Total Corporate Debt Instruments",
    "Total Self-Directed Brokerage Accounts", bare "Total"/"Subtotal"). Requires the
    line to START with total/subtotal AND either name a section (from _SECTION_MAP)
    or be a bare total -- so real funds ("PIMCO Total Return", "Vanguard Total Bond
    Market") are never dropped (they don't start with 'Total ' + a section label)."""
    s = (name or "").strip()
    if not _TOTAL_PREFIX_RE.match(s):
        return False
    if re.fullmatch(r"(?i)\s*(sub[\s-]?)?totals?\s*", s):
        return True
    return any(rx.search(s) for rx, _ in _SECTION_MAP)


def flatten_clean(result: List[Dict]) -> List[Dict]:
    """Flatten an extraction result to a row list, dropping section-subtotal rows
    and de-duplicating identical (issuer, description, value) rows (the core
    extractor's table+text double-pass can emit each holding twice). Isolated to
    Stage 2 -- the core pipeline is unaffected."""
    seen = set()
    out: List[Dict] = []
    for entry in result:
        for r in entry.get("mapped_rows", []):
            name = (r.get("issuer_name") or r.get("investment_description") or "").strip()
            if is_section_subtotal(name):
                continue
            # Name-based CIT catch: force the CIT type so the load gate drops it,
            # even if the section header was missed (row blank- or MF-typed).
            if looks_like_cit(name):
                r["asset_type"] = "Common/Collective Trust Fund"
            key = (
                str(r.get("issuer_name", "") or "").strip().upper(),
                str(r.get("investment_description", "") or "").strip().upper(),
                str(r.get("current_value", "") or "").strip(),
            )
            if key in seen:
                continue
            seen.add(key)
            out.append(r)
    return out
