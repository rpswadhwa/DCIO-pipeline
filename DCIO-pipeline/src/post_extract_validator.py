"""
post_extract_validator.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Post-extraction validation gate for the DCIO Form 5500 pipeline.

Compares per-PDF mutual fund totals against reference totals in the Glue
table `plan_master_index_universe`.  PDFs within the tolerance threshold
have their MF rows written to `plan_mf_history_v3` with the columns:
  ack_id              — pdf_stem (filename without .pdf)
  raw_entity_name     — issuer_name
  plan_investment_amt — current_value (float)

Failures are written to a separate error table.

Required env vars:
    ATHENA_STAGING_S3   — S3 path for Athena query result staging
    VALIDATED_S3_PATH   — S3 path registered for plan_mf_history_v3
"""

import logging
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd

import re as _re

_SHARES_OF_PREFIX_RE = _re.compile(
    r"^[\d,]+(?:\.\d+)?\s+shares?\s+of\s+", _re.IGNORECASE
)

_FUND_KEYWORDS = frozenset({
    "fund", "etf", "trust", "portfolio", "index", "series",
    "blend", "growth", "income", "balanced", "bond", "equity",
    "market", "international", "global", "allocation", "target",
    "stable", "value", "core", "select", "total", "money",
    "retirement", "horizon", "lifecycle", "moderate", "aggressive",
    "conservative", "dividend", "appreciation", "opportunity",
})

_MANAGER_KEYWORDS = frozenset({
    "management", "company", "advisors", "adviser", "partners",
    "associates", "group", "llc", "inc", "corp", "corporation",
    "capital", "investments", "asset", "financial", "securities",
    "services", "solutions", "holdings",
})

_KNOWN_MANAGERS = frozenset({
    "vanguard", "fidelity", "blackrock", "pimco",
    "t rowe price", "t. rowe price", "jpmorgan", "jp morgan",
    "goldman sachs", "state street", "ssga", "charles schwab",
    "schwab", "american funds", "dimensional", "dfa",
    "northern trust", "metlife", "prudential", "principal",
    "empower", "transamerica", "lincoln", "john hancock", "mfs",
    "putnam", "invesco", "franklin templeton", "columbia",
    "american century", "nuveen", "tiaa", "cref", "calvert",
    "dodge and cox", "dodge & cox", "wellington", "parametric",
    "pacific investment management company", "ishares",
    "metropolitan west", "metwest", "neuberger berman", "baird",
    "william blair", "western asset", "loomis sayles",
    "vanguard group", "the vanguard group",
    "fidelity investments", "blackrock inc",
})

_SHARE_CLASS_RE = _re.compile(
    r"\b(class\s+[a-z]|institutional|investor|admiral|signal|"
    r"premium|select|premier|r[\s-]?\d+(?=\s|$)|i\s*shares?)\b",
    _re.IGNORECASE,
)

def _normalize_for_manager_check(text):
    """Strip common wrapper words before checking against known managers."""
    t = text.lower().strip()
    t = _re.sub(r"^the\s+", "", t)
    t = _re.sub(r"\s+(inc\.?|llc\.?|corp\.?|group|company|co\.?)$", "", t).strip()
    return t

_GENERIC_CATEGORIES = frozenset({
    "registered investment company", "pooled separate account",
    "insurance general account", "group annuity contract",
    "stable value fund", "self-directed accounts",
    "self-directed brokerage account", "participant loan fund",
    "common collective trust", "collective investment trust",
    "separate account", "general account", "annuity contract",
    "variable annuity", "fixed annuity", "bank collective fund",
    "guaranteed investment contract", "gic", "brokerage account",
    "mutual fund", "money market", "common stock", "reit",
    "foreign currency", "employer securities",
})

_SHARE_CLASS_STRONG_RE = _re.compile(
    r"\b(r[\s-]?[1-6](?=\s|$)|institutional(?:\s+(?:plus|shares?))?|investor\s+shares?|"
    r"admiral\s+shares?|signal\s+shares?|class\s+[a-z]|i\s*shares?|etf)\b",
    _re.IGNORECASE,
)

# Some Schedule H, 4i tables put the asset-type category (not the fund name) in the
# "Description of Investment" column, followed only by a share/unit count bled in from
# an adjacent column, e.g. "Mutual Fund - 738 Shares" or "Pooled Separate Account - 783".
# That is a near-zero-information placeholder, not a name -- strip the trailing count so
# it can be checked against _GENERIC_CATEGORIES like any other bare category label.
_TRAILING_COUNT_RE = _re.compile(
    r"[-–]\s*[\d,]+(?:\.\d+)?\s*(?:shares?|units?)?\s*$", _re.IGNORECASE,
)

def _score_as_fund_name(text):
    if not text or not text.strip():
        return -999
    t = text.strip().lower()
    # Collapse internal spaces (PDF extraction can add spaces mid-word)
    t_collapsed = _re.sub(r"\s+", " ", t)
    t_nospace = t_collapsed.replace(" ", "")
    # Generic investment category label — strongly penalise
    if t_collapsed in _GENERIC_CATEGORIES:
        return -50
    for cat in _GENERIC_CATEGORIES:
        if cat.replace(" ", "") == t_nospace:
            return -50
    # Same penalty when the category label has a share/unit count tacked on
    # ("mutual fund - 738 shares" -> "mutual fund").
    _decounted = _TRAILING_COUNT_RE.sub("", t_collapsed).strip()
    if _decounted and _decounted != t_collapsed and _decounted in _GENERIC_CATEGORIES:
        return -50
    words = set(_re.findall(r"\w+", t))
    score = 0
    score += len(words & _FUND_KEYWORDS) * 3
    score -= len(words & _MANAGER_KEYWORDS) * 4
    if t in _KNOWN_MANAGERS or _normalize_for_manager_check(t) in _KNOWN_MANAGERS:
        score -= 20
    if _SHARE_CLASS_STRONG_RE.search(text):
        score += 15
    elif _SHARE_CLASS_RE.search(text):
        score += 10
    if _re.search(r"\b20[2-9]\d\b", text):
        score += 20
    word_count = len(text.split())
    if 3 <= word_count <= 12:
        score += 2
    return score

def _clean_description(desc):
    """Strip leading share-count prefix from description."""
    return _SHARES_OF_PREFIX_RE.sub("", desc).strip()

def pick_fund_name(issuer_name, investment_description):
    """Return whichever of issuer_name / investment_description looks more like a fund name.
    Strips share-count prefix (e.g. '3,478,894.31 shares of ') from description first.
    """
    issuer = str(issuer_name or "").strip()
    desc = _clean_description(str(investment_description or "").strip())
    from .ditto_fix import is_junk_name
    if is_junk_name(desc):
        desc = ""
    if not issuer and not desc:
        return ""
    if not issuer:
        return desc
    if not desc:
        return issuer
    return desc if _score_as_fund_name(desc) > _score_as_fund_name(issuer) else issuer





logger = logging.getLogger(__name__)

MF_ASSET_TYPES = frozenset({"mutual fund", "index fund", "etf", "target date fund"})
# Rollup/placeholder line items that are sometimes mistagged with an MF asset_type but are
# NOT a real holding -- summing them double-counts money already captured by the plan's real
# itemized fund rows (e.g. Loyola University Chicago, 2026-09-20: a single "See Attached" row
# equal to the plan's entire certified mutual-fund total, sitting alongside the real per-fund
# breakdown). "see attached" already exists in junk_detect.py's V3_WRONGTYPE_EXACT denylist,
# but _route_mf_from_staging is a raw-SQL router that never calls junk_detect -- this filters
# the same known-junk names directly in the routing query so they can't be reintroduced here.
MF_ROUTING_EXCLUDE_NAMES = frozenset({"see attached"})
BAD_REFERENCE_COMPARISON_OVERRIDES = frozenset({
    ("20251010135251NAL0018754754001", "202777218-002"),
})


# ---------------------------------------------------------------------------
# Numeric helpers
# ---------------------------------------------------------------------------

def parse_currency_value(raw: Optional[str]) -> Optional[float]:
    """Parse a currency string to float, returning None on failure."""
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    cleaned = text.replace(",", "").replace("$", "").replace("(", "-").replace(")", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Reference data loader
# ---------------------------------------------------------------------------

def load_reference(glue_db: str, table: str, workgroup: str,
                   s3_staging: str) -> Dict[str, Dict[str, object]]:
    """Query Athena for ack_id -> reference metadata.

    Returns only rows where amt_mutual_funds is a positive number.
    Rows with null or zero values are excluded (treated as SKIP at call time).
    """
    import awswrangler as wr

    # amt_cit is used by the MF reconciliation (stage 4); tolerate tables that lack it.
    try:
        sql = f"SELECT ack_id, plan_id, amt_mutual_funds, amt_cit FROM {glue_db}.{table}"
        df = wr.athena.read_sql_query(sql=sql, database=glue_db, workgroup=workgroup, s3_output=s3_staging)
        _has_cit = True
    except Exception:
        sql = f"SELECT ack_id, plan_id, amt_mutual_funds FROM {glue_db}.{table}"
        df = wr.athena.read_sql_query(sql=sql, database=glue_db, workgroup=workgroup, s3_output=s3_staging)
        _has_cit = False

    reference: Dict[str, Dict[str, object]] = {}
    for _, row in df.iterrows():
        ack_id = str(row["ack_id"]).strip() if row["ack_id"] is not None else ""
        if not ack_id:
            continue
        val = parse_currency_value(str(row["amt_mutual_funds"]))
        if val and val > 0:
            reference[ack_id] = {
                "plan_id": str(row.get("plan_id", "") or "").strip(),
                "amt_mutual_funds": val,
                "amt_cit": (parse_currency_value(str(row.get("amt_cit"))) or 0.0) if _has_cit else 0.0,
            }
        else:
            logger.debug("Skipping reference row ack_id=%s: amt_mutual_funds=%s", ack_id, row["amt_mutual_funds"])

    logger.info("Loaded %d reference entries from %s.%s", len(reference), glue_db, table)
    return reference


# ---------------------------------------------------------------------------
# MF total aggregator
# ---------------------------------------------------------------------------

def compute_extracted_mf_totals(rows: List[Dict],
                                 mf_types: frozenset = MF_ASSET_TYPES) -> Dict[str, float]:
    """Sum current_value for MF asset types, grouped by pdf_stem.

    Rows with unparseable current_value are skipped.  Missing pdf_stem
    values are skipped with a warning.
    """
    totals: Dict[str, float] = defaultdict(float)
    for row in rows:
        pdf_stem = str(row.get("pdf_stem", "") or "").strip()
        if not pdf_stem:
            logger.warning("Row missing pdf_stem, skipping: issuer=%s", row.get("issuer_name"))
            continue
        asset_type = str(row.get("asset_type", "") or "").strip().lower()
        if asset_type not in mf_types:
            continue
        val = parse_currency_value(row.get("current_value"))
        if val is None:
            logger.debug("Unparseable current_value for pdf_stem=%s: %s", pdf_stem, row.get("current_value"))
            continue
        totals[pdf_stem] += val
    return dict(totals)


def compute_extracted_all_types_totals(rows: List[Dict]) -> Dict[str, float]:
    """Sum extracted value per pdf_stem across ALL asset types, not just MF types.

    Mirrors build_mf_rows_df(rows, mf_only=False)'s population -- i.e. every row
    that would land in plan_holdings_staging, regardless of asset_type (junk /
    unparseable-value / no-name rows are still dropped by that same function).

    This is the correct "extracted" side for the OCR-fallback under-capture
    check: it must be compared against the certified amt_mutual_funds directly,
    NOT against plan_mf_history_v3 (which is already asset_type-filtered).
    Comparing against v3 would conflate a genuine extraction failure (OCR can
    fix this) with a plan that extracted fine but has rows sitting in staging
    with a blank/non-MF asset_type blocking promotion (a separate, deferred
    problem that OCR cannot fix).
    """
    staging_df = build_mf_rows_df(rows, mf_only=False)
    if staging_df.empty:
        return {}
    totals = staging_df.groupby("ack_id")["plan_investment_amt"].sum(min_count=1)
    return {str(k): float(v) for k, v in totals.dropna().items()}


# ---------------------------------------------------------------------------
# Duplicate detection helpers (used by dedup_plan_rows)
# ---------------------------------------------------------------------------
import difflib as _difflib

_DEDUP_VAL_TOL = 3.0        # a duplicate page can re-parse the same value +/- a dollar or two
# filler / share-class tokens that don't distinguish two funds
_DEDUP_STOP = {
    'fund', 'funds', 'fd', 'the', 'of', 'shares', 'share', 'cl', 'class', 'premier', 'inst',
    'institutional', 'adm', 'admiral', 'inv', 'investor', 'r', 'r6', 'r5', 'r4', 'r3', 'r2', 'r1',
    'a', 'b', 'c', 'k', 'i', 'ii', 'iii', 'n', 'y', 'z', 'trust', 'trusts', 'portfolio', 'port',
    'plus', 'pl', 'svc', 'service', 'retirement',
}


def _dd_clean(s):
    return _re.sub(r'\s+', ' ', _re.sub(r'[^a-z0-9 ]', ' ', str(s or '').lower())).strip()


def _dd_norm(s):
    """De-doubling normalize: collapse a repeated word run or a whole-phrase repeat.
    'Vanguard Vanguard Institutional Index' and 'X X' (phrase repeated) -> the single form."""
    toks = _dd_clean(s).split()
    out = []
    for t in toks:
        if not out or out[-1] != t:
            out.append(t)
    toks = out
    n = len(toks)
    for size in range(1, n // 2 + 1):
        if n % size == 0 and all(toks[i] == toks[i % size] for i in range(n)):
            toks = toks[:size]
            break
    return ' '.join(toks)


def _dd_sigtoks(s):
    return [t for t in _dd_clean(s).split() if t and t not in _DEDUP_STOP]


def _dd_tokmatch(a, b):
    if a == b:
        return True
    if len(a) >= 3 and len(b) >= 3 and (a.startswith(b) or b.startswith(a)):
        return True   # abbreviation: FID->FIDELITY, INC->INCOME
    return len(a) >= 4 and len(b) >= 4 and _difflib.SequenceMatcher(None, a, b).ratio() >= 0.82


def _dd_years(ts):
    return set(t for t in ts if _re.fullmatch(r'(19|20)\d\d', t))


def _dd_samefund(n1, n2):
    ta, tb = _dd_sigtoks(n1), _dd_sigtoks(n2)
    if not ta or not tb:
        return False
    ya, yb = _dd_years(ta), _dd_years(tb)
    if ya and yb and ya != yb:
        return False   # GUARD: different target-date years are different funds
    m = sum(1 for x in ta if any(_dd_tokmatch(x, y) for y in tb))
    return m / min(len(ta), len(tb)) >= 0.7


_DD_GENERIC_RE = _re.compile(
    r'(?i)^(college retirement equities fund|investments? at fair value|value of interest in|'
    r'dividends?\s*/?\s*interest|interest[- ]bearing cash|cash equivalents?|other assets)')
_DD_MANAGERS = {
    'fidelity', 'vanguard', 'american funds', 'pimco', 'blackrock', 'jp morgan', 'jpmorgan',
    'columbia', 'nuveen', 'putnam', 'mfs', 'dfa', 'pgim', 'charles schwab', 'schwab', 'bny mellon',
    'empower', 'principal', 'fidelity investments', 'fidelity management trust co',
    'fidelity management trust company', 'the vanguard group inc', 'great gray trust company',
    'sei trust company', 'baird asset management',
}


def _dd_is_generic(name):
    nd = _dd_norm(name)
    return bool(_DD_GENERIC_RE.match(str(name or '').strip())) or nd in _DD_MANAGERS or not _dd_sigtoks(name)


def _dd_is_dupe(v1, n1, v2, n2):
    """Return a rule name if the two (value, name) rows are the same holding, else False."""
    d = abs(v1 - v2)
    if d > _DEDUP_VAL_TOL:
        return False
    if _dd_norm(n1) == _dd_norm(n2):
        return 'norm_equal'                 # de-doubling / case / punctuation
    if _dd_samefund(n1, n2):
        return 'token_fuzzy'                # abbreviations, with year guard
    if d <= 0.5 and (_dd_is_generic(n1) != _dd_is_generic(n2)):
        return 'generic'                    # a generic/wrapper label == a specific holding (exact value)
    return False


def dedup_plan_rows(rows: List[Dict]):
    """Drop scrape-duplicates WITHIN each plan before validation and load.

    Two rows in the same plan (pdf_stem) with values within +/-$3 are the same holding when
    their names match under any of: de-doubling-normalized equality ('Vanguard Vanguard X' ==
    'Vanguard X'); token-fuzzy same-fund (abbreviations like FID->Fidelity, guarded so adjacent
    target-date years never merge); or a generic/wrapper label ('College Retirement Equities
    Fund variable annuities', a bare manager name) sitting at the SAME value as a specific
    holding (an extraction-created phantom copy). Duplicates never a second real position, so
    dropping the extra copies is safe; keeps the most specific/complete name. Returns
    (deduped_rows, n_removed). Rows with no/zero value are never deduped.

    Must run up front -- BEFORE compute_extracted_mf_totals -- so both the pass/fail total and
    the loaded rows exclude duplicates; otherwise dupes inflate a plan's MF total -> false OVER.
    """
    n = len(rows)
    vals = [None] * n
    names = [''] * n
    by_stem = defaultdict(list)
    for i, row in enumerate(rows):
        v = parse_currency_value(row.get("current_value"))
        if v is not None and v > 0:
            vals[i] = v
            names[i] = pick_fund_name(row.get("issuer_name"), row.get("investment_description")) or ""
            by_stem[str(row.get("pdf_stem", "") or "").strip()].append(i)

    removed_idx = set()
    for stem, idxs in by_stem.items():
        m = len(idxs)
        if m < 2:
            continue
        parent = list(range(m))

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        for a in range(m):
            for b in range(a + 1, m):
                ia, ib = idxs[a], idxs[b]
                if _dd_is_dupe(vals[ia], names[ia], vals[ib], names[ib]):
                    ra, rb = find(a), find(b)
                    if ra != rb:
                        parent[ra] = rb
        comp = defaultdict(list)
        for a in range(m):
            comp[find(a)].append(a)
        for members in comp.values():
            if len(members) < 2:
                continue
            # keep the most informative: specific (non-generic), then longest name, then earliest
            best = min(members, key=lambda a: (_dd_is_generic(names[idxs[a]]), -len(names[idxs[a]]), idxs[a]))
            for a in members:
                if a != best:
                    removed_idx.add(idxs[a])

    if not removed_idx:
        return rows, 0
    out = [row for i, row in enumerate(rows) if i not in removed_idx]
    return out, len(removed_idx)


def _write_junk_drop_log(drop_log):
    """Write rows removed by the JUNK_FILTER stage to a CSV for audit/visibility.
    drop_log: list of (ack_id, name, value, reason). Best-effort; never fatal."""
    import csv as _csv
    import os as _os
    from datetime import datetime as _dt
    out_dir = _os.getenv("OUTPUT_DIR", "data/outputs")
    try:
        _os.makedirs(out_dir, exist_ok=True)
        path = _os.path.join(out_dir, "junk_dropped_%s.csv" % _dt.now().strftime("%Y%m%d_%H%M%S"))
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = _csv.writer(f)
            w.writerow(["ack_id", "name", "value", "reason"])
            w.writerows(drop_log)
        logger.info("JUNK_FILTER drop log -> %s (%d rows)", path, len(drop_log))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not write junk drop log: %s", exc)


_SUBTOTAL_PREFIX_RE = _re.compile(r'(?i)^\s*(?:sub)?total\b')


def _trailing_subtotal_type(name: str) -> str:
    """If `name` is a TRAILING type-subtotal line -- either "Total ... <type>" or a bare
    type-label line ("Common/Collective Trusts", "Registered Investment Companies") -- return
    its canonical asset type, else ''. A plain fund name never qualifies: it must start with
    Total/Subtotal, or be exactly a type label (fullmatch)."""
    from .asset_type_patterns import detect_asset_type, detect_asset_type_strict
    n = (name or "").strip().rstrip(':')
    if not n:
        return ''
    t = detect_asset_type(n)
    if not t:
        return ''
    if _SUBTOTAL_PREFIX_RE.match(n):          # "Total investments in mutual funds"
        return t
    if detect_asset_type_strict(n):           # bare type-label line "Common/Collective Trusts"
        return t
    return ''


def apply_trailing_subtotal_types(rows: List[Dict]) -> int:
    """Reverse-propagate asset type from a TRAILING subtotal line to the still-untyped rows
    above it (the block it summarizes). FALLBACK ONLY: fills a row's asset_type only when it
    is still BLANK after section-heading + per-row-type detection. Rows are processed in
    reading order (page, row_id); a type-subtotal -- or an already-typed row -- ends the
    block. Runs BEFORE junk removal, while the subtotal lines are still present. Mutates rows
    in place; returns the number of rows back-filled."""
    def _key(r):
        try:
            return (int(r.get("page_number", 0) or 0), int(r.get("row_id", 0) or 0))
        except (TypeError, ValueError):
            return (0, 0)
    def _name(r):
        return (str(r.get("issuer_name", "") or "") + " " +
                str(r.get("investment_description", "") or "")).strip()
    buffer: List[Dict] = []
    filled = 0
    for r in sorted(rows, key=_key):
        sub_type = _trailing_subtotal_type(_name(r))
        if sub_type:
            for b in buffer:
                if not str(b.get("asset_type", "") or "").strip():
                    b["asset_type"] = sub_type
                    filled += 1
            buffer = []
        elif str(r.get("asset_type", "") or "").strip():
            buffer = []          # already typed by heading/per-row -> block boundary
        else:
            buffer.append(r)
    return filled


# ---------------------------------------------------------------------------
# Tolerance check
# ---------------------------------------------------------------------------

def validate_pdf(extracted: float, expected: float,
                 tolerance: float) -> Tuple[bool, float]:
    """Return (passes, pct_diff) for a single PDF.

    pct_diff = abs(extracted - expected) / expected
    """
    if expected <= 0:
        raise ValueError(f"expected must be > 0, got {expected}")
    pct_diff = abs(extracted - expected) / expected
    return pct_diff <= tolerance, pct_diff


# ---------------------------------------------------------------------------
# DataFrame builders
# ---------------------------------------------------------------------------

_MF_PREFIX_RE = _re.compile(r'^\s*mutual\s+funds?\s*[-,:]?\s*', _re.IGNORECASE)
_MF_NAME_FILLER_RE = _re.compile(
    r'(?i)\b(sub)?totals?\b|\bcontinued\b|\bshares\b|\bregistered\s+investment\s+compan\w*\b'
    r'|\binvestments?\b|\bn/?a\b|[^A-Za-z]')
# "Investments at fair value: <fund>" / "Investments, at net asset value" / etc. --
# a Schedule H column-(b) accounting label extraction boilerplate prepends to the
# fund name (also seen with "determined by quoted market prices" / "as quoted by
# the custodian" inserted before the colon). Ported from the 2026-09-11 v3-cleanup
# reconciliation (282 names / $1.39B contaminated this way in plan_mf_history_v3).
_FAIR_VALUE_PREFIX_RE = _re.compile(
    r'(?i)^investments?,?\s*at\s*(?:fair\s+value|net\s+asset\s+value)'
    r'(?:\s+determined\s+by\s+quoted\s+market\s+prices)?'
    r'(?:\s+as\s+quoted\s+by\s+the\s+custodian)?'
    r'\s*[:,\-]?\s*')
# "Value of Interest in Registered Investment Companies/Mutual Fund(s)/Master Trust(s):
# <fund>" (and the shorter "Interest in ..." variant without "Value of") -- a second
# Schedule H accounting-label boilerplate family, often followed by a region/ticker
# code fragment ("Global Region - USD MFO ...") or a "continued" page-break artifact
# before the real fund name. Ported from the 2026-09-11 v3-cleanup reconciliation
# (105 names / ~$1.85B contaminated this way in plan_mf_history_v3).
_VALUE_OF_INTEREST_PREFIX_RE = _re.compile(
    r'(?i)^(?:value\s+of\s+)?interest\s+in\s*'
    r'(?:registered\s+investment\s+companies|mutual\s+funds?|master\s+trusts?)?'
    r'\s*[:,\-]?\s*')
_VOI_REGION_CODE_RE1 = _re.compile(r'(?i)^[a-z ]*region\s*-\s*usd\s*(?:mfc|mfo)?\s*')
_VOI_REGION_CODE_RE2 = _re.compile(r'(?i)^[a-z ]*-\s*usd\s*(?:mfc|mfo)?\s*')
_VOI_CONTINUED_RE = _re.compile(r'(?i)^continued\s+')
_ANNUITY_VEHICLE_RE = _re.compile(
    r'(?i)(variable\s+annuit|annuity\s+(account|contract|compan|co\b)'
    r'|insurance\s+(and\s+)?annuity|traditional\s+annuity|\bCREF\b'
    r'|college\s+retirement\s+equities|teachers\s+insurance\s+and\s+annuity)')


_TRAILING_VALUE_RE = _re.compile(
    r'(?i)(\s+(\d{1,3}(,\d{3})+|\d{5,}|\d[\d,\.]*\s+(shares?|units?|shs)))+\s*$')


# Total-line / participant-loan-line detector (FIX 19, Mode 2 over-capture).
# Audited 4i schedules end with grand-total / plan-total lines that some layouts
# (e.g. Amazon 401k) capture as a "holding": e.g. the literal line
#   TOTAL INVESTMENTS (EXCLUDING PARTICIPANT LOANS) 34,167,624
# gets loaded as a fund named "EXCLUDING" / "TOTAL INVESTMENTS ..." worth $34.2B,
# swamping the real ~$2B mutual-fund sleeve. Participant-loan lines
# ("...10.5%, MATURING THROUGH 2050") also leak in as junk names ("105 MATURING THROUGH").
# Every total-label pattern is ANCHORED at start-of-name because genuine funds lead with a
# brand/manager token ("Vanguard Total Stock Market", "PIMCO Total Return") -- a total line
# leads with the label itself. "investments" is matched plural-only so singular fund names
# like "Total Investment Grade Credit" are never hit.
_TOTAL_LINE_RE = _re.compile(
    r'(?i)('
    r'excluding\s+participant\s+loans?'            # (EXCLUDING PARTICIPANT LOANS) -- any position
    r'|maturing\s+through'                          # participant-loan line fragment
    r'|^\s*excluding\b'                             # captured fragment 'EXCLUDING'
    r'|^\s*grand\s+total\b'
    r'|^\s*(sub[-\s]?)?total\s*$'                   # bare 'Total' / 'Subtotal' / 'Sub-total'
    r'|^\s*total\s+investments\b'                   # TOTAL INVESTMENTS (plural)
    r'|^\s*total\s+(net\s+)?assets?\b'              # TOTAL (NET) ASSETS
    r'|^\s*total\s+(mutual\s+funds?|common\s+stock|collective|holdings?|value)\b'
    r')')


# CUSIP-continuation-line artifact detector (FIX 20, Mode 1 over-capture).
# Northern-Trust-style master-trust schedules ("5500 Supplemental Schedules") render each
# holding as a TWO-line record: line 1 = "<security desc> <shares> <cost> <current value>",
# line 2 = "CUSIP: <9-char id>". The extractor mis-reads the CUSIP continuation line as its
# own holding -- name becomes "CUSIP" (or a wrapped fund-name tail like "INDEX FD ADMIRAL
# SHS CUSIP") and the 9-digit CUSIP id (e.g. 989207105) parses as a $989M "value".
# Schlumberger master trust alone produced ~896 such rows summing to $352B of fake AUM.
# The token "CUSIP" never appears in a genuine fund name, so matching it anywhere is zero-FP.
_CUSIP_ARTIFACT_RE = _re.compile(r'(?i)\bCUSIP\b')


def _strip_trailing_value_tokens(name: str) -> str:
    """Strip share-count / value tokens that bled onto the end of a fund name from
    adjacent columns (e.g. 'Vanguard Mid-Cap Index Admiral 2,437',
    'ISHARES CORE MSCI EAFE ETF 2127315', 'Fidelity Advisor Intl 15,751 shares 408,266').
    Only removes comma-grouped numbers, >=5-digit bare integers, and 'N shares/units'
    tokens -- so 4-digit target-date years (2050) and index numbers (500/2000) are kept.
    """
    if not name:
        return name
    cleaned = _TRAILING_VALUE_RE.sub('', name).strip()
    return cleaned or name


def _normalize_mf_name(name: str) -> str:
    """Strip a leading 'Mutual Fund(s)' type label from a candidate fund name and reject
    subtotal / type-only rows. Audited MF sub-schedules prepend the asset-type label to
    each fund ('Mutual Fund Fidelity 500 Index') and emit section subtotals
    ('Mutual Funds Total') / nameless placeholders ('Mutual Fund N/A'). Returns the cleaned
    name, or '' for subtotal/type-only rows (so the name-quality gate drops them).
    """
    n = (name or "").strip()
    if not n:
        return ""
    if n.lower().startswith("mutual fund"):
        stripped = _MF_PREFIX_RE.sub("", n).strip()
        residual = _MF_NAME_FILLER_RE.sub(" ", stripped)
        if not _re.search(r"[A-Za-z]", residual):
            return ""
        return stripped
    if _FAIR_VALUE_PREFIX_RE.match(n):
        stripped = _FAIR_VALUE_PREFIX_RE.sub("", n).strip()
        stripped = _re.sub(r"^[\-:,\s]+", "", stripped)
        # a bare "Mutual Fund" residual (e.g. "Investments at fair value: Mutual
        # Fund") names no specific fund either -- drop it like any other
        # subtotal/type-only row instead of writing the boilerplate itself.
        if stripped.strip().lower() in ("mutual fund", "mutual funds"):
            return ""
        residual = _MF_NAME_FILLER_RE.sub(" ", stripped)
        if not _re.search(r"[A-Za-z]", residual):
            return ""
        return stripped
    if _VALUE_OF_INTEREST_PREFIX_RE.match(n):
        stripped = _VALUE_OF_INTEREST_PREFIX_RE.sub("", n).strip()
        stripped = _re.sub(r"^[\-:,\s]+", "", stripped)
        stripped = _VOI_REGION_CODE_RE1.sub("", stripped)
        stripped = _VOI_REGION_CODE_RE2.sub("", stripped)
        stripped = _VOI_CONTINUED_RE.sub("", stripped).strip()
        # a bare "(Mutual Funds)" / "Registered Investment Companies" residual names
        # no specific fund -- drop it like any other subtotal/type-only row.
        if _re.match(
            r'(?i)^[\s\(\)]*(?:registered\s+investment\s+companies|mutual\s+funds?'
            r'|master\s+trusts?)?[\s\(\)]*$', stripped):
            return ""
        if not _re.search(r"[A-Za-z]", stripped):
            return ""
        return stripped
    return n


def build_mf_rows_df(rows: List[Dict],
                     mf_types: frozenset = MF_ASSET_TYPES,
                     validation_status: str = "UNVALIDATED",
                     mf_only: bool = True) -> pd.DataFrame:
    """Build the plan_mf_history_v3 DataFrame from MF rows for a passing PDF.

    Filters to MF asset types only and maps to the three target columns:
      ack_id              ← pdf_stem
      raw_entity_name     ← issuer_name
      plan_investment_amt ← current_value (parsed to float)

    Rows with unparseable current_value are included with NaN.
    """
    # Non-MF asset types that must NOT be loaded into the MF table even if unclassified elsewhere.
    _non_mf = {"common stock","preferred stock","employer stock","common/collective trust fund",
               "commingled fund","separately managed account","self-directed brokerage account",
               "participant loan","guaranteed insurance contract","guaranteed investment contract",
               "stable value fund","insurance general account","group annuity contract",
               "partnership interest","currency",
               "joint venture","real estate","hedge fund","bond","derivative",
               "103-12 investment entity"}
    records = []
    for row in rows:
        asset_type = str(row.get("asset_type", "") or "").strip().lower()
        _val = parse_currency_value(row.get("current_value"))
        # FIX 21 (garbage-value guard): no single mutual-fund holding in one plan is $20B+
        # (largest observed legit position ~$8.5B). A value at/above that is a parse error --
        # e.g. triple-rendered PDFs where every digit repeats 3x ("444000777777000222" ~ 4.4e17),
        # concatenated columns, or master-trust aggregate lines. Treat as no value so the row is
        # dropped (blank-type) / not counted, instead of inflating the plan's MF total.
        if _val is not None and _val >= 2e10:
            _val = None
        # Load MF-typed rows; also load blank/unknown-type rows that have a value
        # (the "no asset type" case -> classify in post-processing). Skip explicit non-MF.
        # mf_only=True (MF table): drop explicit non-MF vehicle types here.
        # mf_only=False (staging): KEEP all vehicle types (tagged with asset_type) so CITs/
        # stocks/bonds land in staging and can be routed to their own tables downstream.
        if mf_only and asset_type in _non_mf:
            continue
        if mf_only and asset_type and asset_type not in mf_types:
            continue
        if not asset_type and _val is None:     # blank type with no value = junk, drop in both
            continue
        # mf_only=True has no staging table downstream to hold a blank-type row for later
        # reclassification -- this branch is the row's only stop before plan_mf_history_v3.
        # Letting a blank-type-with-value row through here writes an unclassified name
        # (individual stocks, bonds, GIC/insurance contract text, "Interest in Master Trust"
        # boilerplate, etc.) straight into the MF-only table with no vetting at all -- the
        # root cause identified for the 2026-09-08 v3 contamination cleanup (2,403 names /
        # $6.41B). mf_only=False keeps these rows deliberately (staging IS the reclassification
        # holding area); mf_only=True must drop them instead.
        if mf_only and not asset_type:
            continue
        _name = _strip_trailing_value_tokens(_normalize_mf_name(pick_fund_name(row.get("issuer_name"), row.get("investment_description"))))
        # Name-quality gate: drop blank / numeric-only (bond rates, share counts, mis-mapped
        # columns) and "Mutual Fund(s) Total/Shares/N-A" subtotal/type-only rows
        # (_normalize_mf_name returns '' for those).
        if not _name or not _re.search(r"[A-Za-z]", _name):
            continue
        # Mode 2 over-capture: drop grand-total / plan-total lines and participant-loan
        # line fragments captured as a holding (e.g. Amazon's $34.2B "TOTAL INVESTMENTS
        # (EXCLUDING PARTICIPANT LOANS)" row). Anchored patterns keep real "Total Return"/
        # "Total Stock Market" funds (see _TOTAL_LINE_RE).
        if _TOTAL_LINE_RE.search(_name):
            continue
        # Mode 1 over-capture: drop CUSIP-continuation-line artifacts (name contains the
        # token "CUSIP"; the 9-digit CUSIP id was mis-read as the value). See _CUSIP_ARTIFACT_RE.
        if _CUSIP_ARTIFACT_RE.search(_name):
            continue
        # Scope: annuity / insurance vehicles (CREF, TIAA Traditional, Voya/Empower
        # Retirement Insurance & Annuity, variable annuity accounts) are not mutual funds --
        # UNLESS the filing itself already tags/describes the row as a mutual fund (e.g. some
        # plans' Sch H tables list CREF accounts under a "Mutual fund" asset type/description;
        # certified totals for those filings include them, so excluding them undercaptures).
        _desc = str(row.get("investment_description", "") or "")
        _explicit_mf = asset_type == "mutual fund" or bool(_re.search(r"\bmutual\s+funds?\b", _desc, _re.IGNORECASE))
        if mf_only and not _explicit_mf and _ANNUITY_VEHICLE_RE.search(_name):
            continue
        records.append({
            "ack_id": str(row.get("pdf_stem", "") or "").strip(),
            "raw_entity_name": _name,
            "raw_sponsor_name": str(row.get("issuer_name", "") or "").strip(),
            "plan_investment_amt": _val,
            "asset_class": "PENDING_AI",
            "asset_sub_class": "PENDING_AI",
            "validation_status": validation_status,
            # Persist the deterministic, file-derived vehicle type so it is never lost.
            # Blank means "not identified from the file" (do NOT treat as mutual fund downstream).
            "asset_type": asset_type,
        })
    return pd.DataFrame(records, columns=[
        "ack_id",
        "raw_entity_name",
        "raw_sponsor_name",
        "plan_investment_amt",
        "asset_class",
        "asset_sub_class",
        "validation_status",
        "asset_type",
    ])




# ---------------------------------------------------------------------------
# Parquet writer (shared for both tables)
# ---------------------------------------------------------------------------

def write_parquet(df: pd.DataFrame, s3_path: str, glue_db: str,
                  table: str, partition_cols: Optional[List[str]],
                  mode: str = "append") -> None:
    """Write a DataFrame to S3 Parquet and register/update the Glue table."""
    import awswrangler as wr

    kwargs = dict(
        df=df,
        path=s3_path,
        dataset=True,
        mode=mode,
        compression="snappy",
        database=glue_db,
        table=table,
    )
    if partition_cols:
        kwargs["partition_cols"] = [c for c in partition_cols if c in df.columns]

    wr.s3.to_parquet(**kwargs)
    logger.info("Wrote %d rows to %s (table: %s.%s)", len(df), s3_path, glue_db, table)



# ---------------------------------------------------------------------------
# Iceberg writer via Athena INSERT INTO
# ---------------------------------------------------------------------------
def write_iceberg_via_athena(df: pd.DataFrame, glue_db: str, table: str,
                             include_asset_type: bool = False) -> None:
    """Write rows to an Iceberg table via Athena INSERT INTO statements.
    include_asset_type=True writes the extra asset_type column (used for the staging table);
    default False keeps the original 7-column write for plan_mf_history_v3 (unchanged)."""
    import awswrangler as wr
    import math
    import os

    if df.empty:
        logger.info("No rows to write to %s.%s", glue_db, table)
        return

    workgroup = os.getenv("ATHENA_WORKGROUP", "primary")
    s3_staging = os.getenv("ATHENA_STAGING_S3")

    for col in ["asset_class", "asset_sub_class"]:
        if col not in df.columns:
            df = df.copy()
            df[col] = "PENDING_AI"
    if include_asset_type and "asset_type" not in df.columns:   # blank = not identified from the file
        df = df.copy()
        df["asset_type"] = ""

    # Delete-then-insert per ack_id (idempotency), one ack_id at a time, so a process kill
    # mid-write (e.g. an SSM command timeout) can orphan at most the single ack_id in flight
    # instead of every ack_id in this run -- a batch-wide DELETE followed by a separate
    # batch-wide INSERT loop left every already-deleted-but-not-yet-reinserted ack_id with
    # zero rows if the process died partway through the INSERT batches.
    # Only works for Iceberg/transactional tables; skips silently for plain Hive tables.
    batch_size = 500
    total = 0
    for ack_id, group in df.groupby("ack_id", sort=False):
        if pd.isna(ack_id) or not str(ack_id).strip():
            continue
        id_sql = "'" + str(ack_id).replace("'", "''") + "'"
        delete_sql = f"DELETE FROM {glue_db}.{table} WHERE ack_id IN ({id_sql})"
        try:
            delete_qid = wr.athena.start_query_execution(
                sql=delete_sql,
                database=glue_db,
                workgroup=workgroup,
                s3_output=s3_staging,
            )
            wr.athena.wait_query(query_execution_id=delete_qid)
        except Exception as e:
            logger.warning("DELETE skipped for %s.%s ack_id=%s (not a transactional table?): %s", glue_db, table, ack_id, e)

        group_total = len(group)
        for start in range(0, group_total, batch_size):
            batch = group.iloc[start:start + batch_size]
            values_parts = []
            for _, row in batch.iterrows():
                def q(v):
                    if v is None:
                        return "NULL"
                    try:
                        if math.isnan(float(v)):
                            return "NULL"
                    except (TypeError, ValueError):
                        pass
                    return "'" + str(v).replace("'", "''") + "'"

                amt = row.get("plan_investment_amt")
                try:
                    _a = float(amt)
                    # DECIMAL(18,2) overflows ~1e16; null obvious overflow/garbage
                    # and use fixed-point (no exponent) formatting for valid values.
                    amt_sql = "NULL" if (math.isnan(_a) or abs(_a) >= 1e15) else ("%.2f" % _a)
                except (TypeError, ValueError):
                    amt_sql = "NULL"

                _vals = [
                    q(row.get("ack_id")),
                    q(row.get("raw_entity_name")),
                    q(row.get("raw_sponsor_name")),
                    amt_sql,
                    q(row.get("asset_class", "PENDING_AI")),
                    q(row.get("asset_sub_class", "PENDING_AI")),
                    q(row.get("validation_status", "UNVALIDATED")),
                ]
                if include_asset_type:
                    _vals.append(q(row.get("asset_type", "")))
                values_parts.append("(" + ", ".join(_vals) + ")")

            _cols = ("ack_id, raw_entity_name, raw_sponsor_name, plan_investment_amt, "
                     "asset_class, asset_sub_class, validation_status")
            if include_asset_type:
                _cols += ", asset_type"
            sql = "INSERT INTO {}.{} ({}) VALUES {}".format(glue_db, table, _cols, ", ".join(values_parts))
            query_id = wr.athena.start_query_execution(
                sql=sql,
                database=glue_db,
                workgroup=workgroup,
                s3_output=s3_staging,
            )
            wr.athena.wait_query(query_execution_id=query_id)
            total += len(batch)
            logger.info("Inserted %d rows for ack_id=%s into Iceberg %s.%s", len(batch), ack_id, glue_db, table)

    logger.info("Wrote %d rows to Iceberg table %s.%s", total, glue_db, table)


def write_validation_summary_via_athena(df: pd.DataFrame, glue_db: str, table: str) -> None:
    """Write ack-level validation summary rows to an Iceberg table via Athena."""
    import awswrangler as wr
    import math
    import os

    if df.empty:
        logger.info("No validation summary rows to write to %s.%s", glue_db, table)
        return

    workgroup = os.getenv("ATHENA_WORKGROUP", "primary")
    s3_staging = os.getenv("ATHENA_STAGING_S3")

    ack_ids = df["ack_id"].dropna().unique().tolist()
    if ack_ids:
        ids_sql = ", ".join("'" + str(a).replace("'", "''") + "'" for a in ack_ids)
        delete_sql = f"DELETE FROM {glue_db}.{table} WHERE ack_id IN ({ids_sql})"
        try:
            delete_qid = wr.athena.start_query_execution(
                sql=delete_sql,
                database=glue_db,
                workgroup=workgroup,
                s3_output=s3_staging,
            )
            wr.athena.wait_query(query_execution_id=delete_qid)
            logger.info(
                "Deleted existing validation summary rows for %d ack_ids from %s.%s",
                len(ack_ids), glue_db, table,
            )
        except Exception as exc:
            logger.warning(
                "DELETE skipped for validation summary %s.%s: %s",
                glue_db, table, exc,
            )

    batch_size = 500
    total = len(df)
    for start in range(0, total, batch_size):
        batch = df.iloc[start:start + batch_size]
        values_parts = []
        for _, row in batch.iterrows():
            def q(v):
                if v is None:
                    return "NULL"
                try:
                    if math.isnan(float(v)):
                        return "NULL"
                except (TypeError, ValueError):
                    pass
                return "'" + str(v).replace("'", "''") + "'"

            def n(v):
                try:
                    return "NULL" if v is None or math.isnan(float(v)) else str(float(v))
                except (TypeError, ValueError):
                    return "NULL"

            values_parts.append(
                "({}, {}, {}, {}, {}, {}, {}, {}, {})".format(
                    q(row.get("ack_id")),
                    q(row.get("plan_id")),
                    n(row.get("extracted_amt_mutual_funds")),
                    n(row.get("reference_amt_mutual_funds")),
                    n(row.get("difference_amt")),
                    n(row.get("difference_pct")),
                    q(row.get("validation_status")),
                    q(row.get("gap_reason")),
                    q(row.get("run_ts")),
                )
            )

        sql = (
            "INSERT INTO {}.{} "
            "("
            "ack_id, plan_id, extracted_amt_mutual_funds, reference_amt_mutual_funds, "
            "difference_amt, difference_pct, validation_status, gap_reason, run_ts"
            ") VALUES {}".format(glue_db, table, ", ".join(values_parts))
        )
        query_id = wr.athena.start_query_execution(
            sql=sql,
            database=glue_db,
            workgroup=workgroup,
            s3_output=s3_staging,
        )
        wr.athena.wait_query(query_execution_id=query_id)
        logger.info(
            "Inserted validation summary rows %d-%d into Iceberg %s.%s",
            start, start + len(batch), glue_db, table,
        )

    logger.info("Wrote %d validation summary rows to %s.%s", total, glue_db, table)

def _route_mf_from_staging(glue_db: str, staging_table: str, target_table: str, ack_ids: list) -> None:
    """Populate the MF table from staging: only rows whose file-derived asset_type is an MF type.
    Deletes the run's acks from target first (idempotent), then inserts the MF subset (7 cols)."""
    import awswrangler as wr
    import os
    if not ack_ids:
        return
    wg = os.getenv("ATHENA_WORKGROUP", "primary")
    s3 = os.getenv("ATHENA_STAGING_S3")
    ids = ", ".join("'" + str(a).replace("'", "''") + "'" for a in ack_ids)
    mf = ", ".join("'" + t + "'" for t in sorted(MF_ASSET_TYPES))
    excl = ", ".join("'" + t + "'" for t in sorted(MF_ROUTING_EXCLUDE_NAMES))
    stmts = [
        f"DELETE FROM {glue_db}.{target_table} WHERE ack_id IN ({ids})",
        ("INSERT INTO {gd}.{tt} "
         "(ack_id, raw_entity_name, raw_sponsor_name, plan_investment_amt, asset_class, asset_sub_class, validation_status) "
         "SELECT ack_id, raw_entity_name, raw_sponsor_name, plan_investment_amt, asset_class, asset_sub_class, validation_status "
         "FROM {gd}.{st} WHERE ack_id IN ({ids}) AND lower(trim(asset_type)) IN ({mf}) "
         "AND lower(trim(raw_entity_name)) NOT IN ({excl})"
         ).format(gd=glue_db, tt=target_table, st=staging_table, ids=ids, mf=mf, excl=excl),
    ]
    for sql in stmts:
        qid = wr.athena.start_query_execution(sql=sql, database=glue_db, workgroup=wg, s3_output=s3)
        wr.athena.wait_query(query_execution_id=qid)
    logger.info("Routed MF rows %s -> %s for %d acks", staging_table, target_table, len(ack_ids))


# ---------------------------------------------------------------------------
# Alternatives routing (real estate / private credit / private equity /
# infrastructure / hedge fund). Unlike the MF router, this does NOT trust the
# file-derived asset_type field to identify matches -- it's unreliable/
# inconsistent for non-MF rows. Classification is keyword-matched against
# raw_entity_name instead. Inclusion-only: a row with no keyword match is left
# in staging untouched, same as any other unrouted non-MF/non-CIT row -- there
# is no catch-all bucket here.
# ---------------------------------------------------------------------------

# (keyword, asset_type, asset_class, confidence, classification_method).
# HIGH-tier phrases are listed before MEDIUM-tier ones and a CASE takes the
# first match, so a specific phrase always wins over a shorter/more ambiguous
# one regardless of category.
ALT_FUND_PATTERNS: List[Tuple[str, str, str, str, str]] = [
    ("private equity fund", "Private Equity Fund", "Private Equity", "HIGH", "keyword:private_equity_fund"),
    ("private equity partners", "Private Equity Fund", "Private Equity", "HIGH", "keyword:private_equity_partners"),
    ("buyout fund", "Private Equity Fund", "Private Equity", "HIGH", "keyword:buyout_fund"),
    ("venture capital fund", "Private Equity Fund", "Private Equity", "HIGH", "keyword:venture_capital_fund"),
    ("private credit fund", "Private Credit Fund", "Private Credit", "HIGH", "keyword:private_credit_fund"),
    ("direct lending fund", "Private Credit Fund", "Private Credit", "HIGH", "keyword:direct_lending_fund"),
    ("senior secured loan fund", "Private Credit Fund", "Private Credit", "HIGH", "keyword:senior_secured_loan_fund"),
    ("real estate investment trust", "Real Estate Fund", "Real Estate", "HIGH", "keyword:reit_full"),
    ("real estate fund", "Real Estate Fund", "Real Estate", "HIGH", "keyword:real_estate_fund"),
    ("infrastructure fund", "Infrastructure Fund", "Infrastructure", "HIGH", "keyword:infrastructure_fund"),
    ("hedge fund", "Hedge Fund", "Hedge Fund", "HIGH", "keyword:hedge_fund"),
    ("private equity", "Private Equity Fund", "Private Equity", "MEDIUM", "keyword:private_equity"),
    ("buyout", "Private Equity Fund", "Private Equity", "MEDIUM", "keyword:buyout"),
    ("venture capital", "Private Equity Fund", "Private Equity", "MEDIUM", "keyword:venture_capital"),
    ("private credit", "Private Credit Fund", "Private Credit", "MEDIUM", "keyword:private_credit"),
    ("direct lending", "Private Credit Fund", "Private Credit", "MEDIUM", "keyword:direct_lending"),
    ("senior loan", "Private Credit Fund", "Private Credit", "MEDIUM", "keyword:senior_loan"),
    ("mezzanine debt", "Private Credit Fund", "Private Credit", "MEDIUM", "keyword:mezzanine_debt"),
    ("real estate", "Real Estate Fund", "Real Estate", "MEDIUM", "keyword:real_estate"),
    ("infrastructure", "Infrastructure Fund", "Infrastructure", "MEDIUM", "keyword:infrastructure"),
]

# Reliable, well-populated literal asset_type values that belong to other
# routers -- excluded here so alternatives never collides with MF or the two
# dominant, unambiguous CIT literal values. NOT a full CIT taxonomy: the long
# tail of asset_type strings is too inconsistent to trust for anything beyond
# these two, which is exactly why this router keys off raw_entity_name instead.
ALT_EXCLUDED_ASSET_TYPES = MF_ASSET_TYPES | frozenset({
    "common/collective trust fund", "commingled fund",
})

# Asset-type strings that reliably indicate an individual public security (a
# stock or ETF ticker) rather than a fund holding. Confirmed against real
# false positives sampled from production staging: "Invesco Senior Loan Etf"
# and "Vanguard Real Estate ETF" filed as exchange-traded funds, "Sterling
# Infrastructure Inc" filed as common stock -- all caught by the loose
# MEDIUM-tier keywords ("real estate", "infrastructure", "senior loan"). This
# is a different bar than ALT_EXCLUDED_ASSET_TYPES above: it's not used to
# identify what a row IS (asset_type is too unreliable for that), only to
# veto a keyword match on the narrow set of security types that can never be
# an alternative-fund unit. It's a partial mitigation, not a full fix --
# asset_type is often NULL/inconsistent for the same security across rows, so
# some public-security noise still gets through; that's why manual_review_
# required stays hardcoded true for every routed row in this first version.
ALT_NOISE_ASSET_TYPES = frozenset({
    "common stock", "employer stock", "exchange traded funds", "stocks",
})


def _alt_case_sql(value_index: int) -> str:
    """Build a CASE expression picking ALT_FUND_PATTERNS[*][value_index] for the
    first keyword (index 0) found in raw_entity_name. Generating all four CASEs
    (asset_type/asset_class/confidence/method) from this one function keeps them
    from ever drifting out of sync with each other."""
    lines = ["CASE"]
    for pattern in ALT_FUND_PATTERNS:
        kw = pattern[0].replace("'", "''")
        val = pattern[value_index].replace("'", "''")
        lines.append(f"        WHEN lower(trim(raw_entity_name)) LIKE '%{kw}%' THEN '{val}'")
    lines.append("        ELSE NULL END")
    return "\n".join(lines)


def _route_alternatives_from_staging(glue_db: str, staging_table: str, target_table: str, ack_ids: list) -> None:
    """Populate the alternatives table from staging: keyword-matches raw_entity_name
    against ALT_FUND_PATTERNS (real estate / private credit / private equity /
    infrastructure / hedge fund). Deletes the run's acks from target first
    (idempotent), then inserts only the matched subset -- non-matching rows are
    left in staging, not swept into a catch-all bucket.
    manual_review_required is unconditionally true for now: these patterns are
    unproven against real data, so every routed row should get a first look
    before this flips to a confidence-based rule."""
    import awswrangler as wr
    import os
    if not ack_ids:
        return
    wg = os.getenv("ATHENA_WORKGROUP", "primary")
    s3 = os.getenv("ATHENA_STAGING_S3")
    ids = ", ".join("'" + str(a).replace("'", "''") + "'" for a in ack_ids)
    excluded = ", ".join("'" + t + "'" for t in sorted(ALT_EXCLUDED_ASSET_TYPES | ALT_NOISE_ASSET_TYPES))
    manager_case = _alt_manager_case_sql()

    select_sql = f"""
        SELECT
            ack_id, raw_entity_name, raw_sponsor_name, plan_investment_amt,
            {_alt_case_sql(2)} AS asset_sub_class,
            validation_status,
            {_alt_vehicle_case_sql()} AS asset_type,
            {_alt_case_sql(3)} AS classification_confidence,
            {_alt_case_sql(4)} AS classification_method,
            true AS manual_review_required,
            current_timestamp AS routed_at,
            'Alternatives' AS asset_class,
            {manager_case} AS matched_manager_name,
            CASE WHEN {manager_case} IS NOT NULL THEN 'HIGH' ELSE NULL END AS manager_match_confidence,
            CASE WHEN {manager_case} IS NOT NULL THEN 'brand_regex_v1' ELSE NULL END AS manager_match_method
        FROM {glue_db}.{staging_table}
        WHERE ack_id IN ({ids})
          AND lower(trim(asset_type)) NOT IN ({excluded})
    """
    insert_sql = (
        f"INSERT INTO {glue_db}.{target_table} "
        "(ack_id, raw_entity_name, raw_sponsor_name, plan_investment_amt, asset_sub_class, "
        "validation_status, asset_type, classification_confidence, classification_method, "
        "manual_review_required, routed_at, asset_class, "
        "matched_manager_name, manager_match_confidence, manager_match_method) "
        f"SELECT * FROM ({select_sql}) t WHERE t.asset_sub_class IS NOT NULL"
    )
    stmts = [
        f"DELETE FROM {glue_db}.{target_table} WHERE ack_id IN ({ids})",
        insert_sql,
    ]
    for sql in stmts:
        qid = wr.athena.start_query_execution(sql=sql, database=glue_db, workgroup=wg, s3_output=s3)
        wr.athena.wait_query(query_execution_id=qid)
    logger.info("Routed alternatives rows %s -> %s for %d acks", staging_table, target_table, len(ack_ids))


# ---------------------------------------------------------------------------
# Brand/manager-name matching -- second alternatives pass, complementary to
# ALT_FUND_PATTERNS above. ALT_FUND_PATTERNS only matches a generic phrase
# ("private equity", "real estate") in raw_entity_name; it structurally
# cannot catch a row whose name is *only* a manager brand with no such
# phrase (e.g. "AEA Investors Fund VII LP", "Silver Lake Alpine II"). This
# list is the vocabulary of known alt-manager brands, built 2026-09-20 from
# the 5,623 already-classified plan_alternatives_history rows and verified
# against a 714-row candidate pool pulled from staging before this router
# existed (see project_dcio_alternatives_router memory for the methodology
# and false-positive/duplicate-position findings that shaped the filters
# below). Grows over time as new managers are confirmed, the same way
# ALT_FUND_PATTERNS grows.
# (brand term, asset_type, asset_class)
ALT_BRAND_PATTERNS: List[Tuple[str, str, str]] = [
    ("aea investors", "Private Equity Fund", "Private Equity"),
    ("alcentra", "Private Credit Fund", "Private Credit"),
    ("ares management", "Private Credit Fund", "Private Credit"),
    ("basalt", "Infrastructure Fund", "Infrastructure"),
    ("blue owl", "Private Credit Fund", "Private Credit"),
    ("clayton, dubilier & rice", "Private Equity Fund", "Private Equity"),
    ("clearlake capital", "Private Equity Fund", "Private Equity"),
    ("clover", "Private Equity Fund", "Private Equity"),
    ("corbin capital", "Hedge Fund", "Hedge Fund"),
    ("crescent capital", "Private Credit Fund", "Private Credit"),
    ("eagle point", "Private Credit Fund", "Private Credit"),
    ("enervest", "Infrastructure Fund", "Infrastructure"),
    ("frazier", "Private Equity Fund", "Private Equity"),
    ("gcm grosvenor", "Private Equity Fund", "Private Equity"),
    ("general atlantic", "Private Equity Fund", "Private Equity"),
    ("genstar capital", "Private Equity Fund", "Private Equity"),
    ("gi partners", "Infrastructure Fund", "Infrastructure"),
    ("goldpoint partners", "Private Credit Fund", "Private Credit"),
    ("gso", "Private Credit Fund", "Private Credit"),
    ("hamilton lane", "Private Equity Fund", "Private Equity"),
    ("hancock natural resource group", "Infrastructure Fund", "Infrastructure"),
    ("harbourvest partners", "Private Equity Fund", "Private Equity"),
    ("harrison street", "Real Estate Fund", "Real Estate"),
    ("harvest partners", "Private Equity Fund", "Private Equity"),
    ("insight partners", "Private Equity Fund", "Private Equity"),
    ("intercontinental", "Real Estate Fund", "Real Estate"),  # overridden below
    ("kayne anderson", "Infrastructure Fund", "Infrastructure"),
    ("landmark", "Private Equity Fund", "Private Equity"),
    ("mc credit", "Private Credit Fund", "Private Credit"),
    ("mcmorgan", "Real Estate Fund", "Real Estate"),
    ("mesirow", "Private Equity Fund", "Private Equity"),
    ("neuberger berman", "Private Equity Fund", "Private Equity"),
    ("nylcap", "Private Credit Fund", "Private Credit"),
    ("oaktree capital", "Private Credit Fund", "Private Credit"),
    ("onex", "Private Equity Fund", "Private Equity"),  # overridden below
    ("owl rock", "Private Credit Fund", "Private Credit"),
    ("pantheon", "Private Equity Fund", "Private Equity"),
    ("partners group", "Private Equity Fund", "Private Equity"),
    ("perella weinberg partners", "Private Equity Fund", "Private Equity"),
    ("pomona capital", "Private Equity Fund", "Private Equity"),
    ("rockpoint", "Real Estate Fund", "Real Estate"),
    ("segal marco", "Private Equity Fund", "Private Equity"),
    ("sentinel capital partners", "Private Equity Fund", "Private Equity"),
    ("siguler guff", "Private Equity Fund", "Private Equity"),
    ("silver lake", "Private Equity Fund", "Private Equity"),
    ("stonepeak", "Infrastructure Fund", "Infrastructure"),
    ("summit partners", "Private Equity Fund", "Private Equity"),
    ("tennenbaum", "Private Credit Fund", "Private Credit"),
    ("thoma bravo", "Private Equity Fund", "Private Equity"),
    ("trilantic capital partners", "Private Equity Fund", "Private Equity"),
    ("ullico", "Infrastructure Fund", "Infrastructure"),
    ("warburg pincus", "Private Equity Fund", "Private Equity"),
    ("white oak global advisors", "Private Credit Fund", "Private Credit"),
    ("whitehorse liquidity partners", "Private Credit Fund", "Private Credit"),
    ("windjammer capital", "Private Equity Fund", "Private Equity"),
]

# Per-term extra restriction, ANDed onto that term's match only. Both entries
# were confirmed false-positive-prone during 2026-09-20 verification: bare
# "intercontinental" mostly matches Intercontinental Exchange Inc (ICE) stock/
# bonds and InterContinental Hotels; bare "onex" coincidentally substring-
# matches Euronext, StoneX, Socionext.
ALT_BRAND_TERM_OVERRIDES: Dict[str, str] = {
    "intercontinental": "regexp_like(lower(raw_entity_name), 'reif|real estate')",
    "onex": "strpos(lower(raw_entity_name), 'onex partners') > 0",
}

# Terms that need a word-boundary match rather than plain substring -- "gso"
# is a substring of unrelated names ("Kingsoft", "GSODLN" swap tickers,
# "GSOF"-named LLCs unrelated to GSO Capital Partners). Found via the
# 2026-09-21 backfill verification (3 confirmed false positives: a HK-listed
# stock and an interest rate swap had been routed in as GSO Capital Partners
# private credit); fixed here so it can't recur on newly processed PDFs.
ALT_BRAND_WORD_BOUNDARY_TERMS = {"gso"}


def _alt_brand_term_cond(term: str) -> str:
    """Shared condition-builder for one ALT_BRAND_PATTERNS/ALT_MANAGER_NAMES
    term: word-boundary regex for terms in ALT_BRAND_WORD_BOUNDARY_TERMS,
    plain substring otherwise, ANDed with ALT_BRAND_TERM_OVERRIDES when
    present. Centralizing this keeps asset_type/asset_class/classification_
    method/matched_manager_name from ever drifting out of sync on which rows
    a given brand term matches."""
    term_sql = term.replace("'", "''")
    if term in ALT_BRAND_WORD_BOUNDARY_TERMS:
        cond = f"regexp_like(lower(trim(raw_entity_name)), '\\b{term_sql}\\b')"
    else:
        cond = f"strpos(lower(trim(raw_entity_name)), '{term_sql}') > 0"
    override = ALT_BRAND_TERM_OVERRIDES.get(term)
    if override:
        cond = f"({cond} AND {override})"
    return cond


# A brand-name match only counts as an alternatives holding if the name also
# carries a private-fund structural marker, OR the staging asset_type is
# already one we trust for alts -- same two-sided gate validated against
# 5,623 ground-truth plan_alternatives_history rows. Without this, "Ares
# Management" would also match "Ares Management Corp Class A" (NYSE common
# stock) since the brand is a substring of the public company's own name too.
ALT_BRAND_STRUCTURAL_MARKER_REGEX = (
    r"\b(l\.?p\.?|llc|fund|partners?|trust|ltd|joint\s+venture|\bjv\b|"
    r"capital\s+partners|feeder|offshore|reif)\b"
)
ALT_BRAND_TRUSTED_ASSET_TYPES = frozenset({
    "hedge fund", "joint venture", "real estate", "private equity funds",
    "103-12 investment entity", "partnership interest",
    "partnership/joint venture interest",
})

# Reject public-market instrument patterns even if a brand term and a
# structural marker both happen to match -- an independent safety net on top
# of the structural-marker gate above.
ALT_BRAND_NOISE_REGEX = (
    r"%|\bsr\.?\s+unsecured\b|\bcallable\s+notes?\b|\bdue\s+\d|"
    r"\bcorp\.?\s+debt\b|\bcorporate\s+(bond|debt)\b|\bcom\b|\badr\b|"
    r"\bcusip\b|\bsedol\b|\bnew\s+issue\b|\bcorporation\b\s*$"
)

# Additional asset_type exclusions beyond ALT_EXCLUDED_ASSET_TYPES |
# ALT_NOISE_ASSET_TYPES above -- public bond/equity instrument types that are
# more likely to slip through a bare brand-name match than a generic-phrase
# match, so they weren't needed on the keyword router but are here.
ALT_BRAND_EXTRA_EXCLUDED_ASSET_TYPES = frozenset({
    "corporate stock - common", "bond", "corp. debt instr. - all other",
    "equities", "corporate stock- preferred",
})

# Specific (ack_id, raw_entity_name) pairs confirmed as false positives
# despite passing every filter above -- found via row-level verification
# against ground truth, not (yet) inferable from any general rule. Kept as
# explicit exclusions rather than folded into a regex, to avoid over-fitting
# a one-off data-quality artifact into a general-purpose filter.
ALT_BRAND_CONFIRMED_EXCLUSIONS: List[Tuple[str, str]] = [
    # NYSE-listed Ares Management Corp Class A common stock, mistagged
    # asset_type='real estate' in one plan's raw filing data (duplicated
    # staging row: one copy blank asset_type, one copy mistagged). See
    # project_dcio_alternatives_router memory, 2026-09-20 verification.
    ("20250813090708NAL0008939265001", "ares management corp cl a"),
]


def _alt_brand_case_sql(value_index: int) -> str:
    """Build a CASE expression picking ALT_BRAND_PATTERNS[*][value_index]
    (1=asset_type, 2=asset_class) for the first brand term found in
    raw_entity_name, honoring ALT_BRAND_TERM_OVERRIDES. Mirrors
    _alt_case_sql() above so the two stay easy to compare/audit side by
    side."""
    lines = ["CASE"]
    for term, asset_type, asset_class in ALT_BRAND_PATTERNS:
        cond = _alt_brand_term_cond(term)
        val = (asset_type if value_index == 1 else asset_class).replace("'", "''")
        lines.append(f"        WHEN {cond} THEN '{val}'")
    lines.append("        ELSE NULL END")
    return "\n".join(lines)


def _alt_brand_method_case_sql() -> str:
    """Build the classification_method CASE for ALT_BRAND_PATTERNS. Kept
    separate from _alt_brand_case_sql since the method string is derived
    from the term itself (not a stored column), unlike asset_type/asset_class."""
    lines = ["CASE"]
    for term, _asset_type, _asset_class in ALT_BRAND_PATTERNS:
        cond = _alt_brand_term_cond(term)
        suffix = (
            term.replace(" ", "_").replace(",", "").replace("&", "and")
                .replace(".", "").replace("'", "")
        )
        lines.append(f"        WHEN {cond} THEN 'manual:brand_match:{suffix}'")
    lines.append("        ELSE NULL END")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Manager/sponsor crosswalk for alternatives rows. Distinct from asset_class/
# asset_type classification: applies to ANY routed row (keyword- or brand-
# matched), so a row like "Frazier Healthcare Growth Buyout Fund VIII LP"
# (caught by the keyword router via "buyout fund", never touching
# ALT_BRAND_PATTERNS) still gets a manager label if the brand name is present.
# Reuses the same 55-manager term list as ALT_BRAND_PATTERNS so the two never
# drift apart on which brands are recognized.
# ---------------------------------------------------------------------------
ALT_MANAGER_NAMES: Dict[str, str] = {
    term: {
        "gso": "GSO Capital Partners",
        "onex": "Onex Partners",
        "landmark": "Landmark Partners",
        "tennenbaum": "Tennenbaum Capital",
        "owl rock": "Owl Rock Capital",
    }.get(term, term.title())
    for term, _asset_type, _asset_class in ALT_BRAND_PATTERNS
}


def _alt_manager_case_sql() -> str:
    """Build the matched_manager_name CASE from ALT_MANAGER_NAMES, using the
    same term-matching rules (word-boundary/override) as the brand router."""
    lines = ["CASE"]
    for term, manager in ALT_MANAGER_NAMES.items():
        cond = _alt_brand_term_cond(term)
        val = manager.replace("'", "''")
        lines.append(f"        WHEN {cond} THEN '{val}'")
    lines.append("        ELSE NULL END")
    return "\n".join(lines)


# Legal vehicle/wrapper for the routed row -- this is what asset_type holds
# under the current taxonomy (asset_class='Alternatives' constant,
# asset_sub_class=alt category, asset_type=wrapper). Checked against real
# name-suffix coverage in the 2026-09-21 backfill: LP/LLC/offshore markers
# and the REIT/BDC/CIT/Separate Account keywords account for ~29% of rows;
# everything else defaults to 'Unknown' rather than guessing.
ALT_VEHICLE_RULES: List[Tuple[str, str]] = [
    (r"\breit\b", "REIT"),
    (r"\bbdc\b", "BDC"),
    (r"\bcit\b|collective investment trust", "CIT"),
    (r"separate account", "Separate Account"),
    (r"\bltd\b|\bplc\b|cayman|luxembourg|sicav|bermuda|ireland", "Offshore Private Fund"),
    (r"\bl\.?l\.?c\.?\b", "Private Fund - LLC"),
    (r"\bl\.?p\.?\b", "Private Fund - LP"),
]


def _alt_vehicle_case_sql() -> str:
    """Build the asset_type (legal vehicle/wrapper) CASE from ALT_VEHICLE_RULES,
    defaulting to 'Unknown' rather than NULL -- unlike the category/manager
    CASEs, this one must never be used as a match/no-match signal since every
    routed row gets some asset_type value."""
    lines = ["CASE"]
    for pattern, label in ALT_VEHICLE_RULES:
        pat_sql = pattern.replace("'", "''")
        lines.append(f"        WHEN regexp_like(lower(raw_entity_name), '{pat_sql}') THEN '{label}'")
    lines.append("        ELSE 'Unknown' END")
    return "\n".join(lines)


def _route_alt_brands_from_staging(glue_db: str, staging_table: str, target_table: str, ack_ids: list) -> None:
    """Second alternatives-routing pass: brand/manager-name matching via
    ALT_BRAND_PATTERNS, for rows the keyword pass (_route_alternatives_from_
    staging, using ALT_FUND_PATTERNS) doesn't catch because the name has no
    generic alt-fund phrase, only a manager brand. Must run AFTER
    _route_alternatives_from_staging for the same ack_ids so the NOT EXISTS
    check below correctly skips rows the keyword pass already inserted.
    INSERT-only, no DELETE: every row this matches is, by construction, not
    yet in target_table for its (ack_id, raw_entity_name), so there is
    nothing to safely clear first -- a scoped DELETE here would risk wiping
    out unrelated rows already routed for the same ack_id.
    classification_confidence is unconditionally MEDIUM (a manager-level
    judgment call, one asset class per brand, not a per-record tiering) and
    manual_review_required is unconditionally true, matching
    _route_alternatives_from_staging's existing behavior."""
    import awswrangler as wr
    import os
    if not ack_ids:
        return
    wg = os.getenv("ATHENA_WORKGROUP", "primary")
    s3 = os.getenv("ATHENA_STAGING_S3")
    ids = ", ".join("'" + str(a).replace("'", "''") + "'" for a in ack_ids)
    excluded = ", ".join(
        "'" + t + "'" for t in sorted(
            ALT_EXCLUDED_ASSET_TYPES | ALT_NOISE_ASSET_TYPES | ALT_BRAND_EXTRA_EXCLUDED_ASSET_TYPES
        )
    )
    trusted = ", ".join("'" + t + "'" for t in sorted(ALT_BRAND_TRUSTED_ASSET_TYPES))
    exclusion_clause = " ".join(
        "AND NOT (ack_id = '%s' AND lower(trim(raw_entity_name)) = '%s')"
        % (a.replace("'", "''"), n.replace("'", "''"))
        for a, n in ALT_BRAND_CONFIRMED_EXCLUSIONS
    )
    manager_case = _alt_manager_case_sql()

    select_sql = f"""
        SELECT
            ack_id, raw_entity_name, raw_sponsor_name, plan_investment_amt,
            {_alt_brand_case_sql(2)} AS asset_sub_class,
            validation_status,
            {_alt_vehicle_case_sql()} AS asset_type,
            'MEDIUM' AS classification_confidence,
            {_alt_brand_method_case_sql()} AS classification_method,
            true AS manual_review_required,
            current_timestamp AS routed_at,
            'Alternatives' AS asset_class,
            {manager_case} AS matched_manager_name,
            CASE WHEN {manager_case} IS NOT NULL THEN 'HIGH' ELSE NULL END AS manager_match_confidence,
            CASE WHEN {manager_case} IS NOT NULL THEN 'brand_regex_v1' ELSE NULL END AS manager_match_method
        FROM {glue_db}.{staging_table}
        WHERE ack_id IN ({ids})
          AND (lower(trim(asset_type)) IS NULL OR lower(trim(asset_type)) NOT IN ({excluded}))
          AND NOT EXISTS (
              SELECT 1 FROM {glue_db}.{target_table} t
              WHERE t.ack_id = {glue_db}.{staging_table}.ack_id
                AND t.raw_entity_name = {glue_db}.{staging_table}.raw_entity_name
          )
          AND (
              regexp_like(lower(raw_entity_name), '{ALT_BRAND_STRUCTURAL_MARKER_REGEX}')
              OR lower(trim(asset_type)) IN ({trusted})
          )
          AND NOT regexp_like(lower(raw_entity_name), '{ALT_BRAND_NOISE_REGEX}')
          {exclusion_clause}
    """
    insert_sql = (
        f"INSERT INTO {glue_db}.{target_table} "
        "(ack_id, raw_entity_name, raw_sponsor_name, plan_investment_amt, asset_sub_class, "
        "validation_status, asset_type, classification_confidence, classification_method, "
        "manual_review_required, routed_at, asset_class, "
        "matched_manager_name, manager_match_confidence, manager_match_method) "
        f"SELECT * FROM ({select_sql}) t WHERE t.asset_sub_class IS NOT NULL"
    )
    qid = wr.athena.start_query_execution(sql=insert_sql, database=glue_db, workgroup=wg, s3_output=s3)
    wr.athena.wait_query(query_execution_id=qid)
    logger.info("Routed brand-matched alternatives rows %s -> %s for %d acks", staging_table, target_table, len(ack_ids))


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_post_extract_validation(
    db_path: str,
    glue_db: str,
    ref_table: str,
    workgroup: str,
    s3_staging: str,
    tolerance: float,
    validated_s3: str,
    error_s3: str,
    validated_glue_db: str,
    validated_table: str,
    error_table: str,
    summary_table: str,
    summary_glue_db: str,
    manual_review_tolerance: float,
) -> Dict[str, int]:
    """Run the post-extraction validation gate and write results to Parquet.

    Decision per PDF:
      SKIP  — pdf_stem not in reference, or expected MF total is zero/null
      PASS  — abs(extracted - expected) / expected <= tolerance
      FAIL  — above threshold; writes one error record

    Returns a dict with keys "passed", "failed", "skipped".
    """
    run_ts = datetime.now(timezone.utc).isoformat()
    counts = {"passed": 0, "failed": 0, "skipped": 0}
    summary_records: List[Dict[str, object]] = []

    rows = load_final_rows(db_path)
    if not rows:
        logger.warning("No rows found in SQLite — validation skipped entirely")
        return counts

    # Data hygiene (always on): remove exact within-plan scrape-duplicates up front, so
    # both the pass/fail total below and the loaded rows exclude them. Safe -- an identical
    # holding+value twice in one plan is a double capture, never a second real position.
    rows, _n_dup = dedup_plan_rows(rows)
    if _n_dup:
        logger.info("Deduped %d exact scrape-duplicate row(s) before validation/load", _n_dup)

    # Trailing-subtotal typing (FALLBACK): reverse-propagate asset type from a "Total ..." /
    # bare-type-label subtotal line to the still-untyped rows above it. Only fills rows still
    # BLANK after the section-heading + per-row-type methods. Per plan, in reading order, and
    # BEFORE junk removal (the subtotal lines get dropped there).
    _rt_by_stem: Dict[str, List[Dict]] = defaultdict(list)
    for _r in rows:
        _rt_by_stem[str(_r.get("pdf_stem", "") or "").strip()].append(_r)
    _rt_filled = 0
    for _pr in _rt_by_stem.values():
        _rt_filled += apply_trailing_subtotal_types(_pr)
    if _rt_filled:
        logger.info("Trailing-subtotal typing filled %d untyped row(s)", _rt_filled)

    # Junk filter (ALWAYS ON). Removes provable non-holdings -- grand-total by math,
    # header/label lexicon, participant loans, accounting lines, dates, bond fragments,
    # generic words. Runs PER PLAN, BEFORE the total and the load, so junk never enters
    # plan_mf_history_v3 and never inflates the pass/fail total. Dropped rows are written to
    # a CSV for visibility. junk_detect reads fund_name + plan_investment_amt, so we attach
    # those (from pick_fund_name / current_value) then strip them off survivors.
    from .junk_detect import clean_plan as _junk_clean_plan
    _by_stem: Dict[str, List[Dict]] = defaultdict(list)
    for _r in rows:
        _by_stem[str(_r.get("pdf_stem", "") or "").strip()].append(_r)
    _kept: List[Dict] = []
    _drops: List[tuple] = []
    for _stem, _prows in _by_stem.items():
        for _r in _prows:
            _r["fund_name"] = pick_fund_name(_r.get("issuer_name"), _r.get("investment_description"))
            _r["plan_investment_amt"] = parse_currency_value(_r.get("current_value"))
        _res = _junk_clean_plan(_prows)
        for _r in _res["keep"]:
            _r.pop("fund_name", None)
            _r.pop("plan_investment_amt", None)
        _kept.extend(_res["keep"])
        for _r, _reason in _res["removed_junk"]:
            _drops.append((_stem, _r.get("fund_name", ""), _r.get("plan_investment_amt"), _reason))
        for _r in _res.get("dedup_removed", []):
            _drops.append((_stem, _r.get("fund_name", ""), _r.get("plan_investment_amt"), "exact duplicate"))
        for _r in _res.get("near_dup_removed", []):
            _drops.append((_stem, _r.get("fund_name", ""), _r.get("plan_investment_amt"), "near-duplicate (share-class variant, value match)"))
    rows = _kept
    if _drops:
        logger.info("junk filter removed %d row(s) across %d plan(s)", len(_drops), len(_by_stem))
        _write_junk_drop_log(_drops)

    reference = load_reference(glue_db, ref_table, workgroup, s3_staging)
    extracted_totals = compute_extracted_mf_totals(rows)

    # Group all rows by pdf_stem for efficient dispatch
    rows_by_stem: Dict[str, List[Dict]] = defaultdict(list)
    for row in rows:
        stem = str(row.get("pdf_stem", "") or "").strip()
        if stem:
            rows_by_stem[stem].append(row)

    # MF RECONCILIATION (post-extraction): reconcile each plan's extracted MF total to the
    # certified amt_mutual_funds. OVER plans: cascade removals (money-market -> other non-MF ->
    # CIT-by-name -> amt_cit reconciliation); UNDER plans: recover blank rows that read as MFs.
    # Recompute totals so validation_status AND routing both reflect it. Gate off with
    # NAME_REMEDIATION=0. Stage-4 (amount-based) changes are flagged needs_review in the returned
    # change list; they are still applied here (no separate review store yet).
    import os as _os_rem
    if _os_rem.getenv("NAME_REMEDIATION", "1") != "0":
        from .mf_reconcile import reconcile_plan
        _remed = 0
        for _stem, _srows in rows_by_stem.items():
            _ref = reference.get(_stem)
            if not _ref:
                continue
            for _r in _srows:
                # reconcile on the COMBINED issuer + description so a type word in EITHER
                # column is seen (pick_fund_name would blank a bare 'Common Stock' desc).
                _r["_rem_name"] = (str(_r.get("issuer_name") or "") + " " + str(_r.get("investment_description") or "")).strip()
            _ch = reconcile_plan(
                _srows,
                certified_mf=float(_ref.get("amt_mutual_funds") or 0),
                certified_cit=float(_ref.get("amt_cit") or 0),
                tolerance=tolerance,
                name_key="_rem_name", type_key="asset_type", value_key="current_value")
            for _r in _srows:
                _r.pop("_rem_name", None)
            _remed += len(_ch)
        if _remed:
            logger.info("MF reconciliation re-typed %d row(s) across over/under-capture plans", _remed)
            extracted_totals = compute_extracted_mf_totals(rows)   # recompute with corrected types

    for pdf_stem in sorted(rows_by_stem):
        stem_rows = rows_by_stem[pdf_stem]

        if pdf_stem not in reference:
            logger.warning("SKIP %s: not found in reference table", pdf_stem)
            summary_records.append({
                "ack_id": pdf_stem,
                "plan_id": None,
                "extracted_amt_mutual_funds": extracted_totals.get(pdf_stem, 0.0),
                "reference_amt_mutual_funds": None,
                "difference_amt": None,
                "difference_pct": None,
                "validation_status": "SKIP",
                "gap_reason": "REFERENCE_NOT_FOUND",
                "run_ts": run_ts,
            })
            counts["skipped"] += 1
            continue

        ref_entry = reference[pdf_stem]
        expected = float(ref_entry["amt_mutual_funds"])
        plan_id = str(ref_entry.get("plan_id", "") or "").strip()
        if expected <= 0:
            logger.warning("SKIP %s: reference amt_mutual_funds is zero/null", pdf_stem)
            summary_records.append({
                "ack_id": pdf_stem,
                "plan_id": plan_id,
                "extracted_amt_mutual_funds": extracted_totals.get(pdf_stem, 0.0),
                "reference_amt_mutual_funds": expected,
                "difference_amt": None,
                "difference_pct": None,
                "validation_status": "SKIP",
                "gap_reason": "REFERENCE_ZERO_OR_NULL",
                "run_ts": run_ts,
            })
            counts["skipped"] += 1
            continue

        extracted = extracted_totals.get(pdf_stem, 0.0)
        bad_reference_override = (pdf_stem, plan_id) in BAD_REFERENCE_COMPARISON_OVERRIDES
        if bad_reference_override:
            passes, pct_diff = True, 0.0
            logger.warning(
                "PASS %s via bad-reference override for plan_id=%s: extracted=%.0f reference=%.0f",
                pdf_stem, plan_id, extracted, expected,
            )
        else:
            passes, pct_diff = validate_pdf(extracted, expected, tolerance)

        difference_amt = extracted - expected
        summary_records.append({
            "ack_id": pdf_stem,
            "plan_id": plan_id,
            "extracted_amt_mutual_funds": extracted,
            "reference_amt_mutual_funds": expected,
            "difference_amt": difference_amt,
            "difference_pct": pct_diff,
            "validation_status": "MANUAL_REVIEW" if pct_diff > manual_review_tolerance else ("PASS" if passes else "FAIL"),
            "gap_reason": "MF_TOTAL_GT_10_PCT_OFF" if pct_diff > manual_review_tolerance else "WITHIN_10_PCT",
            "run_ts": run_ts,
        })

        if passes:
            counts["passed"] += 1
        else:
            counts["failed"] += 1

    if summary_records:
        summary_df = pd.DataFrame(summary_records, columns=[
            "ack_id",
            "plan_id",
            "extracted_amt_mutual_funds",
            "reference_amt_mutual_funds",
            "difference_amt",
            "difference_pct",
            "validation_status",
            "gap_reason",
            "run_ts",
        ])
        write_validation_summary_via_athena(summary_df, summary_glue_db, summary_table)

    # LOAD-ALL. Default path (no staging configured) is UNCHANGED: build MF-only rows and write
    # them to validated_table. If HOLDINGS_STAGING_TABLE is set, write ALL vehicle types (tagged
    # with the file-derived asset_type) to staging, then route only the MF-typed rows into
    # validated_table -- so plan_mf_history_v3 stays MF-only and CITs/etc. remain in staging.
    import os as _os
    staging_table = _os.getenv("HOLDINGS_STAGING_TABLE", "").strip()
    status_by_ack = {r["ack_id"]: r["validation_status"] for r in summary_records}
    all_rows = []
    for _stem, _srows in rows_by_stem.items():
        _st = status_by_ack.get(_stem) or "UNVALIDATED"
        if _st == "SKIP":
            _st = "UNVALIDATED"
        _df = build_mf_rows_df(_srows, validation_status=_st, mf_only=(not staging_table))
        if not _df.empty:
            all_rows.append(_df)
    if all_rows:
        combined = pd.concat(all_rows, ignore_index=True)
        if staging_table:
            write_iceberg_via_athena(combined, validated_glue_db, staging_table, include_asset_type=True)
            _route_mf_from_staging(validated_glue_db, staging_table, validated_table,
                                   combined["ack_id"].dropna().unique().tolist())
            alt_table = _os.getenv("ALTERNATIVES_TABLE", "").strip()
            if alt_table:
                alt_ack_ids = combined["ack_id"].dropna().unique().tolist()
                _route_alternatives_from_staging(validated_glue_db, staging_table, alt_table, alt_ack_ids)
                _route_alt_brands_from_staging(validated_glue_db, staging_table, alt_table, alt_ack_ids)
        else:
            write_iceberg_via_athena(combined, validated_glue_db, validated_table)

    logger.info("Validation complete - passed=%d failed=%d skipped=%d",
                counts["passed"], counts["failed"], counts["skipped"])
    return counts



def load_final_rows(db_path: str):
    import csv as _csv, os
    csv_path = os.path.join(os.path.dirname(db_path), "investments_clean.csv")
    if not os.path.exists(csv_path):
        logger.warning("CSV not found, falling back to SQLite")
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        try:
            rows = [dict(r) for r in con.execute("SELECT * FROM investments").fetchall()]
        finally:
            con.close()
        return rows
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(_csv.DictReader(f))
    logger.info("Loaded %d rows from CSV %s", len(rows), csv_path)
    return rows
