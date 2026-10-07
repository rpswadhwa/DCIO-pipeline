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
import os
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
    s = str(s or '').lower()
    # Fuse a stray punctuation/encoding-glitch char sitting BETWEEN two letters/digits
    # (a hyphenated compound like "Mid-Cap", a garbled apostrophe) instead of letting it
    # split one word into two tokens -- otherwise "Mid-Cap" (-> mid, cap) and "MidCap"
    # (-> midcap) fail to token-match even though they're the same fund name.
    s = _re.sub(r'(?<=[a-z0-9])[^a-z0-9\s](?=[a-z0-9])', '', s)
    return _re.sub(r'\s+', ' ', _re.sub(r'[^a-z0-9 ]', ' ', s)).strip()


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


# CUSIP/SEDOL-continuation-line artifact detector (FIX 20, Mode 1 over-capture).
# Northern-Trust-style master-trust schedules ("5500 Supplemental Schedules") render each
# holding as a TWO-line record: line 1 = "<security desc> <shares> <cost> <current value>",
# line 2 = "CUSIP: <9-char id>" (or, for non-US securities, "SEDOL: <7-char id>"). The extractor
# mis-reads the id-continuation line as its own holding -- name becomes "CUSIP"/"SEDOL" (or a
# wrapped fund-name tail like "INDEX FD ADMIRAL SHS CUSIP") and the id itself (e.g. 989207105)
# parses as a $989M "value". Schlumberger master trust alone produced ~896 such CUSIP rows
# summing to $352B of fake AUM; the single-token "SEDOL" case ($39.8B / 9,122 rows found in the
# 2026-10-01 staging review) is the identical artifact under the sibling identifier scheme --
# junk_detect.py's _MULTI_SEDOL_RE only catches two SEDOLs spliced together, not this bare form.
# Neither token ever appears in a genuine fund name, so matching either anywhere is zero-FP.
_CUSIP_ARTIFACT_RE = _re.compile(r'(?i)\b(CUSIP|SEDOL)\b')


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
        # Mode 1 over-capture: drop CUSIP/SEDOL-continuation-line artifacts (name contains the
        # token "CUSIP" or "SEDOL"; the security id was mis-read as the value). See _CUSIP_ARTIFACT_RE.
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
                else:
                    # Legacy direct write (no staging table): the staging+routing path
                    # resolves sponsors in SQL via _mf_sponsor_case_sql; this path has no
                    # SQL SELECT to attach that to, so mirror it in Python here instead --
                    # same canon.json, same coalesce(raw_sponsor_name, raw_entity_name)
                    # input, so MF rows land pre-matched regardless of which write path ran.
                    _sponsor_input = row.get("raw_sponsor_name") or row.get("raw_entity_name")
                    _match = _mf_sponsor_match(_sponsor_input)
                    _canonical, _token = _match if _match else (None, None)
                    _vals += [
                        q(_canonical),
                        q("MATCHED" if _canonical else "UNMATCHED"),
                        ("CAST(0.90 AS decimal(5,4))" if _canonical else "NULL"),
                        q(_token),
                        q("canon.json" if _canonical else None),
                        q("LOW" if _canonical else None),
                        q("canon_brand_regex_v1" if _canonical else None),
                        "current_timestamp",
                        ("false" if _canonical else "true"),
                    ]
                values_parts.append("(" + ", ".join(_vals) + ")")

            _cols = ("ack_id, raw_entity_name, raw_sponsor_name, plan_investment_amt, "
                     "asset_class, asset_sub_class, validation_status")
            if include_asset_type:
                _cols += ", asset_type"
            else:
                _cols += (", normalized_sponsor_name, sponsor_match_status, sponsor_match_confidence, "
                          "sponsor_match_token, sponsor_match_source, sponsor_match_risk_level, "
                          "sponsor_matched_by, sponsor_cleaned_at, needs_sponsor_cleaning_flag")
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

def _mf_sponsor_tokens() -> List[Tuple[str, str]]:
    """Flatten canon.json into (token, canonical) pairs for MF sponsor
    matching, longest token first so a specific brand (e.g. "FIDELITY
    INSTITUTIONAL") wins over a shorter one it happens to contain, when both
    would otherwise match the same CASE expression's WHEN order."""
    lookup = _load_canon_lookup()  # {UPPER token/alias/canonical: canonical}
    pairs = [(tok, canon) for tok, canon in lookup.items() if tok]
    pairs.sort(key=lambda p: -len(p[0]))
    return pairs


_mf_sponsor_patterns_cache: Optional[List[Tuple["_re.Pattern", str, str]]] = None


def _mf_sponsor_patterns() -> List[Tuple["_re.Pattern", str, str]]:
    """Compile _mf_sponsor_tokens() once into (pattern, token, canonical)
    triples, for the legacy direct-write path below where rows are built as
    literal Python values rather than a SQL SELECT -- keeps that path's
    matching behavior identical to _mf_sponsor_case_sql without re-deriving
    canon.json or recompiling a regex per row."""
    global _mf_sponsor_patterns_cache
    if _mf_sponsor_patterns_cache is not None:
        return _mf_sponsor_patterns_cache
    patterns = [(_re.compile(r"\b" + _re.escape(token.lower()) + r"\b"), token, canonical)
                for token, canonical in _mf_sponsor_tokens()]
    _mf_sponsor_patterns_cache = patterns
    return patterns


def _mf_sponsor_match(name: str) -> Optional[Tuple[str, str]]:
    """Python-side mirror of _mf_sponsor_case_sql's matching: returns
    (canonical, matched_token) for the first canon.json brand found in
    `name`, or None. Used by write_iceberg_via_athena's legacy direct-write
    path (no HOLDINGS_STAGING_TABLE), where _route_mf_from_staging's SQL
    CASE approach doesn't apply."""
    text = (name or "").strip().lower()
    if not text:
        return None
    for pat, token, canonical in _mf_sponsor_patterns():
        if pat.search(text):
            return canonical, token
    return None


def _mf_sponsor_groups() -> List[Tuple[str, str, List[str]]]:
    """Group _mf_sponsor_tokens() by canonical: one entry per canonical
    (~152) instead of one per token (526). A CASE with a WHEN branch per
    token throws Athena's INTERNAL_ERROR_QUERY_ENGINE past ~400 branches
    (confirmed empirically: 400 runs fine, 450 fails) -- 526 tokens collapse
    to ~152 canonicals, comfortably under that ceiling, by combining each
    canonical's tokens into one regexp_like alternation per branch. Groups
    are ordered by their longest token descending (tokens within a group are
    already longest-first from _mf_sponsor_tokens(), so each group's first
    token is its longest), preserving the original longest-token-wins
    precedence at the canonical level. Returns (canonical, representative_
    token, tokens) triples -- representative_token (the group's own longest
    token) stands in for sponsor_match_token, which can no longer name the
    exact token that matched once multiple tokens share a branch."""
    groups: Dict[str, List[str]] = {}
    for token, canonical in _mf_sponsor_tokens():  # already longest-first
        groups.setdefault(canonical, []).append(token)
    ordered = sorted(groups.items(), key=lambda kv: -len(kv[1][0]))
    return [(canonical, tokens[0], tokens) for canonical, tokens in ordered]


def _mf_sponsor_case_sql(column: str, return_token: bool = False) -> str:
    """Build a CASE expression matching canon.json brand tokens, word-boundary,
    against `column` (the caller passes coalesce(raw_sponsor_name,
    raw_entity_name) -- raw_sponsor_name comes from the filing PDF's
    issuer_name and is sometimes blank, in which case the fund's own name is
    the next-best signal of its sponsor). Mirrors _alt_manager_case_sql's
    mechanism (canon.json-driven CASE) but one WHEN branch per canonical via
    _mf_sponsor_groups(), not one per token -- see that function's docstring
    for why. return_token=True builds the parallel CASE that returns each
    group's representative token (for sponsor_match_token) instead of the
    canonical name."""
    lines = ["CASE"]
    for canonical, rep_token, tokens in _mf_sponsor_groups():
        alts = "|".join(_re.escape(t.lower()).replace("'", "''") for t in tokens)
        val = (rep_token if return_token else canonical).replace("'", "''")
        lines.append(f"        WHEN regexp_like(lower(trim({column})), '\\b({alts})\\b') THEN '{val}'")
    lines.append("        ELSE NULL END")
    return "\n".join(lines)


def _route_mf_from_staging(glue_db: str, staging_table: str, target_table: str, ack_ids: list) -> None:
    """Populate the MF table from staging: only rows whose file-derived asset_type is an MF type.
    Deletes the run's acks from target first (idempotent), then inserts the MF subset, now
    including the canon.json-driven sponsor-cleaning columns (normalized_sponsor_name,
    sponsor_match_status/confidence/token/source/risk_level/matched_by/cleaned_at,
    needs_sponsor_cleaning_flag) so MF rows arrive pre-matched at load time instead of
    needing a cleanup pass later."""
    import awswrangler as wr
    import os
    if not ack_ids:
        return
    wg = os.getenv("ATHENA_WORKGROUP", "primary")
    s3 = os.getenv("ATHENA_STAGING_S3")
    ids = ", ".join("'" + str(a).replace("'", "''") + "'" for a in ack_ids)
    mf = ", ".join("'" + t + "'" for t in sorted(MF_ASSET_TYPES))
    excl = ", ".join("'" + t + "'" for t in sorted(MF_ROUTING_EXCLUDE_NAMES))

    sponsor_input = "coalesce(raw_sponsor_name, raw_entity_name)"
    mgr_case = _mf_sponsor_case_sql(sponsor_input, return_token=False)
    token_case = _mf_sponsor_case_sql(sponsor_input, return_token=True)

    insert_sql = """
        INSERT INTO {gd}.{tt} (
            ack_id, raw_entity_name, raw_sponsor_name, plan_investment_amt,
            asset_class, asset_sub_class, validation_status,
            normalized_sponsor_name, sponsor_match_status, sponsor_match_confidence,
            sponsor_match_token, sponsor_match_source, sponsor_match_risk_level,
            sponsor_matched_by, sponsor_cleaned_at, needs_sponsor_cleaning_flag
        )
        WITH staged AS (
            SELECT *,
                {mgr_case} AS _norm_mgr,
                {token_case} AS _norm_token
            FROM {gd}.{st}
            WHERE ack_id IN ({ids}) AND lower(trim(asset_type)) IN ({mf})
              AND lower(trim(raw_entity_name)) NOT IN ({excl})
        )
        SELECT
            ack_id, raw_entity_name, raw_sponsor_name, plan_investment_amt,
            asset_class, asset_sub_class, validation_status,
            _norm_mgr AS normalized_sponsor_name,
            CASE WHEN _norm_mgr IS NOT NULL THEN 'MATCHED' ELSE 'UNMATCHED' END AS sponsor_match_status,
            CASE WHEN _norm_mgr IS NOT NULL THEN CAST(0.90 AS decimal(5,4)) ELSE NULL END AS sponsor_match_confidence,
            _norm_token AS sponsor_match_token,
            CASE WHEN _norm_mgr IS NOT NULL THEN 'canon.json' ELSE NULL END AS sponsor_match_source,
            CASE WHEN _norm_mgr IS NOT NULL THEN 'LOW' ELSE NULL END AS sponsor_match_risk_level,
            CASE WHEN _norm_mgr IS NOT NULL THEN 'canon_brand_regex_v1' ELSE NULL END AS sponsor_matched_by,
            current_timestamp AS sponsor_cleaned_at,
            CASE WHEN _norm_mgr IS NULL THEN true ELSE false END AS needs_sponsor_cleaning_flag
        FROM staged
    """.format(gd=glue_db, tt=target_table, st=staging_table, ids=ids, mf=mf, excl=excl,
               mgr_case=mgr_case, token_case=token_case)

    stmts = [
        f"DELETE FROM {glue_db}.{target_table} WHERE ack_id IN ({ids})",
        insert_sql,
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

# Rollup/pointer line items -- same trap as MF_ROUTING_EXCLUDE_NAMES above, on the
# alternatives side. A Schedule H line like "Limited Partnerships and Other Private
# Equity See Appendix X" is a POINTER to an itemized appendix elsewhere in the filing,
# not a real single holding -- but it contains "private equity" (a MEDIUM-tier keyword
# above), so the router auto-classified and loaded it as one $14.58B "fund" (Western
# Conference of Teamsters, ack_id 20250930114328NAL0016441459001; found 2026-09-20,
# fixed via a one-off manual DELETE + hand-decomposition of the appendix at the time --
# see project_dcio_alternatives_router memory). "See Appendix"/"See Attached"/"See
# Schedule" phrasing is bounded and never occurs inside a real fund name (same
# reasoning as junk_detect.py's "Fidelity Mutual funds - see attachment" entry and
# MF_ROUTING_EXCLUDE_NAMES's "see attached"), so it's safe to exclude unconditionally
# rather than case-by-case. This router is raw-SQL and never calls junk_detect (same
# caveat as MF_ROUTING_EXCLUDE_NAMES above), so the guard is applied directly in
# _route_alternatives_from_staging's WHERE clause.
ALT_ROLLUP_POINTER_REGEX = r"see\s+appendix|see\s+attach(ed|ment)?\b|see\s+schedule\b"

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


_ALT_INSERT_COLUMNS = (
    "ack_id, raw_entity_name, raw_sponsor_name, plan_investment_amt, "
    "asset_sub_class, validation_status, asset_type, classification_confidence, "
    "classification_method, manual_review_required, routed_at, asset_class, "
    "matched_manager_name, manager_match_confidence, manager_match_method"
)


def _route_alternatives_from_staging(glue_db: str, staging_table: str, target_table: str, ack_ids: list) -> None:
    """Populate the alternatives table from staging in a single pass with
    an override lookup plus four match strategies -- override first, then
    phrase, then debt-carveout, then brand, then sponsor -- combined in one
    query so there is no second statement and no ordering dependency between
    them:

    0. Override match (added 2026-09-25, value-keyed 2026-09-25): looks up
       alt_manual_research_overrides, a reference table holding rows whose
       original classification came from one-time manual research
       (classification_method LIKE 'manual:round2_sourced_research',
       'manual:appendix_x_decomposition', 'manual:round3_sponsor_research')
       rather than any of the four reproducible passes below. This exists
       because a full DELETE+INSERT recompute is only as good as what the
       automated passes can currently derive -- a recompute with no override
       table would silently drop any row whose provenance was one-time human
       research never captured as reusable logic (confirmed against a real
       100-ack_id recompute: 12 rows / $297.1M would have been lost without
       it). Takes unconditional top priority: always included for the run's
       ack_ids regardless of what the passes below independently conclude.
       Matched by VALUE on (raw_entity_name, raw_sponsor_name), not by the
       override row's own historical ack_id -- a manually-researched fund
       recurs across many plans' filings over time under many different
       ack_ids, and an ack_id-scoped match only ever helps a re-run of the
       exact filing the research was originally done against, never a new
       one. raw_entity_name alone repeats within an ack_id when filers reuse
       a boilerplate Schedule D label ("Partnership/joint venture interests")
       across many distinct real funds named only in raw_sponsor_name, so
       raw_sponsor_name is part of the key too -- it's what disambiguates the
       boilerplate case correctly (same boilerplate label + same sponsor name
       = same real fund, filed by a different plan). plan_investment_amt and
       ack_id were dropped from the key: they're expected to differ across
       filings of the same fund and were never load-bearing for correctness,
       only for uniqueness within the old snapshot-replay design. The
       4-column tuple this replaces was confirmed unique across all 480
       override rows before this was built.
    1. Phrase match: keyword-matches raw_entity_name against ALT_FUND_PATTERNS
       (real estate / private credit / private equity / infrastructure /
       hedge fund).
    2. Debt carveout (added 2026-09-25): for a handful of managers
       (ALT_MANAGER_DEBT_PATTERNS) whose brand name spans both confirmed
       public debt and equity holdings under one name -- something
       ALT_BRAND_PATTERNS' single static (asset_type, asset_class) per term
       structurally can't express -- routes name-level-confirmed debt rows to
       'Manager Debt Exposure' with a per-row instrument type (Senior Notes/
       CMBS/RMBS/ABS/Corporate Bond), ahead of the generic brand pass so a
       specific debt/equity signal always wins over the coarse brand bucket.
       Excludes rows the phrase pass already claimed, same NOT EXISTS pattern
       as brand match below.
    3. Brand match: for staging rows the phrase and debt-carveout passes
       don't cover (no generic alt-fund phrase in the name, e.g. "AEA
       Investors Fund VII LP"), matches against the ALT_BRAND_PATTERNS
       manager-brand vocabulary instead. Gated tighter than the phrase pass
       (structural marker or trusted asset_type required, extra noise/
       asset_type exclusions) since a bare brand name is a weaker signal than
       an explicit category phrase. Excludes any (ack_id, raw_entity_name)
       already covered by the phrase or debt-carveout pass via a NOT EXISTS
       against those passes' own CTEs, so a name matched by either one never
       also gets a second, weaker-confidence brand row.
    4. Sponsor match (added 2026-09-24, round 3): for rows none of the passes
       above cover -- typically a generic placeholder raw_entity_name
       ("Partnership/joint venture interests", "N/A Limited Partnerships")
       that hides the real manager name in raw_sponsor_name instead --
       reruns the same ALT_BRAND_PATTERNS vocabulary against
       raw_sponsor_name. Confidence is LOW (below brand match's MEDIUM):
       sponsor free text is noisier and, per ALT_SPONSOR_EXCLUDE_REGEX, just
       as likely to name a custodian bank or traditional (non-alternative)
       manager as an alt brand. Also excludes self-referential sponsors
       (sponsor name is just the entity name plus a trailing numeric id, i.e.
       no new information) and anything the phrase, debt-carveout, or brand
       pass already claimed.

    Passes 1-4 each also exclude any staging row already covered by the
    override lookup (same 4-column tuple), so if a future pattern change
    ever makes one of them also match a manually-researched row, the
    override version wins and the automated pass doesn't add a duplicate.

    Non-matching rows are left in staging, not swept into a catch-all bucket.
    Deletes the run's acks from target first (idempotent), then inserts the
    override subset plus all four pattern-matched subsets in one INSERT.
    manual_review_required is unconditionally true for the four pattern
    passes for now: these patterns are unproven against real data, so every
    routed row should get a first look before this flips to a
    confidence-based rule."""
    import awswrangler as wr
    import os
    if not ack_ids:
        return
    wg = os.getenv("ATHENA_WORKGROUP", "primary")
    s3 = os.getenv("ATHENA_STAGING_S3")
    ids = ", ".join("'" + str(a).replace("'", "''") + "'" for a in ack_ids)
    manager_case = _alt_manager_case_sql()
    vehicle_case = _alt_vehicle_case_sql()
    manager_case_sponsor = _alt_manager_case_sql("raw_sponsor_name")
    vehicle_case_sponsor = _alt_vehicle_case_sql("raw_sponsor_name")
    debt_manager_case = _alt_debt_carveout_manager_sql()

    phrase_excluded = ", ".join("'" + t + "'" for t in sorted(ALT_EXCLUDED_ASSET_TYPES | ALT_NOISE_ASSET_TYPES))
    # brand_excluded is a hard, marker-independent block -- reserved for ALT_EXCLUDED_ASSET_TYPES
    # only, since that's the one set that protects against colliding with a DIFFERENT router that
    # already owns those literal asset_type values (MF_ASSET_TYPES -> plan_mf_history_v3;
    # "common/collective trust fund"/"commingled fund" -> plan_cit_history, confirmed 2026-09-25 by
    # sampling both tables against real Boyd Watterson/Harrison Street/Washington Capital rows: 35
    # "mutual fund"-tagged rows and 6+ "common/collective trust fund" rows for these exact ack_ids
    # were already present in those tables, so routing them here too would double-count the same
    # dollars across two target tables. ALT_NOISE_ASSET_TYPES and ALT_BRAND_EXTRA_EXCLUDED_ASSET_TYPES
    # are a different kind of thing -- a heuristic veto against bare public securities that happen to
    # share a brand name (e.g. "Sterling Infrastructure Inc" filed as common stock), with no other
    # router claiming those asset_type values -- so for the brand pass they're demoted from a hard
    # exclusion to a normal marker requirement below: real LP-fund rows mistagged as "common stock"/
    # "bond"/etc. by the filer (same ack_id-and-name evidence as above -- 87/99 mislabeled rows carry
    # a clean "LP"/"Fund"/"Trust" structural marker) still route once the marker is present, while a
    # bare brand name with no marker and no trusted asset_type is still blocked by the unchanged
    # marker-or-trusted gate and noise regex below.
    brand_excluded = ", ".join("'" + t + "'" for t in sorted(ALT_EXCLUDED_ASSET_TYPES))
    trusted = ", ".join("'" + t + "'" for t in sorted(ALT_BRAND_TRUSTED_ASSET_TYPES))

    combined_sql = f"""
        WITH override_lookup AS (
            SELECT
                lower(trim(raw_entity_name)) AS entity_key,
                coalesce(lower(trim(raw_sponsor_name)), '') AS sponsor_key,
                asset_sub_class, asset_type, classification_confidence,
                classification_method, manual_review_required, asset_class,
                matched_manager_name, manager_match_confidence, manager_match_method,
                row_number() OVER (
                    PARTITION BY lower(trim(raw_entity_name)), coalesce(lower(trim(raw_sponsor_name)), '')
                    ORDER BY routed_at DESC
                ) AS rn
            FROM {glue_db}.alt_manual_research_overrides
        ),
        override_matches AS (
            SELECT
                s.ack_id, s.raw_entity_name, s.raw_sponsor_name, s.plan_investment_amt,
                o.asset_sub_class, s.validation_status, o.asset_type, o.classification_confidence,
                o.classification_method, o.manual_review_required, current_timestamp AS routed_at,
                o.asset_class, o.matched_manager_name, o.manager_match_confidence, o.manager_match_method
            FROM {glue_db}.{staging_table} s
            JOIN override_lookup o
              ON lower(trim(s.raw_entity_name)) = o.entity_key
             AND coalesce(lower(trim(s.raw_sponsor_name)), '') = o.sponsor_key
             AND o.rn = 1
            WHERE s.ack_id IN ({ids})
        ),
        entity_mgr_lookup AS (
            -- manager_case is large (grows with ALT_BRAND_PATTERNS) and is needed
            -- identically by both phrase_matches and brand_matches below; computing
            -- it once here and reusing _mgr keeps it out of the combined query twice,
            -- which is what pushed this INSERT past Athena's 262144-char query-string
            -- limit once ALT_BRAND_PATTERNS grew past ~600 terms.
            SELECT
                s.ack_id, s.raw_entity_name, s.raw_sponsor_name, s.plan_investment_amt,
                s.validation_status, s.asset_type,
                {manager_case} AS _mgr
            FROM {glue_db}.{staging_table} s
            WHERE s.ack_id IN ({ids})
        ),
        phrase_matches AS (
            SELECT
                ack_id, raw_entity_name, raw_sponsor_name, plan_investment_amt,
                asset_sub_class, validation_status, asset_type,
                classification_confidence, classification_method,
                manual_review_required, routed_at, asset_class,
                _mgr AS matched_manager_name,
                CASE WHEN _mgr IS NOT NULL THEN 'HIGH' ELSE NULL END AS manager_match_confidence,
                CASE WHEN _mgr IS NOT NULL THEN 'brand_regex_v1' ELSE NULL END AS manager_match_method
            FROM (
                SELECT
                    s.ack_id, s.raw_entity_name, s.raw_sponsor_name, s.plan_investment_amt,
                    {_alt_case_sql(2)} AS asset_sub_class,
                    s.validation_status,
                    {vehicle_case} AS asset_type,
                    {_alt_case_sql(3)} AS classification_confidence,
                    {_alt_case_sql(4)} AS classification_method,
                    true AS manual_review_required,
                    current_timestamp AS routed_at,
                    'Alternatives' AS asset_class,
                    s._mgr AS _mgr
                FROM entity_mgr_lookup s
                WHERE lower(trim(s.asset_type)) NOT IN ({phrase_excluded})
                  AND NOT regexp_like(lower(s.raw_entity_name), '{ALT_ROLLUP_POINTER_REGEX}')
                  AND NOT EXISTS (
                      SELECT 1 FROM override_matches o
                      WHERE o.ack_id = s.ack_id
                        AND o.raw_entity_name = s.raw_entity_name
                        AND o.raw_sponsor_name IS NOT DISTINCT FROM s.raw_sponsor_name
                        AND o.plan_investment_amt IS NOT DISTINCT FROM s.plan_investment_amt
                  )
            )
        ),
        phrase_matched_rows AS (
            SELECT * FROM phrase_matches WHERE asset_sub_class IS NOT NULL
        ),
        debt_carveout_matches AS (
            SELECT
                ack_id, raw_entity_name, raw_sponsor_name, plan_investment_amt,
                asset_sub_class, validation_status, asset_type,
                classification_confidence, classification_method,
                manual_review_required, routed_at, asset_class,
                _mgr AS matched_manager_name,
                CASE WHEN _mgr IS NOT NULL THEN 'HIGH' ELSE NULL END AS manager_match_confidence,
                CASE WHEN _mgr IS NOT NULL THEN 'debt_carveout_regex_v1' ELSE NULL END AS manager_match_method
            FROM (
                SELECT
                    s.ack_id, s.raw_entity_name, s.raw_sponsor_name, s.plan_investment_amt,
                    {_alt_debt_carveout_subclass_sql()} AS asset_sub_class,
                    s.validation_status,
                    {_alt_debt_carveout_instrument_type_sql()} AS asset_type,
                    'MEDIUM' AS classification_confidence,
                    {_alt_debt_carveout_method_sql()} AS classification_method,
                    true AS manual_review_required,
                    current_timestamp AS routed_at,
                    'Alternatives' AS asset_class,
                    {debt_manager_case} AS _mgr
                FROM {glue_db}.{staging_table} s
                WHERE s.ack_id IN ({ids})
                  AND NOT EXISTS (
                      SELECT 1 FROM phrase_matched_rows p
                      WHERE p.ack_id = s.ack_id AND p.raw_entity_name = s.raw_entity_name
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM override_matches o
                      WHERE o.ack_id = s.ack_id
                        AND o.raw_entity_name = s.raw_entity_name
                        AND o.raw_sponsor_name IS NOT DISTINCT FROM s.raw_sponsor_name
                        AND o.plan_investment_amt IS NOT DISTINCT FROM s.plan_investment_amt
                  )
            )
        ),
        debt_carveout_matched_rows AS (
            SELECT * FROM debt_carveout_matches WHERE asset_sub_class IS NOT NULL
        ),
        brand_matches AS (
            SELECT
                ack_id, raw_entity_name, raw_sponsor_name, plan_investment_amt,
                asset_sub_class, validation_status, asset_type,
                classification_confidence, classification_method,
                manual_review_required, routed_at, asset_class,
                _mgr AS matched_manager_name,
                CASE WHEN _mgr IS NOT NULL THEN 'HIGH' ELSE NULL END AS manager_match_confidence,
                CASE WHEN _mgr IS NOT NULL THEN 'brand_regex_v1' ELSE NULL END AS manager_match_method
            FROM (
                SELECT
                    s.ack_id, s.raw_entity_name, s.raw_sponsor_name, s.plan_investment_amt,
                    {_alt_brand_case_sql(2)} AS asset_sub_class,
                    s.validation_status,
                    {vehicle_case} AS asset_type,
                    'MEDIUM' AS classification_confidence,
                    {_alt_brand_method_case_sql()} AS classification_method,
                    true AS manual_review_required,
                    current_timestamp AS routed_at,
                    'Alternatives' AS asset_class,
                    s._mgr AS _mgr
                FROM entity_mgr_lookup s
                WHERE (lower(trim(s.asset_type)) IS NULL OR lower(trim(s.asset_type)) NOT IN ({brand_excluded}))
                  AND (
                      regexp_like(lower(s.raw_entity_name), '{ALT_BRAND_STRUCTURAL_MARKER_REGEX}')
                      OR lower(trim(s.asset_type)) IN ({trusted})
                  )
                  AND NOT regexp_like(lower(s.raw_entity_name), '{ALT_BRAND_NOISE_REGEX}')
                  AND NOT EXISTS (
                      SELECT 1 FROM phrase_matched_rows p
                      WHERE p.ack_id = s.ack_id AND p.raw_entity_name = s.raw_entity_name
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM debt_carveout_matched_rows d
                      WHERE d.ack_id = s.ack_id AND d.raw_entity_name = s.raw_entity_name
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM override_matches o
                      WHERE o.ack_id = s.ack_id
                        AND o.raw_entity_name = s.raw_entity_name
                        AND o.raw_sponsor_name IS NOT DISTINCT FROM s.raw_sponsor_name
                        AND o.plan_investment_amt IS NOT DISTINCT FROM s.plan_investment_amt
                  )
            )
        ),
        brand_matched_rows AS (
            SELECT * FROM brand_matches WHERE asset_sub_class IS NOT NULL
        ),
        sponsor_matches AS (
            SELECT
                ack_id, raw_entity_name, raw_sponsor_name, plan_investment_amt,
                asset_sub_class, validation_status, asset_type,
                classification_confidence, classification_method,
                manual_review_required, routed_at, asset_class,
                _mgr AS matched_manager_name,
                CASE WHEN _mgr IS NOT NULL THEN 'HIGH' ELSE NULL END AS manager_match_confidence,
                CASE WHEN _mgr IS NOT NULL THEN 'sponsor_brand_regex_v1' ELSE NULL END AS manager_match_method
            FROM (
                SELECT
                    s.ack_id, s.raw_entity_name, s.raw_sponsor_name, s.plan_investment_amt,
                    {_alt_brand_case_sql(2, "raw_sponsor_name")} AS asset_sub_class,
                    s.validation_status,
                    {vehicle_case_sponsor} AS asset_type,
                    'LOW' AS classification_confidence,
                    {_alt_brand_method_case_sql("raw_sponsor_name", "manual:sponsor_brand_match:")} AS classification_method,
                    true AS manual_review_required,
                    current_timestamp AS routed_at,
                    'Alternatives' AS asset_class,
                    {manager_case_sponsor} AS _mgr
                FROM {glue_db}.{staging_table} s
                WHERE s.ack_id IN ({ids})
                  AND s.raw_sponsor_name IS NOT NULL AND trim(s.raw_sponsor_name) <> ''
                  AND (lower(trim(s.asset_type)) IS NULL OR lower(trim(s.asset_type)) NOT IN ({brand_excluded}))
                  AND regexp_like(lower(s.raw_sponsor_name), '{ALT_BRAND_STRUCTURAL_MARKER_REGEX}')
                  AND NOT regexp_like(lower(s.raw_sponsor_name), '{ALT_BRAND_NOISE_REGEX}')
                  AND NOT regexp_like(lower(s.raw_sponsor_name), '{ALT_SPONSOR_EXCLUDE_REGEX}')
                  AND lower(trim(regexp_replace(s.raw_sponsor_name, '[0-9\\s]+$', ''))) <> lower(trim(s.raw_entity_name))
                  AND NOT EXISTS (
                      SELECT 1 FROM phrase_matched_rows p
                      WHERE p.ack_id = s.ack_id AND p.raw_entity_name = s.raw_entity_name
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM debt_carveout_matched_rows d
                      WHERE d.ack_id = s.ack_id AND d.raw_entity_name = s.raw_entity_name
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM brand_matched_rows b
                      WHERE b.ack_id = s.ack_id AND b.raw_entity_name = s.raw_entity_name
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM override_matches o
                      WHERE o.ack_id = s.ack_id
                        AND o.raw_entity_name = s.raw_entity_name
                        AND o.raw_sponsor_name IS NOT DISTINCT FROM s.raw_sponsor_name
                        AND o.plan_investment_amt IS NOT DISTINCT FROM s.plan_investment_amt
                  )
            )
        ),
        sponsor_matched_rows AS (
            SELECT * FROM sponsor_matches WHERE asset_sub_class IS NOT NULL
        )
        SELECT {_ALT_INSERT_COLUMNS} FROM override_matches
        UNION ALL
        SELECT {_ALT_INSERT_COLUMNS} FROM phrase_matched_rows
        UNION ALL
        SELECT {_ALT_INSERT_COLUMNS} FROM debt_carveout_matched_rows
        UNION ALL
        SELECT {_ALT_INSERT_COLUMNS} FROM brand_matched_rows
        UNION ALL
        SELECT {_ALT_INSERT_COLUMNS} FROM sponsor_matched_rows
    """
    insert_sql = f"INSERT INTO {glue_db}.{target_table} ({_ALT_INSERT_COLUMNS}) {combined_sql}"
    stmts = [
        f"DELETE FROM {glue_db}.{target_table} WHERE ack_id IN ({ids})",
        insert_sql,
    ]
    for sql in stmts:
        qid = wr.athena.start_query_execution(sql=sql, database=glue_db, workgroup=wg, s3_output=s3)
        wr.athena.wait_query(query_execution_id=qid)
    logger.info("Routed alternatives rows %s -> %s for %d acks", staging_table, target_table, len(ack_ids))


# ---------------------------------------------------------------------------
# Brand/manager-name matching -- fallback match strategy inside
# _route_alternatives_from_staging's brand_matches CTE, complementary to
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
    # Added 2026-09-24 from user's sourced 300-row candidate research (ADV/Form D/
    # 5500 filings) -- see project_dcio_alternatives_router memory. "ara"/"ipi" kept
    # scoped to multi-word phrases rather than bare terms: "ARA Core Property"
    # (American Realty Advisors) and "ARA Fund II LP" (Ara Partners) are two
    # unrelated managers that would otherwise collide on the same 3-letter substring.
    ("american core realty", "Real Estate Fund", "Real Estate"),
    ("ara core property", "Real Estate Fund", "Real Estate"),
    ("camden bonds plus", "Private Credit Fund", "Private Credit"),
    ("copperwood", "Private Equity Fund", "Private Equity"),
    ("crake", "Hedge Fund", "Hedge Fund"),
    ("davidson kempner", "Hedge Fund", "Hedge Fund"),
    ("falcon credit", "Private Credit Fund", "Private Credit"),
    ("farallon", "Hedge Fund", "Hedge Fund"),
    ("ipi data center", "Infrastructure Fund", "Infrastructure"),
    ("ironwood", "Private Credit Fund", "Private Credit"),
    ("madison core property", "Real Estate Fund", "Real Estate"),
    ("redwood opportunity", "Hedge Fund", "Hedge Fund"),
    ("saracen energy", "Private Equity Fund", "Private Equity"),
    ("spf securitized products", "Private Credit Fund", "Private Credit"),
    ("strategic portfolios", "Hedge Fund", "Hedge Fund"),
    ("weatherlow", "Hedge Fund", "Hedge Fund"),
    # Added 2026-09-24 (round 2) from the same sourced 300-row candidate
    # research -- 117 of 127 open_gap rows confirmed high-confidence
    # alternatives via independent web research (not just taxonomy text) on
    # every boundary case. Held back as data-insert-only, NOT added here:
    # "arrowstreet" (Alpha Extension is a 130/30-style equity extension, not
    # an alternative -- see plan_alternatives_history round-2 notes),
    # "crestline"/"viking global"/"cerberus" (each spans multiple asset_sub_
    # class categories across their own confirmed rows -- Cerberus alone hits
    # real estate, two different credit strategies, and PE -- so no single
    # fixed category is safe to force onto future rows).
    ("entrustpermal", "Hedge Fund", "Hedge Fund"),
    ("kirkoswald", "Hedge Fund", "Hedge Fund"),
    ("bridgewater", "Hedge Fund", "Hedge Fund"),  # overridden below
    ("horsley bridge", "Private Equity Fund", "Private Equity"),
    ("digitalbridge", "Infrastructure Fund", "Infrastructure"),
    ("towerbrook", "Private Equity Fund", "Private Equity"),
    ("pretium", "Real Estate Fund", "Real Estate"),
    ("balyasny", "Hedge Fund", "Hedge Fund"),
    ("primavera capital", "Private Equity Fund", "Private Equity"),
    # "washington capital reef" ordered ahead of the broader "washington
    # capital" term below -- CASE picks the first matching WHEN, and the REEF
    # product line is Real Estate while the manager's other product defaults
    # to Private Credit (confirmed via 2026-09-24 research; see
    # project_dcio_alternatives_router memory).
    ("washington capital reef", "Real Estate Fund", "Real Estate"),
    ("washington capital", "Private Credit Fund", "Private Credit"),
    ("grosvenor wilmore", "Hedge Fund", "Hedge Fund"),
    ("foxhaven", "Hedge Fund", "Hedge Fund"),
    ("whitebox", "Hedge Fund", "Hedge Fund"),
    ("systematica", "Hedge Fund", "Hedge Fund"),
    ("alphadyne", "Hedge Fund", "Hedge Fund"),
    ("brevan howard", "Hedge Fund", "Hedge Fund"),
    ("francisco partners", "Private Equity Fund", "Private Equity"),
    ("vitruvian", "Private Equity Fund", "Private Equity"),
    ("goldentree", "Private Credit Fund", "Private Credit"),
    ("encap", "Private Equity Fund", "Private Equity"),
    ("ta realty", "Real Estate Fund", "Real Estate"),
    ("boyd watterson", "Real Estate Fund", "Real Estate"),
    ("trident capital", "Private Equity Fund", "Private Equity"),
    ("waud capital", "Private Equity Fund", "Private Equity"),
    ("patriot financial", "Private Equity Fund", "Private Equity"),
    ("boyu capital", "Private Equity Fund", "Private Equity"),
    ("hellman", "Private Equity Fund", "Private Equity"),
    ("sycamore partners", "Private Equity Fund", "Private Equity"),
    ("gtcr", "Private Equity Fund", "Private Equity"),
    ("cendana", "Private Equity Fund", "Private Equity"),
    # Added 2026-09-24 (round 3) from sponsor-name research on 50 "unclear"
    # generic-entity-name groups (see project_dcio_alternatives_router memory).
    # Specific terms ordered ahead of any broader/colliding term below so the
    # first-match-wins CASE picks the more specific category first.
    ("golden tree", "Private Credit Fund", "Private Credit"),  # space variant of "goldentree"
    ("peak rock capital credit", "Private Credit Fund", "Private Credit"),  # ordered before "peak rock capital"
    ("peak rock capital", "Private Equity Fund", "Private Equity"),
    ("tcw direct lending", "Private Credit Fund", "Private Credit"),  # scoped narrow: TCW Group overall
    # is multi-strategy (confirmed via 2026-09-24 research), so no bare "tcw" term is added.
    # Added 2026-09-30: Brookfield and CBRE are both multi-strategy managers (infra/real
    # estate/PE/credit for Brookfield; private RE/infra/RE credit/listed real assets for
    # CBRE), so neither gets a bare brand-wide term -- see project_dcio_alternatives_router
    # memory for the reconciled dollar-bucket research behind each scoped term below.
    # No bare "brookfield" term: its own parent stock/bonds and public-listed affiliates
    # (Brookfield Infrastructure Partners/BIP, Brookfield Renewable, etc.) would collide.
    ("brookfield capital partners", "Private Equity Fund", "Private Equity"),
    ("brookfield cap ptnrs", "Private Equity Fund", "Private Equity"),
    ("brookfield special investments", "Private Equity Fund", "Private Equity"),  # Brookfield's own
    # annual report places this strategy under Private Equity.
    ("brookfield strategic re", "Real Estate Fund", "Real Estate"),
    ("brookfield infra fund", "Infrastructure Fund", "Infrastructure"),  # narrower than "brookfield
    # infra" alone, which would also match the publicly-listed Brookfield Infrastructure Partners/BIP.
    # No bare "cbre" term: CBRE Group Inc (NYSE: CBRE) public stock and CBRE Services Inc public
    # bonds dominate the raw data (~97% of observed CBRE-brand dollars) and would swamp a bare term.
    ("cbre us logistics partners", "Real Estate Fund", "Real Estate"),
    ("cbre gip", "Infrastructure Fund", "Infrastructure"),  # GIP = CBRE IM's Global Investment
    # Partners infrastructure feeder vehicles.
    ("cbre strategic ptr", "Real Estate Fund", "Real Estate"),  # Strategic Partners US Opportunity
    ("cbre strategic partners", "Real Estate Fund", "Real Estate"),  # fund series; both the observed
    # "Ptr" abbreviation and the spelled-out form are covered.
    # Added 2026-09-30: discovery-search shortlist of single-strategy PE managers
    # (see project_dcio_alternatives_router memory for the distinct-name/dollar
    # reconciliation behind each term). Coller is the one multi-strategy name in
    # this batch (credit vs. PE secondaries) -- "coller credit" is listed first
    # so the more specific credit term wins before the broader PE term below.
    ("adams street partnership", "Private Equity Fund", "Private Equity"),
    ("adams street co-investment", "Private Equity Fund", "Private Equity"),
    ("advent international", "Private Equity Fund", "Private Equity"),
    ("cinven", "Private Equity Fund", "Private Equity"),
    ("welsh carson", "Private Equity Fund", "Private Equity"),
    ("wcas", "Private Equity Fund", "Private Equity"),
    ("american securities partners", "Private Equity Fund", "Private Equity"),
    ("green equity investors", "Private Equity Fund", "Private Equity"),  # Leonard Green's fund brand
    ("coller credit", "Private Credit Fund", "Private Credit"),
    ("coller capital", "Private Equity Fund", "Private Equity"),  # secondaries feeder funds
]

# Per-term extra restriction, ANDed onto that term's match only. Both entries
# were confirmed false-positive-prone during 2026-09-20 verification: bare
# "intercontinental" mostly matches Intercontinental Exchange Inc (ICE) stock/
# bonds and InterContinental Hotels; bare "onex" coincidentally substring-
# matches Euronext, StoneX, Socionext.
ALT_BRAND_TERM_OVERRIDES: Dict[str, str] = {
    "intercontinental": "regexp_like(lower(raw_entity_name), 'reif|real estate')",
    "onex": "strpos(lower(raw_entity_name), 'onex partners') > 0",
    # Bridgewater's All Weather product is officially packaged today as a
    # multi-asset allocation strategy, not an alternative (confirmed via web
    # research 2026-09-24) -- 5 All Weather share classes were held back from
    # the round-2 promotion for this reason. This keeps the "bridgewater" term
    # valid for legitimate future matches (Pure Alpha etc.) while permanently
    # blocking any future All Weather row from auto-routing the same way.
    "bridgewater": "NOT regexp_like(lower(raw_entity_name), 'all weather')",
    # New 2026-10-07 bare "cbre" ALT_MANAGER_ONLY_TERMS entry (see
    # ALT_MANAGER_OVERRIDES) -- excludes "CBRE Group Real estate investment
    # trust" rows, which look like direct holdings of publicly-traded CBRE
    # Group Inc (NYSE: CBRE) stock rather than a CBRE-managed fund.
    "cbre": "NOT regexp_like(lower(raw_entity_name), 'cbre\\s+group')",
    # "Neuberger Berman" retail mutual fund / CIT share classes (Real Estate
    # R6, Mid Cap Growth, Genesis, Large Cap Value, Strategic MultiSector
    # Fixed Income Trust, etc.) legitimately contain "Fund"/"Trust" so they
    # pass the structural-marker gate, and get mistagged with alt-sounding
    # asset_type values (real estate, separate account, GIC) by the source
    # filer, so ALT_EXCLUDED_ASSET_TYPES never catches them either. Requiring
    # a genuine alt-fund keyword instead of a blocklist keeps this correct as
    # new retail products appear. Confirmed via 2026-09-25 row-level
    # verification: 101 rows/$372.3M -> 21 rows/$176.5M, zero genuine
    # Crossroads/Secondary Opportunities/Private Debt/CLO rows lost.
    # "putwrite" added same day: "NB US Equity Index Putwrite Fund LLC"
    # (asset_type "Private Fund - LLC") is a genuine private fund that
    # matched none of the other keywords and would otherwise silently drop
    # out of routing on any future filing of the same fund under a new
    # ack_id (2026-09-25 row-level verification).
    "neuberger berman": r"regexp_like(lower(raw_entity_name), 'crossroads|secondary\s+opp|private\s+debt|\bclo\b|loan\s+advisers|putwrite')",
    # "ullico" and "white oak global advisors" both false-positived on rows
    # like "PORTFOLIO 14 ULLICO INVESTMENT ADVISORS AB0662242 Dreyfus
    # Government Cash Management Short Term Investment Fund" -- a portfolio/
    # account header naming the manager gets concatenated onto a generic
    # Dreyfus cash-sweep MMF line item during extraction, and the brand term
    # matches the header even though the actual instrument is not a fund of
    # that manager's. Found + 6 rows manually deleted from
    # plan_alternatives_history 2026-09-25; this closes the underlying match
    # so it can't recur on a future filing of the same or a similar sweep
    # vehicle. Scoped to the confirmed generic-MMF signature (mirrors the
    # neuberger berman entry above) rather than a blanket rule, since other
    # brand terms haven't been confirmed to hit this same failure mode yet.
    # "class a stock" branch added 2026-09-26 after a full-universe dry run
    # of every ALT_BRAND_PATTERNS term found "Private Equity Fund: Ullico
    # Class A Stock" ($2,199,329) -- the "Private Equity Fund:" label
    # prefixed onto a bare stock-holding line supplies the "Fund" structural
    # marker, same concatenation failure mode, different shape.
    "ullico": r"NOT regexp_like(lower(raw_entity_name), 'dreyfus|government cash management|short term investment fund|class\s+a.*stock')",
    "white oak global advisors": r"NOT regexp_like(lower(raw_entity_name), 'dreyfus|government cash management|short term investment fund')",
    # "Ares Management" false-positived the same way as Ullico/White Oak
    # above, but on the manager's OWN parent-company stock/bond rows rather
    # than a cash sweep: e.g. "Ares Management Corp Cl A" or "Ares
    # Management LP Common Stock" have no fund vehicle at all, yet the bare
    # brand name plus a trailing "Corp"/"LP"/"Inc" satisfies the structural
    # marker gate. This had previously been patched 4 times as one-off
    # (ack_id, raw_entity_name) exclusions -- "ares management corp cl a"
    # (ack_id 20250813090708NAL0008939265001), "ares management lp common
    # stock" (20251009140213NAL0003632563001), "ares management lp"
    # (20251008131116NAL0009463904001), and "ares management corp"
    # (20251010151350NAL0008379297001) -- which only ever protected those
    # exact filings. This entry generalizes that to a term-level guard so it
    # also covers future filings under new ack_ids. Validated 2026-09-26
    # against the full plan_holdings_staging universe of "ares management"
    # rows: catches all 4 rows above plus a new gap row ("Ares Management LP
    # Common Stock", $2,825,399, ack_id 20251009140213NAL0003632563001)
    # found by the same dry run, while leaving both confirmed-legitimate
    # Ares fund names (Landmark Real Estate Partners VII LP, Senior Direct
    # Lending Fund Cayman III LP) and 2 genuinely ambiguous rows ("Ares
    # Management ARES EUROPEAN REAL ESTATE", "Ares Management NA")
    # untouched. The old per-ack_id exclusion list was removed 2026-09-26
    # as redundant with this term-level guard.
    "ares management": r"NOT regexp_like(lower(raw_entity_name), '^ares management(\s+(corp|lp|inc))?\.?(\s+(common\s+stock|cl\.?\s*a))?\.?$')",
    # "Perella Weinberg Partners" (PWP) is a publicly-traded parent company
    # (formed via de-SPAC, hence its own literal "Class A common stock")
    # whose legal name happens to contain "partners", satisfying the
    # structural marker gate on its own stock -- no fund vehicle involved.
    # An initial narrow fix (block bare name + "common/preferred stock" only)
    # missed a wide tail of the same underlying holding recorded with
    # inconsistent suffixes across filings/extractions: bare "Perella
    # Weinberg Partners", "...CL A", "...Equity", and numeric/CUSIP-prefixed
    # variants ("71367G102 Perella Weinberg Partners", "Perella Weinberg
    # Partners 7801/3563/4500/6310", "5882 Perella Weinberg Partners Class
    # A"), none tagged with a trusted asset_type. Switched 2026-09-26 to an
    # allowlist requiring "fund" appear in the name instead, which is what
    # every genuine PWP-managed vehicle in the data has ("...ABV OPPTY
    # FUNDII/FUNDIII", "Other Private Equity Fund PERELLA WEINBERG PARTNERS
    # ABV OPPTY OFFSHORE FD II B") and none of the bare-stock rows do.
    # Mirrors the neuberger berman allowlist entry above for the same reason
    # (brand term collides with the parent company's own plain-English name).
    "perella weinberg partners": r"regexp_like(lower(raw_entity_name), 'fund')",
    # "Partners Group Holding AG" (and its ticker-free/abbreviated forms
    # "Partners Group Holding" and "Partners Group HLG") is the
    # publicly-traded parent; same shape as PWP above -- "partners" in its
    # legal name satisfies the structural marker gate on its own
    # stock/equity/bond, no fund vehicle involved. Broadened 2026-09-26
    # (full-universe dry run, then a second pass over that same dry run's own
    # "still matching" output) beyond the originally-found "...Common Stock
    # CHF.01"/"...Publiclytraded stock" rows (~$8.2M) after finding: the bare
    # parent name untagged ("PARTNERS GROUP HOLDING AG" alone, asset_type
    # "common stock"); "...Equity"/a "bond" line for "PARTNERS GROUP HOLDING"
    # (no "AG"); and "PARTNERS GROUP HLG CHF0.01 (REGD)" (same CHF-par-value
    # signature as the Holding AG rows, "HLG" abbreviating "Holding"). Also
    # blocks two unrelated companies that substring-match "partners group"
    # but aren't the Partners Group PE firm at all: "FleetPartners Group
    # Ltd" (asset_type "corporate stock - common") and "Financial Partners
    # Group Co Ltd" (seen twice, tagged "NPV" and "Publiclytraded stock" --
    # both public-stock signatures, no fund characteristics anywhere in the
    # data). None of the genuine Partners Group fund products (Private
    # Equity Master Fund, Private Credit Strategy, Real Estate Secondary,
    # Client Access, Global Infrastructure, Kingdom LP, etc.) contain
    # "holding"/"hlg"/"fleetpartners"/"financial partners group", so this is
    # safe to key on rather than the narrower "...stock"-suffix-only shape.
    "partners group": r"NOT regexp_like(lower(raw_entity_name), '^(\S+\s+)?partners group (holding|hlg)\b|fleetpartners group|financial partners group')",
}

# Terms that need a word-boundary match rather than plain substring -- "gso"
# is a substring of unrelated names ("Kingsoft", "GSODLN" swap tickers,
# "GSOF"-named LLCs unrelated to GSO Capital Partners). Found via the
# 2026-09-21 backfill verification (3 confirmed false positives: a HK-listed
# stock and an interest rate swap had been routed in as GSO Capital Partners
# private credit); fixed here so it can't recur on newly processed PDFs.
ALT_BRAND_WORD_BOUNDARY_TERMS = {"gso"}

# Same word-boundary protection, extended 2026-10-06 for ALT_MANAGER_ONLY_TERMS
# bare terms short/common enough to collide mid-word: bare "ares" plain-substring
# matched "Vanguard Real Estate Index Fund Admiral SHARES" (29 rows, ~$14M) in
# verification -- "shares" contains "ares" with no word boundary around it under
# plain strpos. "ifm" and "aqr" are defensively boundary-matched too since
# they're 3-char bare terms, even though no live collision was found for them.
ALT_MANAGER_ONLY_WORD_BOUNDARY_TERMS = {"ares", "ifm", "aqr", "kkr", "dfa", "hcp"} | {
    # Added 2026-10-07 from the facets_full_values.txt 551-rule candidate
    # audit (manager-matching rules for matched_manager_name IS NULL rows).
    # Every term below was verified against live plan_alternatives_history
    # data before being added; word-boundary matching is applied broadly to
    # all of them (not just a hand-picked short-term subset) so the SQL
    # implementation faithfully mirrors the source file's own
    # '(?:^| )TERM(?= |$)'-anchored regex design and never drifts looser
    # than what was actually verified.
    "asb", "allegiance real estate", "vanguard", "dws", "prin real estate", "spdr",
    "t rowe price", "t iaa", "teachers insurance", "cref real estate",
    "college retirement equities", "tcp direct lending", "vpc asset",
    "credit suisse real estate", "bpea strategic healthcare", "ifmglobalinfrastructureuslp",
    "mfs global real estate", "mfs vitt", "duff phlp", "duff phil", "centersquare",
    "centersquarreal", "centersquared", "ironsides", "private equity core fund",
    "private advisors", "private adv small co", "pa small company private equity",
    "peg global private", "iif erisa", "us real estate investment fund",
    "u s real estate investment fund", "us real estate invt fund",
    "us real estate invest fund", "baron", "john hancock real estate", "jh real estate",
    "jhlic usa", "hancock us real estate", "john hancock variable insurance",
    "john hancock funds ii real estate", "john hancock usa real estate",
    "john hancock infrastructure", "john hancockinfrastructure", "tenzing",
    "allegience real estate", "mesa west", "iron point", "kps special", "kps", "hig", "h i g",
    "dune real estate", "heitman", "angelo gordon", "alinda", "westbrook real estate",
    "westport capital", "walton street", "walton st", "sequoia capital", "sequoia cap",
    "sequoia us", "sequoia use", "sequoia global", "schroder", "schroders", "sculptor",
    "sculptore", "sculpter", "tiger infrastructure", "stoltz", "rmwc",
    "orion european real estate", "mason wells", "icg", "first eagle direct lending",
    "fort washington", "gtis", "corten real estate", "capital international private equity",
    "capital intl private equity", "centerbridge", "cerberus", "cerebrus", "advent intl",
    "advent latin american", "andreessen horowitz", "aew", "angel oak", "artemis real estate",
    "wheelock street", "waterland private equity", "waterland private equityfund", "wp global",
    "vortus", "unicorn partners", "zouk", "us venture partners", "white oak", "windjammer",
    "xenon private equity", "yorktown energy", "urban american real estate", "valinor cap",
    "waterfall victoria", "unigestion", "union capital", "turn river",
    "turnbridge real estate", "verdane", "weathergage", "veritas capital", "versant venture",
    "versus capital", "tpg real estate partners", "venrock", "arena short duration",
    "two sigma", "volition capital", "volition vpension", "wynnchurch", "yucaipa",
    "sterling group partners", "tailwater", "strategic value ss", "sdc diof",
    "think investment", "think investments", "think invt", "seer cre", "sei private",
    "sei gpa", "sei cit", "scout energy", "seidler equity", "sandbrook", "sango capital",
    "sango pe", "tiff private equity", "senator global", "sentinel real estate",
    "ta associates", "silver creek", "silver creekspecial", "silver hill", "silver oak",
    "silverpeak", "tiger pacific", "singerman real estate", "town lane real estate",
    "sk capital", "skybridge", "slate canada", "slate canadian", "sona credit", "soroban",
    "southern cross latin america", "varde", "ti platform", "teng yue", "scale ventures",
    "torchlight debt", "stellus private credit", "stepstone", "rialto real estate",
    "accomplice fortuity", "adams street", "rockland power", "proa buyout",
    "riverrock european", "rockwood capital", "3i europartner", "prospect venture",
    "regents gate", "quantum energy", "quantum parallel", "ridgewood energy",
    "riverside strategic capital", "raith real estate", "raven rpm", "rcp direct", "rcp fund",
    "rcp xii", "rcp xiii", "rcp xiv", "rcp xv", "rcp xvi", "sun mountain private credit",
    "related real estate", "renaissance venture capital", "ridgemount equity",
    "bridge workforce", "riverstone global energy", "cross ocean", "boundary creek",
    "inflexion", "patria infrastructure", "patria private equity", "patria brazilian",
    "patriabrazilian", "portfolio advisors", "palatine real estate", "matrix capital",
    "monarch capital partners", "moorfield", "mountgrange", "msd real estate",
    "multiples private equity", "park st cap", "pemberton strategic", "permal private equity",
    "pinebridge", "locust point", "nch agribusiness", "new mountain private credit",
    "park presidio", "novaquest", "northwood real estate", "penn square global",
    "oakley capital", "pitango venture", "primavera spring", "ocp asia", "onyxpoint",
    "opera smallcap", "libremax", "lubertadler", "icon infrastructure", "idg china venture",
    "imm rosegold", "indaba capital", "innovatus", "insight venture", "kinterra", "instaragf",
    "highvista", "junto offshore", "jadian real estate", "hosen private equity", "hines",
    "hirtle callaghan", "lindsell train", "lombard odier", "jlc infrastructure", "jmi equity",
    "luxor capital", "m esirow", "m onroe capital", "madison international real estate",
    "mainsail", "madison realty", "entrust cap", "entrust capital", "eqt infrastructure",
    "grain infrastructure", "greenbriar equity", "grosvenor infrastructure",
    "grosvenor institutional", "grosvenor real estate", "gcmgrosvenor", "gcm strat invest",
    "grosvenor private credit", "harbert", "ember infrastructure", "capman nordic",
    "fengate infrastructure", "golub capital", "fir tree real estate", "firebolt ventures",
    "five points small buyout", "hbk", "fortress credit", "fortress lending",
    "fortress real estate opportunities", "foundry venture", "francisco beyondtrust",
    "freeman spogli", "eig energy", "graham absolute return", "great hill equity",
    "garda fixed income", "one river", "harrison st project", "hidden harbor", "gauge capital",
    "anacap", "cim infrastructure", "bdcm offshore", "broad peak", "blue torch",
    "darwin private equity", "alcion real estate", "consonance private equity",
    "panco strategic", "camber capital", "dsf multi family", "dsf multifamily",
    "berkeley partners", "capitala private credit", "capitalworks private equity",
    "clearlake opportunities", "climate adaptive infrastructure", "carmel partners",
    "castlelake", "coller secondaries", "commonfund", "contrarian distressed", "crestline",
    "cat rock", "cyrus opportunities", "blackchamber", "charles river partners",
    "chicago pacific founders", "dunedin buyout", "alpstone", "400 capital",
    "36 south kohinoor", "apax partners", "776 fund", "776 arete", "abbott capital",
    "angeles private credit", "abs direct equity", "abs opp", "abs directional",
    "aetos capital", "aua private equity", "asia alternatives", "bain capital",
    "balance point", "balance legal", "agellus", "agilitas", "axium", "bailard",
    "viking global", "american realty", "vgslx", "1vgslx", "massachusetts financial services",
    "duff phelps", "vrts dp", "janus henderson", "dw s r real estate", "deutsche real estate",
    "deutsche funds deutsche real estate", "deutsche bank real estate", "ishares",
    "blrk global", "blrk", "blackrck", "br real estate", "princial real estate",
    "pri real estate", "pgi us real estate", "pgi cit us real estate", "df a",
    "northern global", "northern gbl", "northern glob", "northern trust investments",
    "nt collective global", "mfc flexshares", "flexshares", "state street",
    "real estate select sctr spdr", "russell global", "schwab fundamental", "third avenue",
    "1tarzx", "amcen", "amercent", "amer cent", "columbia real estate", "columbia creyx",
    "coheen steers", "davis real estate", "fid real estate", "franklin real estate",
    "franklin templeton", "lazard", "global x", "wellington real estate", "westwood",
    "pacer benchmark", "bny mellon developed", "mellon eb us real estate",
    "bank of new york mellon eb us", "impax global", "clarion real estate", "clarion global",
    "clarion glbl", "clarion partners", "lion industrial", "alliancebern",
    "ab global real estate", "delaware real estate", "delaware vip real estate",
    "neurberer berman", "nueberger ber", "meeder miller", "manning napier", "aon", "mercer",
    "merer erisa", "willis towers watson", "towers watson", "wacap", "wa cap",
    "washington cap", "gateway real estate", "isq global infrastructure", "north haven",
    "breit", "goldman sachs", "jp morgan", "jpmorgan", "jp m organ", "jpm", "jpg peg", "jpmcb",
    "dra growth", "antin infrastructure", "barings core property", "greystar real estate",
    "stockbridge", "townsend real estate", "north bridge venture", "north bridge vntre",
    "rho ventures", "transpose platform", "sun capital partners", "brookwood real estate",
    "graycliff", "m d sass", "highbridge", "lcn core income", "lexington cap",
    "lexington private equity", "glenmede private equity", "crow holdings",
    "bluerock total income", "altantic creek", "panda power generation",
    "normandy real estate", "infrared active real estate", "europa secondary", "first sentier",
    "elliot int", "ubs trumbull", "ubs archmore", "churchill middle market",
    "churchhill middle market", "glouston", "ag direct lending", "wcp real estate",
    "wcp special core", "global infrastructure ptnrs", "global infrastructure prt", "valic",
    "variable annuity life insurance", "corebridge", "empower real estate",
    "greatwest real estate", "mywayretirement", "mywayret", "mywayrtmt", "myway retirement",
    "voya private credit", "voya senior loan", "sentry life insurance",
    "virtus global real estate", "virtus opportunities trust real estate",
    "nvesco real estate", "sigular guff", "star america infrastructure", "whi real estate",
    "ara fund ii"
} | {
    # added 2026-10-07 from facets_full_values_2.txt manager-matching update
    # (80 rules reviewed: 78 agreed [17 revised + 61 added] implemented here,
    # 2 disagreed [R449 bare "bpif" -- superseded by more precise bpif
    # nontaxable/non taxable entries below; R600 bare "perella weinberg" --
    # would reopen the de-SPAC'd-stock false positive the "fund"-qualified
    # ALT_BRAND_TERM_OVERRIDES entry already guards against] -- each
    # independently re-verified against live plan_alternatives_history
    # matched_manager_name IS NULL rows before inclusion).
    "sjc onshore direct lending", "dover street", "prime property fund", "klcp",
    "tci real estate", "strategic partners offshore real estate", "bcp infrastructure",
    "blue own digital infrastructure", "berkshire fund", "metropolitan real estate",
    "sweetwater private equity", "trident viii", "pramerica real estate",
    "prisa real estate", "duration transportation infrastructure", "wng aircraft",
    "gi data infrastructure", "asf vii", "1788 paumanok fund a", "k3 private investors",
    "ac carbon cayman", "ae real estate partnership", "blg turkish real estate",
    "italian real estate special situations ii", "wilton private equity fund",
    "pgi global real estate", "drc european real estate", "drc euro real estate",
    "select manager fund iii", "aacp japan buyout", "hsi real estate", "hsrep vi",
    "oak street real estate", "crown global secondaries", "benson elliot real estate",
    "bhdg systematic", "bsof parallel offshore", "bluemountain montenvers",
    "bpif nontaxable", "bpif non taxable", "amp capital global infrastructure",
    "tpg real estate ptns", "park street capital", "fid intl real estate",
    "first t rust senior loan", "vangaurd real estate", "real estate index fund admiral",
    "real estate index admiral", "real estate idx admiral", "real estate index admr",
    "real estate idx adm", "real estate admiral", "american strategic value realty",
    "graduate hotels real estate", "pw real estate fund iii",
    "high street real estate fund vi", "ara europe active real estate",
    "gaticule managed fund", "oakhurst real estate fund", "abpci direct lending",
    "bluebay direct lending", "baring india private equity", "dlj real estate",
    "geam international private equity", "mellon venture capital",
    "radcliffe spac opportunities", "radcliffe private equity", "icapital infrastructure",
    "guidestone global real estate", "parametric defensive equity",
    "northern trust government short term", "nvit real estate",
    "nationwide nvit real estate", "ecofin global reninfrastructure", "recurrent mlp",
    "centre globalinfrastructure", "catalyst mlp and infrastructure", "c&h steers",
}

# Sponsor-name fallback pass (added 2026-09-24, round 3): raw_sponsor_name
# carries the real manager/fund name for rows where raw_entity_name is a
# generic placeholder ("Partnership/joint venture interests", "N/A Limited
# Partnerships", "Limited Liability Company") -- confirmed against 786 rows
# across 50 such placeholder groups (see project_dcio_alternatives_router
# memory). Reuses ALT_BRAND_PATTERNS/_alt_manager_display_name against raw_sponsor_
# name instead, but a sponsor field just as often names a custodian bank or
# traditional (non-alternative) manager rather than an alt brand, so those
# must be excluded here even though they'd never appear in ALT_BRAND_PATTERNS
# in the first place -- this is a different failure mode (false "this row IS
# an alt, just under the wrong category" vs. false "this row is an alt at
# all"). List sourced from this round's manual candidate-extraction exclusion
# filter, verified against real data before promoting the 141-row round-3
# batch.
ALT_SPONSOR_EXCLUDE_REGEX = (
    r"dodge|pimco|blackrock|jp\s*morgan|fidelity|brandywine|"
    r"boston trust walden|bny mellon|amalgamated bank|dimensional fund advisors|"
    r"john hancock|franklin|putnam|invesco|lord abbett|american funds|"
    r"eaton vance|federated|brown brothers harriman|alliance bernstein|"
    r"tiaa|calvert|amana|aon hewitt|aon enhanced|guaranteed investment contract"
)


def _alt_brand_term_cond(term: str, column: str = "raw_entity_name") -> str:
    """Shared condition-builder for one ALT_BRAND_PATTERNS term (consumed by
    _alt_manager_case_sql/_alt_manager_display_name): word-boundary regex for
    terms in ALT_BRAND_WORD_BOUNDARY_TERMS,
    plain substring otherwise, ANDed with ALT_BRAND_TERM_OVERRIDES when
    present. Centralizing this keeps asset_type/asset_class/classification_
    method/matched_manager_name from ever drifting out of sync on which rows
    a given brand term matches.

    `column` defaults to raw_entity_name (the original, higher-trust match
    target) but accepts raw_sponsor_name too, for the sponsor-name fallback
    pass added 2026-09-24 -- ALT_BRAND_TERM_OVERRIDES conditions themselves
    still reference raw_entity_name literally since every existing override
    was written/verified against that column; the sponsor-name pass doesn't
    use per-term overrides today, but this keeps the override behavior
    unchanged if it ever does."""
    term_sql = term.replace("'", "''")
    if term in ALT_BRAND_WORD_BOUNDARY_TERMS or term in ALT_MANAGER_ONLY_WORD_BOUNDARY_TERMS:
        cond = f"regexp_like(lower(trim({column})), '\\b{term_sql}\\b')"
    else:
        cond = f"strpos(lower(trim({column})), '{term_sql}') > 0"
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
    r"capital\s+partners|feeder|offshore|reif|limited\s+partnership)\b"
)
# "limited partnership" added 2026-09-30: the abbreviated "l.p."/"partners" forms above don't
# match a name that spells it out in full (e.g. "Blue Owl GP Stakes Pension Investors IV Limited
# Partnership" -- $2.75M confirmed missed for exactly this reason; see
# project_dcio_alternatives_router memory).
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


# ---------------------------------------------------------------------------
# Manager debt carve-out (added 2026-09-25). ALT_BRAND_PATTERNS can only map
# a brand term to ONE static (asset_type, asset_class) pair, so it can't
# express what row-level research proved true for these managers: the same
# brand name spans confirmed public debt (bonds/notes) and equity (common
# stock) holdings under one name, each needing different routing, and for
# Starwood specifically an unrelated same-prefix brand (Starwood Hotels &
# Resorts, now part of Marriott) has to be excluded explicitly. This pass
# runs before brand_matches so a specific per-manager debt/equity signal
# always wins over the generic brand bucket -- see _route_alternatives_
# from_staging's docstring for where it sits in the overall pass order.
#
# Built from row-level-verified ground truth, not from trusting asset_type or
# a regex blindly: Blue Owl's signals are copied from classify_blueowl.py
# (validated against a 247-row candidate pool over 6 iterations of fixes,
# used to route the manually-confirmed 123-row/$85.3M debt batch inserted
# 2026-09-25). Starwood's are newly derived from starwood_debt_confirmed.csv
# (89 rows) and cross-checked against known equity/private-fund/unrelated-
# brand examples from the same session's research. See scratchpad/
# validate_debt_carveout_patterns.py for the regression harness: 0
# mismatches against Blue Owl's full 247-row ground truth; 87/89 against
# Starwood's confirmed-debt set, with the 2 remaining rows (bare misspelled
# names, zero vocabulary or asset_type signal anywhere) correctly left
# unrouted for manual review rather than force-matched -- that's the
# deliberately conservative behavior, not a gap to close.
#
# Deliberately does NOT handle the private-fund case for either manager:
# Blue Owl's private-fund LP batch ($81.5M, 11 rows) is a separate, still-
# undecided item, and Starwood's one known private-fund row ("Starwood REIT
# CL I LP", $919) is left unrouted rather than guessing at a category for a
# single low-dollar row with no broader authorization. Both fall through to
# the existing brand_matches/sponsor_matches passes unchanged (Blue Owl
# already has a generic "blue owl" -> Private Credit Fund brand entry there;
# Starwood has none, so its one fund row simply stays unrouted, same as
# today).
ALT_MANAGER_DEBT_PATTERNS: Dict[str, Dict] = {
    "blue owl": dict(
        manager_name="Blue Owl Capital",
        exclude_regex=None,
        equity_regex=(
            r"\bcommon\s+stock\b|\bcorporate\s+stock\b|\bshares\b|\bcom\s+cl\s+a\b|"
            r"\bcl\s+a\b\s*$|\bclass\s+a\b|\bcom\s+ci\s+a\b|\bcorp\.?\s+ord\b|"
            r"\bord\b\s*$|\bequity\b|\bcom\b\s*$"
        ),
        debt_strong_regex=(
            r"\d+\.\d+%|\b144a\b|\bpvtpl\b|\bsr\.?\s*(nt|unsecured|notes?)\b|"
            r"\bsenior\s+unsecured\b|\bunsecured\s*global\s+notes?\b|"
            r"\bunsecured\s+notes?\b|\bnotes?\s+semi\s+annual\b|\bmatures?\b|"
            r"\bdue\b|\bdd\s+\d|\bcallable\b|\bfixed\s+income\b|"
            r"\bcorporate\s+(bond|debt)\b|\bbond\b|\bnt\b|\bser\b.*\bfltg\b|"
            r"\bfltg\s+rt\b|\bcompany\s+guar\b|\basset\s+leas\b|"
            r"\d\.\d{2}\s+\d{1,2}/\d{1,2}/\d{2,4}|\bnt\s+\d{3,4}\s+\d{3,4}\b|"
            r"\bn/?a\s+\d{2}/\d{2}/\d{4}\b"
        ),
        debt_weak_numeric_regex=r"^\d{4,}\s|\b\d{3,4}\s+\d{4,8}\b",
        debt_asset_types={
            "bond", "corporate bonds - other", "corp. debt instr. - preferred",
            "corp. debt instr. - all other", "corporate debt instruments",
        },
        instrument_type_rules=[
            (r"\basset\s+leas\b", "ABS"),
            (
                r"\bcompany\s+guar\b|\bsr\.?\s*(nt|unsecured|notes?)\b|"
                r"\bsenior\s+unsecured\b|\bsenior\b|144a|pvtpl|\bcallable\b",
                "Senior Notes",
            ),
        ],
        instrument_type_default="Corporate Bond",
    ),
    "starwood": dict(
        manager_name="Starwood Capital Group",
        exclude_regex=r"starwood\s+hotels",
        equity_regex=r"\bcom\b|\bcommon\s+stock\b|\bcorporate\s+stock\b\s*$",
        debt_strong_regex=(
            r"\d+\.\d+%|\b144a\b|\bpvtpl\b|\bsr\.?\s*(nt|unsecured|notes?)\b|"
            r"\bsenior\s+unsecured\b|\bdue\b|\bmatures?\b|\bcallable\b|"
            r"\bfltg\s+rt\b|\bcorporate\s+(bond|obligation|debt)\b|"
            r"\bcorp\.?\s+debt\b|\bbond\b|\bnt\b|\bmortgage\b|\bmtg\b|"
            r"\bcommercial\s+mortgage\b"
        ),
        debt_weak_numeric_regex=r"^\d{4,}\s|\b\d{3,4}\s+\d{4,8}\b",
        debt_asset_types={
            "bond", "corporate bonds - other", "corp. debt instr. - preferred",
            "corp. debt instr. - all other", "corporate debt instruments",
        },
        instrument_type_rules=[
            (r"commercial\s+mortgage|retail\s+ppty|retail\s+property", "CMBS"),
            (r"mortgage\s+residential|mortgage\s+re\b", "RMBS"),
            (r"\bsr\.?\s*(nt|unsecured|notes?)\b|\bsenior\b|144a|pvtpl", "Senior Notes"),
        ],
        instrument_type_default="Corporate Bond",
    ),
}


def _alt_debt_carveout_qualifies_sql(term: str, spec: Dict) -> str:
    """Boolean SQL expression: does this staging row (aliased `s`) belong to
    `term`'s manager debt carve-out? Gated on the manager name appearing in
    raw_entity_name, not an excluded unrelated same-prefix brand, not a row
    whose asset_type belongs to a different router entirely (mutual fund /
    CIT / commingled fund -- ALT_EXCLUDED_ASSET_TYPES) or a known bare
    public-security type (ALT_NOISE_ASSET_TYPES), not an equity-signal name,
    and carrying at least one debt signal -- a trusted asset_type, the
    strong vocabulary in the name/sponsor/asset_type (the last one catches
    data-quality artifacts like a coupon "4.750%" landing in the asset_type
    column instead of a real type tag, found verifying Starwood's ground
    truth), or -- name only, since sponsor free text carries unrelated
    reference numbers -- the weaker bare-digit-pair pattern.

    The asset_type exclusion was added after the generalized-survey pass
    (2026-09-25) found it missing here: unlike the brand-match pass, this
    gate had no guard against ordinary retail bond mutual funds/CITs whose
    name happens to contain both a manager brand term and the bare word
    "bond" (e.g. "Neuberger Berman Core Bond Fund"). The already-shipped
    Blue Owl/Starwood carveout rows happened to come out clean -- their
    debt_strong_regex vocabulary (144A, coupon %, "sr unsecured", etc.)
    didn't collide with retail fund names in practice -- but the gap was
    real and would have misfired on a manager with a blunter regex."""
    term_sql = term.replace("'", "''")
    gate = f"strpos(lower(trim(s.raw_entity_name)), '{term_sql}') > 0"
    if spec.get("exclude_regex"):
        gate += f" AND NOT regexp_like(lower(s.raw_entity_name), '{spec['exclude_regex']}')"
    non_alt_types = ", ".join(
        "'" + t.replace("'", "''") + "'" for t in sorted(ALT_EXCLUDED_ASSET_TYPES | ALT_NOISE_ASSET_TYPES)
    )
    not_other_router = f"lower(trim(s.asset_type)) NOT IN ({non_alt_types})"
    equity = f"regexp_like(lower(s.raw_entity_name), '{spec['equity_regex']}')"
    debt_types = ", ".join("'" + t + "'" for t in sorted(spec["debt_asset_types"]))
    is_debt = (
        f"(lower(trim(s.asset_type)) IN ({debt_types})"
        f" OR regexp_like(lower(s.raw_entity_name), '{spec['debt_strong_regex']}')"
        f" OR regexp_like(lower(s.raw_sponsor_name), '{spec['debt_strong_regex']}')"
        f" OR regexp_like(lower(s.asset_type), '{spec['debt_strong_regex']}')"
        f" OR regexp_like(lower(s.raw_entity_name), '{spec['debt_weak_numeric_regex']}'))"
    )
    return f"({gate} AND {not_other_router} AND NOT {equity} AND {is_debt})"


def _alt_debt_carveout_subclass_sql() -> str:
    """asset_sub_class CASE: 'Manager Debt Exposure' for any row qualifying
    under any manager in ALT_MANAGER_DEBT_PATTERNS, else NULL (filtered out
    by the wrapping *_matched_rows CTE, same pattern as brand_matches)."""
    lines = ["CASE"]
    for term, spec in ALT_MANAGER_DEBT_PATTERNS.items():
        lines.append(f"        WHEN {_alt_debt_carveout_qualifies_sql(term, spec)} THEN 'Manager Debt Exposure'")
    lines.append("        ELSE NULL END")
    return "\n".join(lines)


def _alt_debt_carveout_instrument_type_sql() -> str:
    """asset_type (instrument type) CASE, e.g. 'Senior Notes'/'CMBS'/'ABS',
    per manager's instrument_type_rules with instrument_type_default as the
    per-manager fallback."""
    lines = ["CASE"]
    for term, spec in ALT_MANAGER_DEBT_PATTERNS.items():
        qualifies = _alt_debt_carveout_qualifies_sql(term, spec)
        for pattern, label in spec["instrument_type_rules"]:
            pat_sql = pattern.replace("'", "''")
            lines.append(
                f"        WHEN {qualifies} AND regexp_like(lower(s.raw_entity_name), '{pat_sql}') THEN '{label}'"
            )
        default = spec["instrument_type_default"].replace("'", "''")
        lines.append(f"        WHEN {qualifies} THEN '{default}'")
    lines.append("        ELSE NULL END")
    return "\n".join(lines)


def _alt_debt_carveout_manager_sql() -> str:
    """matched_manager_name CASE for ALT_MANAGER_DEBT_PATTERNS."""
    lines = ["CASE"]
    for term, spec in ALT_MANAGER_DEBT_PATTERNS.items():
        name = spec["manager_name"].replace("'", "''")
        lines.append(f"        WHEN {_alt_debt_carveout_qualifies_sql(term, spec)} THEN '{name}'")
    lines.append("        ELSE NULL END")
    return "\n".join(lines)


def _alt_debt_carveout_method_sql() -> str:
    """classification_method CASE for ALT_MANAGER_DEBT_PATTERNS."""
    lines = ["CASE"]
    for term, spec in ALT_MANAGER_DEBT_PATTERNS.items():
        suffix = term.replace(" ", "_")
        lines.append(f"        WHEN {_alt_debt_carveout_qualifies_sql(term, spec)} THEN 'debt_carveout_v1:{suffix}'")
    lines.append("        ELSE NULL END")
    return "\n".join(lines)


def _alt_brand_case_sql(value_index: int, column: str = "raw_entity_name") -> str:
    """Build a CASE expression picking ALT_BRAND_PATTERNS[*][value_index]
    (1=asset_type, 2=asset_class) for the first brand term found in
    `column`, honoring ALT_BRAND_TERM_OVERRIDES. Mirrors _alt_case_sql()
    above so the two stay easy to compare/audit side by side."""
    lines = ["CASE"]
    for term, asset_type, asset_class in ALT_BRAND_PATTERNS:
        cond = _alt_brand_term_cond(term, column)
        val = (asset_type if value_index == 1 else asset_class).replace("'", "''")
        lines.append(f"        WHEN {cond} THEN '{val}'")
    lines.append("        ELSE NULL END")
    return "\n".join(lines)


def _alt_brand_method_case_sql(column: str = "raw_entity_name", method_prefix: str = "manual:brand_match:") -> str:
    """Build the classification_method CASE for ALT_BRAND_PATTERNS. Kept
    separate from _alt_brand_case_sql since the method string is derived
    from the term itself (not a stored column), unlike asset_type/asset_class.
    `method_prefix` lets the sponsor-name fallback pass tag its rows
    distinctly (manual:sponsor_brand_match:*) from entity-name brand matches."""
    lines = ["CASE"]
    for term, _asset_type, _asset_class in ALT_BRAND_PATTERNS:
        cond = _alt_brand_term_cond(term, column)
        suffix = (
            term.replace(" ", "_").replace(",", "").replace("&", "and")
                .replace(".", "").replace("'", "")
        )
        lines.append(f"        WHEN {cond} THEN '{method_prefix}{suffix}'")
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
#
# Display-string resolution order per term, aligned with CIT's canon.json
# dictionary (ai-cit Glue job, core-data-platform) so the same real manager
# doesn't get two different spellings depending on which table/pipeline
# routed the row (e.g. HarbourVest Partners showing as "HARBOURVEST" in CIT
# but "Harbourvest Partners" in ALT prior to this alignment):
#   1. ALT_MANAGER_OVERRIDES  -- hand-researched, ALT-specific (fund-brand-
#      to-true-manager resolutions that canon.json has no reason to carry,
#      e.g. "madison core property" -> "NYL Investors")
#   2. canon.json (S3, same CANON_BUCKET/CANON_KEY as ai-cit)
#   3. term.title() -- last-resort naive casing
# ---------------------------------------------------------------------------
ALT_MANAGER_OVERRIDES: Dict[str, str] = {
    "gso": "Blackstone Credit",
    "onex": "Onex",
    "landmark": "Landmark Partners",
    "tennenbaum": "BlackRock",
    "owl rock": "Blue Owl Capital",
    "ipi data center": "IPI Partners",
    "spf securitized products": "SPF Investment Management",
    "ara core property": "American Realty Advisors",
    "american core realty": "American Realty Advisors",
    "madison core property": "NYL Investors",
    "davidson kempner": "Davidson Kempner Capital Management",
    "farallon": "Farallon Capital Management",
    "entrustpermal": "EnTrust Global",
    "bridgewater": "Bridgewater Associates",
    "digitalbridge": "DigitalBridge",
    "towerbrook": "TowerBrook Capital Partners",
    "grosvenor wilmore": "GCM Grosvenor",
    "goldentree": "GoldenTree Asset Management",
    "encap": "EnCap Investments",
    "ta realty": "TA Realty",
    "gtcr": "GTCR",
    "hellman": "Hellman & Friedman",
    "golden tree": "GoldenTree Asset Management",
    "washington capital reef": "Washington Capital Management",
    "washington capital": "Washington Capital Management",
    "peak rock capital credit": "Peak Rock Capital",
    "peak rock capital": "Peak Rock Capital",
    "tcw direct lending": "TCW",
    # Added 2026-10-06: manager_canonical_hierarchy round-3 collapse (see
    # project_manager_canonicalization memory) -- these terms previously had
    # no override and fell through to canon.json/.title(), producing raw/
    # inconsistent display names ("Gcm Grosvenor", "Boyd Watterson", "Ullico",
    # "Brookfield Cap Ptnrs", etc.) that the live table's 623-row backfill UPDATE
    # already corrected retroactively. These entries make new rows resolve to
    # the same canonical name directly, so the backfill doesn't have to be
    # re-run after every future load.
    "alcentra": "Benefit Street Partners",
    "basalt": "Basalt Infrastructure Partners",
    "clearlake capital": "Clearlake Capital Group",
    "eagle point": "Eagle Point Credit Management",
    "frazier": "Frazier Healthcare Partners",
    "gcm grosvenor": "GCM Grosvenor",
    "goldpoint partners": "Apogem Capital",
    "hancock natural resource group": "Manulife Investment Management",
    "intercontinental": "Intercontinental Real Estate Corporation",
    # "kayne anderson" (Infrastructure Fund asset_type) targets Kayne Anderson
    # Capital Advisors' energy/infra funds -- a different firm from Kayne
    # Anderson Rudnick Investment Management (Virtus-owned equity SMA manager).
    # canon.json previously resolved this bare term to "Kayne Anderson Rudnick",
    # which looks like a conflation of the two distinct firms; forcing the
    # correct firm here per 2026-10-06 decision (not yet re-verified against
    # the underlying rows -- see project_manager_canonicalization memory).
    "kayne anderson": "Kayne Anderson Capital Advisors",
    # Changed 2026-10-07 (per user request): Oaktree Capital Management has
    # been majority-owned/controlled by Brookfield since 2019. Rolling the
    # subsidiary's narrow phrase term up to the parent's canonical name here,
    # same convention already used for "tennenbaum" -> "BlackRock". Paired
    # with the new bare "oaktree" entry below so ALL Oaktree rows (not just
    # ones literally containing "capital") resolve to one consistent name.
    "oaktree capital": "Brookfield Asset Management",
    "ullico": "Ullico Investment Advisors",
    "windjammer capital": "Windjammer Capital Investors",
    "copperwood": "Copperwood Asset Management",
    "ironwood": "Ironwood Capital Management",
    "horsley bridge": "Horsley Bridge Partners",
    "balyasny": "Balyasny Asset Management",
    "primavera capital": "Primavera Capital Group",
    "whitebox": "Whitebox Advisors",
    "systematica": "Systematica Investments",
    "alphadyne": "AlphaDyne Asset Management",
    "vitruvian": "Vitruvian Partners",
    "boyd watterson": "Boyd Watterson Asset Management",
    "waud capital": "Waud Capital Partners",
    "patriot financial": "Patriot Financial Partners",
    "cendana": "Cendana Capital",
    "american securities partners": "American Securities",
    "green equity investors": "Leonard Green & Partners",
    "coller credit": "Coller Capital",
    "forest investment advisors": "Forest Investment Associates",
    "brookfield capital partners": "Brookfield Asset Management",
    "brookfield cap ptnrs": "Brookfield Asset Management",
    "brookfield special investments": "Brookfield Asset Management",
    "brookfield strategic re": "Brookfield Asset Management",
    "brookfield infra fund": "Brookfield Asset Management",
    "cbre us logistics partners": "CBRE Investment Management",
    "cbre gip": "CBRE Investment Management",
    "cbre strategic ptr": "CBRE Investment Management",
    "cbre strategic partners": "CBRE Investment Management",
    "adams street partnership": "Adams Street Partners",
    "adams street co-investment": "Adams Street Partners",
    "blackrock": "BlackRock",
    # Brand-router default (term.title()) previously diverged from the
    # debt-carveout manager_name for these two terms (ALT_MANAGER_DEBT_
    # PATTERNS above uses "Blue Owl Capital"/"Starwood Capital Group"),
    # producing two matched_manager_name spellings for the same firm
    # depending on which pass classified a given row. Confirmed via
    # 2026-09-27 web + raw-data research that both spellings refer to one
    # firm in each case; aligning here so future rows agree regardless of
    # which pass routes them. See project_dcio_alternatives_router memory.
    "blue owl": "Blue Owl Capital",
    "starwood": "Starwood Capital Group",
    # Added 2026-10-06: these four have full-phrase terms in ALT_BRAND_PATTERNS
    # already ("ares management", "corbin capital", "crescent capital"), but the
    # 78.6%-of-rows ALT ledger missing-manager-record audit found most real rows
    # for these managers don't include that qualifier word (e.g. "Ares Multi-
    # Credit Fund LLC", "Corbin ERISA Opportunity Fund LP", "Crescent Direct
    # Lending"). Bare terms below (ALT_MANAGER_ONLY_TERMS) catch those for
    # matched_manager_name only -- deliberately NOT added to ALT_BRAND_PATTERNS
    # itself so the asset-class router keeps using the existing, narrower,
    # already-verified phrase for asset_type/asset_class assignment. canon.json
    # has no bare "ARES"/"CORBIN"/"CRESCENT"/"IFM" token (by design, to avoid
    # collisions in CIT/MF's broader matching), so these resolve here instead.
    "ares": "Ares Management",
    "corbin": "Corbin Capital Partners",
    "crescent": "Crescent Capital Group",
    "ifm": "IFM Investors",
    # Added 2026-10-06: no-match-bucket review (see project_manager_
    # canonicalization memory) -- $206.9M/23 rows of real KKR fund names
    # (KKR Diversified Core Infrastructure, KKR Global Infrastructure IV,
    # KKR Asia Real Estate Partners, etc.) had zero override entry and
    # zero matched_manager_name. "kkr" is bare/3-char so word-boundary
    # protected below, same defensive treatment as "ifm"/"aqr".
    "kkr": "KKR",
    # Alexandria Real Estate Equities (NYSE: ARE) is a publicly-traded REIT
    # held directly as a stock/bond security in these plans, not a fund
    # sponsor -- $149.6M/259 rows, all unmatched. Mapped to itself since
    # there's no external "manager" relationship to resolve to; term is the
    # two-word phrase (not bare "alexandria") to stay unambiguous.
    "alexandria real estate": "Alexandria Real Estate Equities",
    # Added 2026-10-06: Cohen & Steers Real Estate Securities / Global
    # Infrastructure funds had zero override entry -- 238 rows/$81.4M,
    # zero matched_manager_name. Bare "cohen" is safe as a plain substring
    # (no word-boundary needed): verified against live raw_entity_name data,
    # the only two non-"cohen steers"/"cohen & steers" spellings that contain
    # "cohen" are "Cohen & Steer Real Estate" and "COHEN STRS REAL ESTATE SEC"
    # -- both still Cohen & Steers, just further misspellings. No collisions.
    "cohen": "Cohen & Steers",
    # Added 2026-10-06: DFA / Dimensional Fund Advisors Real Estate Securities
    # funds had zero override entry -- 173 rows, zero matched_manager_name.
    # Two terms needed: some variants say only "DFA" (e.g. "DFA Real Estate
    # Secs Port Ins"), others say only "Dimensional..." with no "DFA" at all
    # (e.g. "Dimensional US Real Estate ETF"). "dfa" is bare/3-char so
    # word-boundary protected below, same as kkr/ifm/aqr/ares. "dimensional"
    # verified as a safe plain substring -- no non-Dimensional-Fund-Advisors
    # collisions found in live data.
    "dfa": "Dimensional Fund Advisors",
    "dimensional": "Dimensional Fund Advisors",
    # Added 2026-10-06: DWS RREEF Real Estate Securities funds had zero
    # override entry -- 59 rows, zero matched_manager_name. "rreef" verified
    # as a safe plain substring (distinctive brand term, no collisions).
    "rreef": "DWS RREEF",
    # Added 2026-10-06: Fidelity Real Estate / Advisor Real Estate / VIP Real
    # Estate / Infrastructure funds had zero override entry -- 95 rows, zero
    # matched_manager_name. Bare "fidelity" verified as a safe plain substring
    # against live data: checked specifically for unrelated "Fidelity
    # National"/"Fidelity Bond"/"Fidelity & (and) Guaranty" entities and found
    # zero such rows in plan_alternatives_history -- every "fidelity" row is a
    # genuine Fidelity Investments fund.
    "fidelity": "Fidelity Investments",
    # Added 2026-10-06: Global Infrastructure Partners (GIP) had zero override
    # entry -- 24 rows, zero matched_manager_name. Bare "global infrastructure"
    # was rejected as unsafe: it collides with IFM ("IFM Global
    # Infrastructure..."), KKR ("KKR GLOBAL INFRASTRUCTURE IV"), Cohen & Steers
    # ("Cohen Steers Global Infrastructure Fund..."), ISQ ("ISQ Global
    # Infrastructure Fund..."), and DWS ("DWS Global Infrastructure Fund...").
    # The narrower "global infrastructure part" substring (catches "PARTNERS"
    # and the abbreviated "PART IVAB"/"PARTIV" spellings) was verified against
    # live data to have zero collisions with any of the above managers.
    "global infrastructure part": "Global Infrastructure Partners",
    # Added 2026-10-07 per user request, verified against live data (batch3/
    # batch4 checks): each term below had either zero override entry or a
    # too-narrow existing term, with genuine unmatched rows in
    # plan_alternatives_history and no false-positive collisions found.
    #
    # Nuveen: canon.json already resolves bare "tiaa" -> "Nuveen" (confirmed
    # live), so TIAA needed no fix. This bare "nuveen" term catches the
    # separate "Nuveen Real Estate..." rows that don't also say "TIAA".
    "nuveen": "Nuveen",
    "american century": "American Century Investments",
    # User said "apollo global" -- using the firm's actual formal name.
    "apollo": "Apollo Global Management",
    # C&S must be listed/checked before "fidelity" in ALT_MANAGER_ONLY_TERMS
    # (CASE order = list order) -- fixes a bug from the "fidelity" backfill
    # above, which plain-substring-matched 2 rows that are actually Cohen &
    # Steers funds nested inside Fidelity BrokerageLink/Fidelity Management
    # Trust Co platform wrappers ("Fidelity Brokeragelink C&S Real Estate Z",
    # "Fidelity Management Trust Company C & S Real Estate A").
    "c&s": "Cohen & Steers",
    "c & s": "Cohen & Steers",
    # Narrower genuine-gap terms added first, same pattern used for "global
    # infrastructure part", before the bare term below existed.
    "brookfield premier re": "Brookfield Asset Management",
    "brookfield us premier real estate": "Brookfield Asset Management",
    "brookfield real estate solutions": "Brookfield Asset Management",
    "brookfield reit": "Brookfield Asset Management",
    # Bare "brookfield" (added 2026-10-07, per explicit user request after
    # reviewing the live rows it catches): collides with publicly-traded
    # Brookfield Infrastructure Partners/Corp (BIP/BIPC) stock/bond/preferred
    # holdings -- confirmed live (13 rows, ~$8.6M) -- which get tagged
    # "Brookfield Asset Management" as matched_manager_name even though
    # they're direct public-security holdings, not a plan-menu fund managed
    # by Brookfield. Deliberately kept OUT of ALT_BRAND_PATTERNS (same as
    # "cbre"/"pimco"/"principal" above) so this never forces an asset_type/
    # asset_class guess onto a new staging row -- manager-name-only risk,
    # same reasoning as the Crestline/Viking Global/Cerberus exclusion.
    "brookfield": "Brookfield Asset Management",
    # Carlyle's actual formal name is "The Carlyle Group", not "Carlyle
    # Global" (no such entity) -- using the correct name; all live rows are
    # genuine Carlyle fund variants, no collisions found.
    "carlyle": "Carlyle Group",
    # User asked for literal "CBRE to CBRE" (not "CBRE Investment
    # Management") -- honoring that. The 4 narrow cbre-* terms above already
    # take priority (checked first, in ALT_BRAND_PATTERNS) so those specific
    # institutional funds keep the fuller "CBRE Investment Management" name;
    # this bare term only catches everything else (retail CBRE-branded
    # mutual fund share classes). Excludes "cbre group" rows ("CBRE Group
    # Real estate investment trust") since those look like the publicly-
    # traded CBRE Group Inc (NYSE: CBRE) stock, not a fund relationship.
    "cbre": "CBRE",
    "harbourvest": "HarbourVest Partners",
    # 3-char acronym, word-boundary protected below (see
    # ALT_MANAGER_ONLY_WORD_BOUNDARY_TERMS) same as kkr/dfa/ares/ifm/aqr.
    "hcp": "HCP",
    # Broader than the existing "neuberger berman" ALT_BRAND_PATTERNS term,
    # which is deliberately allowlist-scoped to specific institutional
    # product lines (crossroads/secondary opp/private debt/CLO/putwrite) to
    # keep retail mutual funds out of the asset-class router. This bare term
    # is for matched_manager_name only and is unrestricted, so retail
    # Neuberger Berman Real Estate share classes also get the name. Using
    # bare "neuberger" (not "neuberger berman") since several live variants
    # drop "Berman" entirely (e.g. "Neuberger Real Estate R6") -- no
    # unrelated "Neuberger"-branded manager found in the data.
    "neuberger": "Neuberger Berman",
    # Bare "oaktree" (see "oaktree capital" rename above for rationale) --
    # catches the many Oaktree fund variants that don't say "capital".
    "oaktree": "Brookfield Asset Management",
    # Verified against live data (scratchpad pimco_principal_check.sql):
    # every "pimco"/"principal" row is a genuine PIMCO / Principal Financial
    # Group / Principal Real Estate Investors fund -- no collisions found.
    "pimco": "PIMCO",
    "principal": "Principal",

    # --- 2026-10-07: facets_full_values.txt 551-rule candidate audit ---
    # (matched_manager_name IS NULL backfill; see analysis_551_results.json)
    "asb": "ASB Real Estate Investments",
    "allegiance real estate": "ASB Real Estate Investments",
    "vanguard": "Vanguard",
    "dws": "DWS",
    "prin real estate": "Principal",
    "spdr": "State Street Investment Management",
    "t rowe price": "T. Rowe Price",
    "t iaa": "Nuveen",
    "teachers insurance": "Nuveen",
    "cref real estate": "Nuveen",
    "college retirement equities": "Nuveen",
    "tcp direct lending": "BlackRock",
    "vpc asset": "Victory Park Capital",
    "credit suisse real estate": "Credit Suisse",
    "bpea strategic healthcare": "Baring Private Equity Asia",
    "ifmglobalinfrastructureuslp": "IFM Investors",
    "mfs global real estate": "MFS Investment Management",
    "mfs vitt": "MFS Investment Management",
    "duff phlp": "Duff & Phelps Investment Management",
    "duff phil": "Duff & Phelps Investment Management",
    "centersquare": "CenterSquare Investment Management",
    "centersquarreal": "CenterSquare Investment Management",
    "centersquared": "CenterSquare Investment Management",
    "ironsides": "Constitution Capital Partners",
    "private equity core fund": "50 South Capital",
    "private advisors": "Apogem Capital",
    "private adv small co": "Apogem Capital",
    "pa small company private equity": "Apogem Capital",
    "peg global private": "J.P. Morgan Asset Management",
    "iif erisa": "J.P. Morgan Asset Management",
    "us real estate investment fund": "Intercontinental Real Estate Corporation",
    "u s real estate investment fund": "Intercontinental Real Estate Corporation",
    "us real estate invt fund": "Intercontinental Real Estate Corporation",
    "us real estate invest fund": "Intercontinental Real Estate Corporation",
    "baron": "Baron Capital",
    "john hancock real estate": "John Hancock",
    "jh real estate": "John Hancock",
    "jhlic usa": "John Hancock",
    "hancock us real estate": "John Hancock",
    "john hancock variable insurance": "John Hancock",
    "john hancock funds ii real estate": "John Hancock",
    "john hancock usa real estate": "John Hancock",
    "john hancock infrastructure": "John Hancock",
    "john hancockinfrastructure": "John Hancock",
    "tenzing": "Tenzing",
    "allegience real estate": "ASB Real Estate Investments",
    "mesa west": "Mesa West Capital",
    "iron point": "Iron Point Partners",
    "kps special": "KPS Capital Partners",
    "kps": "KPS Capital Partners",
    "hig": "H.I.G. Capital",
    "h i g": "H.I.G. Capital",
    "dune real estate": "Dune Real Estate Partners",
    "heitman": "Heitman",
    "angelo gordon": "Angelo Gordon",
    "alinda": "Alinda Capital Partners",
    "westbrook real estate": "Westbrook Partners",
    "westport capital": "Westport Capital Partners",
    "walton street": "Walton Street Capital",
    "walton st": "Walton Street Capital",
    "sequoia capital": "Sequoia Capital",
    "sequoia cap": "Sequoia Capital",
    "sequoia us": "Sequoia Capital",
    "sequoia use": "Sequoia Capital",
    "sequoia global": "Sequoia Capital",
    "schroder": "Schroders",
    "schroders": "Schroders",
    "sculptor": "Sculptor Capital Management",
    "sculptore": "Sculptor Capital Management",
    "sculpter": "Sculptor Capital Management",
    "tiger infrastructure": "Tiger Infrastructure Partners",
    "stoltz": "Stoltz Real Estate Partners",
    "rmwc": "RMWC",
    "orion european real estate": "Orion Capital Managers",
    "mason wells": "Mason Wells",
    "icg": "ICG",
    "first eagle direct lending": "First Eagle Investments",
    "fort washington": "Fort Washington Investment Advisors",
    "gtis": "GTIS Partners",
    "corten real estate": "Corten Real Estate",
    "capital international private equity": "Capital Group",
    "capital intl private equity": "Capital Group",
    "centerbridge": "Centerbridge Partners",
    "cerberus": "Cerberus Capital Management",
    "cerebrus": "Cerberus Capital Management",
    "advent intl": "Advent International",
    "advent latin american": "Advent International",
    "andreessen horowitz": "Andreessen Horowitz",
    "aew": "AEW",
    "angel oak": "Angel Oak Capital Advisors",
    "artemis real estate": "Artemis Real Estate Partners",
    "wheelock street": "Wheelock Street Capital",
    "waterland private equity": "Waterland Private Equity",
    "waterland private equityfund": "Waterland Private Equity",
    "wp global": "WP Global Partners",
    "vortus": "Vortus Investments",
    "unicorn partners": "Unicorn Partners",
    "zouk": "Zouk Capital",
    "us venture partners": "U.S. Venture Partners",
    "white oak": "White Oak Global Advisors",
    "windjammer": "Windjammer Capital Investors",
    "xenon private equity": "Xenon Private Equity",
    "yorktown energy": "Yorktown Partners",
    "urban american real estate": "Urban American",
    "valinor cap": "Valinor Management",
    "waterfall victoria": "Waterfall Asset Management",
    "unigestion": "Unigestion",
    "union capital": "Union Capital",
    "turn river": "Turn River Capital",
    "turnbridge real estate": "Turnbridge Equities",
    "verdane": "Verdane",
    "weathergage": "Weathergage Capital",
    "veritas capital": "Veritas Capital",
    "versant venture": "Versant Ventures",
    "versus capital": "Versus Capital",
    "tpg real estate partners": "TPG",
    "venrock": "Venrock",
    "arena short duration": "Arena Investors",
    "two sigma": "Two Sigma",
    "volition capital": "Volition Capital",
    "volition vpension": "Volition Capital",
    "wynnchurch": "Wynnchurch Capital",
    "yucaipa": "Yucaipa Companies",
    "sterling group partners": "The Sterling Group",
    "tailwater": "Tailwater Capital",
    "strategic value ss": "Strategic Value Partners",
    "sdc diof": "SDC Capital Partners",
    "think investment": "Think Investments",
    "think investments": "Think Investments",
    "think invt": "Think Investments",
    "seer cre": "SEER Capital Management",
    "sei private": "SEI",
    "sei gpa": "SEI",
    "sei cit": "SEI",
    "scout energy": "Scout Energy Partners",
    "seidler equity": "Seidler Equity Partners",
    "sandbrook": "Sandbrook Capital",
    "sango capital": "Sango Capital",
    "sango pe": "Sango Capital",
    "tiff private equity": "TIFF Investment Management",
    "senator global": "Senator Investment Group",
    "sentinel real estate": "Sentinel Real Estate Corporation",
    "ta associates": "TA Associates",
    "silver creek": "Silver Creek Capital Management",
    "silver creekspecial": "Silver Creek Capital Management",
    "silver hill": "Silver Hill Energy Partners",
    "silver oak": "Silver Oak Services Partners",
    "silverpeak": "Silverpeak",
    "tiger pacific": "Tiger Pacific Capital",
    "singerman real estate": "Singerman Real Estate",
    "town lane real estate": "Town Lane",
    "sk capital": "SK Capital Partners",
    "skybridge": "SkyBridge Capital",
    "slate canada": "Slate Asset Management",
    "slate canadian": "Slate Asset Management",
    "sona credit": "Sona Asset Management",
    "soroban": "Soroban Capital Partners",
    "southern cross latin america": "Southern Cross Group",
    "varde": "Varde Partners",
    "ti platform": "TI Platform",
    "teng yue": "Teng Yue Partners",
    "scale ventures": "Scale Venture Partners",
    "torchlight debt": "Torchlight Investors",
    "stellus private credit": "Stellus Capital Management",
    "stepstone": "StepStone Group",
    "rialto real estate": "Rialto Capital",
    "accomplice fortuity": "Accomplice",
    "adams street": "Adams Street Partners",
    "rockland power": "Rockland Capital",
    "proa buyout": "PROA Capital",
    "riverrock european": "RiverRock",
    "rockwood capital": "Rockwood Capital",
    "3i europartner": "3i",
    "prospect venture": "Prospect Venture Partners",
    "regents gate": "Regents Gate Capital",
    "quantum energy": "Quantum Capital Group",
    "quantum parallel": "Quantum Capital Group",
    "ridgewood energy": "Ridgewood Energy",
    "riverside strategic capital": "The Riverside Company",
    "raith real estate": "Raith Capital Partners",
    "raven rpm": "Raven Capital Management",
    "rcp direct": "RCP Advisors",
    "rcp fund": "RCP Advisors",
    "rcp xii": "RCP Advisors",
    "rcp xiii": "RCP Advisors",
    "rcp xiv": "RCP Advisors",
    "rcp xv": "RCP Advisors",
    "rcp xvi": "RCP Advisors",
    "sun mountain private credit": "Sun Mountain Capital",
    "related real estate": "Related Fund Management",
    "renaissance venture capital": "Renaissance Venture Capital",
    "ridgemount equity": "Ridgemont Equity Partners",
    "bridge workforce": "Bridge Investment Group",
    "riverstone global energy": "Riverstone Holdings",
    "cross ocean": "Cross Ocean Partners",
    "boundary creek": "Boundary Creek Advisors",
    "inflexion": "Inflexion",
    "patria infrastructure": "Patria Investments",
    "patria private equity": "Patria Investments",
    "patria brazilian": "Patria Investments",
    "patriabrazilian": "Patria Investments",
    "portfolio advisors": "Portfolio Advisors",
    "palatine real estate": "Palatine Private Equity",
    "matrix capital": "Matrix Capital Management",
    "monarch capital partners": "Monarch Alternative Capital",
    "moorfield": "Moorfield Group",
    "mountgrange": "Mountgrange",
    "msd real estate": "MSD Partners",
    "multiples private equity": "Multiples Alternate Asset Management",
    "park st cap": "Park Street Capital",
    "pemberton strategic": "Pemberton Asset Management",
    "permal private equity": "Permal",
    "pinebridge": "PineBridge Investments",
    "locust point": "Locust Point Capital",
    "nch agribusiness": "NCH Capital",
    "new mountain private credit": "New Mountain Capital",
    "park presidio": "Park Presidio Capital",
    "novaquest": "NovaQuest Capital Management",
    "northwood real estate": "Northwood Investors",
    "penn square global": "Penn Square Real Estate Group",
    "oakley capital": "Oakley Capital",
    "pitango venture": "Pitango",
    "primavera spring": "Primavera Capital Group",
    "ocp asia": "OCP Asia",
    "onyxpoint": "OnyxPoint Global Management",
    "opera smallcap": "Opera",
    "libremax": "LibreMax Capital",
    "lubertadler": "Lubert-Adler Partners",
    "icon infrastructure": "icon Infrastructure",
    "idg china venture": "IDG Capital",
    "imm rosegold": "IMM Private Equity",
    "indaba capital": "Indaba Capital Management",
    "innovatus": "Innovatus Capital Partners",
    "insight venture": "Insight Partners",
    "kinterra": "Kinterra Capital",
    "instaragf": "Instar",
    "highvista": "HighVista Strategies",
    "junto offshore": "Junto Capital Management",
    "jadian real estate": "Jadian Capital",
    "hosen private equity": "Hosen Capital",
    "hines": "Hines",
    "hirtle callaghan": "Hirtle Callaghan",
    "lindsell train": "Lindsell Train",
    "lombard odier": "Lombard Odier",
    "jlc infrastructure": "JLC Infrastructure",
    "jmi equity": "JMI Equity",
    "luxor capital": "Luxor Capital Group",
    "m esirow": "Mesirow",
    "m onroe capital": "Monroe Capital",
    "madison international real estate": "Madison International Realty",
    "mainsail": "Mainsail Partners",
    "madison realty": "Madison Realty Capital",
    "entrust cap": "EnTrust Global",
    "entrust capital": "EnTrust Global",
    "eqt infrastructure": "EQT",
    "grain infrastructure": "Grain Management",
    "greenbriar equity": "Greenbriar Equity Group",
    "grosvenor infrastructure": "GCM Grosvenor",
    "grosvenor institutional": "GCM Grosvenor",
    "grosvenor real estate": "GCM Grosvenor",
    "gcmgrosvenor": "GCM Grosvenor",
    "gcm strat invest": "GCM Grosvenor",
    "grosvenor private credit": "GCM Grosvenor",
    "harbert": "Harbert Management Corporation",
    "ember infrastructure": "Ember Infrastructure",
    "capman nordic": "CapMan",
    "fengate infrastructure": "Fengate Asset Management",
    "golub capital": "Golub Capital",
    "fir tree real estate": "Fir Tree Partners",
    "firebolt ventures": "Firebolt Ventures",
    "five points small buyout": "Five Points Capital",
    "hbk": "HBK Capital Management",
    "fortress credit": "Fortress Investment Group",
    "fortress lending": "Fortress Investment Group",
    "fortress real estate opportunities": "Fortress Investment Group",
    "foundry venture": "Foundry Group",
    "francisco beyondtrust": "Francisco Partners",
    "freeman spogli": "Freeman Spogli",
    "eig energy": "EIG",
    "graham absolute return": "Graham Capital Management",
    "great hill equity": "Great Hill Partners",
    "garda fixed income": "Garda Capital Partners",
    "one river": "One River Asset Management",
    "harrison st project": "Harrison Street",
    "hidden harbor": "Hidden Harbor Capital Partners",
    "gauge capital": "Gauge Capital",
    "anacap": "AnaCap",
    "cim infrastructure": "CIM Group",
    "bdcm offshore": "Black Diamond Capital Management",
    "broad peak": "Broad Peak Investment Advisers",
    "blue torch": "Blue Torch Capital",
    "darwin private equity": "Darwin Private Equity",
    "alcion real estate": "Alcion Ventures",
    "consonance private equity": "Consonance Capital Partners",
    "panco strategic": "Panco Management",
    "camber capital": "Camber Capital Management",
    "dsf multi family": "DSF Group",
    "dsf multifamily": "DSF Group",
    "berkeley partners": "Berkeley Partners",
    "capitala private credit": "Capitala Group",
    "capitalworks private equity": "Capitalworks",
    "clearlake opportunities": "Clearlake Capital",
    "climate adaptive infrastructure": "Climate Adaptive Infrastructure",
    "carmel partners": "Carmel Partners",
    "castlelake": "Castlelake",
    "coller secondaries": "Coller Capital",
    "commonfund": "Commonfund",
    "contrarian distressed": "Contrarian Capital Management",
    "crestline": "Crestline Investors",
    "cat rock": "Cat Rock Capital Management",
    "cyrus opportunities": "Cyrus Capital Partners",
    "blackchamber": "BlackChamber Group",
    "charles river partners": "Charles River Ventures",
    "chicago pacific founders": "Chicago Pacific Founders",
    "dunedin buyout": "Dunedin",
    "alpstone": "Alpstone Capital",
    "400 capital": "400 Capital Management",
    "36 south kohinoor": "36 South Capital Advisors",
    "apax partners": "Apax Partners",
    "776 fund": "Seven Seven Six",
    "776 arete": "Seven Seven Six",
    "abbott capital": "Abbott Capital Management",
    "angeles private credit": "Angeles Investment Advisors",
    "abs direct equity": "ABS Investment Management",
    "abs opp": "ABS Investment Management",
    "abs directional": "ABS Investment Management",
    "aetos capital": "Aetos Capital",
    "aua private equity": "AUA Private Equity Partners",
    "asia alternatives": "Asia Alternatives",
    "bain capital": "Bain Capital",
    "balance point": "Balance Point Capital",
    "balance legal": "Balance Legal Capital",
    "agellus": "Agellus Capital",
    "agilitas": "Agilitas",
    "axium": "Axium Infrastructure",
    "bailard": "Bailard",
    "viking global": "Viking Global Investors",
    "american realty": "American Realty Advisors",
    "vgslx": "Vanguard",
    "1vgslx": "Vanguard",
    "massachusetts financial services": "MFS Investment Management",
    "duff phelps": "Duff & Phelps Investment Management",
    "vrts dp": "Duff & Phelps Investment Management",
    "janus henderson": "Janus Henderson",
    "dw s r real estate": "DWS",
    "deutsche real estate": "DWS",
    "deutsche funds deutsche real estate": "DWS",
    "deutsche bank real estate": "DWS",
    "ishares": "BlackRock",
    "blrk global": "BlackRock",
    "blrk": "BlackRock",
    "blackrck": "BlackRock",
    "br real estate": "BlackRock",
    "princial real estate": "Principal Asset Management",
    "pri real estate": "Principal Asset Management",
    "pgi us real estate": "Principal Asset Management",
    "pgi cit us real estate": "Principal Asset Management",
    "df a": "Dimensional Fund Advisors",
    "northern global": "Northern Trust Asset Management",
    "northern gbl": "Northern Trust Asset Management",
    "northern glob": "Northern Trust Asset Management",
    "northern trust investments": "Northern Trust Asset Management",
    "nt collective global": "Northern Trust Asset Management",
    "mfc flexshares": "Northern Trust Asset Management",
    "flexshares": "Northern Trust Asset Management",
    "state street": "State Street Investment Management",
    "real estate select sctr spdr": "State Street Investment Management",
    "russell global": "Russell Investments",
    "schwab fundamental": "Charles Schwab Investment Management",
    "third avenue": "Third Avenue Management",
    "1tarzx": "Third Avenue Management",
    "amcen": "American Century Investments",
    "amercent": "American Century Investments",
    "amer cent": "American Century Investments",
    "columbia real estate": "Columbia Threadneedle Investments",
    "columbia creyx": "Columbia Threadneedle Investments",
    "coheen steers": "Cohen & Steers",
    "davis real estate": "Davis Advisors",
    "fid real estate": "Fidelity Investments",
    "franklin real estate": "Franklin Templeton",
    "franklin templeton": "Franklin Templeton",
    "lazard": "Lazard Asset Management",
    "global x": "Global X",
    "wellington real estate": "Wellington Management",
    "westwood": "Westwood Holdings Group",
    "pacer benchmark": "Pacer Advisors",
    "bny mellon developed": "BNY Investments",
    "mellon eb us real estate": "BNY Investments",
    "bank of new york mellon eb us": "BNY Investments",
    "impax global": "Impax Asset Management",
    "clarion real estate": "Clarion Partners",
    "clarion global": "Clarion Partners",
    "clarion glbl": "Clarion Partners",
    "clarion partners": "Clarion Partners",
    "lion industrial": "Clarion Partners",
    "alliancebern": "AllianceBernstein",
    "ab global real estate": "AllianceBernstein",
    "delaware real estate": "Delaware Funds / Macquarie Asset Management",
    "delaware vip real estate": "Delaware Funds / Macquarie Asset Management",
    "neurberer berman": "Neuberger Berman",
    "nueberger ber": "Neuberger Berman",
    "meeder miller": "Meeder Asset Management",
    "manning napier": "Manning & Napier",
    "aon": "Aon",
    "mercer": "Mercer",
    "merer erisa": "Mercer",
    "willis towers watson": "Willis Towers Watson",
    "towers watson": "Willis Towers Watson",
    "wacap": "Washington Capital Management",
    "wa cap": "Washington Capital Management",
    "washington cap": "Washington Capital Management",
    "gateway real estate": "Gaw Capital Partners",
    "isq global infrastructure": "I Squared Capital",
    "north haven": "Morgan Stanley Investment Management",
    "breit": "Blackstone",
    "goldman sachs": "Goldman Sachs Asset Management",
    "jp morgan": "J.P. Morgan Asset Management",
    "jpmorgan": "J.P. Morgan Asset Management",
    "jp m organ": "J.P. Morgan Asset Management",
    "jpm": "J.P. Morgan Asset Management",
    "jpg peg": "J.P. Morgan Asset Management",
    "jpmcb": "J.P. Morgan Asset Management",
    "dra growth": "DRA Advisors",
    "antin infrastructure": "Antin Infrastructure Partners",
    "barings core property": "Barings",
    "greystar real estate": "Greystar",
    "stockbridge": "Stockbridge Capital Group",
    "townsend real estate": "The Townsend Group",
    "north bridge venture": "North Bridge Venture Partners",
    "north bridge vntre": "North Bridge Venture Partners",
    "rho ventures": "Rho Capital Partners",
    "transpose platform": "Transpose Platform",
    "sun capital partners": "Sun Capital Partners",
    "brookwood real estate": "Brookwood Financial Partners",
    "graycliff": "Graycliff Partners",
    "m d sass": "M.D. Sass",
    "highbridge": "Highbridge Capital Management",
    "lcn core income": "LCN Capital Partners",
    "lexington cap": "Lexington Partners",
    "lexington private equity": "Lexington Partners",
    "glenmede private equity": "Glenmede",
    "crow holdings": "Crow Holdings",
    "bluerock total income": "Bluerock",
    "altantic creek": "Atlantic Creek Real Estate Partners",
    "panda power generation": "Panda Power Funds",
    "normandy real estate": "Normandy Real Estate Partners",
    "infrared active real estate": "InfraRed Capital Partners",
    "europa secondary": "Europa Capital",
    "first sentier": "First Sentier Investors",
    "elliot int": "Elliott Investment Management",
    "ubs trumbull": "UBS Asset Management",
    "ubs archmore": "UBS Asset Management",
    "churchill middle market": "Churchill Asset Management",
    "churchhill middle market": "Churchill Asset Management",
    "glouston": "Glouston Capital Partners",
    "ag direct lending": "Angelo Gordon",
    "wcp real estate": "Westport Capital Partners",
    "wcp special core": "Westport Capital Partners",
    "global infrastructure ptnrs": "Global Infrastructure Partners",
    "global infrastructure prt": "Global Infrastructure Partners",
    "valic": "Corebridge Financial",
    "variable annuity life insurance": "Corebridge Financial",
    "corebridge": "Corebridge Financial",
    "empower real estate": "Empower",
    "greatwest real estate": "Empower",
    "mywayretirement": "Voya",
    "mywayret": "Voya",
    "mywayrtmt": "Voya",
    "myway retirement": "Voya",
    "voya private credit": "Voya",
    "voya senior loan": "Voya",
    "sentry life insurance": "Sentry Life Insurance Company",
    "virtus global real estate": "Virtus Investment Partners",
    "virtus opportunities trust real estate": "Virtus Investment Partners",
    "nvesco real estate": "Invesco",
    "sigular guff": "Siguler Guff",
    "star america infrastructure": "Star America Infrastructure Partners",
    "whi real estate": "WHI Real Estate Partners",
    "ara fund ii": "Ara Partners",

    # added 2026-10-07 from facets_full_values_2.txt manager-matching update
    # (78 of 80 reviewed rules agreed and implemented; see the fuller note
    # above ALT_MANAGER_ONLY_WORD_BOUNDARY_TERMS's matching addition).
    # "sweetwater private equity" and "park street capital" are omitted here
    # deliberately -- their term.title() already equals the desired display
    # name, so no override entry is needed for them.
    "sjc onshore direct lending": "Czech Asset Management",
    "dover street": "HarbourVest Partners",
    "prime property fund": "Morgan Stanley Investment Management",
    "klcp": "Kennedy Lewis Investment Management",
    "tci real estate": "TCI Fund Management",
    "strategic partners offshore real estate": "Blackstone",
    "bcp infrastructure": "Bernhard Capital Partners",
    "blue own digital infrastructure": "Blue Owl Capital",
    "berkshire fund": "Berkshire Partners",
    "metropolitan real estate": "Metropolitan Real Estate Equity Management",
    "trident viii": "Stone Point Capital",
    "pramerica real estate": "PGIM Real Estate",
    "prisa real estate": "PGIM Real Estate",
    "duration transportation infrastructure": "Duration Capital Partners",
    "wng aircraft": "WNG Capital",
    "gi data infrastructure": "GI Partners",
    "asf vii": "Ardian",
    "1788 paumanok fund a": "Aksia",
    "k3 private investors": "K1 Investment Management",
    "ac carbon cayman": "Aetos Capital",
    "ae real estate partnership": "A&E Real Estate Holdings",
    "blg turkish real estate": "BLG Capital (Turkey)",
    "italian real estate special situations ii": "GWM Asset Management",
    "wilton private equity fund": "Wilton Asset Management",
    "pgi global real estate": "Principal Asset Management",
    "drc european real estate": "DRC Savills Investment Management",
    "drc euro real estate": "DRC Savills Investment Management",
    "select manager fund iii": "GoldPoint Partners",
    "aacp japan buyout": "Asia Alternatives",
    "hsi real estate": "Hemisferio Sul Investimentos",
    "hsrep vi": "Harrison Street",
    "oak street real estate": "Oak Street Real Estate Capital",
    "crown global secondaries": "LGT Capital Partners",
    "benson elliot real estate": "PineBridge Benson Elliot",
    "bhdg systematic": "BH-DG Systematic Trading",
    "bsof parallel offshore": "Blackstone",
    "bluemountain montenvers": "BlueMountain Capital Management",
    "bpif nontaxable": "Blackstone",
    "bpif non taxable": "Blackstone",
    "amp capital global infrastructure": "AMP Capital",
    "tpg real estate ptns": "TPG",
    "fid intl real estate": "Fidelity Investments",
    "first t rust senior loan": "First Trust Advisors",
    "vangaurd real estate": "Vanguard",
    "real estate index fund admiral": "Vanguard",
    "real estate index admiral": "Vanguard",
    "real estate idx admiral": "Vanguard",
    "real estate index admr": "Vanguard",
    "real estate idx adm": "Vanguard",
    "real estate admiral": "Vanguard",
    "american strategic value realty": "American Realty Advisors",
    "graduate hotels real estate": "AJ Capital Partners",
    "pw real estate fund iii": "Aermont Capital",
    "high street real estate fund vi": "High Street Logistics Properties",
    "ara europe active real estate": "ESR Europe",
    "gaticule managed fund": "Graticule Asia Macro Advisors",
    "oakhurst real estate fund": "Oakhurst Advisors",
    "abpci direct lending": "AllianceBernstein",
    "bluebay direct lending": "BlueBay Asset Management",
    "baring india private equity": "Baring Private Equity Partners India",
    "dlj real estate": "DLJ Real Estate Capital Partners",
    "geam international private equity": "GE Asset Management",
    "mellon venture capital": "Mellon Ventures",
    "radcliffe spac opportunities": "Radcliffe Capital Management",
    "radcliffe private equity": "Radcliffe Capital Management",
    "icapital infrastructure": "iCapital",
    "guidestone global real estate": "GuideStone",
    "parametric defensive equity": "Parametric",
    "northern trust government short term": "Northern Trust Asset Management",
    "nvit real estate": "Nationwide",
    "nationwide nvit real estate": "Nationwide",
    "ecofin global reninfrastructure": "Ecofin",
    "recurrent mlp": "Recurrent Investment Advisors",
    "centre globalinfrastructure": "Centre Asset Management",
    "catalyst mlp and infrastructure": "Catalyst Funds",
    "c&h steers": "Cohen & Steers",
}

# Terms used ONLY for matched_manager_name resolution (_alt_manager_case_sql),
# never fed into the asset-class router's ALT_BRAND_PATTERNS -- added 2026-10-06
# from the ALT ledger missing-manager-record audit (7,152/9,097 rows, 78.6%,
# $53.55B unresolved). Each term below was verified against live
# plan_alternatives_history data (see scratchpad verify_terms queries) to
# confirm no false-positive collisions before being added. Kept out of
# ALT_BRAND_PATTERNS specifically so a bad asset_type/asset_class guess never
# gets forced onto a NEW staging row just because its manager name resolved --
# same reasoning as the existing Crestline/Viking Global/Cerberus exclusion.
ALT_MANAGER_ONLY_TERMS: List[str] = [
    "blackstone",
    "ifm",
    "audax",
    "pathway private equity",
    "tudor bvi",
    "forest investment advisors",
    "lone star",
    "ares",
    "corbin",
    "crescent",
    # ASB/Allegiance MUST precede "blackrock" -- a live collision row
    # ("ASB Allegiance Real Estate Fund-Blackrock Liquid Fds Interest-
    # Bearing Cash") contains both; ASB is the primary fund identity, the
    # BlackRock mention is just the sweep-cash sub-line. Added 2026-10-07.
    "asb",
    "allegiance real estate",
    "blackrock",
    "tiaa",
    "pgim",
    "prudential",
    "morgan stanley",
    "aqr",
    "invesco",
    "kkr",
    "alexandria real estate",
    "cohen",
    "dfa",
    "dimensional",
    "rreef",
    # c&s / c & s MUST precede "fidelity" -- see ALT_MANAGER_OVERRIDES comment.
    "c&s",
    "c & s",
    "fidelity",
    "global infrastructure part",
    "nuveen",
    "american century",
    "apollo",
    "brookfield premier re",
    "brookfield us premier real estate",
    "brookfield real estate solutions",
    "brookfield reit",
    "brookfield",
    "carlyle",
    "cbre",
    "harbourvest",
    "hcp",
    "neuberger",
    "oaktree",
    "pimco",
    "principal",

    # --- 2026-10-07: facets_full_values.txt 551-rule candidate audit ---
    "vanguard",
    "dws",
    "prin real estate",
    "spdr",
    "t rowe price",
    "t iaa",
    "teachers insurance",
    "cref real estate",
    "college retirement equities",
    "tcp direct lending",
    "vpc asset",
    "credit suisse real estate",
    "bpea strategic healthcare",
    "ifmglobalinfrastructureuslp",
    "mfs global real estate",
    "mfs vitt",
    "duff phlp",
    "duff phil",
    "centersquare",
    "centersquarreal",
    "centersquared",
    "ironsides",
    "private equity core fund",
    "private advisors",
    "private adv small co",
    "pa small company private equity",
    "peg global private",
    "iif erisa",
    "us real estate investment fund",
    "u s real estate investment fund",
    "us real estate invt fund",
    "us real estate invest fund",
    "baron",
    "john hancock real estate",
    "jh real estate",
    "jhlic usa",
    "hancock us real estate",
    "john hancock variable insurance",
    "john hancock funds ii real estate",
    "john hancock usa real estate",
    "john hancock infrastructure",
    "john hancockinfrastructure",
    "tenzing",
    "allegience real estate",
    "mesa west",
    "iron point",
    "kps special",
    "kps",
    "hig",
    "h i g",
    "dune real estate",
    "heitman",
    "angelo gordon",
    "alinda",
    "westbrook real estate",
    "westport capital",
    "walton street",
    "walton st",
    "sequoia capital",
    "sequoia cap",
    "sequoia us",
    "sequoia use",
    "sequoia global",
    "schroder",
    "schroders",
    "sculptor",
    "sculptore",
    "sculpter",
    "tiger infrastructure",
    "stoltz",
    "rmwc",
    "orion european real estate",
    "mason wells",
    "icg",
    "first eagle direct lending",
    "fort washington",
    "gtis",
    "corten real estate",
    "capital international private equity",
    "capital intl private equity",
    "centerbridge",
    "cerberus",
    "cerebrus",
    "advent intl",
    "advent latin american",
    "andreessen horowitz",
    "aew",
    "angel oak",
    "artemis real estate",
    "wheelock street",
    "waterland private equity",
    "waterland private equityfund",
    "wp global",
    "vortus",
    "unicorn partners",
    "zouk",
    "us venture partners",
    "white oak",
    "windjammer",
    "xenon private equity",
    "yorktown energy",
    "urban american real estate",
    "valinor cap",
    "waterfall victoria",
    "unigestion",
    "union capital",
    "turn river",
    "turnbridge real estate",
    "verdane",
    "weathergage",
    "veritas capital",
    "versant venture",
    "versus capital",
    "tpg real estate partners",
    "venrock",
    "arena short duration",
    "two sigma",
    "volition capital",
    "volition vpension",
    "wynnchurch",
    "yucaipa",
    "sterling group partners",
    "tailwater",
    "strategic value ss",
    "sdc diof",
    "think investment",
    "think investments",
    "think invt",
    "seer cre",
    "sei private",
    "sei gpa",
    "sei cit",
    "scout energy",
    "seidler equity",
    "sandbrook",
    "sango capital",
    "sango pe",
    "tiff private equity",
    "senator global",
    "sentinel real estate",
    "ta associates",
    "silver creek",
    "silver creekspecial",
    "silver hill",
    "silver oak",
    "silverpeak",
    "tiger pacific",
    "singerman real estate",
    "town lane real estate",
    "sk capital",
    "skybridge",
    "slate canada",
    "slate canadian",
    "sona credit",
    "soroban",
    "southern cross latin america",
    "varde",
    "ti platform",
    "teng yue",
    "scale ventures",
    "torchlight debt",
    "stellus private credit",
    "stepstone",
    "rialto real estate",
    "accomplice fortuity",
    "adams street",
    "rockland power",
    "proa buyout",
    "riverrock european",
    "rockwood capital",
    "3i europartner",
    "prospect venture",
    "regents gate",
    "quantum energy",
    "quantum parallel",
    "ridgewood energy",
    "riverside strategic capital",
    "raith real estate",
    "raven rpm",
    "rcp direct",
    "rcp fund",
    "rcp xii",
    "rcp xiii",
    "rcp xiv",
    "rcp xv",
    "rcp xvi",
    "sun mountain private credit",
    "related real estate",
    "renaissance venture capital",
    "ridgemount equity",
    "bridge workforce",
    "riverstone global energy",
    "cross ocean",
    "boundary creek",
    "inflexion",
    "patria infrastructure",
    "patria private equity",
    "patria brazilian",
    "patriabrazilian",
    "portfolio advisors",
    "palatine real estate",
    "matrix capital",
    "monarch capital partners",
    "moorfield",
    "mountgrange",
    "msd real estate",
    "multiples private equity",
    "park st cap",
    "pemberton strategic",
    "permal private equity",
    "pinebridge",
    "locust point",
    "nch agribusiness",
    "new mountain private credit",
    "park presidio",
    "novaquest",
    "northwood real estate",
    "penn square global",
    "oakley capital",
    "pitango venture",
    "primavera spring",
    "ocp asia",
    "onyxpoint",
    "opera smallcap",
    "libremax",
    "lubertadler",
    "icon infrastructure",
    "idg china venture",
    "imm rosegold",
    "indaba capital",
    "innovatus",
    "insight venture",
    "kinterra",
    "instaragf",
    "highvista",
    "junto offshore",
    "jadian real estate",
    "hosen private equity",
    "hines",
    "hirtle callaghan",
    "lindsell train",
    "lombard odier",
    "jlc infrastructure",
    "jmi equity",
    "luxor capital",
    "m esirow",
    "m onroe capital",
    "madison international real estate",
    "mainsail",
    "madison realty",
    "entrust cap",
    "entrust capital",
    "eqt infrastructure",
    "grain infrastructure",
    "greenbriar equity",
    "grosvenor infrastructure",
    "grosvenor institutional",
    "grosvenor real estate",
    "gcmgrosvenor",
    "gcm strat invest",
    "grosvenor private credit",
    "harbert",
    "ember infrastructure",
    "capman nordic",
    "fengate infrastructure",
    "golub capital",
    "fir tree real estate",
    "firebolt ventures",
    "five points small buyout",
    "hbk",
    "fortress credit",
    "fortress lending",
    "fortress real estate opportunities",
    "foundry venture",
    "francisco beyondtrust",
    "freeman spogli",
    "eig energy",
    "graham absolute return",
    "great hill equity",
    "garda fixed income",
    "one river",
    "harrison st project",
    "hidden harbor",
    "gauge capital",
    "anacap",
    "cim infrastructure",
    "bdcm offshore",
    "broad peak",
    "blue torch",
    "darwin private equity",
    "alcion real estate",
    "consonance private equity",
    "panco strategic",
    "camber capital",
    "dsf multi family",
    "dsf multifamily",
    "berkeley partners",
    "capitala private credit",
    "capitalworks private equity",
    "clearlake opportunities",
    "climate adaptive infrastructure",
    "carmel partners",
    "castlelake",
    "coller secondaries",
    "commonfund",
    "contrarian distressed",
    "crestline",
    "cat rock",
    "cyrus opportunities",
    "blackchamber",
    "charles river partners",
    "chicago pacific founders",
    "dunedin buyout",
    "alpstone",
    "400 capital",
    "36 south kohinoor",
    "apax partners",
    "776 fund",
    "776 arete",
    "abbott capital",
    "angeles private credit",
    "abs direct equity",
    "abs opp",
    "abs directional",
    "aetos capital",
    "aua private equity",
    "asia alternatives",
    "bain capital",
    "balance point",
    "balance legal",
    "agellus",
    "agilitas",
    "axium",
    "bailard",
    "viking global",
    "american realty",
    "vgslx",
    "1vgslx",
    "massachusetts financial services",
    "duff phelps",
    "vrts dp",
    "janus henderson",
    "dw s r real estate",
    "deutsche real estate",
    "deutsche funds deutsche real estate",
    "deutsche bank real estate",
    "ishares",
    "blrk global",
    "blrk",
    "blackrck",
    "br real estate",
    "princial real estate",
    "pri real estate",
    "pgi us real estate",
    "pgi cit us real estate",
    "df a",
    "northern global",
    "northern gbl",
    "northern glob",
    "northern trust investments",
    "nt collective global",
    "mfc flexshares",
    "flexshares",
    "state street",
    "real estate select sctr spdr",
    "russell global",
    "schwab fundamental",
    "third avenue",
    "1tarzx",
    "amcen",
    "amercent",
    "amer cent",
    "columbia real estate",
    "columbia creyx",
    "coheen steers",
    "davis real estate",
    "fid real estate",
    "franklin real estate",
    "franklin templeton",
    "lazard",
    "global x",
    "wellington real estate",
    "westwood",
    "pacer benchmark",
    "bny mellon developed",
    "mellon eb us real estate",
    "bank of new york mellon eb us",
    "impax global",
    "clarion real estate",
    "clarion global",
    "clarion glbl",
    "clarion partners",
    "lion industrial",
    "alliancebern",
    "ab global real estate",
    "delaware real estate",
    "delaware vip real estate",
    "neurberer berman",
    "nueberger ber",
    "meeder miller",
    "manning napier",
    "aon",
    "mercer",
    "merer erisa",
    "willis towers watson",
    "towers watson",
    "wacap",
    "wa cap",
    "washington cap",
    "gateway real estate",
    "isq global infrastructure",
    "north haven",
    "breit",
    "goldman sachs",
    "jp morgan",
    "jpmorgan",
    "jp m organ",
    "jpm",
    "jpg peg",
    "jpmcb",
    "dra growth",
    "antin infrastructure",
    "barings core property",
    "greystar real estate",
    "stockbridge",
    "townsend real estate",
    "north bridge venture",
    "north bridge vntre",
    "rho ventures",
    "transpose platform",
    "sun capital partners",
    "brookwood real estate",
    "graycliff",
    "m d sass",
    "highbridge",
    "lcn core income",
    "lexington cap",
    "lexington private equity",
    "glenmede private equity",
    "crow holdings",
    "bluerock total income",
    "altantic creek",
    "panda power generation",
    "normandy real estate",
    "infrared active real estate",
    "europa secondary",
    "first sentier",
    "elliot int",
    "ubs trumbull",
    "ubs archmore",
    "churchill middle market",
    "churchhill middle market",
    "glouston",
    "ag direct lending",
    "wcp real estate",
    "wcp special core",
    "global infrastructure ptnrs",
    "global infrastructure prt",
    "valic",
    "variable annuity life insurance",
    "corebridge",
    "empower real estate",
    "greatwest real estate",
    "mywayretirement",
    "mywayret",
    "mywayrtmt",
    "myway retirement",
    "voya private credit",
    "voya senior loan",
    "sentry life insurance",
    "virtus global real estate",
    "virtus opportunities trust real estate",
    "nvesco real estate",
    "sigular guff",
    "star america infrastructure",
    "whi real estate",
    "ara fund ii",

    # added 2026-10-07 from facets_full_values_2.txt manager-matching update
    # (78 of 80 reviewed rules agreed and implemented; see the fuller note
    # above ALT_MANAGER_ONLY_WORD_BOUNDARY_TERMS's matching addition)
    "sjc onshore direct lending",
    "dover street",
    "prime property fund",
    "klcp",
    "tci real estate",
    "strategic partners offshore real estate",
    "bcp infrastructure",
    "blue own digital infrastructure",
    "berkshire fund",
    "metropolitan real estate",
    "sweetwater private equity",
    "trident viii",
    "pramerica real estate",
    "prisa real estate",
    "duration transportation infrastructure",
    "wng aircraft",
    "gi data infrastructure",
    "asf vii",
    "1788 paumanok fund a",
    "k3 private investors",
    "ac carbon cayman",
    "ae real estate partnership",
    "blg turkish real estate",
    "italian real estate special situations ii",
    "wilton private equity fund",
    "pgi global real estate",
    "drc european real estate",
    "drc euro real estate",
    "select manager fund iii",
    "aacp japan buyout",
    "hsi real estate",
    "hsrep vi",
    "oak street real estate",
    "crown global secondaries",
    "benson elliot real estate",
    "bhdg systematic",
    "bsof parallel offshore",
    "bluemountain montenvers",
    "bpif nontaxable",
    "bpif non taxable",
    "amp capital global infrastructure",
    "tpg real estate ptns",
    "park street capital",
    "fid intl real estate",
    "first t rust senior loan",
    "vangaurd real estate",
    "real estate index fund admiral",
    "real estate index admiral",
    "real estate idx admiral",
    "real estate index admr",
    "real estate idx adm",
    "real estate admiral",
    "american strategic value realty",
    "graduate hotels real estate",
    "pw real estate fund iii",
    "high street real estate fund vi",
    "ara europe active real estate",
    "gaticule managed fund",
    "oakhurst real estate fund",
    "abpci direct lending",
    "bluebay direct lending",
    "baring india private equity",
    "dlj real estate",
    "geam international private equity",
    "mellon venture capital",
    "radcliffe spac opportunities",
    "radcliffe private equity",
    "icapital infrastructure",
    "guidestone global real estate",
    "parametric defensive equity",
    "northern trust government short term",
    "nvit real estate",
    "nationwide nvit real estate",
    "ecofin global reninfrastructure",
    "recurrent mlp",
    "centre globalinfrastructure",
    "catalyst mlp and infrastructure",
    "c&h steers",
]

# S3 location of the shared canonical-manager dictionary -- same bucket/key
# ai-cit's load_canonical_json() reads in core-data-platform, so ALT and CIT
# draw display-string casing from one literal source instead of two copies
# that can drift. Fetched lazily (first time a canon-dependent CASE is
# actually built, not at module import) and cached for the process lifetime;
# any failure (no creds, no network, missing key) degrades to the term.title()
# fallback rather than breaking the pipeline run.
_CANON_S3_BUCKET = "retirementinsights-reference"
_CANON_S3_KEY = "manager_canonical/v1/manager_canonical.json"
_canon_lookup_cache: Optional[Dict[str, str]] = None


def _load_canon_lookup() -> Dict[str, str]:
    """Fetch+flatten canon.json into {UPPERCASE token/alias/canonical: display}.
    Cached after first call; returns {} on any failure so callers fall back
    to term.title() instead of raising."""
    global _canon_lookup_cache
    if _canon_lookup_cache is not None:
        return _canon_lookup_cache

    lookup: Dict[str, str] = {}
    try:
        import json as _json

        import boto3

        region = os.environ.get("AWS_REGION", "us-east-1")
        s3 = boto3.client("s3", region_name=region)
        obj = s3.get_object(Bucket=_CANON_S3_BUCKET, Key=_CANON_S3_KEY)
        entries = _json.loads(obj["Body"].read())
        for entry in entries:
            canonical = entry.get("canonical", "")
            if not canonical:
                continue
            keys = [canonical] + entry.get("tokens", []) + entry.get("aliases", [])
            for k in keys:
                if k:
                    lookup[k.strip().upper()] = canonical
    except Exception as exc:
        logging.warning("ALT manager crosswalk: canon.json fetch failed, "
                         "falling back to term.title() for unmapped terms: %s", exc)
        lookup = {}

    _canon_lookup_cache = lookup
    return lookup


def _alt_manager_display_name(term: str) -> str:
    """Resolve one ALT_BRAND_PATTERNS term to its display manager name using
    the override -> canon.json -> term.title() order described above.
    Computed lazily (not a module-level dict) so the canon.json S3 fetch
    happens on first real use -- i.e. when a CASE expression is actually
    built for a pipeline run -- not merely on importing this module."""
    if term in ALT_MANAGER_OVERRIDES:
        return ALT_MANAGER_OVERRIDES[term]
    canon_hit = _load_canon_lookup().get(term.strip().upper())
    if canon_hit:
        return canon_hit
    return term.title()


def _alt_manager_case_sql(column: str = "raw_entity_name") -> str:
    """Build the matched_manager_name CASE, resolving each ALT_BRAND_PATTERNS
    term's display name via _alt_manager_display_name (override -> canon.json
    -> term.title()), using the same term-matching rules (word-boundary/
    override) as the brand router. ALT_MANAGER_ONLY_TERMS branches are
    appended after ALT_BRAND_PATTERNS (not mixed in) so any brand already
    covered by the router's curated list keeps resolving the same way it
    always has -- the extra terms only catch rows the curated list missed."""
    lines = ["CASE"]
    for term, _asset_type, _asset_class in ALT_BRAND_PATTERNS:
        manager = _alt_manager_display_name(term)
        cond = _alt_brand_term_cond(term, column)
        val = manager.replace("'", "''")
        lines.append(f"        WHEN {cond} THEN '{val}'")
    for term in ALT_MANAGER_ONLY_TERMS:
        manager = _alt_manager_display_name(term)
        cond = _alt_brand_term_cond(term, column)
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


def _alt_vehicle_case_sql(column: str = "raw_entity_name") -> str:
    """Build the asset_type (legal vehicle/wrapper) CASE from ALT_VEHICLE_RULES,
    defaulting to 'Unknown' rather than NULL -- unlike the category/manager
    CASEs, this one must never be used as a match/no-match signal since every
    routed row gets some asset_type value.

    `column` defaults to raw_entity_name; the sponsor-name fallback pass
    (2026-09-24) passes raw_sponsor_name instead, since for those rows the
    real fund name and its LP/LLC/offshore suffix lives in the sponsor
    field, not the (generic placeholder) entity name."""
    lines = ["CASE"]
    for pattern, label in ALT_VEHICLE_RULES:
        pat_sql = pattern.replace("'", "''")
        lines.append(f"        WHEN regexp_like(lower({column}), '{pat_sql}') THEN '{label}'")
    lines.append("        ELSE 'Unknown' END")
    return "\n".join(lines)


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
