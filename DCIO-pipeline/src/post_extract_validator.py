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
    r"(class\s+[a-z]|institutional|investor|admiral|signal|"
    r"premium|select|premier|r\d+|i\s*shares?)",
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
    r"(r[1-6]|institutional(?:\s+(?:plus|shares?))?|investor\s+shares?|"
    r"admiral\s+shares?|signal\s+shares?|class\s+[a-z]|i\s*shares?|etf)",
    _re.IGNORECASE,
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
    if _re.search(r"20[2-9]\d", text):
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

MF_ASSET_TYPES = frozenset({"mutual fund", "index fund", "money market fund", "etf", "target date fund"})
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

    sql = f"SELECT ack_id, plan_id, amt_mutual_funds FROM {glue_db}.{table}"
    df = wr.athena.read_sql_query(
        sql=sql,
        database=glue_db,
        workgroup=workgroup,
        s3_output=s3_staging,
    )

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
_ANNUITY_VEHICLE_RE = _re.compile(
    r'(?i)(variable\s+annuit|annuity\s+(account|contract|compan|co\b)'
    r'|insurance\s+(and\s+)?annuity|traditional\s+annuity|\bCREF\b'
    r'|college\s+retirement\s+equities|teachers\s+insurance\s+and\s+annuity)')


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
    return n


def build_mf_rows_df(rows: List[Dict],
                     mf_types: frozenset = MF_ASSET_TYPES,
                     validation_status: str = "UNVALIDATED") -> pd.DataFrame:
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
               "partnership interest","currency"}
    records = []
    for row in rows:
        asset_type = str(row.get("asset_type", "") or "").strip().lower()
        _val = parse_currency_value(row.get("current_value"))
        # Load MF-typed rows; also load blank/unknown-type rows that have a value
        # (the "no asset type" case -> classify in post-processing). Skip explicit non-MF.
        if asset_type in _non_mf:
            continue
        if asset_type and asset_type not in mf_types:
            continue
        if not asset_type and _val is None:
            continue
        _name = _normalize_mf_name(pick_fund_name(row.get("issuer_name"), row.get("investment_description")))
        # Name-quality gate: drop blank / numeric-only (bond rates, share counts, mis-mapped
        # columns) and "Mutual Fund(s) Total/Shares/N-A" subtotal/type-only rows
        # (_normalize_mf_name returns '' for those).
        if not _name or not _re.search(r"[A-Za-z]", _name):
            continue
        # Scope: annuity / insurance vehicles (CREF, TIAA Traditional, Voya/Empower
        # Retirement Insurance & Annuity, variable annuity accounts) are not mutual funds.
        if _ANNUITY_VEHICLE_RE.search(_name):
            continue
        records.append({
            "ack_id": str(row.get("pdf_stem", "") or "").strip(),
            "raw_entity_name": _name,
            "raw_sponsor_name": str(row.get("issuer_name", "") or "").strip(),
            "plan_investment_amt": _val,
            "asset_class": "PENDING_AI",
            "asset_sub_class": "PENDING_AI",
            "validation_status": validation_status,
        })
    return pd.DataFrame(records, columns=[
        "ack_id",
        "raw_entity_name",
        "raw_sponsor_name",
        "plan_investment_amt",
        "asset_class",
        "asset_sub_class",
        "validation_status",
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
def write_iceberg_via_athena(df: pd.DataFrame, glue_db: str, table: str) -> None:
    """Write rows to an Iceberg table via Athena INSERT INTO statements."""
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

    # Delete existing rows for these ack_ids before inserting (idempotency)
    # Only works for Iceberg/transactional tables; skips silently for plain Hive tables
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
            logger.info("Deleted existing rows for %d ack_ids from %s.%s", len(ack_ids), glue_db, table)
        except Exception as e:
            logger.warning("DELETE skipped for %s.%s (not a transactional table?): %s", glue_db, table, e)

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

            amt = row.get("plan_investment_amt")
            try:
                _a = float(amt)
                # DECIMAL(18,2) overflows ~1e16; null obvious overflow/garbage
                # and use fixed-point (no exponent) formatting for valid values.
                amt_sql = "NULL" if (math.isnan(_a) or abs(_a) >= 1e15) else ("%.2f" % _a)
            except (TypeError, ValueError):
                amt_sql = "NULL"

            values_parts.append(
                "({}, {}, {}, {}, {}, {}, {})".format(
                    q(row.get("ack_id")),
                    q(row.get("raw_entity_name")),
                    q(row.get("raw_sponsor_name")),
                    amt_sql,
                    q(row.get("asset_class", "PENDING_AI")),
                    q(row.get("asset_sub_class", "PENDING_AI")),
                    q(row.get("validation_status", "UNVALIDATED")),
                )
            )

        sql = (
            "INSERT INTO {}.{} "
            "(ack_id, raw_entity_name, raw_sponsor_name, plan_investment_amt, asset_class, asset_sub_class, validation_status) "
            "VALUES {}".format(glue_db, table, ", ".join(values_parts))
        )
        query_id = wr.athena.start_query_execution(
            sql=sql,
            database=glue_db,
            workgroup=workgroup,
            s3_output=s3_staging,
        )
        wr.athena.wait_query(query_execution_id=query_id)
        logger.info("Inserted rows %d-%d into Iceberg %s.%s", start, start + len(batch), glue_db, table)

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

    reference = load_reference(glue_db, ref_table, workgroup, s3_staging)
    extracted_totals = compute_extracted_mf_totals(rows)

    # Group all rows by pdf_stem for efficient dispatch
    rows_by_stem: Dict[str, List[Dict]] = defaultdict(list)
    for row in rows:
        stem = str(row.get("pdf_stem", "") or "").strip()
        if stem:
            rows_by_stem[stem].append(row)

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

    # LOAD-ALL: write every extracted mutual-fund row to the MF table, tagged with
    # its summary status (UNVALIDATED when there is no usable certified reference).
    status_by_ack = {r["ack_id"]: r["validation_status"] for r in summary_records}
    all_mf = []
    for _stem, _srows in rows_by_stem.items():
        _st = status_by_ack.get(_stem) or "UNVALIDATED"
        if _st == "SKIP":
            _st = "UNVALIDATED"
        _df = build_mf_rows_df(_srows, validation_status=_st)
        if not _df.empty:
            all_mf.append(_df)
    if all_mf:
        write_iceberg_via_athena(pd.concat(all_mf, ignore_index=True), validated_glue_db, validated_table)

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
