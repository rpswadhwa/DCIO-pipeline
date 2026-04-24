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

def _score_as_fund_name(text):
    if not text or not text.strip():
        return -999
    t = text.strip().lower()
    words = set(_re.findall(r"\w+", t))
    score = 0
    score += len(words & _FUND_KEYWORDS) * 3
    score -= len(words & _MANAGER_KEYWORDS) * 4
    # Check exact match and normalized match against known managers
    if t in _KNOWN_MANAGERS or _normalize_for_manager_check(t) in _KNOWN_MANAGERS:
        score -= 20
    if _SHARE_CLASS_RE.search(text):
        score += 10
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
    if not issuer and not desc:
        return ""
    if not issuer:
        return desc
    if not desc:
        return issuer
    return desc if _score_as_fund_name(desc) > _score_as_fund_name(issuer) else issuer





logger = logging.getLogger(__name__)

MF_ASSET_TYPES = frozenset({"mutual fund", "index fund", "money market fund", "etf", "target date fund", "stable value fund", "commingled fund"})


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
                   s3_staging: str) -> Dict[str, float]:
    """Query Athena for ack_id -> amt_mutual_funds mapping.

    Returns only rows where amt_mutual_funds is a positive number.
    Rows with null or zero values are excluded (treated as SKIP at call time).
    """
    import awswrangler as wr

    sql = f"SELECT ack_id, amt_mutual_funds FROM {glue_db}.{table}"
    df = wr.athena.read_sql_query(
        sql=sql,
        database=glue_db,
        workgroup=workgroup,
        s3_output=s3_staging,
    )

    reference: Dict[str, float] = {}
    for _, row in df.iterrows():
        ack_id = str(row["ack_id"]).strip() if row["ack_id"] is not None else ""
        if not ack_id:
            continue
        val = parse_currency_value(str(row["amt_mutual_funds"]))
        if val and val > 0:
            reference[ack_id] = val
        else:
            logger.debug("Skipping reference row ack_id=%s: amt_mutual_funds=%s", ack_id, row["amt_mutual_funds"])

    logger.info("Loaded %d reference entries from %s.%s", len(reference), glue_db, table)
    return reference


# ---------------------------------------------------------------------------
# Final row loader (post-LLM SQLite)
# ---------------------------------------------------------------------------

def load_final_rows(db_path: str) -> List[Dict]:
    """Read the investments table from the post-LLM SQLite database."""
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        cursor = con.execute("SELECT * FROM investments")
        rows = [dict(r) for r in cursor.fetchall()]
    finally:
        con.close()
    logger.info("Loaded %d rows from SQLite investments table", len(rows))
    return rows


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

def build_mf_rows_df(rows: List[Dict],
                     mf_types: frozenset = MF_ASSET_TYPES) -> pd.DataFrame:
    """Build the plan_mf_history_v3 DataFrame from MF rows for a passing PDF.

    Filters to MF asset types only and maps to the three target columns:
      ack_id              ← pdf_stem
      raw_entity_name     ← issuer_name
      plan_investment_amt ← current_value (parsed to float)

    Rows with unparseable current_value are included with NaN.
    """
    records = []
    for row in rows:
        asset_type = str(row.get("asset_type", "") or "").strip().lower()
        if asset_type not in mf_types:
            continue
        records.append({
            "ack_id": str(row.get("pdf_stem", "") or "").strip(),
            "raw_entity_name": pick_fund_name(row.get("issuer_name"), row.get("investment_description")),
            "plan_investment_amt": parse_currency_value(row.get("current_value")),
            "asset_class": "PENDING_AI",
            "asset_sub_class": "PENDING_AI",
        })
    return pd.DataFrame(records, columns=["ack_id", "raw_entity_name", "plan_investment_amt", "asset_class", "asset_sub_class"])




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
    ack_ids = df["ack_id"].dropna().unique().tolist()
    if ack_ids:
        ids_sql = ", ".join("'" + str(a).replace("'", "''") + "'" for a in ack_ids)
        delete_sql = f"DELETE FROM {glue_db}.{table} WHERE ack_id IN ({ids_sql})"
        delete_qid = wr.athena.start_query_execution(
            sql=delete_sql,
            database=glue_db,
            workgroup=workgroup,
            s3_output=s3_staging,
        )
        wr.athena.wait_query(query_execution_id=delete_qid)
        logger.info("Deleted existing rows for %d ack_ids from %s.%s", len(ack_ids), glue_db, table)

    batch_size = 200
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
                amt_sql = "NULL" if (amt is None or math.isnan(float(amt))) else str(float(amt))
            except (TypeError, ValueError):
                amt_sql = "NULL"

            values_parts.append(
                "({}, {}, {}, {}, {})".format(
                    q(row.get("ack_id")),
                    q(row.get("raw_entity_name")),
                    amt_sql,
                    q(row.get("asset_class", "PENDING_AI")),
                    q(row.get("asset_sub_class", "PENDING_AI")),
                )
            )

        sql = (
            "INSERT INTO {}.{} "
            "(ack_id, raw_entity_name, plan_investment_amt, asset_class, asset_sub_class) "
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
            counts["skipped"] += 1
            continue

        expected = reference[pdf_stem]
        if expected <= 0:
            logger.warning("SKIP %s: reference amt_mutual_funds is zero/null", pdf_stem)
            counts["skipped"] += 1
            continue

        extracted = extracted_totals.get(pdf_stem, 0.0)
        passes, pct_diff = validate_pdf(extracted, expected, tolerance)

        if passes:
            df = build_mf_rows_df(stem_rows)
            if df.empty:
                logger.warning("PASS %s but no MF rows to write", pdf_stem)
                counts["passed"] += 1
                continue
            write_iceberg_via_athena(df, validated_glue_db, validated_table)
            logger.info("PASS %s: extracted=%.0f expected=%.0f diff=%.2f%% (%d MF rows written)",
                        pdf_stem, extracted, expected, pct_diff * 100, len(df))
            counts["passed"] += 1
        else:
            error_df = build_mf_rows_df(stem_rows)
            if not error_df.empty:
                write_iceberg_via_athena(error_df, validated_glue_db, error_table)
            logger.warning("FAIL %s: extracted=%.0f expected=%.0f diff=%.2f%% (%d MF rows)",
                           pdf_stem, extracted, expected, pct_diff * 100, len(error_df))
            counts["failed"] += 1

    logger.info("Validation complete — passed=%d failed=%d skipped=%d",
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
