#!/usr/bin/env python3
"""
Asset class / sub-asset class classification pipeline.
Runs after the main pipeline completes successfully.

Flow:
  1. Seed fund_intelligence_mapping_mf with new PENDING_AI fund names
  2. Run asset_class SQL     (s3://retirementinsights-reference/sqls/4updateassetmf.sql)
  3. Run asset_sub_class SQL (s3://retirementinsights-reference/sqls/5updatesubassetmf.sql)
  4. Merge classifications back into plan_mf_history_v3
"""
import boto3
import time
import sys

# ── Config ────────────────────────────────────────────────────────────────────
DATABASE       = 'default'
RESULTS_BUCKET = 's3://retirementinsights-silver/athena-results/'
TRANCHE        = 1
SQL_ASSET      = 's3://retirementinsights-reference/sqls/4updateassetmf.sql'
SQL_SUB_ASSET  = 's3://retirementinsights-reference/sqls/5updatesubassetmf.sql'
POLL_INTERVAL  = 3  # seconds between status checks

athena = boto3.client('athena', region_name='us-east-1')
s3     = boto3.client('s3',     region_name='us-east-1')


# ── Helpers ───────────────────────────────────────────────────────────────────
def run_query(sql: str, description: str) -> str:
    """Submit SQL to Athena and block until complete. Returns execution ID."""
    print(f"\n[Athena] {description}...")
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': RESULTS_BUCKET},
    )
    exec_id = resp['QueryExecutionId']

    while True:
        status = athena.get_query_execution(QueryExecutionId=exec_id)
        state  = status['QueryExecution']['Status']['State']
        if state == 'SUCCEEDED':
            print(f"  Done ({exec_id})")
            return exec_id
        elif state in ('FAILED', 'CANCELLED'):
            reason = status['QueryExecution']['Status'].get('StateChangeReason', '')
            print(f"  FAILED [{state}]: {reason}")
            sys.exit(1)
        time.sleep(POLL_INTERVAL)


def load_s3_sql(s3_uri: str) -> str:
    """Download a SQL file from S3 and return its contents as a string."""
    bucket, key = s3_uri.replace('s3://', '').split('/', 1)
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj['Body'].read().decode('utf-8')


# ── SQL Statements ────────────────────────────────────────────────────────────
SEED_SQL = f"""
INSERT INTO fund_intelligence_mapping_mf
    (raw_entity_name, asset_class, asset_sub_class, tranche, last_processed_date)
SELECT
    raw_entity_name,
    'PENDING_AI'      AS asset_class,
    'PENDING_AI'      AS asset_sub_class,
    {TRANCHE}         AS tranche,
    current_timestamp AS last_processed_date
FROM (
    SELECT
        h.raw_entity_name,
        row_number() OVER (
            PARTITION BY lower(trim(h.raw_entity_name))
            ORDER BY h.raw_entity_name
        ) AS rn
    FROM plan_mf_history_v3 h
    WHERE h.asset_class = 'PENDING_AI'
      AND NOT EXISTS (
          SELECT 1
          FROM fund_intelligence_mapping_mf f
          WHERE lower(trim(f.raw_entity_name)) = lower(trim(h.raw_entity_name))
      )
)
WHERE rn = 1
"""

WRITEBACK_SQL = """
MERGE INTO default.plan_mf_history_v3 h
USING (
    SELECT 
        trim(upper(raw_entity_name)) AS k, 
        asset_class, 
        asset_sub_class,
        is_passive
    FROM (
        SELECT 
            raw_entity_name, 
            asset_class, 
            asset_sub_class,
            is_passive,
            row_number() OVER (
                PARTITION BY trim(upper(raw_entity_name))
                ORDER BY 
                    CASE WHEN asset_class <> 'PENDING_AI' THEN 0 ELSE 1 END,
                    asset_class, 
                    asset_sub_class
            ) AS rn
        FROM default.fund_intelligence_mapping_mf
        WHERE asset_class IS NOT NULL AND asset_class <> 'PENDING_AI'
    )
    WHERE rn = 1
) m
ON (trim(upper(h.raw_entity_name)) = m.k)
WHEN MATCHED THEN
    UPDATE SET 
        asset_class = m.asset_class,
        asset_sub_class = m.asset_sub_class,
        is_passive = m.is_passive
"""

DROP_STAGING_SQL = """
DROP TABLE IF EXISTS mf_entity_sponsor_map_staging
"""

CREATE_STAGING_SQL = """
CREATE TABLE mf_entity_sponsor_map_staging AS
WITH entities AS (
    SELECT DISTINCT
        raw_entity_name,
        trim(
            regexp_replace(
                regexp_replace(
                    upper(raw_entity_name),
                    '[^A-Z0-9 ]+',
                    ' '
                ),
                '\\s+',
                ' '
            )
        ) AS entity_norm
    FROM plan_mf_history_v3
    WHERE coalesce(needs_sponsor_cleaning_flag, true) = true
      AND raw_entity_name IS NOT NULL
),

matches AS (
    SELECT
        e.raw_entity_name,
        e.entity_norm,
        t.canonical_manager AS normalized_sponsor_name,
        t.token_raw,
        t.token_norm,
        t.risk_level,
        t.match_mode,
        t.source_type,
        CASE
            WHEN t.source_type = 'canonical' THEN 1
            WHEN t.source_type = 'alias' THEN 2
            WHEN t.source_type = 'token' THEN 3
            ELSE 9
        END AS source_priority,
        length(t.token_norm) AS token_priority
    FROM entities e
    JOIN manager_canonical_tokens t
      ON regexp_like(
            e.entity_norm,
            '(^|[^A-Z0-9])' || t.token_norm || '([^A-Z0-9]|$)'
         )
),

ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY entity_norm
            ORDER BY
                CASE risk_level
                    WHEN 'LOW' THEN 1
                    WHEN 'MEDIUM' THEN 2
                    WHEN 'HIGH' THEN 3
                    ELSE 9
                END,
                source_priority,
                token_priority DESC
        ) AS rn
    FROM matches
)

SELECT
    entity_norm,
    raw_entity_name AS raw_entity_name_example,
    normalized_sponsor_name,
    token_raw AS sponsor_match_token,
    token_norm,
    risk_level AS sponsor_match_risk_level,
    match_mode,
    source_type,
    'MATCHED_CANONICAL' AS sponsor_match_status,
    CAST(0.95 AS decimal(5,4)) AS sponsor_match_confidence,
    'ENTITY_FIRST' AS sponsor_match_source,
    'dictionary:ENTITY_FIRST' AS sponsor_matched_by,
    CAST(current_timestamp AS timestamp) AS sponsor_cleaned_at
FROM ranked
WHERE rn = 1
"""

MERGE_SPONSOR_SQL = """
MERGE INTO plan_mf_history_v3 t
USING mf_entity_sponsor_map_staging s
ON trim(
       regexp_replace(
         regexp_replace(
           upper(t.raw_entity_name),
           '[^A-Z0-9 ]+',
           ' '
         ),
         '\\s+',
         ' '
       )
   ) = s.entity_norm

WHEN MATCHED THEN UPDATE SET
    normalized_sponsor_name = s.normalized_sponsor_name,
    sponsor_match_status = s.sponsor_match_status,
    sponsor_match_confidence = s.sponsor_match_confidence,
    sponsor_match_token = s.sponsor_match_token,
    sponsor_match_source = s.sponsor_match_source,
    sponsor_match_risk_level = s.sponsor_match_risk_level,
    sponsor_matched_by = s.sponsor_matched_by,
    sponsor_cleaned_at = s.sponsor_cleaned_at,
    needs_sponsor_cleaning_flag = false
"""

CREATE_AUDIT_SQL = """
CREATE TABLE IF NOT EXISTS mf_sponsor_match_audit (
    audit_run_at timestamp,
    total_rows_needing_cleaning bigint,
    staged_entity_matches bigint,
    updated_rows_estimate bigint,
    unmatched_rows bigint
)
LOCATION 's3://retirementinsights-silver/mf_sponsor_match_audit/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
)
"""

INSERT_AUDIT_SQL = """
INSERT INTO mf_sponsor_match_audit
SELECT
    CAST(current_timestamp AS timestamp) AS audit_run_at,

    (
        SELECT count(*)
        FROM plan_mf_history_v3
        WHERE coalesce(needs_sponsor_cleaning_flag, true) = true
          AND raw_entity_name IS NOT NULL
    ) AS total_rows_needing_cleaning,

    (
        SELECT count(*)
        FROM mf_entity_sponsor_map_staging
    ) AS staged_entity_matches,

    (
        SELECT count(*)
        FROM plan_mf_history_v3 t
        JOIN mf_entity_sponsor_map_staging s
          ON trim(
               regexp_replace(
                 regexp_replace(
                   upper(t.raw_entity_name),
                   '[^A-Z0-9 ]+',
                   ' '
                 ),
                 '\\s+',
                 ' '
               )
             ) = s.entity_norm
    ) AS updated_rows_estimate,

    (
        SELECT count(*)
        FROM plan_mf_history_v3
        WHERE coalesce(needs_sponsor_cleaning_flag, true) = true
          AND raw_entity_name IS NOT NULL
    ) AS unmatched_rows
"""


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("ASSET CLASS CLASSIFICATION PIPELINE")
    print("=" * 60)

    run_query(SEED_SQL,                        "Step 1: Seed fund_intelligence_mapping_mf")
    run_query(load_s3_sql(SQL_ASSET),          "Step 2: Classify asset_class")
    run_query(load_s3_sql(SQL_SUB_ASSET),      "Step 3: Classify asset_sub_class")
    run_query(WRITEBACK_SQL,                   "Step 4: Write back to plan_mf_history_v3")
    run_query(DROP_STAGING_SQL,   "Step 5: Drop old sponsor staging table")
    run_query(CREATE_STAGING_SQL, "Step 6: Build sponsor staging map")
    run_query(MERGE_SPONSOR_SQL,  "Step 7: Merge sponsor names into plan_mf_history_v3")
    run_query(CREATE_AUDIT_SQL,   "Step 8: Create sponsor audit table")
    run_query(INSERT_AUDIT_SQL,   "Step 9: Insert sponsor audit record")

    print("\n" + "=" * 60)
    print("Classification pipeline complete")
    print("=" * 60)


if __name__ == '__main__':
    main()
