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
SELECT DISTINCT
    h.raw_entity_name,
    'PENDING_AI'      AS asset_class,
    'PENDING_AI'      AS asset_sub_class,
    {TRANCHE}         AS tranche,
    current_timestamp AS last_processed_date
FROM plan_mf_history_v3 h
WHERE h.asset_class = 'PENDING_AI'
  AND NOT EXISTS (
      SELECT 1
      FROM fund_intelligence_mapping_mf f
      WHERE f.raw_entity_name = h.raw_entity_name
  )
"""

WRITEBACK_SQL = """
MERGE INTO default.plan_mf_history_v3 p
USING (
  SELECT
    raw_entity_name_key,
    asset_class,
    asset_sub_class
  FROM (
    SELECT
      trim(upper(raw_entity_name)) AS raw_entity_name_key,
      asset_class,
      asset_sub_class,
      row_number() OVER (
        PARTITION BY trim(upper(raw_entity_name))
        ORDER BY
          CASE WHEN asset_class <> 'PENDING_AI' THEN 0 ELSE 1 END,
          asset_class,
          asset_sub_class
      ) AS rn
    FROM default.fund_intelligence_mapping_mf
    WHERE asset_class IS NOT NULL
      AND asset_class <> 'PENDING_AI'
  ) x
  WHERE rn = 1
) f
ON trim(upper(p.raw_entity_name)) = f.raw_entity_name_key
WHEN MATCHED THEN UPDATE SET
  asset_class = f.asset_class,
  asset_sub_class = f.asset_sub_class
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
