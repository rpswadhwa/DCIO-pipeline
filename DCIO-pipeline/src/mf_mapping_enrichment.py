"""
mf_mapping_enrichment.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Copies asset_class and asset_sub_class from mapping table into the MF table,
then backfills any unmapped MF raw_entity_name into the mapping table with
PENDING_AI placeholders.

Intended for Athena over Iceberg tables.
"""
from typing import List, Optional


def _run_athena(sql: str, database: str, workgroup: str, s3_output: str) -> None:
    import awswrangler as wr

    qid = wr.athena.start_query_execution(
        sql=sql,
        database=database,
        workgroup=workgroup,
        s3_output=s3_output,
    )
    wr.athena.wait_query(query_execution_id=qid)


def _ack_filter(ack_ids, alias="m"):
    """AND-clause limiting {alias} to ack_ids ('' = whole table)."""
    if ack_ids is None:
        return ""
    ids = ", ".join("'" + str(a).replace("'", "''") + "'" for a in ack_ids)
    return f" AND {alias}.ack_id IN ({ids})"


def enrich_mf_classes(
    *,
    mf_db: str,
    mf_table: str,
    mapping_db: str,
    mapping_table: str,
    workgroup: str,
    s3_staging: str,
    overwrite_existing: bool = False,
    ack_ids: Optional[List[str]] = None,
) -> None:
    """
    Update MF table's asset_class/asset_sub_class from mapping on raw_entity_name,
    then insert unmapped names into mapping with PENDING_AI.

    ack_ids limits both statements to those filings' MF rows (None = whole table). The chunk
    runner scopes each batch to its own filings and runs one whole-table pass per chunk
    (python -m src.mf_mapping_enrichment), so rows an earlier pass left unclassified are
    still picked up (2026-11-05).

    The two full-table COUNT queries this used to run (~90s each, every batch) are gone: the
    first only fed a log line, the second only skipped an INSERT that is a no-op when no
    name is missing (2026-11-05).
    """
    if ack_ids is not None and not ack_ids:
        return
    scope = _ack_filter(ack_ids)

    # 1) MERGE to copy classes into MF table
    # The mapping table can carry more than one row for the same fund (e.g.
    # case/whitespace variants of raw_entity_name that collapse to the same
    # normalized key). A MERGE whose USING side has duplicate keys errors
    # with MERGE_TARGET_ROW_MULTIPLE_MATCHES, so pick a single row per
    # normalized key here, preferring an already-classified row over a
    # PENDING_AI placeholder.
    dedup_source = f"""
        SELECT k, asset_class, asset_sub_class
        FROM (
          SELECT
            lower(trim(raw_entity_name)) AS k,
            asset_class,
            asset_sub_class,
            row_number() OVER (
              PARTITION BY lower(trim(raw_entity_name))
              ORDER BY
                CASE WHEN asset_class <> 'PENDING_AI' THEN 0 ELSE 1 END,
                asset_class,
                asset_sub_class
            ) AS rn
          FROM {mapping_db}.{mapping_table}
        )
        WHERE rn = 1
    """
    if overwrite_existing:
        merge_sql = f"""
        MERGE INTO {mf_db}.{mf_table} AS m
        USING ({dedup_source}) f
        ON lower(trim(m.raw_entity_name)) = f.k
        WHEN MATCHED AND TRUE{scope} THEN UPDATE SET
          asset_class = f.asset_class,
          asset_sub_class = f.asset_sub_class
        """
    else:
        merge_sql = f"""
        MERGE INTO {mf_db}.{mf_table} AS m
        USING ({dedup_source}) f
        ON lower(trim(m.raw_entity_name)) = f.k
        WHEN MATCHED AND (
          coalesce(m.asset_class, '') = '' OR coalesce(m.asset_sub_class, '') = ''
        ){scope} THEN UPDATE SET
          asset_class = f.asset_class,
          asset_sub_class = f.asset_sub_class
        """
    _run_athena(merge_sql, mf_db, workgroup, s3_staging)

    # 2) Insert unmapped names into mapping with PENDING_AI
    insert_sql = f"""
    INSERT INTO {mapping_db}.{mapping_table} (raw_entity_name, asset_class, asset_sub_class)
    SELECT DISTINCT m.raw_entity_name, 'PENDING_AI', 'PENDING_AI'
    FROM {mf_db}.{mf_table} m
    LEFT JOIN {mapping_db}.{mapping_table} f
      ON lower(trim(m.raw_entity_name)) = lower(trim(f.raw_entity_name))
    WHERE f.raw_entity_name IS NULL
      AND m.raw_entity_name IS NOT NULL
      AND trim(m.raw_entity_name) <> ''{scope}
    """
    _run_athena(insert_sql, mf_db, workgroup, s3_staging)


def main():
    """Whole-table pass (the chunk runner's per-chunk catch-up; see enrich_mf_classes)."""
    import os
    from dotenv import load_dotenv
    load_dotenv()
    mf_db = os.getenv("VALIDATED_GLUE_DB", "default")
    enrich_mf_classes(
        mf_db=mf_db,
        mf_table=os.getenv("VALIDATED_TABLE", "plan_mf_history_v3"),
        mapping_db=os.getenv("FUND_MAPPING_GLUE_DB", mf_db),
        mapping_table=os.getenv("FUND_MAPPING_TABLE", "fund_intelligence_mapping_mf"),
        workgroup=os.getenv("ATHENA_WORKGROUP", "primary"),
        s3_staging=os.getenv("ATHENA_STAGING_S3", ""),
        overwrite_existing=os.getenv("ENRICH_MF_OVERWRITE", "0") == "1",
    )
    print("[MF ENRICH] whole-table pass done", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
