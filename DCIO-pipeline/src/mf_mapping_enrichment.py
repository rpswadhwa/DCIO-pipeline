"""
mf_mapping_enrichment.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Copies asset_class and asset_sub_class from mapping table into the MF table,
then backfills any unmapped MF raw_entity_name into the mapping table with
PENDING_AI placeholders.

Intended for Athena over Iceberg tables.
"""
from typing import Tuple


def _run_athena(sql: str, database: str, workgroup: str, s3_output: str) -> None:
    import awswrangler as wr

    qid = wr.athena.start_query_execution(
        sql=sql,
        database=database,
        workgroup=workgroup,
        s3_output=s3_output,
    )
    wr.athena.wait_query(query_execution_id=qid)


def _read_athena(sql: str, database: str, workgroup: str, s3_output: str):
    import awswrangler as wr

    return wr.athena.read_sql_query(
        sql=sql,
        database=database,
        workgroup=workgroup,
        s3_output=s3_output,
    )


def enrich_mf_classes(
    *,
    mf_db: str,
    mf_table: str,
    mapping_db: str,
    mapping_table: str,
    workgroup: str,
    s3_staging: str,
    overwrite_existing: bool = False,
) -> Tuple[int, int]:
    """
    Update MF table's asset_class/asset_sub_class from mapping on raw_entity_name,
    then insert unmapped names into mapping with PENDING_AI.

    Returns: (updated_row_count_estimate, backfilled_name_count)
    Note: updated_row_count_estimate is based on a pre-merge count heuristic.
    """

    # 1) Estimate count of MF rows eligible for update
    if overwrite_existing:
        eligible_sql = f"""
        SELECT COUNT(1) AS cnt
        FROM {mf_db}.{mf_table} m
        INNER JOIN {mapping_db}.{mapping_table} f
          ON lower(trim(m.raw_entity_name)) = lower(trim(f.raw_entity_name))
        """
    else:
        eligible_sql = f"""
        SELECT COUNT(1) AS cnt
        FROM {mf_db}.{mf_table} m
        INNER JOIN {mapping_db}.{mapping_table} f
          ON lower(trim(m.raw_entity_name)) = lower(trim(f.raw_entity_name))
        WHERE coalesce(m.asset_class, '') = '' OR coalesce(m.asset_sub_class, '') = ''
        """
    df_elig = _read_athena(eligible_sql, mf_db, workgroup, s3_staging)
    to_update = int(df_elig.iloc[0]["cnt"]) if not df_elig.empty else 0

    # 2) MERGE to copy classes into MF table
    if overwrite_existing:
        merge_sql = f"""
        MERGE INTO {mf_db}.{mf_table} AS m
        USING (
          SELECT lower(trim(raw_entity_name)) AS k, asset_class, asset_sub_class
          FROM {mapping_db}.{mapping_table}
        ) f
        ON lower(trim(m.raw_entity_name)) = f.k
        WHEN MATCHED THEN UPDATE SET
          asset_class = f.asset_class,
          asset_sub_class = f.asset_sub_class
        """
    else:
        merge_sql = f"""
        MERGE INTO {mf_db}.{mf_table} AS m
        USING (
          SELECT lower(trim(raw_entity_name)) AS k, asset_class, asset_sub_class
          FROM {mapping_db}.{mapping_table}
        ) f
        ON lower(trim(m.raw_entity_name)) = f.k
        WHEN MATCHED AND (
          coalesce(m.asset_class, '') = '' OR coalesce(m.asset_sub_class, '') = ''
        ) THEN UPDATE SET
          asset_class = f.asset_class,
          asset_sub_class = f.asset_sub_class
        """
    _run_athena(merge_sql, mf_db, workgroup, s3_staging)

    # 3) Count how many unmapped names need backfill
    count_missing_sql = f"""
    SELECT COUNT(DISTINCT m.raw_entity_name) AS cnt
    FROM {mf_db}.{mf_table} m
    LEFT JOIN {mapping_db}.{mapping_table} f
      ON lower(trim(m.raw_entity_name)) = lower(trim(f.raw_entity_name))
    WHERE f.raw_entity_name IS NULL
      AND m.raw_entity_name IS NOT NULL
      AND trim(m.raw_entity_name) <> ''
    """
    df_missing = _read_athena(count_missing_sql, mf_db, workgroup, s3_staging)
    to_backfill = int(df_missing.iloc[0]["cnt"]) if not df_missing.empty else 0

    # 4) Insert unmapped names into mapping with PENDING_AI
    if to_backfill > 0:
        insert_sql = f"""
        INSERT INTO {mapping_db}.{mapping_table} (raw_entity_name, asset_class, asset_sub_class)
        SELECT DISTINCT m.raw_entity_name, 'PENDING_AI', 'PENDING_AI'
        FROM {mf_db}.{mf_table} m
        LEFT JOIN {mapping_db}.{mapping_table} f
          ON lower(trim(m.raw_entity_name)) = lower(trim(f.raw_entity_name))
        WHERE f.raw_entity_name IS NULL
          AND m.raw_entity_name IS NOT NULL
          AND trim(m.raw_entity_name) <> ''
        """
        _run_athena(insert_sql, mf_db, workgroup, s3_staging)

    return to_update, to_backfill
