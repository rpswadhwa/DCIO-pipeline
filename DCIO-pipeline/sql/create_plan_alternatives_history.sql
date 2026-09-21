-- One-time DDL for the alternatives router (see src/post_extract_validator.py:
-- _route_alternatives_from_staging). Mirrors plan_mf_history_v3's Iceberg shape
-- (partitioned by asset_class, same S3 warehouse). Run manually in Athena --
-- not executed automatically by the pipeline.
CREATE TABLE IF NOT EXISTS default.plan_alternatives_history (
    ack_id                     string,
    raw_entity_name            string,
    raw_sponsor_name           string,
    plan_investment_amt        decimal(18,2),
    asset_sub_class            string,
    validation_status          string,
    asset_type                 string,
    classification_confidence  string,
    classification_method      string,
    manual_review_required     boolean,
    routed_at                  timestamp,
    asset_class                string
)
PARTITIONED BY (asset_class)
LOCATION 's3://retirementinsights-silver/iceberg-warehouse/plan_alternatives_history'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'PARQUET',
    'write_compression' = 'ZSTD'
);
