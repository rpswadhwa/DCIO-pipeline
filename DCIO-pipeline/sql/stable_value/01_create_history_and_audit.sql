-- Downstream-only design. No staging or CIT schema alterations.

CREATE TABLE IF NOT EXISTS default.plan_insurance_history (
  candidate_id string,
  ack_id string,
  raw_entity_name string,
  normalized_entity_name string,
  raw_sponsor_name string,
  plan_investment_amt decimal(18,2),
  asset_type string,
  asset_class string,
  asset_sub_class string,
  validation_status string,
  normalized_manager string,
  manager_match_status string,
  manager_match_method string,
  manager_match_confidence double,
  needs_cleaning_flag boolean,
  vehicle_type string,
  account_structure string,
  contract_type string,
  resolution_status string,
  routing_rule string,
  routing_version string,
  routing_reason string,
  source_table string,
  routing_run_id string,
  canon_version string,
  created_at timestamp,
  updated_at timestamp
)
LOCATION 's3://retirementinsights-silver/tables/default.db/plan_insurance_history'
TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet');

CREATE TABLE IF NOT EXISTS default.plan_separately_managed_history (
  candidate_id string,
  ack_id string,
  raw_entity_name string,
  normalized_entity_name string,
  raw_sponsor_name string,
  plan_investment_amt decimal(18,2),
  asset_type string,
  asset_class string,
  asset_sub_class string,
  validation_status string,
  normalized_manager string,
  manager_match_status string,
  manager_match_method string,
  manager_match_confidence double,
  needs_cleaning_flag boolean,
  vehicle_type string,
  account_structure string,
  contract_type string,
  resolution_status string,
  routing_rule string,
  routing_version string,
  routing_reason string,
  source_table string,
  routing_run_id string,
  canon_version string,
  created_at timestamp,
  updated_at timestamp
)
LOCATION 's3://retirementinsights-silver/tables/default.db/plan_separately_managed_history'
TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet');

CREATE TABLE IF NOT EXISTS default.plan_stable_value_unresolved_history (
  candidate_id string,
  ack_id string,
  raw_entity_name string,
  normalized_entity_name string,
  raw_sponsor_name string,
  plan_investment_amt decimal(18,2),
  asset_type string,
  asset_class string,
  asset_sub_class string,
  validation_status string,
  normalized_manager string,
  manager_match_status string,
  manager_match_method string,
  manager_match_confidence double,
  needs_cleaning_flag boolean,
  vehicle_type string,
  account_structure string,
  contract_type string,
  resolution_status string,
  routing_rule string,
  routing_version string,
  routing_reason string,
  source_table string,
  routing_run_id string,
  canon_version string,
  created_at timestamp,
  updated_at timestamp
)
LOCATION 's3://retirementinsights-silver/tables/default.db/plan_stable_value_unresolved_history'
TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet');

CREATE TABLE IF NOT EXISTS default.plan_stable_value_routing_audit (
  audit_id string,
  routing_run_id string,
  candidate_id string,
  ack_id string,
  raw_entity_name string,
  raw_sponsor_name string,
  plan_investment_amt decimal(18,2),
  source_asset_type string,
  validation_status string,
  staging_row_count bigint,
  destination_table string,
  routing_status string,
  routing_reason string,
  matched_history_table string,
  matched_history_name string,
  normalized_manager string,
  manager_match_status string,
  manager_match_method string,
  manager_match_confidence double,
  routing_version string,
  canon_version string,
  recorded_at timestamp
)
LOCATION 's3://retirementinsights-silver/tables/default.db/plan_stable_value_routing_audit'
TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet');
