CREATE VIEW default.v_plan_stable_value_holdings AS
      SELECT *, count(*) OVER (PARTITION BY ack_id,
      lower(trim(regexp_replace(raw_entity_name, '\s+', ' '))), plan_investment_amt)
      AS equivalent_pair_amount_record_count
      FROM (SELECT 'plan_insurance_history' source_table, ack_id, raw_entity_name,
          raw_sponsor_name, plan_investment_amt, asset_class, asset_sub_class, asset_type,
          normalized_manager, vehicle_type, account_structure, resolution_status,
          validation_status, manager_match_status, 'stable_value_router' record_origin
          FROM default.plan_insurance_history UNION ALL SELECT 'plan_separately_managed_history' source_table, ack_id, raw_entity_name,
          raw_sponsor_name, plan_investment_amt, asset_class, asset_sub_class, asset_type,
          normalized_manager, vehicle_type, account_structure, resolution_status,
          validation_status, manager_match_status, 'stable_value_router' record_origin
          FROM default.plan_separately_managed_history UNION ALL SELECT 'plan_stable_value_unresolved_history' source_table, ack_id, raw_entity_name,
          raw_sponsor_name, plan_investment_amt, asset_class, asset_sub_class, asset_type,
          normalized_manager, vehicle_type, account_structure, resolution_status,
          validation_status, manager_match_status, 'stable_value_router' record_origin
          FROM default.plan_stable_value_unresolved_history UNION ALL SELECT 'plan_cit_history', ack_id, raw_entity_name, raw_sponsor_name,
      plan_investment_amt, asset_class, asset_sub_class, cast(null as varchar),
      coalesce(proposed_manager_ai, normalized_manager),
      CASE WHEN row_order LIKE 'SV:%' THEN 'collective_investment_trust' ELSE 'legacy_vehicle_unverified' END,
      CASE WHEN row_order LIKE 'SV:%' THEN 'pooled_trust' ELSE 'unknown' END,
      CASE WHEN row_order LIKE 'SV:%' THEN 'vehicle_resolved' ELSE 'legacy_unverified' END,
      cast(null as varchar), manager_status_ai,
      CASE WHEN row_order LIKE 'SV:%' THEN 'stable_value_router' ELSE 'legacy_history' END
      FROM default.plan_cit_history WHERE asset_class='Stable Value'
      OR regexp_like(raw_entity_name, '(?i)(^|[^a-z0-9])stable\s+value([^a-z0-9]|$)') UNION ALL SELECT 'plan_mf_history_v3', ack_id, raw_entity_name, raw_sponsor_name,
      plan_investment_amt, asset_class, asset_sub_class, cast(null as varchar),
      cast(null as varchar), 'legacy_vehicle_unverified', 'unknown', 'legacy_unverified',
      cast(null as varchar), 'UNVERIFIED', 'legacy_history'
      FROM default.plan_mf_history_v3 WHERE asset_class='Stable Value'
      OR regexp_like(raw_entity_name, '(?i)(^|[^a-z0-9])stable\s+value([^a-z0-9]|$)'));
