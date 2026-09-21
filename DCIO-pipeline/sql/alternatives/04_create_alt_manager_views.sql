CREATE OR REPLACE VIEW default.v_manager_alt_holdings AS
SELECT
  b.manager_id,
  b.manager_name,
  b.filing_year,
  CAST(a.vehicle_type AS varchar) AS wrapper,
  a.raw_entity_name AS product_name,
  a.asset_class,
  a.asset_sub_class,
  SUM(CAST(a.plan_investment_amt AS double)) AS aum
FROM default.v_manager_plan_base b
INNER JOIN default.plan_alternative_history a
  ON b.ack_id = a.ack_id
WHERE trim(upper(a.normalized_manager)) = trim(upper(b.manager_name))
GROUP BY 1, 2, 3, 4, 5, 6, 7;

CREATE OR REPLACE VIEW default.v_manager_alt_holdings_plan AS
SELECT
  u.manager_id,
  u.manager_name,
  u.filing_year,
  u.plan_id,
  u.sponsor_ein,
  u.sponsor_name,
  u.state AS plan_state,
  CAST(a.vehicle_type AS varchar) AS wrapper,
  a.raw_entity_name AS product_name,
  a.asset_class,
  a.asset_sub_class,
  SUM(CAST(a.plan_investment_amt AS double)) AS aum
FROM default.v_manager_plan_universe u
INNER JOIN default.plan_alternative_history a
  ON u.ack_id = a.ack_id
 AND trim(upper(u.manager_name)) = trim(upper(a.normalized_manager))
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;

CREATE OR REPLACE VIEW default.v_manager_holdings_plan AS
SELECT
  manager_id,
  manager_name,
  plan_id,
  plan_state,
  sponsor_ein,
  sponsor_name,
  CAST(wrapper AS varchar) AS wrapper,
  product_name,
  asset_class,
  asset_sub_class,
  aum
FROM default.v_manager_cit_holdings_plan
UNION ALL
SELECT
  manager_id,
  manager_name,
  plan_id,
  plan_state,
  sponsor_ein,
  sponsor_name,
  CAST(wrapper AS varchar) AS wrapper,
  product_name,
  asset_class,
  asset_sub_class,
  aum
FROM default.v_manager_mf_holdings_plan
UNION ALL
SELECT
  manager_id,
  manager_name,
  plan_id,
  plan_state,
  sponsor_ein,
  sponsor_name,
  CAST(wrapper AS varchar) AS wrapper,
  product_name,
  asset_class,
  asset_sub_class,
  aum
FROM default.v_manager_alt_holdings_plan;
