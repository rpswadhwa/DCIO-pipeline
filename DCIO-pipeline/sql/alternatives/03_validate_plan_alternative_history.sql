SELECT
  dedupe_status,
  classification_confidence,
  asset_class,
  vehicle_type,
  count(*) AS rows,
  count(DISTINCT ack_id) AS distinct_ack_ids,
  cast(sum(try_cast(plan_investment_amt AS double)) AS decimal(18,2)) AS total_amount
FROM default.plan_alternative_history
GROUP BY 1, 2, 3, 4
ORDER BY rows DESC;

SELECT
  source_table,
  count(*) AS rows,
  count(DISTINCT ack_id) AS distinct_ack_ids,
  cast(sum(try_cast(plan_investment_amt AS double)) AS decimal(18,2)) AS total_amount
FROM default.plan_alternative_history
GROUP BY 1;
