WITH latest AS (
    SELECT ack_id, MAX(run_ts) AS max_ts
    FROM plan_mf_history_validation_summary
    GROUP BY ack_id
),
summ AS (
    SELECT s.ack_id, s.plan_id, s.extracted_amt_mutual_funds, s.reference_amt_mutual_funds,
           s.difference_amt, s.difference_pct, s.validation_status, s.gap_reason, s.run_ts
    FROM plan_mf_history_validation_summary s
    JOIN latest l ON s.ack_id = l.ack_id AND s.run_ts = l.max_ts
),
staging AS (
    SELECT ack_id, COUNT(*) AS staging_row_count,
           SUM(TRY(CAST(plan_investment_amt AS DOUBLE))) AS staging_total
    FROM plan_holdings_staging
    GROUP BY ack_id
)
SELECT summ.ack_id,
       summ.plan_id,
       summ.reference_amt_mutual_funds AS certified,
       summ.extracted_amt_mutual_funds AS extracted,
       summ.difference_pct,
       summ.run_ts,
       COALESCE(st.staging_row_count, 0) AS staging_row_count,
       COALESCE(st.staging_total, 0) AS staging_total
FROM summ
LEFT JOIN staging st ON summ.ack_id = st.ack_id
WHERE summ.difference_amt < 0
  AND summ.difference_pct > 0.75
  AND summ.reference_amt_mutual_funds > 1000000
  AND COALESCE(st.staging_row_count, 0) <= 10
ORDER BY summ.reference_amt_mutual_funds DESC
LIMIT 500
