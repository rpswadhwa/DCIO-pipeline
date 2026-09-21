WITH staging_cit_like AS (
    SELECT
        h.ack_id,
        pm.plan_id,
        pm.plan_name_clean,
        pm.filing_year,
        h.raw_entity_name,
        h.raw_sponsor_name,
        h.asset_type AS source_asset_type,
        h.asset_class AS source_asset_class,
        h.asset_sub_class AS source_asset_sub_class,
        h.validation_status,
        h.plan_investment_amt,
        upper(trim(regexp_replace(regexp_replace(coalesce(h.raw_entity_name, ''), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS entity_norm,
        upper(trim(regexp_replace(regexp_replace(coalesce(h.raw_sponsor_name, ''), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS sponsor_norm
    FROM default.plan_holdings_staging h
    LEFT JOIN default.plan_master pm
      ON pm.ack_id = h.ack_id
    WHERE lower(coalesce(h.asset_type, '')) IN ('common/collective trust fund', 'commingled fund')
),
cit_norm AS (
    SELECT
        c.ack_id,
        c.plan_id,
        c.raw_entity_name AS cit_raw_entity_name,
        c.raw_sponsor_name AS cit_raw_sponsor_name,
        c.normalized_manager AS cit_normalized_manager,
        c.asset_class AS cit_asset_class,
        c.asset_sub_class AS cit_asset_sub_class,
        c.plan_investment_amt AS cit_plan_investment_amt,
        upper(trim(regexp_replace(regexp_replace(coalesce(c.raw_entity_name, ''), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS cit_entity_norm,
        upper(trim(regexp_replace(regexp_replace(coalesce(c.raw_sponsor_name, ''), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS cit_sponsor_norm
    FROM default.plan_cit_history c
),
matched AS (
    SELECT
        s.*,
        c.cit_raw_entity_name,
        c.cit_raw_sponsor_name,
        c.cit_normalized_manager,
        c.cit_asset_class,
        c.cit_asset_sub_class,
        c.cit_plan_investment_amt,
        CASE
            WHEN c.ack_id IS NOT NULL THEN 'YES'
            ELSE 'NO'
        END AS already_in_plan_cit_history,
        CASE
            WHEN c.ack_id IS NOT NULL AND s.entity_norm = c.cit_entity_norm THEN 'ACK_ID_AND_RAW_ENTITY'
            WHEN c.ack_id IS NOT NULL AND s.sponsor_norm = c.cit_entity_norm THEN 'ACK_ID_AND_RAW_SPONSOR_TO_CIT_ENTITY'
            WHEN c.ack_id IS NOT NULL AND s.entity_norm = c.cit_sponsor_norm THEN 'ACK_ID_AND_RAW_ENTITY_TO_CIT_SPONSOR'
            WHEN c.ack_id IS NOT NULL AND s.sponsor_norm = c.cit_sponsor_norm THEN 'ACK_ID_AND_RAW_SPONSOR'
        END AS cit_match_method
    FROM staging_cit_like s
    LEFT JOIN cit_norm c
      ON c.ack_id = s.ack_id
     AND (
            s.entity_norm = c.cit_entity_norm
         OR s.sponsor_norm = c.cit_entity_norm
         OR s.entity_norm = c.cit_sponsor_norm
         OR s.sponsor_norm = c.cit_sponsor_norm
     )
)
SELECT
    filing_year,
    plan_id,
    plan_name_clean,
    ack_id,
    raw_entity_name,
    raw_sponsor_name,
    source_asset_type,
    source_asset_class,
    source_asset_sub_class,
    validation_status,
    plan_investment_amt,
    already_in_plan_cit_history,
    cit_match_method,
    cit_raw_entity_name,
    cit_raw_sponsor_name,
    cit_normalized_manager,
    cit_asset_class,
    cit_asset_sub_class,
    cit_plan_investment_amt
FROM matched
ORDER BY
    already_in_plan_cit_history DESC,
    try_cast(plan_investment_amt AS double) DESC NULLS LAST,
    plan_name_clean,
    raw_entity_name
