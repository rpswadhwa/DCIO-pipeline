WITH source_rows AS (
    SELECT
        'plan_holdings_staging' AS source_table,
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
        h.plan_investment_amt
    FROM default.plan_holdings_staging h
    LEFT JOIN default.plan_master pm
      ON pm.ack_id = h.ack_id

    UNION ALL

    SELECT
        'plan_cit_history' AS source_table,
        c.ack_id,
        c.plan_id,
        pm.plan_name_clean,
        c.filing_year,
        c.raw_entity_name,
        c.raw_sponsor_name,
        CAST(NULL AS varchar) AS source_asset_type,
        c.asset_class AS source_asset_class,
        c.asset_sub_class AS source_asset_sub_class,
        CAST(NULL AS varchar) AS validation_status,
        c.plan_investment_amt
    FROM default.plan_cit_history c
    LEFT JOIN default.plan_master pm
      ON pm.ack_id = c.ack_id
     AND pm.filing_year = c.filing_year

    UNION ALL

    SELECT
        'plan_mf_history_v3' AS source_table,
        m.ack_id,
        pm.plan_id,
        pm.plan_name_clean,
        pm.filing_year,
        m.raw_entity_name,
        m.raw_sponsor_name,
        CAST(NULL AS varchar) AS source_asset_type,
        m.asset_class AS source_asset_class,
        m.asset_sub_class AS source_asset_sub_class,
        m.validation_status,
        m.plan_investment_amt
    FROM default.plan_mf_history_v3 m
    LEFT JOIN default.plan_master pm
      ON pm.ack_id = m.ack_id
),
normed AS (
    SELECT
        *,
        upper(trim(regexp_replace(regexp_replace(coalesce(raw_entity_name, ''), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS entity_norm,
        upper(trim(regexp_replace(regexp_replace(coalesce(raw_sponsor_name, ''), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS sponsor_norm,
        upper(trim(regexp_replace(regexp_replace(coalesce(source_asset_type, ''), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS asset_type_norm
    FROM source_rows
    WHERE raw_entity_name IS NOT NULL OR raw_sponsor_name IS NOT NULL
),
signals AS (
    SELECT
        *,
        CASE
            WHEN regexp_like(entity_norm || ' ' || sponsor_norm, '(^| )STWD( |$)') THEN 'STARWOOD'
            WHEN regexp_like(entity_norm || ' ' || sponsor_norm, '(^| )BX( |$)|BLACKSTONE') THEN 'BLACKSTONE'
            WHEN regexp_like(entity_norm || ' ' || sponsor_norm, '(^| )ARES( |$)|ARCC|ARES CAPITAL') THEN 'ARES'
            WHEN regexp_like(entity_norm || ' ' || sponsor_norm, '(^| )KKR( |$)|FS KKR') THEN 'KKR'
            WHEN regexp_like(entity_norm || ' ' || sponsor_norm, '(^| )BAM( |$)|BROOKFIELD') THEN 'BROOKFIELD'
            WHEN regexp_like(entity_norm || ' ' || sponsor_norm, '(^| )OWL( |$)|OWL ROCK|BLUE OWL|OBDC') THEN 'BLUE OWL'
            WHEN regexp_like(entity_norm || ' ' || sponsor_norm, '(^| )HTGC( |$)|HERCULES CAPITAL') THEN 'HERCULES'
            WHEN regexp_like(entity_norm || ' ' || sponsor_norm, '(^| )MAIN( |$)|MAIN STREET CAPITAL') THEN 'MAIN STREET'
        END AS proposed_manager,
        CASE
            WHEN regexp_like(entity_norm || ' ' || sponsor_norm || ' ' || asset_type_norm, 'CMBS|CLO|ABS|MORTGAGE|MTG|COMMERCIAL MORTGAGE|144A|PRIVATE CREDIT|DIRECT LENDING|MIDDLE MARKET|MEZZANINE|DISTRESSED|OPPORTUNITY|SPECIAL SITUATIONS|INFRASTRUCTURE|REAL ESTATE|REIT|BDC|LP|L P|FEEDER|FUND [IVXLCDM0-9]+|PARTNERSHIP|JOINT VENTURE|COMMINGLED FUND|HEDGE FUND')
                THEN regexp_extract(entity_norm || ' ' || sponsor_norm || ' ' || asset_type_norm, '(CMBS|CLO|ABS|MORTGAGE|MTG|COMMERCIAL MORTGAGE|144A|PRIVATE CREDIT|DIRECT LENDING|MIDDLE MARKET|MEZZANINE|DISTRESSED|OPPORTUNITY|SPECIAL SITUATIONS|INFRASTRUCTURE|REAL ESTATE|REIT|BDC|LP|L P|FEEDER|FUND [IVXLCDM0-9]+|PARTNERSHIP|JOINT VENTURE|COMMINGLED FUND|HEDGE FUND)', 1)
        END AS fuzzy_strategy_signal,
        CASE
            WHEN regexp_like(entity_norm || ' ' || sponsor_norm, 'CALIFORNIA STWD CMNT|JOBSOHIO|US GOVERNMENT|TREASURY|MUNICIPAL|MUNI|STWD LIQ')
                THEN 'LIKELY_FALSE_POSITIVE'
        END AS negative_signal,
        CASE
            WHEN regexp_like(entity_norm || ' ' || sponsor_norm, '(^| )STWD( |$)') THEN 'matched ticker/abbreviation STWD'
            WHEN regexp_like(entity_norm || ' ' || sponsor_norm, '(^| )BX( |$)') THEN 'matched ticker/abbreviation BX'
            WHEN regexp_like(entity_norm || ' ' || sponsor_norm, '(^| )ARCC( |$)') THEN 'matched ticker ARCC'
            WHEN regexp_like(entity_norm || ' ' || sponsor_norm, '(^| )OBDC( |$)') THEN 'matched ticker OBDC'
            ELSE 'matched abbreviation or noisy manager variant'
        END AS fuzzy_match_reason
    FROM normed
),
classified AS (
    SELECT
        *,
        CASE
            WHEN proposed_manager = 'STARWOOD' AND fuzzy_strategy_signal IN ('MORTGAGE', 'MTG', 'COMMERCIAL MORTGAGE', 'CMBS', 'TRUST', '144A')
                THEN 'Private Credit'
            WHEN fuzzy_strategy_signal IN ('PRIVATE CREDIT', 'DIRECT LENDING', 'MIDDLE MARKET', 'MEZZANINE', 'DISTRESSED', 'SPECIAL SITUATIONS', 'CLO', 'BDC', 'ABS', 'CMBS', 'MORTGAGE', 'MTG', 'COMMERCIAL MORTGAGE')
                THEN 'Private Credit'
            WHEN fuzzy_strategy_signal IN ('REAL ESTATE', 'REIT')
                THEN 'Real Estate'
            WHEN fuzzy_strategy_signal = 'INFRASTRUCTURE'
                THEN 'Infrastructure'
            WHEN fuzzy_strategy_signal IN ('LP', 'L P', 'FEEDER', 'PARTNERSHIP', 'JOINT VENTURE', 'COMMINGLED FUND', 'HEDGE FUND')
                THEN 'Private Markets'
        END AS proposed_asset_class,
        CASE
            WHEN proposed_manager = 'STARWOOD' AND fuzzy_strategy_signal IN ('MORTGAGE', 'MTG', 'COMMERCIAL MORTGAGE', 'CMBS', 'TRUST', '144A')
                THEN 'Structured Credit / Mortgage'
            WHEN fuzzy_strategy_signal IS NOT NULL
                THEN fuzzy_strategy_signal
        END AS proposed_asset_sub_class,
        CASE
            WHEN fuzzy_strategy_signal IN ('CMBS', 'ABS', 'MORTGAGE', 'MTG', 'COMMERCIAL MORTGAGE', 'CLO', '144A')
                THEN 'structured credit'
            WHEN fuzzy_strategy_signal = 'BDC'
                THEN 'public BDC'
            WHEN fuzzy_strategy_signal = 'REIT'
                THEN 'public REIT'
            WHEN fuzzy_strategy_signal IN ('LP', 'L P', 'FEEDER', 'PARTNERSHIP', 'JOINT VENTURE')
                THEN 'private fund'
            ELSE source_asset_type
        END AS proposed_asset_type,
        CASE
            WHEN proposed_manager IS NOT NULL
             AND fuzzy_strategy_signal IS NOT NULL
             AND negative_signal IS NULL
                THEN 'FUZZY_REVIEW'
            WHEN proposed_manager IS NOT NULL
                THEN 'ABBREV_REVIEW'
            ELSE 'LOW'
        END AS confidence_level
    FROM signals
)
SELECT
    source_table,
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
    proposed_manager,
    proposed_asset_type,
    proposed_asset_class,
    proposed_asset_sub_class,
    confidence_level,
    trim(both '; ' FROM concat(
        fuzzy_match_reason || '; ',
        CASE WHEN fuzzy_strategy_signal IS NOT NULL THEN 'strategy=' || fuzzy_strategy_signal || '; ' ELSE '' END,
        CASE WHEN negative_signal IS NOT NULL THEN 'negative=' || negative_signal || '; ' ELSE '' END,
        CASE WHEN source_asset_type IS NOT NULL AND trim(source_asset_type) <> '' THEN 'source_asset_type=' || source_asset_type ELSE '' END
    )) AS match_reason,
    CAST(NULL AS varchar) AS reviewer_manager,
    CAST(NULL AS varchar) AS reviewer_asset_type,
    CAST(NULL AS varchar) AS reviewer_asset_class,
    CAST(NULL AS varchar) AS reviewer_asset_sub_class,
    CAST(NULL AS varchar) AS reviewer_notes
FROM classified
WHERE confidence_level IN ('FUZZY_REVIEW', 'ABBREV_REVIEW')
ORDER BY
    CASE confidence_level WHEN 'FUZZY_REVIEW' THEN 1 ELSE 2 END,
    try_cast(plan_investment_amt AS double) DESC NULLS LAST,
    plan_name_clean,
    raw_entity_name

