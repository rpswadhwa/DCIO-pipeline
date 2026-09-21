WITH source_rows AS (
    SELECT
        'plan_holdings_staging' AS source_table,
        h.ack_id,
        pm.plan_id,
        pm.plan_name_clean,
        pm.filing_year,
        h.raw_entity_name,
        h.raw_sponsor_name,
        CAST(NULL AS varchar) AS normalized_manager,
        CAST(NULL AS varchar) AS normalized_sponsor_name,
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
        c.normalized_manager,
        c.normalized_sponsor_name,
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
        CAST(NULL AS varchar) AS normalized_manager,
        m.normalized_sponsor_name,
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
        upper(trim(regexp_replace(regexp_replace(coalesce(normalized_manager, ''), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS manager_norm,
        upper(trim(regexp_replace(regexp_replace(coalesce(normalized_sponsor_name, ''), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS normalized_sponsor_norm,
        upper(trim(regexp_replace(regexp_replace(coalesce(source_asset_type, ''), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS asset_type_norm
    FROM source_rows
    WHERE raw_entity_name IS NOT NULL OR raw_sponsor_name IS NOT NULL
),
signals AS (
    SELECT
        *,
        regexp_extract(
            entity_norm || ' ' || sponsor_norm || ' ' || manager_norm || ' ' || normalized_sponsor_norm,
            '(^| )(BLACKSTONE|STARWOOD|BROOKFIELD|APOLLO|ARES|KKR|CARLYLE|BAIN|TPG|OAKTREE|BLUE OWL|HAMILTON LANE|HARBOURVEST|PARTNERS GROUP|PANTHEON|ARDIAN|STEPSTONE|GCM GROSVENOR|NEUBERGER BERMAN|HINES|LASALLE|HEITMAN|AEW|CLARION|ANGELO GORDON|OWL ROCK|FS KKR)( |$)',
            2
        ) AS proposed_manager,
        regexp_extract(
            entity_norm || ' ' || sponsor_norm || ' ' || asset_type_norm,
            '(PRIVATE EQUITY|PRIVATE CREDIT|DIRECT LENDING|MIDDLE MARKET|MEZZANINE|DISTRESSED|OPPORTUNITY|OPPORTUNISTIC|SPECIAL SITUATIONS|SECONDAR|BUYOUT|GROWTH EQUITY|VENTURE|REAL ESTATE|INFRASTRUCTURE|NATURAL RESOURCES|TIMBER|FARMLAND|HEDGE FUND|FUND OF FUNDS|CO INVEST|CLO|BDC|REIT|INTERVAL FUND|TENDER OFFER|LIMITED PARTNERSHIP|PARTNERSHIP INTEREST|COMMINGLED FUND|JOINT VENTURE|PRIVATE FUND|MORTGAGE RE|COMMERCIAL MORTGAGE)',
            1
        ) AS strategy_signal,
        CASE
            WHEN regexp_like(entity_norm || ' ' || sponsor_norm, '(^| )FUND [IVXLCDM0-9]+( |$)|(^| )LP( |$)|(^| )L P( |$)|FEEDER|MASTER FUND|OFFSHORE|INSTITUTIONAL|CO INVEST|PRIVATE FUND|CAPITAL CALL')
                THEN 'PRIVATE_FUND_STRUCTURE'
        END AS structure_signal,
        CASE
            WHEN regexp_like(entity_norm || ' ' || sponsor_norm, 'TARGET DATE|REALPATH|STABLE VAL|STABLE VALUE|CORE PLUS BOND|INCOME FUND|ALL ASSET FUND|STOCKPLUS|INDEX|MONEY MARKET|CASH EQUIVALENT|TREASURY|US GOVERNMENT|JOBSOHIO|CALIFORNIA STWD CMNT')
                THEN 'NOISY_OR_PUBLIC_MARKET_SIGNAL'
        END AS negative_signal
    FROM normed
),
classified AS (
    SELECT
        *,
        CASE
            WHEN strategy_signal IN ('BUYOUT', 'GROWTH EQUITY', 'VENTURE', 'SECONDAR', 'CO INVEST', 'PRIVATE EQUITY')
                THEN 'Private Equity'
            WHEN strategy_signal IN ('PRIVATE CREDIT', 'DIRECT LENDING', 'MIDDLE MARKET', 'MEZZANINE', 'DISTRESSED', 'SPECIAL SITUATIONS', 'OPPORTUNITY', 'OPPORTUNISTIC', 'CLO', 'BDC')
                THEN 'Private Credit'
            WHEN strategy_signal IN ('REAL ESTATE', 'REIT', 'MORTGAGE RE', 'COMMERCIAL MORTGAGE')
                THEN 'Real Estate'
            WHEN strategy_signal = 'INFRASTRUCTURE'
                THEN 'Infrastructure'
            WHEN strategy_signal IN ('HEDGE FUND', 'FUND OF FUNDS')
                THEN 'Hedge Fund'
            WHEN strategy_signal IN ('NATURAL RESOURCES', 'TIMBER', 'FARMLAND')
                THEN 'Natural Resources'
            WHEN asset_type_norm IN ('PARTNERSHIP INTEREST', 'PARTNERSHIP JOINT VENTURE INTEREST', 'JOINT VENTURE')
                THEN 'Private Markets'
            WHEN asset_type_norm = 'COMMINGLED FUND'
                THEN 'Commingled Fund'
            WHEN asset_type_norm = 'HEDGE FUND'
                THEN 'Hedge Fund'
            WHEN asset_type_norm = 'REAL ESTATE'
                THEN 'Real Estate'
            WHEN structure_signal IS NOT NULL
                THEN 'Private Markets'
        END AS proposed_asset_class,
        CASE
            WHEN strategy_signal IN ('BUYOUT', 'GROWTH EQUITY', 'VENTURE', 'SECONDAR', 'CO INVEST', 'PRIVATE EQUITY')
                THEN strategy_signal
            WHEN strategy_signal IN ('PRIVATE CREDIT', 'DIRECT LENDING', 'MIDDLE MARKET', 'MEZZANINE', 'DISTRESSED', 'SPECIAL SITUATIONS', 'OPPORTUNITY', 'OPPORTUNISTIC', 'CLO', 'BDC')
                THEN strategy_signal
            WHEN strategy_signal IN ('REAL ESTATE', 'REIT', 'MORTGAGE RE', 'COMMERCIAL MORTGAGE')
                THEN strategy_signal
            WHEN strategy_signal = 'INFRASTRUCTURE'
                THEN 'INFRASTRUCTURE'
            WHEN strategy_signal IN ('HEDGE FUND', 'FUND OF FUNDS')
                THEN strategy_signal
            WHEN strategy_signal IN ('NATURAL RESOURCES', 'TIMBER', 'FARMLAND')
                THEN strategy_signal
            WHEN structure_signal IS NOT NULL
                THEN structure_signal
            WHEN source_asset_type IS NOT NULL AND trim(source_asset_type) <> ''
                THEN source_asset_type
        END AS proposed_asset_sub_class,
        CASE
            WHEN asset_type_norm IN ('PARTNERSHIP INTEREST', 'PARTNERSHIP JOINT VENTURE INTEREST', 'JOINT VENTURE')
                THEN 'partnership/joint venture interest'
            WHEN asset_type_norm = 'COMMINGLED FUND'
                THEN 'commingled fund'
            WHEN asset_type_norm = 'HEDGE FUND'
                THEN 'hedge fund'
            WHEN asset_type_norm = 'REAL ESTATE'
                THEN 'real estate'
            WHEN structure_signal IS NOT NULL
                THEN 'private fund'
            WHEN strategy_signal = 'REIT'
                THEN 'public REIT'
            WHEN strategy_signal IN ('CLO', 'BDC', 'MORTGAGE RE', 'COMMERCIAL MORTGAGE')
                THEN 'credit/security'
            ELSE source_asset_type
        END AS proposed_asset_type
    FROM signals
),
scored AS (
    SELECT
        *,
        CASE
            WHEN proposed_manager IS NOT NULL
             AND proposed_asset_class IS NOT NULL
             AND negative_signal IS NULL
                THEN 'HIGH'
            WHEN proposed_asset_class IS NOT NULL
             AND negative_signal IS NULL
                THEN 'MEDIUM'
            WHEN proposed_manager IS NOT NULL OR proposed_asset_class IS NOT NULL OR structure_signal IS NOT NULL
                THEN 'REVIEW'
            ELSE 'LOW'
        END AS confidence_level
    FROM classified
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
        CASE WHEN proposed_manager IS NOT NULL THEN 'manager=' || proposed_manager || '; ' ELSE '' END,
        CASE WHEN strategy_signal IS NOT NULL THEN 'strategy=' || strategy_signal || '; ' ELSE '' END,
        CASE WHEN structure_signal IS NOT NULL THEN 'structure=' || structure_signal || '; ' ELSE '' END,
        CASE WHEN negative_signal IS NOT NULL THEN 'negative=' || negative_signal || '; ' ELSE '' END,
        CASE WHEN source_asset_type IS NOT NULL AND trim(source_asset_type) <> '' THEN 'source_asset_type=' || source_asset_type ELSE '' END
    )) AS match_reason,
    CAST(NULL AS varchar) AS reviewer_manager,
    CAST(NULL AS varchar) AS reviewer_asset_type,
    CAST(NULL AS varchar) AS reviewer_asset_class,
    CAST(NULL AS varchar) AS reviewer_asset_sub_class,
    CAST(NULL AS varchar) AS reviewer_notes
FROM scored
WHERE confidence_level = 'HIGH'
ORDER BY
    try_cast(plan_investment_amt AS double) DESC NULLS LAST,
    plan_name_clean,
    raw_entity_name
