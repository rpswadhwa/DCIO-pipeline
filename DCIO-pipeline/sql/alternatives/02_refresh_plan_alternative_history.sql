-- Refresh promoted non-MF/non-CIT alternative holdings from plan_holdings_staging.
--
-- Dedupe policy:
--   1. Exclude rows already represented in plan_cit_history or plan_mf_history_v3
--      by exact normalized name match within the same ack_id.
--   2. Exclude Alabama-style near duplicates when same ack_id, same amount, and
--      normalized names contain one another after common suffix cleanup.
--   3. Keep rows with only weak amount/name signals out of the promoted table.
--
-- This script is intentionally delete+insert because the source is a derived
-- candidate population. The table preserves row-level dedupe metadata.

DELETE FROM default.plan_alternative_history;

INSERT INTO default.plan_alternative_history (
  ack_id,
  plan_id,
  filing_year,
  source_table,
  source_asset_type,
  source_asset_class,
  source_asset_sub_class,
  validation_status,
  raw_entity_name,
  raw_sponsor_name,
  raw_entity_name_norm,
  raw_sponsor_name_norm,
  normalized_manager,
  plan_investment_amt,
  vehicle_type,
  asset_class_family,
  asset_class,
  asset_sub_class,
  classification_confidence,
  match_method,
  match_reason,
  dedupe_status,
  dedupe_match_table,
  dedupe_match_method,
  dedupe_match_raw_entity_name,
  dedupe_match_raw_sponsor_name,
  dedupe_match_amount,
  cit_alt_exists_for_ack,
  mf_alt_exists_for_ack,
  review_status,
  reviewed_manager,
  reviewed_vehicle_type,
  reviewed_asset_class,
  reviewed_asset_sub_class,
  reviewer_notes,
  source_row_fingerprint,
  created_at,
  updated_at
)
WITH staging_norm AS (
  SELECT
    h.ack_id,
    pm.plan_id,
    pm.filing_year,
    h.asset_type AS source_asset_type,
    h.asset_class AS source_asset_class,
    h.asset_sub_class AS source_asset_sub_class,
    h.validation_status,
    h.raw_entity_name,
    h.raw_sponsor_name,
    h.plan_investment_amt,
    upper(trim(regexp_replace(regexp_replace(coalesce(h.raw_entity_name, ''), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS entity_norm,
    upper(trim(regexp_replace(regexp_replace(coalesce(h.raw_sponsor_name, ''), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS sponsor_norm,
    upper(trim(regexp_replace(regexp_replace(coalesce(h.asset_type, ''), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS asset_type_norm
  FROM default.plan_holdings_staging h
  LEFT JOIN default.plan_master pm
    ON pm.ack_id = h.ack_id
  WHERE h.raw_entity_name IS NOT NULL OR h.raw_sponsor_name IS NOT NULL
),
signals AS (
  SELECT
    *,
    regexp_extract(
      entity_norm || ' ' || sponsor_norm,
      '(^| )(BLACKSTONE|STARWOOD|BROOKFIELD|APOLLO|ARES|KKR|CARLYLE|BAIN|TPG|OAKTREE|BLUE OWL|HAMILTON LANE|HARBOURVEST|PARTNERS GROUP|PANTHEON|ARDIAN|STEPSTONE|GCM GROSVENOR|NEUBERGER BERMAN|HINES|LASALLE|HEITMAN|AEW|CLARION|ANGELO GORDON|OWL ROCK|FS KKR|MAIN STREET|HERCULES)( |$)',
      2
    ) AS manager_signal,
    CASE
      WHEN regexp_like(entity_norm || ' ' || sponsor_norm || ' ' || asset_type_norm, 'PRIVATE EQUITY|BUYOUT|GROWTH EQUITY|VENTURE|SECONDAR|CO INVEST') THEN 'Private Equity'
      WHEN regexp_like(entity_norm || ' ' || sponsor_norm || ' ' || asset_type_norm, 'PRIVATE CREDIT|DIRECT LENDING|MIDDLE MARKET|MEZZANINE|DISTRESSED|SPECIAL SITUATIONS|CLO|BDC|ABS|CMBS|MORTGAGE|COMMERCIAL MORTGAGE') THEN 'Private Credit'
      WHEN regexp_like(entity_norm || ' ' || sponsor_norm || ' ' || asset_type_norm, 'REAL ESTATE|REIT|PROPERTY') THEN 'Real Estate'
      WHEN regexp_like(entity_norm || ' ' || sponsor_norm || ' ' || asset_type_norm, 'INFRASTRUCTURE') THEN 'Infrastructure'
      WHEN regexp_like(entity_norm || ' ' || sponsor_norm || ' ' || asset_type_norm, 'HEDGE FUND|FUND OF FUNDS') THEN 'Hedge Fund'
      WHEN regexp_like(entity_norm || ' ' || sponsor_norm || ' ' || asset_type_norm, 'NATURAL RESOURCES|TIMBER|FARMLAND') THEN 'Natural Resources'
      WHEN asset_type_norm IN ('PARTNERSHIP INTEREST', 'PARTNERSHIP JOINT VENTURE INTEREST', 'JOINT VENTURE', 'COMMINGLED FUND', 'COMMON COLLECTIVE TRUST FUND') THEN 'Private Markets'
      WHEN regexp_like(entity_norm || ' ' || sponsor_norm, '(^| )FUND [IVXLCDM0-9]+( |$)|(^| )LP( |$)|(^| )L P( |$)|FEEDER|MASTER FUND|OFFSHORE|PRIVATE FUND|CAPITAL CALL') THEN 'Private Markets'
    END AS inferred_asset_class,
    regexp_extract(
      entity_norm || ' ' || sponsor_norm || ' ' || asset_type_norm,
      '(PRIVATE EQUITY|BUYOUT|GROWTH EQUITY|VENTURE|SECONDAR|CO INVEST|PRIVATE CREDIT|DIRECT LENDING|MIDDLE MARKET|MEZZANINE|DISTRESSED|SPECIAL SITUATIONS|CLO|BDC|ABS|CMBS|MORTGAGE|COMMERCIAL MORTGAGE|REAL ESTATE|REIT|PROPERTY|INFRASTRUCTURE|HEDGE FUND|FUND OF FUNDS|NATURAL RESOURCES|TIMBER|FARMLAND)',
      1
    ) AS strategy_signal,
    CASE
      WHEN regexp_like(entity_norm || ' ' || sponsor_norm, '(^| )FUND [IVXLCDM0-9]+( |$)|(^| )LP( |$)|(^| )L P( |$)|FEEDER|MASTER FUND|OFFSHORE|PRIVATE FUND|CAPITAL CALL') THEN true
      ELSE false
    END AS structure_signal,
    CASE
      WHEN regexp_like(entity_norm || ' ' || sponsor_norm, 'TARGET DATE|REALPATH|STABLE VAL|STABLE VALUE|CORE PLUS BOND|INCOME FUND|ALL ASSET FUND|STOCKPLUS|INDEX|MONEY MARKET|CASH EQUIVALENT|TREASURY|US GOVERNMENT|JOBSOHIO|CALIFORNIA STWD CMNT') THEN true
      ELSE false
    END AS noisy_signal
  FROM staging_norm
),
classified AS (
  SELECT
    *,
    CASE
      WHEN regexp_like(entity_norm || ' ' || sponsor_norm, '(^| )STWD( |$)|(^| )BX( |$)|(^| )ARCC( |$)|(^| )OBDC( |$)') THEN 'FUZZY_REVIEW'
      WHEN manager_signal IS NOT NULL AND inferred_asset_class IS NOT NULL AND noisy_signal = false THEN 'HIGH'
      WHEN inferred_asset_class IS NOT NULL AND noisy_signal = false THEN 'MEDIUM'
      WHEN manager_signal IS NOT NULL OR inferred_asset_class IS NOT NULL OR structure_signal THEN 'REVIEW'
      ELSE 'LOW'
    END AS classification_confidence,
    CASE
      WHEN regexp_like(entity_norm || ' ' || sponsor_norm, '(^| )STWD( |$)|(^| )BX( |$)|(^| )ARCC( |$)|(^| )OBDC( |$)') THEN 'ABBREVIATION_OR_TICKER'
      WHEN manager_signal IS NOT NULL THEN 'MANAGER_AND_ALT_SIGNAL'
      WHEN inferred_asset_class IS NOT NULL THEN 'STRATEGY_OR_STRUCTURE_SIGNAL'
      ELSE 'UNCLASSIFIED'
    END AS match_method,
    CASE
      WHEN regexp_like(entity_norm || ' ' || sponsor_norm || ' ' || asset_type_norm, 'REIT') THEN 'Public REIT'
      WHEN regexp_like(entity_norm || ' ' || sponsor_norm || ' ' || asset_type_norm, 'BDC') THEN 'Public BDC'
      WHEN regexp_like(entity_norm || ' ' || sponsor_norm || ' ' || asset_type_norm, 'CLO|ABS|CMBS|MORTGAGE|COMMERCIAL MORTGAGE|144A') THEN 'Structured Credit'
      WHEN regexp_like(entity_norm || ' ' || sponsor_norm, '(^| )LP( |$)|(^| )L P( |$)|FEEDER|PRIVATE FUND|CAPITAL CALL') THEN 'Private Fund'
      WHEN asset_type_norm = 'COMMINGLED FUND' THEN 'Commingled Fund'
      WHEN asset_type_norm IN ('COMMON COLLECTIVE TRUST FUND', 'COMMON COLLECTIVE') THEN 'Common/Collective Trust Fund'
      WHEN asset_type_norm IN ('PARTNERSHIP INTEREST', 'PARTNERSHIP JOINT VENTURE INTEREST', 'JOINT VENTURE') THEN 'Partnership / Joint Venture'
      ELSE coalesce(source_asset_type, 'Other')
    END AS vehicle_type,
    coalesce(strategy_signal, CASE WHEN structure_signal THEN 'Private Fund Structure' END, source_asset_sub_class) AS inferred_asset_sub_class
  FROM signals
),
candidate AS (
  SELECT *
  FROM classified
  WHERE classification_confidence IN ('HIGH', 'MEDIUM', 'FUZZY_REVIEW')
    AND coalesce(lower(source_asset_type), '') NOT IN ('mutual fund', 'mf', 'cit')
),
cit_norm AS (
  SELECT
    ack_id,
    raw_entity_name,
    raw_sponsor_name,
    plan_investment_amt,
    upper(trim(regexp_replace(regexp_replace(coalesce(raw_entity_name, ''), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS entity_norm,
    upper(trim(regexp_replace(regexp_replace(coalesce(raw_sponsor_name, ''), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS sponsor_norm,
    upper(trim(regexp_replace(regexp_replace(regexp_replace(coalesce(raw_entity_name, ''), '(?i)\\b(ACCT|ACCOUNT|FUND|TRUST|CL|CLASS|FD)\\b', ' '), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS entity_core_norm,
    upper(trim(regexp_replace(regexp_replace(regexp_replace(coalesce(raw_sponsor_name, ''), '(?i)\\b(ACCT|ACCOUNT|FUND|TRUST|CL|CLASS|FD)\\b', ' '), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS sponsor_core_norm
  FROM default.plan_cit_history
),
mf_norm AS (
  SELECT
    ack_id,
    raw_entity_name,
    raw_sponsor_name,
    plan_investment_amt,
    upper(trim(regexp_replace(regexp_replace(coalesce(raw_entity_name, ''), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS entity_norm,
    upper(trim(regexp_replace(regexp_replace(coalesce(raw_sponsor_name, ''), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS sponsor_norm,
    upper(trim(regexp_replace(regexp_replace(regexp_replace(coalesce(raw_entity_name, ''), '(?i)\\b(ACCT|ACCOUNT|FUND|TRUST|CL|CLASS|FD)\\b', ' '), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS entity_core_norm,
    upper(trim(regexp_replace(regexp_replace(regexp_replace(coalesce(raw_sponsor_name, ''), '(?i)\\b(ACCT|ACCOUNT|FUND|TRUST|CL|CLASS|FD)\\b', ' '), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS sponsor_core_norm
  FROM default.plan_mf_history_v3
),
candidate_core AS (
  SELECT
    *,
    upper(trim(regexp_replace(regexp_replace(regexp_replace(coalesce(raw_entity_name, ''), '(?i)\\b(ACCT|ACCOUNT|FUND|TRUST|CL|CLASS|FD)\\b', ' '), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS entity_core_norm,
    upper(trim(regexp_replace(regexp_replace(regexp_replace(coalesce(raw_sponsor_name, ''), '(?i)\\b(ACCT|ACCOUNT|FUND|TRUST|CL|CLASS|FD)\\b', ' '), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS sponsor_core_norm
  FROM candidate
),
deduped AS (
  SELECT
    c.*,
    coalesce(cit_exact.raw_entity_name, mf_exact.raw_entity_name, cit_near.raw_entity_name, mf_near.raw_entity_name) AS dedupe_match_raw_entity_name,
    coalesce(cit_exact.raw_sponsor_name, mf_exact.raw_sponsor_name, cit_near.raw_sponsor_name, mf_near.raw_sponsor_name) AS dedupe_match_raw_sponsor_name,
    coalesce(cit_exact.plan_investment_amt, mf_exact.plan_investment_amt, cit_near.plan_investment_amt, mf_near.plan_investment_amt) AS dedupe_match_amount,
    CASE
      WHEN cit_exact.ack_id IS NOT NULL THEN 'DUPLICATE_CIT_EXACT_NAME'
      WHEN mf_exact.ack_id IS NOT NULL THEN 'DUPLICATE_MF_EXACT_NAME'
      WHEN cit_near.ack_id IS NOT NULL THEN 'DUPLICATE_CIT_AMOUNT_NEAR_NAME'
      WHEN mf_near.ack_id IS NOT NULL THEN 'DUPLICATE_MF_AMOUNT_NEAR_NAME'
      ELSE 'NEW_OTHER_ASSET_TYPE'
    END AS dedupe_status,
    CASE
      WHEN cit_exact.ack_id IS NOT NULL OR cit_near.ack_id IS NOT NULL THEN 'plan_cit_history'
      WHEN mf_exact.ack_id IS NOT NULL OR mf_near.ack_id IS NOT NULL THEN 'plan_mf_history_v3'
    END AS dedupe_match_table,
    CASE
      WHEN cit_exact.ack_id IS NOT NULL OR mf_exact.ack_id IS NOT NULL THEN 'ACK_ID_NORMALIZED_NAME'
      WHEN cit_near.ack_id IS NOT NULL OR mf_near.ack_id IS NOT NULL THEN 'ACK_ID_SAME_AMOUNT_CORE_NAME_CONTAINS'
    END AS dedupe_match_method
  FROM candidate_core c
  LEFT JOIN cit_norm cit_exact
    ON cit_exact.ack_id = c.ack_id
   AND (c.entity_norm = cit_exact.entity_norm OR c.sponsor_norm = cit_exact.entity_norm OR c.entity_norm = cit_exact.sponsor_norm OR c.sponsor_norm = cit_exact.sponsor_norm)
  LEFT JOIN mf_norm mf_exact
    ON mf_exact.ack_id = c.ack_id
   AND (c.entity_norm = mf_exact.entity_norm OR c.sponsor_norm = mf_exact.entity_norm OR c.entity_norm = mf_exact.sponsor_norm OR c.sponsor_norm = mf_exact.sponsor_norm)
  LEFT JOIN cit_norm cit_near
    ON cit_exact.ack_id IS NULL
   AND cit_near.ack_id = c.ack_id
   AND try_cast(c.plan_investment_amt AS decimal(18,2)) = try_cast(cit_near.plan_investment_amt AS decimal(18,2))
   AND (
        (length(c.entity_core_norm) >= 10 AND length(cit_near.entity_core_norm) >= 10 AND (strpos(c.entity_core_norm, cit_near.entity_core_norm) > 0 OR strpos(cit_near.entity_core_norm, c.entity_core_norm) > 0))
     OR (length(c.sponsor_core_norm) >= 10 AND length(cit_near.entity_core_norm) >= 10 AND (strpos(c.sponsor_core_norm, cit_near.entity_core_norm) > 0 OR strpos(cit_near.entity_core_norm, c.sponsor_core_norm) > 0))
   )
  LEFT JOIN mf_norm mf_near
    ON mf_exact.ack_id IS NULL
   AND mf_near.ack_id = c.ack_id
   AND try_cast(c.plan_investment_amt AS decimal(18,2)) = try_cast(mf_near.plan_investment_amt AS decimal(18,2))
   AND (
        (length(c.entity_core_norm) >= 10 AND length(mf_near.entity_core_norm) >= 10 AND (strpos(c.entity_core_norm, mf_near.entity_core_norm) > 0 OR strpos(mf_near.entity_core_norm, c.entity_core_norm) > 0))
     OR (length(c.sponsor_core_norm) >= 10 AND length(mf_near.entity_core_norm) >= 10 AND (strpos(c.sponsor_core_norm, mf_near.entity_core_norm) > 0 OR strpos(mf_near.entity_core_norm, c.sponsor_core_norm) > 0))
   )
),
ack_flags AS (
  SELECT
    d.*,
    EXISTS (
      SELECT 1
      FROM default.plan_cit_history cit
      WHERE cit.ack_id = d.ack_id
        AND regexp_like(upper(coalesce(cit.asset_class, '') || ' ' || coalesce(cit.asset_sub_class, '') || ' ' || coalesce(cit.raw_entity_name, '') || ' ' || coalesce(cit.normalized_manager, '')),
            'ALTERNATIVE|PRIVATE EQUITY|PRIVATE CREDIT|REAL ESTATE|INFRASTRUCTURE|HEDGE|NATURAL RESOURCES|VENTURE|BUYOUT|MEZZANINE|DIRECT LENDING|OPPORTUNISTIC|SPECIAL SITUATIONS|BLACKSTONE|STARWOOD|BROOKFIELD|APOLLO|ARES|KKR|CARLYLE|BAIN|TPG|OAKTREE|HAMILTON LANE|HARBOURVEST|PARTNERS GROUP')
    ) AS cit_alt_exists_for_ack,
    EXISTS (
      SELECT 1
      FROM default.plan_mf_history_v3 mf
      WHERE mf.ack_id = d.ack_id
        AND regexp_like(upper(coalesce(mf.asset_class, '') || ' ' || coalesce(mf.asset_sub_class, '') || ' ' || coalesce(mf.raw_entity_name, '') || ' ' || coalesce(mf.normalized_sponsor_name, '')),
            'ALTERNATIVE|PRIVATE EQUITY|PRIVATE CREDIT|REAL ESTATE|INFRASTRUCTURE|HEDGE|NATURAL RESOURCES|VENTURE|BUYOUT|MEZZANINE|DIRECT LENDING|OPPORTUNISTIC|SPECIAL SITUATIONS|BLACKSTONE|STARWOOD|BROOKFIELD|APOLLO|ARES|KKR|CARLYLE|BAIN|TPG|OAKTREE|HAMILTON LANE|HARBOURVEST|PARTNERS GROUP')
    ) AS mf_alt_exists_for_ack
  FROM deduped d
)
SELECT
  ack_id,
  plan_id,
  filing_year,
  'plan_holdings_staging' AS source_table,
  source_asset_type,
  source_asset_class,
  source_asset_sub_class,
  validation_status,
  raw_entity_name,
  raw_sponsor_name,
  entity_norm AS raw_entity_name_norm,
  sponsor_norm AS raw_sponsor_name_norm,
  manager_signal AS normalized_manager,
  plan_investment_amt,
  vehicle_type,
  'Alternatives' AS asset_class_family,
  inferred_asset_class AS asset_class,
  inferred_asset_sub_class AS asset_sub_class,
  classification_confidence,
  match_method,
  trim(both '; ' FROM concat(
    CASE WHEN manager_signal IS NOT NULL THEN 'manager=' || manager_signal || '; ' ELSE '' END,
    CASE WHEN strategy_signal IS NOT NULL THEN 'strategy=' || strategy_signal || '; ' ELSE '' END,
    CASE WHEN structure_signal THEN 'structure=PRIVATE_FUND_STRUCTURE; ' ELSE '' END,
    CASE WHEN source_asset_type IS NOT NULL THEN 'source_asset_type=' || source_asset_type ELSE '' END
  )) AS match_reason,
  dedupe_status,
  dedupe_match_table,
  dedupe_match_method,
  dedupe_match_raw_entity_name,
  dedupe_match_raw_sponsor_name,
  dedupe_match_amount,
  cit_alt_exists_for_ack,
  mf_alt_exists_for_ack,
  CASE WHEN classification_confidence = 'FUZZY_REVIEW' THEN 'PENDING_REVIEW' ELSE 'MODEL_CLASSIFIED' END AS review_status,
  CAST(NULL AS varchar) AS reviewed_manager,
  CAST(NULL AS varchar) AS reviewed_vehicle_type,
  CAST(NULL AS varchar) AS reviewed_asset_class,
  CAST(NULL AS varchar) AS reviewed_asset_sub_class,
  CAST(NULL AS varchar) AS reviewer_notes,
  md5(coalesce(ack_id, '') || '|' || coalesce(raw_entity_name, '') || '|' || coalesce(raw_sponsor_name, '') || '|' || coalesce(cast(plan_investment_amt AS varchar), '') || '|' || coalesce(source_asset_type, '')) AS source_row_fingerprint,
  current_timestamp AS created_at,
  current_timestamp AS updated_at
FROM ack_flags
WHERE dedupe_status = 'NEW_OTHER_ASSET_TYPE';
