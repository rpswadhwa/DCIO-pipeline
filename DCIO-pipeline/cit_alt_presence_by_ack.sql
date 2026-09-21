WITH target_ack AS (
    SELECT DISTINCT ack_id
    FROM (
        SELECT h.ack_id
        FROM default.plan_holdings_staging h
        WHERE lower(coalesce(h.asset_type, '')) IN ('common/collective trust fund', 'commingled fund')
          AND (
              regexp_like(upper(coalesce(h.raw_entity_name, '') || ' ' || coalesce(h.raw_sponsor_name, '')),
                  'BLACKSTONE|STARWOOD|BROOKFIELD|APOLLO|ARES|KKR|CARLYLE|BAIN|TPG|OAKTREE|BLUE OWL|HAMILTON LANE|HARBOURVEST|PARTNERS GROUP|PANTHEON|ARDIAN|STEPSTONE|GCM GROSVENOR|NEUBERGER BERMAN|HINES|LASALLE|HEITMAN|AEW|CLARION|ANGELO GORDON|OWL ROCK|FS KKR')
              OR regexp_like(upper(coalesce(h.raw_entity_name, '') || ' ' || coalesce(h.raw_sponsor_name, '') || ' ' || coalesce(h.asset_type, '')),
                  'PRIVATE EQUITY|PRIVATE CREDIT|DIRECT LENDING|MIDDLE MARKET|MEZZANINE|DISTRESSED|OPPORTUNITY|OPPORTUNISTIC|SPECIAL SITUATIONS|SECONDAR|BUYOUT|GROWTH EQUITY|VENTURE|REAL ESTATE|INFRASTRUCTURE|NATURAL RESOURCES|TIMBER|FARMLAND|HEDGE FUND|FUND OF FUNDS|CO INVEST|CLO|BDC|REIT|INTERVAL FUND|TENDER OFFER|LIMITED PARTNERSHIP|PARTNERSHIP INTEREST|JOINT VENTURE|PRIVATE FUND')
          )
    )
),
cit_rows AS (
    SELECT
        c.ack_id,
        c.plan_id,
        pm.plan_name_clean,
        c.filing_year,
        c.raw_entity_name,
        c.raw_sponsor_name,
        c.normalized_manager,
        c.asset_class,
        c.asset_sub_class,
        c.plan_investment_amt,
        upper(trim(regexp_replace(regexp_replace(coalesce(c.raw_entity_name, ''), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS entity_norm,
        upper(trim(regexp_replace(regexp_replace(coalesce(c.raw_sponsor_name, ''), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS sponsor_norm,
        upper(trim(regexp_replace(regexp_replace(coalesce(c.normalized_manager, ''), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS manager_norm,
        upper(trim(regexp_replace(regexp_replace(coalesce(c.asset_class, ''), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS asset_class_norm,
        upper(trim(regexp_replace(regexp_replace(coalesce(c.asset_sub_class, ''), '[^A-Z0-9 ]+', ' '), '\\s+', ' '))) AS asset_sub_class_norm
    FROM default.plan_cit_history c
    INNER JOIN target_ack t
      ON t.ack_id = c.ack_id
    LEFT JOIN default.plan_master pm
      ON pm.ack_id = c.ack_id
     AND pm.filing_year = c.filing_year
),
scored AS (
    SELECT
        *,
        CASE
            WHEN regexp_like(asset_class_norm || ' ' || asset_sub_class_norm,
                'ALTERNATIVE|PRIVATE EQUITY|PRIVATE CREDIT|REAL ESTATE|INFRASTRUCTURE|HEDGE|NATURAL RESOURCES|TIMBER|FARMLAND|VENTURE|BUYOUT|MEZZANINE|DIRECT LENDING|OPPORTUNISTIC|SPECIAL SITUATIONS')
                THEN 'CLASSIFIED_ALT'
            WHEN regexp_like(entity_norm || ' ' || sponsor_norm || ' ' || manager_norm,
                'BLACKSTONE|STARWOOD|BROOKFIELD|APOLLO|ARES|KKR|CARLYLE|BAIN|TPG|OAKTREE|BLUE OWL|HAMILTON LANE|HARBOURVEST|PARTNERS GROUP|PANTHEON|ARDIAN|STEPSTONE|GCM GROSVENOR|NEUBERGER BERMAN|HINES|LASALLE|HEITMAN|AEW|CLARION|ANGELO GORDON|OWL ROCK|FS KKR')
                THEN 'MANAGER_ALT_SIGNAL'
            WHEN regexp_like(entity_norm || ' ' || sponsor_norm,
                'PRIVATE EQUITY|PRIVATE CREDIT|DIRECT LENDING|MIDDLE MARKET|MEZZANINE|DISTRESSED|OPPORTUNITY|OPPORTUNISTIC|SPECIAL SITUATIONS|SECONDAR|BUYOUT|GROWTH EQUITY|VENTURE|REAL ESTATE|INFRASTRUCTURE|NATURAL RESOURCES|TIMBER|FARMLAND|HEDGE FUND|FUND OF FUNDS|CO INVEST|CLO|BDC|REIT|INTERVAL FUND|TENDER OFFER|LIMITED PARTNERSHIP|PARTNERSHIP INTEREST|JOINT VENTURE|PRIVATE FUND')
                THEN 'NAME_ALT_SIGNAL'
        END AS cit_alt_signal
    FROM cit_rows
)
SELECT
    ack_id,
    max(plan_id) AS plan_id,
    max(plan_name_clean) AS plan_name_clean,
    count(*) AS cit_total_rows,
    count_if(cit_alt_signal IS NOT NULL) AS cit_alt_signal_rows,
    cast(sum(CASE WHEN cit_alt_signal IS NOT NULL THEN try_cast(plan_investment_amt AS double) ELSE 0 END) AS decimal(18,2)) AS cit_alt_signal_amount,
    array_join(array_sort(array_distinct(array_agg(cit_alt_signal) FILTER (WHERE cit_alt_signal IS NOT NULL))), '; ') AS cit_alt_signal_types
FROM scored
GROUP BY ack_id
ORDER BY cit_alt_signal_rows DESC, cit_alt_signal_amount DESC NULLS LAST
