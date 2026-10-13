"""Pure manager rules snapshotted from the active ai-cit job.
No Glue initialization, credentials, LLM calls, or writes.
Source SHA256: 55add93b63b1853f9875885b5aa659dac0141ea591de0bf6c80a309c3d9ab984
"""
from __future__ import annotations
import re
from typing import Optional, Tuple, List, Dict
Override = Tuple[str, float, str]
CANON_LOOKUP = {}
ALL_CANONICAL_TOKENS = []

AM_SPONSOR_HINTS = ['ASSET MANAGEMENT', 'INVESTMENT MANAGEMENT', 'INVESTMENT MGMT', 'INVESTMENT MANAGERS', 'INVESTMENT ADVISERS', 'INVESTMENT ADVISORS', 'CAPITAL MANAGEMENT', 'GLOBAL INVESTORS', 'ASSET MANAGERS', 'WEALTH MANAGEMENT']

JPM_SPONSOR_EXTRA_HINTS = ['ASSET', 'INVEST', 'ADVISOR', 'ADVISER', 'MANAGEMENT']

PLATFORM_SPONSOR_BLOCKLIST = ['TRUST', 'TRUST COMPANY', 'BANK', 'CUSTOD', 'CUSTODY', 'INSURANCE', 'LIFE', 'ANNUITY', 'RETIREMENT', 'COLLECTIVE', 'CTF', 'CIT', 'SEPARATE ACCOUNT', 'RECORDKEEP', 'PLAN', 'BENEFITS']

RULES: List[Dict] = [{'rule_type': 'SPONSOR', 'match': {'contains_any': ['BLACKROCK INSTITUTIONAL TRUST']}, 'result': {'manager': 'Blackrock', 'conf': 0.98}, 'tag': 'hard_sponsor:blackrock_proprietary_trust'}, {'rule_type': 'ENTITY', 'match': {'startswith_any': ['TRANSAMERICA ']}, 'dispatch': [{'when': {'contains_any': [' WITH BLK', ' WITH BLKRCK', ' WITH BLA', ' WITH BLACK']}, 'result': {'manager': 'Blackrock', 'conf': 0.98}, 'tag': 'hard_entity:transamerica_with_blackrock_override'}], 'fallback': {'result': {'manager': 'Transamerica Asset Management', 'conf': 0.93}, 'tag': 'hard_entity:transamerica_branded_fallback'}}, {'rule_type': 'ENTITY', 'match': {'startswith_any': ['NIS ']}, 'result': {'manager': 'National Investment Services (NIS)', 'conf': 0.98}, 'tag': 'hard_rule:nis_prefix_with_space', 'sponsor_guard': {'block_if_contains_any': ['NISA']}}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['DB SL', 'M DB SL', 'DB SL', 'NSL', 'BNY MELLON NSL']}, 'dispatch': [{'when': {'contains_any': [' WITH BLK', ' WITH BLKRCK', ' BLACKROCK', ' BLKRCK']}, 'result': {'manager': 'Blackrock', 'conf': 0.98}, 'tag': 'hard_entity:bnym_program_with_blk'}], 'fallback': {'result': {'manager': 'BNY Mellon Investment Management', 'conf': 0.9}, 'tag': 'hard_entity:bnym_program_default_bnymim'}}, {'rule_type': 'ENTITY', 'match': {'startswith_any': ['NT '], 'contains_any': ['INDEX', 'INDX', 'SANDP', 'S&P', 'SP 500', 'AGG', 'AGGREG', 'COLLECTIVE', 'CIT', 'STIF']}, 'result': {'manager': 'Northern Trust Asset Management', 'conf': 0.93}, 'tag': 'hard_entity:nt_prefix_index_collective'}, {'rule_type': 'ENTITY', 'match': {'startswith_any': ['NB '], 'contains_any': ['CAP', 'EQUITY', 'VALUE', 'GROWTH', 'BOND', 'FIXED', 'CORE']}, 'result': {'manager': 'Neuberger Berman', 'conf': 0.95}, 'tag': 'hard_entity:nb_neuberger_berman_strict'}, {'rule_type': 'ENTITY', 'match': {'startswith_any': ['SS ', 'SSGA ', 'SSTR ', 'SSC ', 'SSB ', 'SSR ', 'SSG '], 'contains_any': ['INDEX', 'IDX', 'S&P', 'SANDP', 'RUSSELL', 'RSL', 'RUSL', 'RUSS', 'MSCI', 'EAFE', 'TARGET', 'TRGT', 'TGT', 'RETIRE', 'RTMT', 'BOND', 'EQUITY', 'EQTY', 'AGGREGATE', 'REAL ASSET', 'TREASURY', 'MONEY MARKET', 'EMERGING', 'EMRG', 'GACEQ', 'ACWI', 'REIT', 'INFL PRO', 'TIPS', 'STRIPS', 'WORLD']}, 'result': {'manager': 'State Street Global Advisors', 'conf': 0.99}, 'tag': 'hard_rule:ssga_exhaustive_signature_logic'}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['INDEXSELECT', 'INDEX SELECT', 'FUTUREFUNDS']}, 'result': {'manager': 'Empower (Advised Assets Group)', 'conf': 0.98}, 'tag': 'hard_rule:empower_proprietary_decisioning'}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['DRIVE INCOME', 'DRIVE BALANCED', 'DRIVE BAL'], 'contains_any_2': ['EMPOWER', 'AAG', 'GREAT-WEST', 'GWL']}, 'result': {'manager': 'Empower (Advised Assets Group)', 'conf': 0.96}, 'tag': 'hard_rule:empower_drive_models'}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['WTC', 'WILMINGTON TRUST', 'WILMGT TR', 'GREAT GRAY', 'GREAT GREY', 'WELLINGTON MANAGEMENT', 'WMC ', 'WGPFM']}, 'dispatch': [{'when': {'contains_any': ['-CIF', '-CTF', 'WTC - ']}, 'manager': 'Wellington Management', 'conf': 0.99, 'tag': 'hard_rule:wellington_trust_proprietary'}, {'when': {'contains_any': ['EUROPAC', 'AF ', 'AMERICAN FUNDS']}, 'manager': 'Capital Group', 'conf': 0.98, 'tag': 'hard_rule:wilmington_capital_group_lookthrough'}, {'when': {'contains_any': ['FXP', 'FXPTH', 'FLEXPATH', 'FLEX FOCUS', 'FLEXFIT']}, 'manager': 'Great Gray Trust Company', 'conf': 0.96, 'tag': 'hard_rule:wilmington_flexpath_lookthrough'}, {'when': {'contains_any': ['GAL RET', 'G RETIRE']}, 'manager': 'Great Gray Trust Company', 'conf': 0.92, 'tag': 'hard_rule:wilmington_gallagher_lookthrough'}], 'fallback': {'manager': 'Great Gray Trust Company', 'conf': 0.85, 'tag': 'hard_rule:wilmington_generic_fallback'}}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['EUROPAC', 'EUROPACIFIC', 'EUROPACFC'], 'contains_all': ['GROWTH']}, 'result': {'manager': 'Capital Group', 'conf': 0.92}, 'tag': 'hard_entity:europac_capital_group', 'sponsor_guard': {'boost_if_contains_any': ['GREAT GRAY', 'WILMINGTON', 'SEI', 'NORTHERN TRUST']}}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['AM FUNDS', 'AM. FUNDS', 'AMERI FUNDS', 'AMER FNDS', 'AMERICAN F']}, 'result': {'manager': 'Capital Group', 'conf': 0.92}, 'tag': 'hard_entity:american_funds_generic_cg', 'sponsor_guard': {'boost_if_contains_any': ['GREAT GRAY', 'WILMINGTON', 'SEI', 'NORTHERN TRUST']}}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['WASHINGTON MUTUAL INVESTORS', 'WASHINGTON MUTUAL INVESTORS FUND']}, 'result': {'manager': 'Capital Group', 'conf': 0.98}, 'tag': 'hard_entity:washington_mutual_investors_capital_group'}, {'rule_type': 'ENTITY', 'match': {'startswith_any': ['AC '], 'contains_any_2': ['RD 20', 'RETIRE TR', 'TR I', 'TR III', 'TR V', 'TR X', 'TR XXII', 'TR IX', 'TR XII', 'TR L', 'TR S', 'SMALL CAP', 'MID CAP', 'LARGE CAP', 'LG CP', 'LGCPGR', 'VAL YIELD', 'EMRG MKT', 'GOV MONEY', 'HERITAGE', 'GROWTH FUND', 'CONCENTRATED GROWTH']}, 'result': {'manager': 'American Century Investments', 'conf': 0.99}, 'tag': 'hard_rule:ac_shorthand_exhaustive_signature', 'sponsor_guard': {'block_if_contains_any': ['ACCENTURE', 'ACIPCO', 'ACME', 'ACCESS', 'ACCOUNT', 'ACTUARIAL']}}, {'rule_type': 'ENTITY', 'match': {'startswith_any': ['FA '], 'contains_any': ['STABLE VALUE']}, 'result': {'manager': 'Fidelity Investments', 'conf': 0.93}, 'tag': 'hard_entity:fidelity_stable_value'}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['COHEN', 'COHEN & STEERS', 'C&S']}, 'result': {'manager': 'Cohen & Steers', 'conf': 0.93}, 'tag': 'hard_entity:cohen_steers_realty'}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['FLEXPATH', 'FLEX FOCUS', 'FLEXFIT']}, 'dispatch': [{'when': {'contains_any': ['AF ', 'AMFNDS', 'AMERICAN FUNDS', 'EUROPACIFIC']}, 'result': {'manager': 'Capital Group', 'conf': 0.98}, 'tag': 'hard_rule:great_gray_lookthrough_cg'}, {'when': {'contains_any': ['METLIFE', 'MET SERIES']}, 'result': {'manager': 'MetLife', 'conf': 0.98}, 'tag': 'hard_rule:great_gray_lookthrough_metlife'}, {'when': {'contains_any': ['INDEX', 'IDX', 'INDX']}, 'result': {'manager': 'Blackrock', 'conf': 0.95}, 'tag': 'hard_rule:great_gray_blackrock_index_decisioning'}], 'fallback': {'result': {'manager': 'Great Gray Trust Company', 'conf': 0.92}, 'tag': 'hard_rule:great_gray_canonical_manager'}}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['MYWAYRETIREMENT', 'MYWAYRET', 'MYWAYRTMT', 'MYWAYRETMENT', 'MYWAY']}, 'result': {'manager': None, 'conf': 0.85}, 'tag': 'hard_entity:myway_platform_unassigned'}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['AMERICAN FUNDS', 'AMERICAN FUND', 'AMERICA FUNDS', 'AF '], 'contains_all': ['TARGET', 'TRGTD', 'RETIRE']}, 'dispatch': [{'when': {'contains_any': ['DATE', 'RETIRE']}, 'result': {'manager': 'Capital Group', 'conf': 0.95}, 'tag': 'hard_entity:american_target_date_capital_group'}], 'fallback': {'manager': None, 'conf': 0.4, 'tag': 'hard_entity:american_target_ambiguous'}}, {'rule_type': 'ENTITY', 'match': {'startswith_any': ['BR ', 'GG BR']}, 'result': {'manager': 'Blackrock', 'conf': 0.95}, 'tag': 'hard_entity:br_abbrev_blackrock'}, {'rule_type': 'ENTITY', 'match': {'contains_any': [' WITH BLK', ' WITH BLKRCK', ' W BLK', ' BLKRCK', 'BLACKROC', 'WITH BLA', 'WITH BL A']}, 'result': {'manager': 'Blackrock', 'conf': 0.98}, 'tag': 'hard_entity:with_blk_shorthand'}, {'rule_type': 'ENTITY', 'match': {'contains_any': [' WITH AMER']}, 'result': {'manager': 'Capital Group', 'conf': 0.98}, 'tag': 'hard_entity:with_cg_shorthand'}, {'rule_type': 'ENTITY', 'match': {'startswith_any': ['CG 20']}, 'result': {'manager': 'Capital Group', 'conf': 0.98}, 'tag': 'hard_entity:cg_target_date_capital_group'}, {'rule_type': 'ENTITY', 'match': {'startswith_any': ['MM SELECT ']}, 'result': {'manager': 'MassMutual Asset Management', 'conf': 0.9}, 'tag': 'hard_entity:mm_select_massmutual'}, {'rule_type': 'ENTITY', 'match': {'startswith_any': ['VNGRD ', 'VNGD ', 'VANG ', 'VG ', 'VANGUAR', 'VAN T']}, 'result': {'manager': 'Vanguard', 'conf': 0.94}, 'tag': 'hard_entity:vanguard_abbrev'}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['TIAA REAL ESTATE']}, 'result': {'manager': 'TIAA', 'conf': 0.95}, 'tag': 'hard_entity:tiaa_real_estate'}, {'rule_type': 'ENTITY', 'match': {'contains_all': ['BARON', 'GROWTH']}, 'result': {'manager': 'Baron Capital', 'conf': 0.98}, 'tag': 'hard_entity:baron_growth'}, {'rule_type': 'ENTITY', 'match': {'startswith_any': ['MET GG ']}, 'result': {'manager': 'MetLife', 'conf': 0.98}, 'tag': 'hard_rule:met_shorthand_metlife'}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['EMPOWER', 'FUTUREFUND', 'GWL', 'DRIVE INCOME', 'DRIVE BAL']}, 'result': {'manager': 'Empower (Advised Assets Group)', 'conf': 0.98}, 'tag': 'hard_rule:empower_aag_proprietary'}, {'rule_type': 'ENTITY', 'match': {'startswith_any': ['HARTFORD ', 'HARFORD']}, 'dispatch': [{'when': {'contains_any': ['SCHRODER', 'SCHRODERS']}, 'result': {'manager': 'Schroders', 'conf': 0.98}, 'tag': 'hard_rule:hartford_schroders_subadvised'}, {'when': {'contains_any': ['WELLINGTON', 'WMC']}, 'result': {'manager': 'Wellington Management', 'conf': 0.98}, 'tag': 'hard_rule:hartford_wellington_subadvised'}], 'fallback': {'manager': 'Wellington Management', 'conf': 0.85, 'tag': 'hartford_default_to_wellington'}}, {'rule_type': 'ENTITY', 'match': {'startswith_any': ['AF ']}, 'dispatch': [{'when': {'contains_any': ['EUROPAC', 'EUROPACIFIC']}, 'result': {'manager': 'Capital Group', 'conf': 0.94}, 'tag': 'hard_rule:af_europac_capital_group'}, {'when': {'contains_any': ['NEW PER', 'NEW PERSPECTIVE']}, 'result': {'manager': 'Capital Group', 'conf': 0.94}, 'tag': 'hard_rule:af_new_perspective_capital_group'}, {'when': {'contains_any': ['GROWTH FUND OF AMERICA', 'THE GROWTH FUND OF AMERICA', 'GROWTH OF AM', 'GROWTH OF A']}, 'result': {'manager': 'Capital Group', 'conf': 0.94}, 'tag': 'hard_rule:af_growth_fund_of_america_capital_group'}, {'when': {'contains_any': ['INCOME FUND OF AMERICA', 'THE INCOME FUND OF AMERICA']}, 'result': {'manager': 'Capital Group', 'conf': 0.94}, 'tag': 'hard_rule:af_income_fund_of_america_capital_group'}, {'when': {'contains_any': ['AMERICAN BALANCED FUND']}, 'result': {'manager': 'Capital Group', 'conf': 0.94}, 'tag': 'hard_rule:af_american_balanced_capital_group'}, {'when': {'contains_any': ['CAPITAL INCOME BUILDER']}, 'result': {'manager': 'Capital Group', 'conf': 0.94}, 'tag': 'hard_rule:af_capital_income_builder_capital_group'}, {'when': {'contains_any': ['US GOVERNMENT SECURITIES', 'US GOV', 'US GOV SEC']}, 'result': {'manager': 'Capital Group', 'conf': 0.94}, 'tag': 'hard_rule:af_us_government_securities_capital_group'}], 'fallback': {'manager': 'Capital Group', 'conf': 0.84, 'tag': 'hard_rule:af_ambiguous'}, 'sponsor_guard': {'block_if_contains_any': ['FIDELITY', 'VANGUARD', 'STATE STREET', 'SEI', 'GREAT GRAY', 'WILMINGTON', 'BNY', 'PRINCIPAL']}}, {'rule_type': 'ENTITY', 'match': {'startswith_any': ['JHANCOCK ', 'JHancock ']}, 'result': {'manager': 'John Hancock Investment Management', 'conf': 0.95}, 'tag': 'hard_entity:jhanock_prefix_jhim'}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['INCOMEFLEX', 'INCOME FLEX'], 'startswith_any': ['PRU ', 'PRUDENTIAL ', 'PGIM ', 'DAY ONE ']}, 'result': {'manager': 'PGIM', 'conf': 0.98}, 'tag': 'hard_rule:pgim_incomeflex_precise'}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['PRIN ', 'PRIN/', 'PRINC ', 'PRINC/', 'PRINCIPAL', 'PRINL', 'PIF', 'PLT ', 'PRINICPAL', 'PRNCPL']}, 'dispatch': [{'when': {'contains_token_any': ['BR', 'BLK', 'BLACKROCK']}, 'result': {'manager': 'Blackrock', 'conf': 0.96}, 'tag': 'hard_rule:prin_br_blackrock'}, {'when': {'contains_any': ['TRP', 'T ROWE', 'TROWE']}, 'result': {'manager': 'T. Rowe Price', 'conf': 0.96}, 'tag': 'hard_rule:prin_trp_subadvised'}, {'when': {'contains_any': ['MFS']}, 'result': {'manager': 'MFS Investment Management', 'conf': 0.96}, 'tag': 'hard_rule:prin_mfs_subadvised'}, {'when': {'contains_any': ['MACQUARIE']}, 'result': {'manager': 'Macquarie Asset Management', 'conf': 0.96}, 'tag': 'hard_rule:prin_macquarie_subadvised'}, {'when': {'contains_any': ['LAZARD']}, 'result': {'manager': 'Lazard Asset Management', 'conf': 0.94}, 'tag': 'hard_rule:prin_lazard_subadvised'}, {'when': {'contains_any': ['REAL ESTATE', 'MID CAP', 'MDCAP', 'HYBRID', 'LIFETIME', 'STABLE VALUE', 'FIXED INCOME', 'BLUE CHIP', 'EQUITY', 'BOND', 'SMALL CAP', 'CORE PLUS']}, 'result': {'manager': 'Principal', 'conf': 0.94}, 'tag': 'hard_rule:prin_OWN'}], 'fallback': {'manager': 'Principal', 'conf': 0.93, 'tag': 'hard_rule:prin_general_fallback'}}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['GREAT GRAY', 'GREAT GREY', 'WILMINGTON TRUST', 'WTC-CIF']}, 'dispatch': [{'when': {'contains_any': ['AF ', 'AMFNDS', 'CAPITAL GROUP', 'EUROPACIFIC']}, 'result': {'manager': 'Capital Group', 'conf': 0.98}, 'tag': 'hard_rule:great_gray_lookthrough_cg'}, {'when': {'contains_any': ['BLK', 'BLACKROCK', 'FLEXPATH']}, 'result': {'manager': 'Blackrock', 'conf': 0.98}, 'tag': 'hard_rule:great_gray_lookthrough_blk'}, {'when': {'contains_any': ['FEDERATED', 'FH ']}, 'result': {'manager': 'Federated Hermes', 'conf': 0.98}, 'tag': 'hard_rule:great_gray_lookthrough_fh'}], 'fallback': {'manager': 'Great Gray Trust Company', 'conf': 0.85, 'tag': 'hard_rule:great_gray_generic_fallback'}}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['MAP ', 'MAP SEPARATE', 'MAP CONTRACT']}, 'dispatch': [{'when': {'contains_any': ['FREEDOM', 'FIDELITY']}, 'result': {'manager': 'Fidelity Investments', 'conf': 0.94}, 'tag': 'hard_rule:map_subadvised_fidelity'}], 'fallback': {'manager': 'TruStage', 'conf': 0.9, 'tag': 'hard_rule:map_platform_trustage'}}, {'rule_type': 'ENTITY', 'match': {'startswith_any': ['JH LIFETIME', 'JH MULTIMANAGER', 'JH LIFESTYLE']}, 'result': {'manager': 'John Hancock Investment Management', 'conf': 0.95}, 'tag': 'hard_rule:jhim_series_prefix'}, {'rule_type': 'ENTITY', 'match': {'startswith_any': ['JH ']}, 'result': {'manager': 'John Hancock Investment Management', 'conf': 0.85}, 'tag': 'hard_rule:jhim_jh_prefix_catchall'}, {'rule_type': 'ENTITY', 'match': {'startswith_any': ['JOHN HANCOCK', 'JH ', 'JOHN HACOCK']}, 'dispatch': [{'when': {'contains_any': ['LT BL', 'LT BI', 'LIFETIME', 'MULTIMANAGER', 'LIFESTYLE', 'LS BLEND', 'DISCIPLINE', 'ESG', 'INDEX', '500']}, 'manager': 'John Hancock Investment Management', 'conf': 0.98, 'tag': 'hard_rule:jh_investment_decisioning'}, {'when': {'contains_any': ['STABLE VAL', 'STABLE FUND', 'STBL VAL', 'SVF', 'STABLEVAL', 'GUAR INT']}, 'manager': 'John Hancock Investment Management', 'conf': 0.98, 'tag': 'hard_rule:jh_stable_value_decisioning'}, {'when': {'contains_any': ['LIFE INSURANCE', 'ANNUITY', 'USA PSA', 'USA SUB', 'QUALIFIED SEP', 'ARA GROUP', 'TIP & SIP']}, 'manager': 'John Hancock Life Insurance', 'conf': 0.95, 'tag': 'hard_rule:jh_insurance_platform'}], 'fallback': {'manager': 'John Hancock Investment Management', 'conf': 0.9, 'tag': 'hard_rule:jh_generic_fallback'}}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['VRIAC', 'VARIABLE ANNUITY ACCOUNT', 'VOYA RETIREMENT']}, 'dispatch': [{'when': {'contains_any': ['VANGUARD', 'FIDELITY', 'BLACKROCK', 'JPM']}, 'manager': None, 'conf': 0.95, 'tag': 'hard_rule:voya_platform_bypass'}], 'fallback': {'manager': 'Voya Investment Management', 'conf': 0.95, 'tag': 'hard_rule:voya_vriac_default'}}, {'rule_type': 'ENTITY', 'match': {'contains_any': [' JPM ', 'JPMCB', 'JP MORGAN', 'JPMORGAN', 'JP MO']}, 'result': {'manager': 'JP Morgan Investment Management', 'conf': 0.85}, 'tag': 'hard_rule:jpm_abbrev'}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['HARBOR MID CAP', 'HARBOR MIDCAP', 'HARBOR INTL', 'HARBOR SMALL']}, 'result': {'manager': 'Harbor Funds', 'conf': 0.94}, 'tag': 'hard_rule:harbor_specific_series'}, {'rule_type': 'ENTITY', 'match': {'startswith_any': ['NT COL ', 'NT COLL ', 'NTCC ']}, 'result': {'manager': 'Northern Trust Asset Management', 'conf': 0.95}, 'tag': 'hard_entity:nt_collective_shorthand'}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['PRU ', 'PRUDENTIAL ', 'MEDLEY', 'PESP ', 'PGIM']}, 'dispatch': [{'when': {'contains_any': ['DAY ONE', 'TARGET DATE', 'TDF']}, 'result': {'manager': 'PGIM', 'conf': 0.95}, 'tag': 'hard_entity:pru_target_date_pgim'}, {'when': {'contains_any': ['JENNISON', 'GROWTH', 'VCA', 'EQUITY']}, 'result': {'manager': 'PGIM', 'conf': 0.95}, 'tag': 'hard_entity:pru_equity_jennison_pgim'}, {'when': {'contains_any': ['CORE PLUS', 'STABLE VALUE', 'SHORT TERM', 'SHORT-TERM', 'LONG DURATION', 'BOND', 'BD ']}, 'result': {'manager': 'PGIM', 'conf': 0.95}, 'tag': 'hard_entity:pru_fixed_income_pgim'}, {'when': {'contains_any': ['REAL ESTATE', 'PRISA']}, 'result': {'manager': 'PGIM', 'conf': 0.95}, 'tag': 'hard_entity:pru_real_estate_pgim'}], 'fallback': {'manager': 'PGIM', 'conf': 0.8, 'tag': 'hard_entity:pru_generic_catchall'}}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['MANULIFE ACTIVE BOND']}, 'result': {'manager': 'Manulife Investment Management', 'conf': 0.95}, 'tag': 'hard_entity:manulife_active_bond'}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['YOURPATH AM FUNDS']}, 'result': {'manager': 'Lincoln Financial', 'conf': 0.85}, 'tag': 'hard_entity:lincoln_yourpath_program'}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['GOLDMAN SACHS', 'GSAM', 'GS '], 'contains_any_2': ['STABLE VAL', 'CAPITAL', 'EQUITY', 'BOND', 'SMALL CAP', 'MID CAP', 'LARGE CAP', 'INCOME', 'GROWTH']}, 'result': {'manager': 'Goldman Sachs Asset Management', 'conf': 0.95}, 'tag': 'hard_rule:goldman_sachs_global_catchall'}, {'rule_type': 'ENTITY', 'match': {'contains_all': ['EB DV'], 'contains_any': ['MID CAP', 'STOCK INDEX']}, 'result': {'manager': None, 'conf': 0.6}, 'tag': 'hard_entity:eb_dv_program_unknown'}, {'rule_type': 'ENTITY', 'match': {'contains_any': ['FEDERATED', 'FH ', 'HERMES', 'KAUFMANN'], 'contains_any_2': ['CAP PRESRVN', 'TOTAL RETURN', 'BOND', 'EQUITY', 'VALUE', 'INCOME', 'GOV', 'GOVERNMENT']}, 'result': {'manager': 'Federated Hermes', 'conf': 0.95}, 'tag': 'hard_rule:federated_hermes_global_catchall'}, {'rule_type': 'ENTITY', 'match': {'startswith_any': ['AON ']}, 'result': {'manager': 'Aon Hewitt', 'conf': 0.96}, 'tag': 'hard_rule:aon_discretionary_manager'}, {'rule_type': 'SPONSOR', 'match': {'contains_any': ['BARON CAPITAL GROUP']}, 'result': {'manager': 'Baron Capital', 'conf': 0.95}, 'tag': 'hard_sponsor:baron_capital_group'}, {'rule_type': 'SPONSOR', 'match': {'contains_any': ['NORTHERN TRUST INVESTMENTS', 'NORTHERN TRUST GLOBAL INVESTMENTS']}, 'result': {'manager': 'Northern Trust Asset Management', 'conf': 0.95}, 'tag': 'hard_sponsor:nt_group'}, {'rule_type': 'SPONSOR', 'match': {'contains_any': ['AON HEWITT INVESTMENT']}, 'result': {'manager': 'Aon Hewitt', 'conf': 0.95}, 'tag': 'hard_sponsor:aon_group'}, {'rule_type': 'SPONSOR', 'match': {'contains_any': ['VANGUARD FIDUCIARY TRUST']}, 'dispatch': [{'when': {'contains_any': ['TARGET RET', 'TARGET RETIREMENT']}, 'result': {'manager': 'Vanguard', 'conf': 0.98}, 'tag': 'hard_sponsor:vanguard_target_date'}], 'fallback': {'manager': 'Vanguard', 'conf': 0.9, 'tag': 'hard_sponsor:vanguard_fiduciary_default'}}, {'rule_type': 'SPONSOR', 'match': {'contains_any': ['STATE STREET GLOBAL ADVISORS TRUST']}, 'fallback': {'manager': 'State Street Global Advisors', 'conf': 0.9, 'tag': 'hard_sponsor:ssga_default'}}, {'rule_type': 'SPONSOR', 'match': {'contains_any': ['STATE STREET BANK AND TRUST']}, 'dispatch': [{'when': {'contains_any': ['TARGET RETIREMENT', 'TARGET RET']}, 'result': {'manager': 'State Street Global Advisors', 'conf': 0.95}, 'tag': 'hard_sponsor:ssga_target_date'}], 'fallback': {'manager': None, 'conf': 0.4, 'tag': 'hard_sponsor:ssga_non_tdf_no_guess'}}, {'rule_type': 'SPONSOR', 'match': {'contains_any': ['ASSOCIATED TRUST COMPANY']}, 'fallback': {'manager': 'Associated Bank', 'conf': 0.9, 'tag': 'hard_sponsor:associated_trust'}}, {'rule_type': 'SPONSOR', 'match': {'contains_any': ['T ROWE PRICE']}, 'fallback': {'manager': 'T. Rowe Price', 'conf': 0.9, 'tag': 'hard_sponsor:t_rowe_price'}}, {'rule_type': 'SPONSOR', 'match': {'contains_any': ['FIDELITY INSTITUTIONAL ASSET MANAGEMENT', 'FIAM TRUST']}, 'fallback': {'manager': 'Fidelity Investments', 'conf': 0.9, 'tag': 'hard_sponsor:fiam_trust'}}, {'rule_type': 'SPONSOR', 'match': {'contains_any': ['GOLDMAN SACHS TRUST COMPANY']}, 'fallback': {'manager': 'Goldman Sachs Asset Management', 'conf': 0.9, 'tag': 'hard_sponsor:goldman_sachs_trust'}}, {'rule_type': 'SPONSOR', 'match': {'contains_any': ['PUTNAM FIDUCIARY TRUST']}, 'fallback': {'manager': 'Putnam Investments', 'conf': 0.9, 'tag': 'hard_sponsor:putnam_fiduciary_trust'}}, {'rule_type': 'SPONSOR', 'match': {'contains_any': ['PRINCIPAL COLLECTIVE TRUST FUNDS', 'PRINCIPAL GLOBAL INVESTORS TRUST']}, 'fallback': {'manager': 'Principal', 'conf': 0.9, 'tag': 'hard_sponsor:principal_trust'}}, {'rule_type': 'SPONSOR', 'match': {'contains_any': ['FEDERATED INVESTORS']}, 'fallback': {'manager': 'Federated Hermes', 'conf': 0.9, 'tag': 'hard_sponsor:federated_investors'}}, {'rule_type': 'SPONSOR', 'match': {'contains_any': ['CAPITAL BANK AND TRUST', 'CAPITAL BANK & TRUST']}, 'fallback': {'manager': 'Capital Group', 'conf': 0.9, 'tag': 'hard_sponsor:capital_bank_and_trust'}}, {'rule_type': 'SPONSOR', 'match': {'contains_any': ['HARBOR TRUST COMPANY']}, 'fallback': {'manager': 'Harbor Capital Advisors', 'conf': 0.9, 'tag': 'hard_sponsor:harbor_trust'}}, {'rule_type': 'SPONSOR', 'match': {'contains_any': ['MFS HERITAGE TRUST']}, 'fallback': {'manager': 'MFS Investment Management', 'conf': 0.9, 'tag': 'hard_sponsor:mfs_heritage_trust'}}, {'rule_type': 'SPONSOR', 'match': {'contains_any': ['INVESCO NATIONAL TRUST']}, 'fallback': {'manager': 'Invesco', 'conf': 0.9, 'tag': 'hard_sponsor:invesco_national_trust'}}, {'rule_type': 'SPONSOR', 'match': {'contains_all': ['GLOBAL TRUST COMPANY', 'JOHN HANCOCK']}, 'fallback': {'manager': 'John Hancock Investment Management', 'conf': 0.9, 'tag': 'hard_sponsor:global_trust_john_hancock'}}, {'rule_type': 'SPONSOR', 'match': {'contains_any': ['WELLINGTON TRUST COMPANY']}, 'fallback': {'manager': 'Wellington Management', 'conf': 0.9, 'tag': 'hard_sponsor:wellington_trust'}}, {'rule_type': 'SPONSOR', 'match': {'contains_any': ['ALLSPRING GLOBAL INVESTMENTS', 'ALLSPING GLOBAL INVESTMENTS']}, 'fallback': {'manager': 'Allspring Global Investments', 'conf': 0.9, 'tag': 'hard_sponsor:allspring_global'}}]

SHORT_TOKEN_ALLOW = {'AQR', 'MFS', 'DFA', 'PGI', 'PRU', 'JPM', 'PRIN', 'AON'}

SPONSOR_SECONDARY_ALLOWLIST = ['MASSMUTUAL', 'MASS MUTUAL', 'PRINCIPAL', 'TRANSAMERICA', 'NATIONWIDE', 'TIAA', 'EMPOWER', 'LINCOLN']

STRICT_TOKEN_ALLOW = {'AQR', 'MFS', 'DFA', 'PGI', 'PRU', 'JPM', 'JH', 'NT', 'PRIN', 'AON'}

def _extract_result(obj: dict) -> Tuple[Optional[str], Optional[float]]:
    """
    Supports either:
      A) {"result": {"manager": "...", "conf": 0.9}}
      B) {"manager": "...", "conf": 0.9}
    """
    if not obj:
        return (None, None)
    if 'result' in obj and isinstance(obj['result'], dict):
        mgr = obj['result'].get('manager')
        conf = obj['result'].get('conf')
        return (mgr, conf)
    mgr = obj.get('manager')
    conf = obj.get('conf')
    return (mgr, conf)

def _extract_tag(rule: dict, obj: dict, default_tag: str) -> str:
    return obj.get('tag') or rule.get('tag') or default_tag

def _guard_conf(rule: dict, sponsor: str, base_conf: float) -> Optional[float]:
    guard = rule.get('sponsor_guard')
    if not guard:
        return base_conf
    if sponsor and any((x in sponsor for x in guard.get('block_if_contains_any', []))):
        return None
    if sponsor and any((x in sponsor for x in guard.get('boost_if_contains_any', []))):
        return min(1.0, base_conf + 0.02)
    if sponsor:
        return max(0.0, base_conf - 0.03)
    return base_conf

def _match(rule_match: dict, haystack: str) -> bool:
    if 'contains_token_any' in rule_match:
        return any((contains_token(haystack, norm(x)) for x in rule_match['contains_token_any']))
    if 'startswith_any' in rule_match:
        if not any((haystack.startswith(x) for x in rule_match['startswith_any'])):
            return False
    if 'contains_all' in rule_match:
        if not all((x in haystack for x in rule_match['contains_all'])):
            return False
    if 'contains_any' in rule_match:
        if not any((x in haystack for x in rule_match['contains_any'])):
            return False
    if 'contains_any_2' in rule_match:
        if not any((x in haystack for x in rule_match['contains_any_2'])):
            return False
    return True

def apply_rules(entity_norm: str, sponsor_norm: str='', rule_type: str='ENTITY') -> Optional[Override]:
    e = entity_norm or ''
    s = sponsor_norm or ''
    haystack = e if rule_type == 'ENTITY' else s
    for rule in RULES:
        if rule.get('rule_type', 'ENTITY') != rule_type:
            continue
        if not _match(rule['match'], haystack):
            continue
        if 'dispatch' in rule:
            dispatch_hay = e
            for d in rule['dispatch']:
                if _match(d['when'], dispatch_hay):
                    mgr, base_conf = _extract_result(d)
                    if base_conf is None:
                        raise KeyError(f'Dispatch entry missing conf: {d}')
                    conf = _guard_conf(rule, s, float(base_conf))
                    if conf is None:
                        return None
                    tag = _extract_tag(rule, d, default_tag='dispatch:unknown')
                    return (mgr, conf, tag)
            fb = rule.get('fallback', {})
            mgr, base_conf = _extract_result(fb)
            if base_conf is None:
                raise KeyError(f'Fallback entry missing conf: {fb}')
            conf = _guard_conf(rule, s, float(base_conf))
            if conf is None:
                return None
            tag = _extract_tag(rule, fb, default_tag='fallback:unknown')
            return (mgr, conf, tag)
        base = rule.get('result') or rule.get('fallback', {})
        mgr = base.get('manager')
        base_conf = base.get('conf')
        if base_conf is None:
            raise KeyError(f'Rule result missing conf: {rule}')
        conf = _guard_conf(rule, s, float(base_conf))
        if conf is None:
            return None
        return (mgr, conf, rule.get('tag', 'rule:unknown'))
    return None

def contains_token(haystack_norm: str, token_norm: str) -> bool:
    if not haystack_norm or not token_norm:
        return False
    if token_norm in STRICT_TOKEN_ALLOW:
        return f' {token_norm} ' in f' {haystack_norm} ' or haystack_norm.startswith(token_norm + ' ')
    hay_compact = haystack_norm.replace(' ', '')
    tok_compact = token_norm.replace(' ', '')
    if f' {token_norm} ' in f' {haystack_norm} ':
        return True
    if len(tok_compact) >= 6:
        return tok_compact in hay_compact
    return False

def deterministic_match_debug(entity: str, sponsor: str, canonical_mgrs: list):
    e_norm = norm(entity)
    s_norm = norm(sponsor)
    ho_e = apply_rules(e_norm, s_norm, rule_type='ENTITY')
    if ho_e:
        manager, conf, tag = ho_e
        return ('HARD_ENTITY', tag, manager, conf)
    if e_norm in CANON_LOOKUP:
        return ('DICT_FAST', 'dictionary:FAST_LOOKUP', CANON_LOOKUP[e_norm], 0.95)
    for t_norm in ALL_CANONICAL_TOKENS:
        if contains_token(e_norm, t_norm):
            return ('DICT_TOKEN', 'dictionary:ENTITY_FIRST', CANON_LOOKUP.get(t_norm), 0.95)
    ho_s = apply_rules(e_norm, s_norm, rule_type='SPONSOR')
    if ho_s:
        manager, conf, tag = ho_s
        return ('HARD_SPONSOR', tag, manager, conf)
    if s_norm and sponsor_allows_secondary(s_norm) and (not entity_contains_known_manager(e_norm)):
        for m in canonical_mgrs:
            for t in m.get('tokens', []):
                tn = norm(t)
                if tn and contains_token(s_norm, tn):
                    return ('SPONSOR_SECONDARY', 'dictionary:SPONSOR_SECONDARY', m.get('canonical'), 0.9)
    return ('NO_MATCH', None, None, None)

def entity_contains_known_manager(entity_norm: str) -> bool:
    return any((contains_token(entity_norm, norm(t)) for t in ALL_CANONICAL_TOKENS))

def force_canonical(name: str):
    if not name:
        return name
    return CANON_LOOKUP.get(norm(name), name)

def norm(s: str) -> str:
    if s is None:
        return ''
    s = s.upper()
    s = re.sub('[^A-Z0-9 ]+', ' ', s)
    s = re.sub('\\s+', ' ', s).strip()
    return s

def sponsor_allows_secondary(sponsor_norm: str) -> bool:
    """
    True = allow sponsor-based canonical matching
    False = sponsor looks like a platform/trustee/insurer and should NOT drive mapping
    """
    s = sponsor_norm or ''
    if 'BLACKROCK' in s:
        return True
    if 'JPMORGAN' in s or 'JP MORGAN' in s:
        return any((h in s for h in JPM_SPONSOR_EXTRA_HINTS))
    if 'JOHN HANCOCK' in s or 'MANULIFE' in s:
        return any((h in s for h in AM_SPONSOR_HINTS))
    if any((x in s for x in SPONSOR_SECONDARY_ALLOWLIST)):
        return True
    if any((h in s for h in AM_SPONSOR_HINTS)):
        return True
    return not sponsor_is_platform(s)

def sponsor_is_platform(sponsor_norm: str) -> bool:
    s = sponsor_norm or ''
    return any((x in s for x in PLATFORM_SPONSOR_BLOCKLIST))
