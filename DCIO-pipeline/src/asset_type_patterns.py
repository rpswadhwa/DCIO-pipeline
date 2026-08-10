import re

# Canonical asset type patterns — ordered most-specific first.
# Used by both text_extract.py (section heading detection) and
# data_cleaner.py (asset type inference from field text).
ASSET_TYPE_PATTERNS = [
    (r'Investments?\s+in\s+mutual\s+funds?',                    'Mutual Fund'),
    (r'Investments?\s+in\s+money\s+markets?',                   'Money Market Fund'),
    (r'Investments?\s+in\s+common\s+collective\s+trusts?',      'Common/Collective Trust Fund'),
    (r'Investments?\s+in\s+pooled\s+separate\s+(?:investment\s+)?accounts?', 'Separate Account'),
    (r'Investments?\s+in\s+investment\s+contracts?',            'Stable Value Fund'),
    (r'Insurance\s+Company\s+General\s+Account\s+Contracts?',   'Insurance General Account'),
    (r'General\s+Account\s+Contracts?',                         'Insurance General Account'),
    (r'Group\s+Annuity\s+Contracts?',                           'Group Annuity Contract'),
    (r'CREF\s+Accounts?',                                       'Group Annuity Contract'),
    (r'Fully[\-\s]Benefit[\-\s]Responsive\s+Contracts?',       'Stable Value Fund'),
    (r'Non[\-\s]Benefit[\-\s]Responsive\s+Contracts?',         'Stable Value Fund'),
    # COMBINED heading: one section labeled BOTH mutual funds AND collective trusts, over a
    # MIXED block with no per-row type to split them. Must NOT be typed as pure CIT -- that
    # wrongly excludes the real MFs -- so it gets its OWN label, identifiable for a later
    # MF-vs-CIT apportionment step. MUST precede the Collective Trust patterns below.
    (r'Mutual\s+Funds?\s+and\s+Common\s*/?\s*Collective\s+Trusts?', 'Mutual Fund/Collective Trust'),
    (r'Mutual\s+Funds?\s+and\s+Collective\s+Trusts?',              'Mutual Fund/Collective Trust'),
    (r'Short[\-\s]Term\s+Investment\s+Funds?',                     'Short-Term Investment Fund'),
    (r'Common\s*/\s*Collective\s+Trust\s+Funds?',               'Common/Collective Trust Fund'),
    (r'Common\s*/\s*Collective\s+Trusts?',                      'Common/Collective Trust Fund'),
    (r'Collective\s*/\s*Common\s+Trust\s+Funds?',               'Common/Collective Trust Fund'),
    (r'Common\s+Collective\s+Trust\s+Funds?',                   'Common/Collective Trust Fund'),
    (r'Collective\s+Investment\s+Trusts?',                      'Common/Collective Trust Fund'),
    (r'Collective\s+Investment\s+Funds?',                       'Common/Collective Trust Fund'),
    (r'Collective\s+Trust\s+Funds?',                            'Common/Collective Trust Fund'),
    (r'Collective\s+Trusts?',                                   'Common/Collective Trust Fund'),
    (r'Common\s+Collective\s+Trusts?',                          'Common/Collective Trust Fund'),
    (r'Collecti\w{0,3}e\s+Trusts?',                             'Common/Collective Trust Fund'),  # OCR-tolerant: "Collecti11e Trusts"
    (r'Pooled\s+Separate\s+(?:Investment\s+)?Accounts?',        'Separate Account'),
    (r'Pooled\s+Funds?',                                        'Commingled Fund'),
    (r'Separately\s+Managed\s+Accounts?',                       'Separately Managed Account'),
    (r'Self[\-\s]Directed\s+Brokerage\s+Accounts?',             'Self-Directed Brokerage Account'),
    (r'Commingled\s+Funds?',                                    'Commingled Fund'),
    (r'Commingled\s+Pools?',                                    'Commingled Fund'),
    (r'Commingled',                                             'Commingled Fund'),
    (r'Company\s+Stocks?',                                      'Employer Stock'),
    (r'Self[\-\s]?Directed\s+Brokerage',                       'Self-Directed Brokerage Account'),
    (r'Collective\s+Funds?',                                    'Commingled Fund'),
    (r'Stable\s+Value\s+Funds?',                                'Stable Value Fund'),
    (r'Money\s+Market\s+Funds?',                                'Money Market Fund'),
    (r'\bMMRK\b',                                                'Money Market Fund'),
    (r'Variable\s+Annuit(?:y|ies)(?:\s+(?:Contracts?|Accounts?))?', 'Variable Annuity Contract'),
    (r'Registered\s+Investment\s+Compan(?:y|ies)',              'Mutual Fund'),
    (r'Registered\s+Investment\s+Funds?',                       'Mutual Fund'),
    (r'Institutional\s+Funds?',                                 'Mutual Fund'),
    (r'Funds?\s+of\s+Funds?',                                   'Mutual Fund'),
    (r'Mutual\s+Funds?',                                        'Mutual Fund'),
    (r'Employer\s+Stocks?',                                     'Employer Stock'),
    (r'Employer\s+Securities',                                  'Employer Stock'),
    (r'Preferred\s+Stocks?',                                    'Preferred Stock'),
    (r'Common\s+Stocks?',                                       'Employer Stock'),
    (r'Publicly[\-\s]traded\s+Stocks?',                         'Common Stock'),
    # --- additional non-MF vehicle categories (DOL Sch H taxonomy): partnerships/joint
    # ventures, real estate, hedge funds, bonds/debt, derivatives, 103-12 IEs. These match
    # SECTION HEADINGS and strict full-label descriptions only (both safe: _detect_section_heading
    # fires on value-less lines, detect_asset_type_strict on whole labels). Row-level detection
    # is kept conservative in ROW_TYPE_PATTERNS so a bond/real-estate MUTUAL FUND name is never
    # mistyped as a direct holding. Ordered specific-first; combined partnership/JV precedes both.
    (r'103[\-\s]?12\s+Investment\s+Entit(?:y|ies)',             '103-12 Investment Entity'),
    (r'103[\-\s]?12\s+IEs?',                                    '103-12 Investment Entity'),
    (r'Partnerships?\s*(?:/|&|and)\s*Joint\s+Ventures?',        'Partnership Interest'),
    (r'Joint\s+Ventures?',                                      'Joint Venture'),
    (r'Real\s+Estate\s+Investment\s+Trusts?',                   'Real Estate'),
    (r'Real\s+Propert(?:y|ies)',                                'Real Estate'),
    (r'Real\s+Estate(?!\s+(?:Funds?|Securit|Index|Mutual|Stock))', 'Real Estate'),
    (r'Funds?\s+of\s+Hedge\s+Funds?',                           'Hedge Fund'),
    (r'Hedge\s+Funds?',                                         'Hedge Fund'),
    (r'Corporate\s+Debt\s+Instruments?',                        'Bond'),
    (r'Corporate\s+Debt',                                       'Bond'),
    (r'Corporate\s+Bonds?',                                     'Bond'),
    (r'Government\s+Bonds?',                                     'Bond'),
    (r'Municipal\s+Bonds?',                                      'Bond'),
    (r'U\.?\s*S\.?\s+Government\s+Securities',                   'Bond'),
    (r'U\.?\s*S\.?\s+Treasury\s+(?:Securities|Obligations|Notes?|Bonds?|Bills?)', 'Bond'),
    (r'Government\s+Obligations',                               'Bond'),
    (r'Debt\s+Securities',                                      'Bond'),
    (r'Fixed\s+Income\s+Securities',                            'Bond'),
    (r'Debentures?',                                            'Bond'),
    (r'Derivatives?',                                           'Derivative'),
    (r'(?:Call|Put)\s+Options?',                                'Derivative'),
    (r'Futures\s+Contracts?',                                   'Derivative'),
    (r'(?:Interest\s+Rate\s+)?Swaps?\s+Contracts?',             'Derivative'),
    (r'Partnership\s+Interests?',                               'Partnership Interest'),
    (r'Participant\s+Loans?',                                   'Participant Loan'),
    (r'ETFs?',                                                  'ETF'),
    (r'Currenc(?:y|ies)',                                       'Currency'),
]


def detect_asset_type(text: str) -> str:
    """Return canonical asset type if text matches any known pattern, else empty string."""
    for pattern, asset_type in ASSET_TYPE_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return asset_type
    return ''


def detect_asset_type_strict(text: str) -> str:
    """Return canonical asset type only if text IS a type label (fullmatch).
    Unlike detect_asset_type, this will not match fund names that contain
    type keywords as substrings (e.g. 'BlackRock Index Fund' returns '')."""
    if not text:
        return ''
    text = text.strip().rstrip(':')
    for pattern, asset_type in ASSET_TYPE_PATTERNS:
        if re.fullmatch(pattern, text, re.IGNORECASE):
            return asset_type
    return ''


# Per-row asset-type DECLARATIONS carried in the row itself -- a dedicated "Type:" column
# value, or a vehicle-type phrase at the start/end of the column-(c) description
# ("Common/Collective Trust", "Insurance Company Separate Account", "mutual fund,",
# "Collective investment in ...", "Pooled Separate Accounts"). Used when there is NO
# section heading. Deliberately EXCLUDES bare name-ish fund types ("Index Fund", "Fund")
# so a fund NAME never triggers a type -- only an explicit vehicle declaration does.
# Ordered non-MF-first so an explicit CIT / separate account / stock wins over an
# incidental trailing "fund" token in the same string.
ROW_TYPE_PATTERNS = [
    (r'insurance\s+company\s+separate\s+accounts?',    'Separately Managed Account'),
    (r'pooled\s+separate\s+(?:investment\s+)?accounts?', 'Separate Account'),
    (r'separate(?:ly)?\s+managed\s+accounts?',         'Separately Managed Account'),
    (r'common\s*/?\s*collective\s+trusts?',            'Common/Collective Trust Fund'),
    (r'collective\s+investment\s+trusts?',             'Common/Collective Trust Fund'),
    (r'collective\s+investment',                       'Common/Collective Trust Fund'),
    (r'collecti\w{0,3}e\s+trusts?',                     'Common/Collective Trust Fund'),  # OCR-tolerant
    (r'commingled',                                    'Commingled Fund'),
    (r'pooled\s+funds?',                               'Commingled Fund'),
    (r'group\s+annuity',                               'Group Annuity Contract'),
    # guaranteed annuity contracts (TIAA Traditional etc.) -- real holdings, NOT mutual funds
    (r'tiaa\s+traditional',                            'Group Annuity Contract'),
    (r'traditional\s+annuit',                          'Group Annuity Contract'),
    (r'annuity\s+contract',                            'Group Annuity Contract'),
    (r'fixed\s+annuit',                                'Group Annuity Contract'),
    (r'guaranteed\s+portfolio',                        'Group Annuity Contract'),
    (r'(?:non[\-\s]?)?benefit[\-\s]?responsive',       'Group Annuity Contract'),
    (r'ins(?:urance)?\s+(?:co(?:mpany)?\s+)?general\s+account', 'Insurance General Account'),
    (r'personal\s+choice\s+retirement\s+account',      'Self-Directed Brokerage Account'),
    (r'guaranteed\s+(?:investment\s+contract|income)', 'Stable Value Fund'),
    (r'stable\s+value',                                'Stable Value Fund'),
    # Variable annuities are now typed as their own non-MF vehicle (Variable Annuity Contract)
    # per direct PDF review -- these ARE their own asset-type heading in the filing, not a
    # mutual fund. A prior 2500-plan run flagged -45 PASS when this was excluded; that was
    # accepted as a known tradeoff rather than reverting the classification.
    (r'variable\s+annuit(?:y|ies)',                    'Variable Annuity Contract'),
    # fixed / guaranteed / GIC principal-preservation vehicles -> Stable Value (non-MF)
    (r'fixed\s+account',                               'Stable Value Fund'),
    (r'fixed\s+interest',                              'Stable Value Fund'),
    (r'stable\s+return',                               'Stable Value Fund'),
    (r'\bgic\b',                                       'Stable Value Fund'),
    (r'guarante(?:ed|y)',                              'Stable Value Fund'),
    # limited partnership interests (non-MF)
    (r'limited\s+partnership|partnership\s+interest|interest\s+in\s+limited\s+part', 'Partnership Interest'),
    # additional non-MF DIRECT holdings -- ONLY phrases a mutual-fund NAME never carries.
    # detect_asset_type_row is searched against the issuer NAME too, so NO bare 'bond' /
    # 'real estate' / 'government' here (those live in fund names and would cause under-capture).
    (r'joint\s+venture',                               'Joint Venture'),
    (r'103[\-\s]?12\s+investment\s+entit',             '103-12 Investment Entity'),
    (r'hedge\s+fund',                                  'Hedge Fund'),
    (r'real\s+propert(?:y|ies)',                       'Real Estate'),
    (r'\bdebentures?\b',                               'Bond'),
    (r'corporate\s+debt\s+instrument',                 'Bond'),
    (r'\d(?:\.\d+)?\s*%[^%]{0,40}\b(?:due|matur)',      'Bond'),   # coupon% + maturity => direct bond
    (r'(?:call|put)\s+options?\b',                     'Derivative'),
    (r'\bfutures\s+contracts?\b',                      'Derivative'),
    (r'interest\s+rate\s+swaps?\b',                    'Derivative'),
    (r'registered\s+investment\s+compan(?:y|ies)',     'Mutual Fund'),
    (r'money\s+market',                                'Money Market Fund'),
    (r'employer\s+securit(?:y|ies)',                   'Employer Stock'),
    (r'(?:self[\-\s]?directed\s+)?brokerage\s+account', 'Self-Directed Brokerage Account'),
    (r'money\s+mkt',                                   'Money Market Fund'),   # "MONEY MKT" abbrev
    (r'\bmmrk\b',                                      'Money Market Fund'),   # "MMRK" abbrev
    (r'common\s+stock',                                'Employer Stock'),
    (r'common\s+shares',                               'Employer Stock'),
    (r'\ber\s+stock',                                  'Employer Stock'),   # "... ER Stock Fund" (employer)
    # a corporate entity (Inc/Corp/PLC/Ltd) whose name ENDS in bare "stock" -> common stock
    (r'(?:\binc\b|\bcorp\b|corporation|\bplc\b|\bltd\b)\b.*\bstock\s*$', 'Employer Stock'),
    # a directly-held company security: a corporate-entity name carrying a share COUNT and no
    # fund/trust/account vehicle word -> employer stock (a mutual fund is never named this way)
    (r'(?:corporation|incorporated|\bcorp\b|\binc\b|\bplc\b|\bco\b|company)\b(?![^\d]*\b(fund|trust|account|portfolio|index)\b)[^\d]*\d[\d,]*\s*shares?', 'Employer Stock'),
    (r'mutual\s+funds?',                               'Mutual Fund'),
]


def detect_asset_type_row(text: str) -> str:
    """Detect a per-row asset-type DECLARATION carried in the row (a Type-column value or a
    vehicle-type phrase in the description). Non-name-based: only explicit vehicle-type
    phrases match, so a fund NAME never triggers a type. Returns '' if the row carries no
    explicit type. Used when the section heading gave nothing."""
    if not text:
        return ''
    for pattern, asset_type in ROW_TYPE_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return asset_type
    return ''
