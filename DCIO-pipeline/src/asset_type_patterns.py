import re

# Canonical asset type patterns — ordered most-specific first.
# Used by both text_extract.py (section heading detection) and
# data_cleaner.py (asset type inference from field text).
ASSET_TYPE_PATTERNS = [
    (r'Investments?\s+in\s+mutual\s+funds?',                    'Mutual Fund'),
    (r'Investments?\s+in\s+money\s+markets?',                   'Money Market Fund'),
    (r'Investments?\s+in\s+common\s+collective\s+trusts?',      'Common/Collective Trust Fund'),
    (r'Investments?\s+in\s+pooled\s+separate\s+accounts?',      'Commingled Fund'),
    (r'Investments?\s+in\s+investment\s+contracts?',            'Stable Value Fund'),
    (r'Investments?\s+in\s+index\s+funds?',                     'Index Fund'),
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
    (r'Pooled\s+Separate\s+Accounts?',                          'Commingled Fund'),
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
    (r'Registered\s+Investment\s+Compan(?:y|ies)',              'Mutual Fund'),
    (r'Registered\s+Investment\s+Funds?',                       'Mutual Fund'),
    (r'Institutional\s+Funds?',                                 'Mutual Fund'),
    (r'Target[\-\s]Date\s+Funds?',                              'Target Date Fund'),
    (r'Target\s+Retirement\s+Funds?',                           'Target Date Fund'),
    (r'Index\s+Funds?',                                         'Index Fund'),
    (r'Mutual\s+Funds?',                                        'Mutual Fund'),
    (r'Employer\s+Stocks?',                                     'Employer Stock'),
    (r'Employer\s+Securities',                                  'Employer Stock'),
    (r'Preferred\s+Stocks?',                                    'Preferred Stock'),
    (r'Common\s+Stocks?',                                       'Common Stock'),
    (r'Publicly[\-\s]traded\s+Stocks?',                         'Common Stock'),
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
    (r'pooled\s+separate\s+accounts?',                 'Commingled Fund'),
    (r'separate(?:ly)?\s+managed\s+accounts?',         'Separately Managed Account'),
    (r'common\s*/?\s*collective\s+trusts?',            'Common/Collective Trust Fund'),
    (r'collective\s+investment\s+trusts?',             'Common/Collective Trust Fund'),
    (r'collective\s+investment',                       'Common/Collective Trust Fund'),
    (r'collecti\w{0,3}e\s+trusts?',                     'Common/Collective Trust Fund'),  # OCR-tolerant
    (r'commingled',                                    'Commingled Fund'),
    (r'pooled\s+funds?',                               'Commingled Fund'),
    (r'group\s+annuity',                               'Group Annuity Contract'),
    (r'guaranteed\s+(?:investment\s+contract|income)', 'Stable Value Fund'),
    (r'stable\s+value',                                'Stable Value Fund'),
    (r'registered\s+investment\s+compan(?:y|ies)',     'Mutual Fund'),
    (r'money\s+market',                                'Money Market Fund'),
    (r'employer\s+securit(?:y|ies)',                   'Employer Stock'),
    (r'(?:self[\-\s]?directed\s+)?brokerage\s+account', 'Self-Directed Brokerage Account'),
    (r'common\s+stock',                                'Common Stock'),
    (r'lifecycle\s+investment\s+option',               'Target Date Fund'),
    (r'target[\-\s]date',                              'Target Date Fund'),
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
