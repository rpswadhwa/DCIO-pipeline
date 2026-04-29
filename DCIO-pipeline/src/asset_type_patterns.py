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
    (r'Common\s*/\s*Collective\s+Trust\s+Funds?',               'Common/Collective Trust Fund'),
    (r'Common\s*/\s*Collective\s+Trusts?',                      'Common/Collective Trust Fund'),
    (r'Collective\s*/\s*Common\s+Trust\s+Funds?',               'Common/Collective Trust Fund'),
    (r'Common\s+Collective\s+Trust\s+Funds?',                   'Common/Collective Trust Fund'),
    (r'Collective\s+Investment\s+Trusts?',                      'Common/Collective Trust Fund'),
    (r'Collective\s+Trust\s+Funds?',                            'Common/Collective Trust Fund'),
    (r'Common\s+Collective\s+Trusts?',                          'Common/Collective Trust Fund'),
    (r'Pooled\s+Separate\s+Accounts?',                          'Commingled Fund'),
    (r'Separately\s+Managed\s+Accounts?',                       'Separately Managed Account'),
    (r'Self[\-\s]Directed\s+Brokerage\s+Accounts?',             'Self-Directed Brokerage Account'),
    (r'Commingled\s+Funds?',                                    'Commingled Fund'),
    (r'Stable\s+Value\s+Funds?',                                'Stable Value Fund'),
    (r'Money\s+Market\s+Funds?',                                'Money Market Fund'),
    (r'Registered\s+Investment\s+Compan(?:y|ies)',              'Mutual Fund'),
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
