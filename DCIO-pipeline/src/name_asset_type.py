"""name_asset_type.py -- NAME-BASED asset-type remediation for over-capture plans.

This is intentionally NAME-BASED, unlike the extraction-time typers (which read the
document's structure and never guess from a name). It exists to catch non-MF vehicles
that inherited a "Mutual Funds" section heading and are inflating the MF total -- e.g.
a TIAA Traditional / stable-value / guaranteed contract, a limited partnership, or a
directly-held common stock that landed as `mutual fund`.

Because guessing a vehicle from a name is risky, this runs ONLY as an END-OF-PIPELINE
pass and ONLY on plans that are OVER-CAPTURING (extracted MF > certified beyond
tolerance). On those plans the downside of leaving a mis-typed non-MF in the MF total
is concrete (false over-capture); the upside of a rare wrong reclass is bounded because
we never touch plans that already pass or under-capture.

classify_by_name(name) -> canonical asset_type or ''.
remediate_overcapture_plan(rows, certified, ...) -> (rows, changes).
"""
import re

MF_ASSET_TYPES = frozenset({"mutual fund", "index fund", "money market fund", "etf", "target date fund"})

# Ordered, non-MF first. A name matches at the first hit.
_PATTERNS = [
    # --- guaranteed annuity contracts (TIAA Traditional etc.) ---
    (r'benefit[\s-]*responsive',                       'Group Annuity Contract'),
    (r'tiaa\s+traditional',                            'Group Annuity Contract'),
    (r'traditional\s+annuit',                          'Group Annuity Contract'),
    (r'annuity\s+contract',                            'Group Annuity Contract'),
    (r'fixed\s+annuit',                                'Group Annuity Contract'),
    # --- fixed / guaranteed / GIC / stable value (principal preservation) ---
    (r'guarante(?:e|ed|y|ie)\w*\s+(?:option|fund|income|interest|account|portfolio|contract|annuit|value)',
                                                       'Stable Value Fund'),
    (r'\bguar\s+(?:option|opt|inc|income)',            'Stable Value Fund'),   # 'Guar Option'
    (r'guaranteed\s+(?:investment|interest)\s+contract', 'Stable Value Fund'),
    (r'fixed\s+(?:account|income|interest)',           'Stable Value Fund'),
    (r'\bfxd\s+(?:sel|inc|acct|account)',              'Stable Value Fund'),   # 'NW FXD SEL OPTN'
    (r'stable\s+(?:value|return)',                     'Stable Value Fund'),
    (r'\bgic\b',                                       'Stable Value Fund'),
    (r'\bsv\s+[a-z]?\d',                               'Stable Value Fund'),   # 'CMFG SV A24'
    (r'\binvestment\s+contract\b',                     'Stable Value Fund'),
    # --- insurance company general account ---
    (r'insurance\s+(?:company\s+)?general\s+(?:account|contract)', 'Insurance General Account'),
    (r'\bgeneral\s+(?:account|contract)\b',            'Insurance General Account'),
    # --- limited partnership ---
    (r'limited\s+partnership|partnership\s+interest|interest\s+in\s+limited\s+part', 'Partnership Interest'),
    # --- money market / cash (JUDGMENT CALL: still counts as MF, but identify distinctly) ---
    (r'money\s+mar?ket|money\s+mkt|money\s+fund',      'Money Market Fund'),
    (r'government\s+money|govt\s+money|treasury\s+(?:money|obligations|portfolio)', 'Money Market Fund'),
    (r'bank\s+sweep|\bsweep\b|prime\s+(?:portfolio|money|obligation)|savings\s+fund|interest[\s-]*bearing\s+cash',
                                                       'Money Market Fund'),
]
_COMPILED = [(re.compile(p, re.IGNORECASE), t) for p, t in _PATTERNS]

_STOCK_COM = re.compile(r'(?i)\bcom(?:mon)?\s+stock\s*$')
_STOCK_CORP = re.compile(r'(?i)(?:corporation|incorporated|\binc\b|\bcorp\b|\bplc\b|\bltd\b|\bco\b|company)\b.*\bstock\s*$')
_STOCK_END = re.compile(r'(?i)\bstock\s*$')
_FUND_WORDS = re.compile(r'(?i)\b(fund|trust|index|portfolio|account|market|etf)\b')


def classify_by_name(name: str) -> str:
    """Return a canonical asset_type inferred from the name, or '' if none."""
    n = str(name or '').strip()
    if not n:
        return ''
    for rx, typ in _COMPILED:
        if rx.search(n):
            return typ
    # directly-held common stock: "... COM/COMMON STOCK", a corporate entity ending in
    # "stock", or an ALL-CAPS ticker-style name ending in "stock" (never a fund/index).
    if _STOCK_COM.search(n) or _STOCK_CORP.search(n):
        return 'Common Stock'
    if _STOCK_END.search(n) and not _FUND_WORDS.search(n):
        letters = [c for c in n if c.isalpha()]
        if letters and sum(1 for c in letters if c.isupper()) / len(letters) >= 0.8:
            return 'Common Stock'
    return ''


def remediate_overcapture_plan(rows, certified, tolerance=0.05,
                               name_key='raw_entity_name', type_key='asset_type', value_key='plan_investment_amt'):
    """Re-type MF rows of an OVER-CAPTURING plan when their name says non-MF.

    rows: list of dicts for ONE plan. certified: amt_mutual_funds (float).
    Only acts if the plan's MF total exceeds certified*(1+tolerance). Never re-types a
    row to another MF type (money-market stays MF); only non-MF reclassifications reduce
    the MF total. Returns (rows, changes) where changes is a list of
    (name, value, old_type, new_type). rows are mutated in place (type_key updated).
    """
    def fval(x):
        try:
            return float(x)
        except (TypeError, ValueError):
            return 0.0

    def mf_total(rs):
        return sum(fval(r.get(value_key)) for r in rs
                   if str(r.get(type_key, '') or '').strip().lower() in MF_ASSET_TYPES)

    changes = []
    if not certified or certified <= 0:
        return rows, changes
    if mf_total(rows) <= certified * (1 + tolerance):
        return rows, changes   # not over-capturing -> leave untouched

    for r in rows:
        cur = str(r.get(type_key, '') or '').strip().lower()
        if cur not in MF_ASSET_TYPES:
            continue
        inferred = classify_by_name(r.get(name_key))
        if inferred and inferred.lower() not in MF_ASSET_TYPES:
            changes.append((r.get(name_key), fval(r.get(value_key)), r.get(type_key), inferred))
            r[type_key] = inferred
    return rows, changes
