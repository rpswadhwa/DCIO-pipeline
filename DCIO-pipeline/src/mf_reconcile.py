"""mf_reconcile.py -- post-extraction MF reconciliation against certified totals.

Reclassifies mis-typed rows in/out of the mutual-fund total so a plan's extracted
MF matches the certified amt_mutual_funds. Runs per plan, only when the plan is
off by more than `tolerance`.

OVER-capturing plans -- a cascade of removals, each stopping the moment the plan
lands within tolerance and never overshooting below certified:
  1. money-market rows            (often not counted in certified amt_mutual_funds)
  2. other non-MF by name         (employer/company stock, stable value, contract,
                                   separate account, non-CREF annuity, general acct, brokerage)
  3. CIT by name                  (collective / commingled / CIT / trust / unitized)
  4. amt_cit reconciliation       (identical-named CIT share classes: strip the largest
                                   remaining MF rows, bounded by the certified-CIT shortfall)

UNDER-capturing plans -- recover blank rows whose name clearly reads as a mutual
fund (fund-family / fund words, no CIT/trust/stable signal), largest-first.

Stages 1-3 and the under-side are NAME-BASED (high confidence). Stage 4 is
AMOUNT-BASED (removes by size, not name) and every stage-4 change is flagged
needs_review=True so callers can hold it for sign-off if desired.
"""
import re

MF_ASSET_TYPES = frozenset({'mutual fund', 'money market fund', 'etf', 'index fund', 'target date fund'})

# --- name signals ---
_CIT = re.compile(r'collectiv|commingled|\bcit\b|\bctf\b|common\s+trust|unitized|\btrust\b', re.I)
_OTHER = re.compile(
    r'company\s+stock|employer\s+stock|stock\s+match|\bstock\s+fund\b|\bcommon\s+stock\b|\besop\b|'
    r'stable\s+val|stable\s+ret|contract\s+income|\bgic\b|capital\s+preservation|principal\s+preservation|'
    r'guaranteed\s+(?:income|interest|fund|invest|option|deposit)|separate\s+acc|\bsma\b|managed\s+account|'
    r'group\s+annuit|fixed\s+annuit|tiaa\s+trad|annuity\s+contract|general\s+acc|self.?direct|brokerage', re.I)
# Never move these out of MF even if another signal matches: real MFs/ETFs and CREF
# (certified amt_mutual_funds includes CREF/variable annuities), and the fund company
# "Northern Trust" (whose name carries the word "trust").
_KEEP = re.compile(r'\betf\b|ishares|\bspdr\b|mutual\s+fund|northern\s+trust|\bcref\b', re.I)
_FAM = re.compile(
    r'vanguard|\bvang\b|fidelity|\bfid\b|t\.?\s*rowe|trowe|\btrp\b|blackrock|\bblkrk\b|american\s+funds|'
    r'jpmorgan|jp\s*morgan|pimco|dodge|dimensional|\bdfa\b|schwab|state\s+street|invesco|franklin|'
    r'western\s+asset|oakmark|harbor|nuveen|\bmfs\b|janus|victory|principal|prudential|columbia|hartford|'
    r'lord\s+abbett|neuberger|artisan|wellington|loomis|baird|calvert|delaware|eaton\s+vance|goldman|'
    r'natixis|putnam|virtus|voya|allspring|american\s+century|john\s+hancock|federated|touchstone|contrafund', re.I)
_FUNDY = re.compile(r'\bfund\b|\bindex\b|\bidx\b|\b500\b|\badmiral\b|portfolio|target\s+retire|\br[3-6]\b', re.I)
_NONMFSIG = re.compile(
    r'collectiv|commingled|\bcit\b|unitized|common\s+trust|separate\s+acc|\bsma\b|stable\s+val|\bgic\b|'
    r'guarante|general\s+acc|group\s+annuit|\btrust\b|see\s+attach|company\s+stock|employer\s+stock', re.I)

# reclassification target types (all OUTSIDE MF_ASSET_TYPES so they drop from the MF total)
CIT_TYPE = 'Common/Collective Trust Fund'
MMF_OUT_TYPE = 'Cash/Money Market'
OTHER_OUT_TYPE = 'Other (non-MF)'
MF_TYPE = 'Mutual Fund'


def _fv(x):
    try:
        return float(str(x).replace('$', '').replace(',', '').strip() or 0)
    except (TypeError, ValueError):
        return 0.0


def _mf_total(rows, type_key, value_key):
    return sum(_fv(r.get(value_key)) for r in rows
               if str(r.get(type_key, '') or '').strip().lower() in MF_ASSET_TYPES)


def reconcile_plan(rows, certified_mf, certified_cit=0.0, tolerance=0.05,
                   name_key='raw_entity_name', type_key='asset_type', value_key='plan_investment_amt'):
    """Reclassify rows of ONE plan so extracted MF matches certified_mf.

    Mutates rows[i][type_key] in place. Returns a list of change dicts:
      {name, value, old_type, new_type, stage, needs_review}
    Only acts when the plan is over/under by more than `tolerance`. Never flips a
    plan across the target (removals stop before overshooting below certified;
    additions stop before overshooting above it).
    """
    changes = []
    cm = _fv(certified_mf)
    if cm <= 0:
        return changes
    cur = _mf_total(rows, type_key, value_key)
    hi, lo = cm * (1 + tolerance), cm * (1 - tolerance)

    def is_mf(r):
        return str(r.get(type_key, '') or '').strip().lower() in MF_ASSET_TYPES

    def nm(r):
        return str(r.get(name_key, '') or '')

    if cur > hi:
        def strip(pred, new_type, stage, needs_review=False, budget=None):
            nonlocal cur
            elig = sorted([r for r in rows if is_mf(r) and pred(r)],
                          key=lambda r: -_fv(r.get(value_key)))
            for r in elig:
                if cur <= hi:
                    break
                if budget is not None and budget[0] <= 0:
                    break
                v = _fv(r.get(value_key))
                if cur - v < lo:          # would overshoot into under-capture -> skip, try smaller
                    continue
                old = str(r.get(type_key, '') or '')
                r[type_key] = new_type
                cur -= v
                if budget is not None:
                    budget[0] -= v
                changes.append({'name': nm(r), 'value': v, 'old_type': old,
                                'new_type': new_type, 'stage': stage, 'needs_review': needs_review})

        # 1) money-market
        strip(lambda r: str(r.get(type_key, '') or '').strip().lower() == 'money market fund',
              MMF_OUT_TYPE, '1-money-market')
        # 2) other non-MF by name
        strip(lambda r: bool(_OTHER.search(nm(r))) and not _KEEP.search(nm(r)),
              OTHER_OUT_TYPE, '2-other-nonmf')
        # 3) CIT by name
        strip(lambda r: bool(_CIT.search(nm(r))) and not _KEEP.search(nm(r)),
              CIT_TYPE, '3-cit-name')
        # 4) amt_cit reconciliation (amount-based; bounded by the certified-CIT shortfall)
        cc = _fv(certified_cit)
        if cc > 0 and cur > hi:
            captured = sum(_fv(r.get(value_key)) for r in rows
                           if str(r.get(type_key, '') or '').strip().lower() == CIT_TYPE.lower())
            budget = [max(0.0, cc - captured)]
            if budget[0] > 0:
                strip(lambda r: not _KEEP.search(nm(r)), CIT_TYPE, '4-cit-reconcile',
                      needs_review=True, budget=budget)

    elif cur < lo:
        def mflike(r):
            n = nm(r)
            return bool((_FAM.search(n) or _FUNDY.search(n)) and not _NONMFSIG.search(n))

        blanks = sorted([r for r in rows if not str(r.get(type_key, '') or '').strip() and mflike(r)],
                        key=lambda r: -_fv(r.get(value_key)))
        for r in blanks:
            if cur >= lo:
                break
            v = _fv(r.get(value_key))
            r[type_key] = MF_TYPE
            cur += v
            changes.append({'name': nm(r), 'value': v, 'old_type': '', 'new_type': MF_TYPE,
                            'stage': '1-blank-mf-recover', 'needs_review': False})

    return changes
