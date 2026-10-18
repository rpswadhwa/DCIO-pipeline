"""Pure holding matching and routing, independent of upstream extraction."""
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher
from functools import lru_cache
import hashlib
import re

VERSION = 'stable_value_router_v2'
TARGETS = ('plan_insurance_history', 'plan_separately_managed_history',
           'plan_stable_value_unresolved_history')
STABLE = re.compile(r'(?i)(?<![a-z0-9])stable\s+value(?![a-z0-9])')


@lru_cache(maxsize=200000)
def normalize_name(value):
    return ' '.join((value or '').strip().lower().split())


def amount(value):
    try:
        parsed = Decimal(str(value))
        return parsed.quantize(Decimal('0.01')) if parsed.is_finite() else None
    except (InvalidOperation, ValueError, TypeError):
        return None


@lru_cache(maxsize=200000)
def loose_name(value):
    return ' '.join(re.sub(r'[^a-z0-9]+', ' ', normalize_name(value)).split())


@lru_cache(maxsize=200000)
def name_tokens(value):
    return frozenset(loose_name(value).split()) - {'fund', 'trust', 'the', 'value', 'stable', 'account', 'portfolio'}


@lru_cache(maxsize=200000)
def stable_name_signal(value):
    return bool(re.search(r'\bstable\b|\bstabl\w*|\bstbl\w*|\bst\s+val\b|\bmanaged\s+inc|\bmip\b|\bcapital\s+preserv|\binterest\s+income|\bsagic\b|\bgic\b|\bguaranteed\s+(?:interest|income|investment)|\bfixed\s+account|\bretirement\s+savings\s+trust|\bmanaged\s+income\s+portfolio|\bpreservation|\bfixed\s?interest', normalize_name(value)))


def similar_name(left, right):
    a, b = loose_name(left), loose_name(right)
    if a and a == b:
        return True
    # A shared brand with an unrelated equity/bond fund is not evidence of a
    # duplicate stable value holding. Same-amount checks remain independent.
    if not stable_name_signal(right):
        return False
    ta, tb = name_tokens(left), name_tokens(right)
    if ta and tb and (ta <= tb or tb <= ta):
        return True
    if not a or not b:
        return False
    matcher = SequenceMatcher(None, a, b)
    return matcher.real_quick_ratio() >= .88 and matcher.quick_ratio() >= .88 and matcher.ratio() >= .88


def choose_vehicle(row):
    name = normalize_name(row.get('raw_entity_name'))
    vehicle = normalize_name(row.get('asset_type'))
    signals = set()
    # Provider disclosures identify these named products as general-account
    # group annuities. A contradictory staging CIT label stays unresolved.
    # https://www.ta-retirement.com/funds/FundFootnotes.aspx?cat=60
    known_insurance_core = bool(re.search(r'transamerica\s+stable\s+value\s+core\s+(?:account|option)\b', name))
    # Verified Series Class VI product: Empower general-account group annuity.
    # https://docs.retirementpartner.com/fundov/EMPOWER_GWAQ10_FUNDOV.PDF
    known_ei = bool(re.fullmatch(r'(?:empower )?ei fixed account\s*-?\s*series class vi', name))
    if known_insurance_core or known_ei:
        signals.add('insurance')
    insurance_types = {'insurance general account', 'insurance separate account',
                       'guaranteed insurance contract', 'guaranteed investment contract',
                       'group annuity contract', 'general account', 'annuity contract', 'gic'}
    if vehicle in insurance_types or re.search(r'insurance\s+(?:company\s+)?(?:general|separate)\s+account|group\s+annuity|guaranteed\s+(?:investment|insurance)\s+contract', name):
        signals.add('insurance')
    if vehicle in {'common/collective trust fund', 'collective investment trust', 'collective trust fund'} or re.search(r'common\s*/?\s*collective\s+trust|collective\s+investment\s+trust|\bcit\b|common\s+trust', name):
        signals.add('cit')
    # Existing detector mislabels insurance separate accounts as separately
    # managed. Require corroboration in the name; generic labels go unresolved.
    if re.search(r'separately?\s+managed\s+account', name):
        signals.add('managed')
    if len(signals) != 1:
        reason = 'conflicting_vehicle_evidence' if len(signals) > 1 else 'vehicle_unresolved'
        return 'plan_stable_value_unresolved_history', 'unknown', 'unknown', 'unknown', reason
    signal = next(iter(signals))
    if signal == 'cit':
        return 'plan_cit_history', 'collective_investment_trust', 'pooled_trust', 'unknown', 'explicit_collective_trust'
    if signal == 'managed':
        return 'plan_separately_managed_history', 'separately_managed_account', 'plan_specific_account', 'unknown', 'explicit_plan_specific_account'
    account = ('general_account' if known_insurance_core or known_ei else 'insurance_separate_account' if 'separate account' in vehicle or 'separate account' in name
               else 'general_account' if 'general account' in vehicle or 'general account' in name else 'unknown')
    contract = ('group_annuity' if known_insurance_core or known_ei or 'annuity' in vehicle or 'annuity' in name
                else 'gic' if vehicle in {'gic', 'guaranteed investment contract', 'guaranteed insurance contract'} or 'guaranteed investment contract' in name else 'unknown')
    return 'plan_insurance_history', 'insurance_contract', account, contract, 'explicit_insurance_vehicle'


def build_decisions(staging, history, resolver):
    by_ack = defaultdict(list)
    for holding in history:
        by_ack[holding['ack_id']].append(holding)
    pair_count = Counter()
    amount_names = defaultdict(set)
    for row in staging:
        pair_count[(row['ack_id'], normalize_name(row['raw_entity_name']))] += int(row.get('staging_row_count', 1))
        value = amount(row.get('plan_investment_amt'))
        if value is not None and value > 0:
            amount_names[(row['ack_id'], value)].add(normalize_name(row['raw_entity_name']))
    decisions = []
    for row in staging:
        decision = dict(row)
        name = normalize_name(row['raw_entity_name'])
        value = amount(row.get('plan_investment_amt'))
        identity = '\x1f'.join([row['ack_id'] or '', name,
                               normalize_name(row.get('raw_sponsor_name')), str(value)])
        decision['candidate_id'] = hashlib.sha256(identity.encode()).hexdigest()
        decision['normalized_entity_name'] = name
        decision['plan_investment_amt'] = str(value) if value is not None else None
        decision['routing_version'] = VERSION
        decision['matched_history_table'] = None
        decision['matched_history_name'] = None
        destination, vehicle, account, contract, rule = choose_vehicle(row)
        decision.update(destination_table=destination, vehicle_type=vehicle,
                        account_structure=account, contract_type=contract, routing_rule=rule)
        decision.update(resolver.resolve(row['raw_entity_name'], row.get('raw_sponsor_name')))
        decision['routing_status'] = 'READY'
        decision['routing_reason'] = rule
        existing = by_ack[row['ack_id']]
        exact = [h for h in existing if normalize_name(h['raw_entity_name']) == name]
        if not row['ack_id'] or not row['ack_id'].strip() or value is None:
            decision.update(routing_status='REVIEW', routing_reason='missing_ack_or_amount')
        elif exact:
            same = [h for h in exact if amount(h.get('plan_investment_amt')) == value]
            own = [h for h in same if h.get('candidate_id') == decision['candidate_id']]
            if own and len(exact) == 1:
                decision.update(routing_status='ALREADY_EXISTS', routing_reason='router_candidate_already_present')
            elif len(exact) == 1 and len(same) == 1 and pair_count[(row['ack_id'], name)] == 1:
                decision.update(routing_status='ALREADY_EXISTS', routing_reason='same_ack_name_amount')
            else:
                decision.update(routing_status='REVIEW', routing_reason='existing_pair_amount_or_multiplicity_conflict')
            decision['matched_history_table'] = '; '.join(sorted({h['source_table'] for h in exact}))
            decision['matched_history_name'] = exact[0]['raw_entity_name']
        elif pair_count[(row['ack_id'], name)] > 1:
            decision.update(routing_status='REVIEW', routing_reason='repeated_staging_ack_name_pair')
        elif value < 0:
            decision.update(routing_status='REVIEW', routing_reason='negative_amount')
        elif len(amount_names[(row['ack_id'], value)]) > 1:
            decision.update(routing_status='REVIEW', routing_reason='same_staging_ack_amount_different_names')
        elif normalize_name(row.get('asset_class')) not in {'', 'pending_ai', 'stable value'}:
            decision.update(routing_status='REVIEW', routing_reason='conflicting_stable_value_classification')
        elif re.search(r'(?i)(yourpath|lifepath|freedom|target\s+date|target\s+retirement).*20\d{2}|stable\s+value\s+core\s+2023', row['raw_entity_name']):
            decision.update(routing_status='REVIEW', routing_reason='target_date_name_with_stable_value_text')
        else:
            near = [h for h in existing if similar_name(row['raw_entity_name'], h['raw_entity_name'])]
            # Same amount under a materially different name is also held for
            # review; a conservative false positive is preferable to a duplicate.
            same_amount = [h for h in existing if value > 0 and amount(h.get('plan_investment_amt')) == value]
            suspected = near or same_amount
            if suspected:
                decision.update(routing_status='REVIEW', routing_reason='possible_existing_holding_name_variant' if near else 'same_ack_amount_different_name')
                decision['matched_history_table'] = '; '.join(sorted({h['source_table'] for h in suspected}))
                decision['matched_history_name'] = suspected[0]['raw_entity_name']
        decisions.append(decision)
    return decisions
