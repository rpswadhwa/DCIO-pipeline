"""Deterministic preview decisions. Never writes a business table.

Names, amounts and provider roles remain separate evidence. Amount alone is
never an existing-holding match; share classes and years survive normalization.
"""
from collections import defaultdict
from functools import lru_cache
import hashlib
import re

from .stable_value_routing import amount, choose_vehicle, normalize_name, similar_name

VERSION = 'holding_reconciliation_preview_v1'
SOURCES = ('plan_cit_history', 'plan_mf_history_v3', 'plan_alternatives_history',
           'plan_insurance_history', 'plan_separately_managed_history',
           'plan_stable_value_unresolved_history')
FIELDS = ('ack_id', 'raw_entity_name', 'raw_sponsor_name', 'plan_investment_amt',
          'asset_class', 'asset_sub_class', 'validation_status', 'asset_type')
ABBREVIATIONS = {'stab': 'stable', 'stabl': 'stable', 'stbl': 'stable',
                 'val': 'value', 'cl': 'class', 'acct': 'account'}
STABLE = re.compile(r'\bstable\s+value\b')
# (2026-10-17) Stable value products whose names never say "stable value". Each alias is a
# product family seen in staging (Vanguard RST, Fidelity MIP, insurer guaranteed/fixed-interest
# general-account funds, preservation funds). Bare "fixed interest", "guaranteed interest",
# synthetic GIC/wrap components and TIAA Traditional stay out: they need review, not an alias.
SV_PRODUCT = re.compile(
    r'\b(retirement savings trust|managed income portfolio|(?:fid(?:elity)? )?mip(?: ii| iv)? class|fidelity mip|'
    r'guaranteed (?:income|interest|investment|long term) fund|guaranteed interest (?:account|option)|'
    r'fixed income guaranteed option|principal guaranteed option|guaranteed portfolio fund|select guaranteed fund|'
    r'fixed ?interest (?:option|fund|account)|capital preservation (?:fund|account|income fund)|'
    r'principal preservation|interest income fund|stable (?:asset|return) fund)\b')
MANAGED = re.compile(r'\bseparately?\s+managed\s+account\b')
SECURITY = re.compile(r'\b(common stock|preferred stock|convertible|cusip|ticker|shares of|collateral|continued)\b')
CONFLICT = re.compile(r'\b(small cap|large cap|mid cap|equity|growth|target date|target retirement|yourpath|lifepath|freedom)\b')
CONTRACT = re.compile(r'\b(guaranteed (?:investment|insurance) contract|group annuity|insurance (?:general|separate) account)\b')
PRODUCT = re.compile(r'\b(fixed account|guaranteed (?:fixed interest|interest account|interest accounts)|fully benefitresponsive investment contract|fully benefit responsive investment contract)\b')
SEPARATE = re.compile(r'\bseparate accounts?\b|\bseparately? managed accounts?\b|\bsma\b')
POINTER = re.compile(r'\bsee (?:appendix|attached|attachment|schedule)\b')


@lru_cache(maxsize=100000)
def product_name(value):
    tokens = re.sub(r'[^a-z0-9]+', ' ', normalize_name(value)).split()
    return ' '.join(ABBREVIATIONS.get(t, t) for t in tokens)


def identity(row):
    text = '\x1f'.join([normalize_name(row.get('ack_id')),
                        product_name(row.get('raw_entity_name')),
                        normalize_name(row.get('raw_sponsor_name')),
                        str(amount(row.get('plan_investment_amt')))])
    return hashlib.sha256(text.encode()).hexdigest()


def classify(row, alternative_patterns=()):
    name = product_name(row.get('raw_entity_name'))
    typ = normalize_name(row.get('asset_type'))
    cls = normalize_name(row.get('asset_class'))
    alias_sv = not STABLE.search(name) and bool(SV_PRODUCT.search(name))
    explicit_sv = bool(STABLE.search(name)) or alias_sv
    typed_sv = 'stable value' in typ or cls == 'stable value'
    contract = bool(CONTRACT.search(name))
    contract_mandate = contract and bool(re.search(
        r'\bguaranteed (?:investment|insurance) contract\b|\bfixed (?:account|series|annuity)\b|'
        r'\bguaranteed (?:interest|income)\b|\bprincipal preserv\w*\b', name))
    product = bool(PRODUCT.search(name))
    separate = bool(SEPARATE.search(' '.join([name, typ, cls, normalize_name(row.get('asset_sub_class'))])))
    managed = bool(MANAGED.search(name))
    if POINTER.search(name):
        return 'OUTSIDE_SCOPE', '', 'schedule_pointer', 'pointer_exclusion'
    if SECURITY.search(name) or typ in {'common stock', 'employer stock', 'stocks', 'exchange traded funds'}:
        if explicit_sv or typed_sv or contract or product:
            return 'UNRESOLVED', '', 'stable_value_security_conflict', 'security_conflict'
        return 'OUTSIDE_SCOPE', '', 'underlying_security', 'security_exclusion'
    if explicit_sv or typed_sv or contract or product:
        if CONFLICT.search(name) or cls not in {'', 'pending_ai', 'stable value'}:
            return 'UNRESOLVED', '', 'conflicting_stable_value_evidence', 'classification_conflict'
        if contract and not (explicit_sv or typed_sv or contract_mandate):
            return 'UNRESOLVED', '', 'insurance_wrapper_without_stable_value_mandate', 'insurance_mandate_evidence_required'
        # A generic fixed-account label or extracted type suggests a candidate,
        # but does not establish the product's investment mandate.
        if not (explicit_sv or contract_mandate or (product and typed_sv)):
            return 'UNRESOLVED', '', 'stable_value_product_evidence_required', 'type_or_product_only'
        destination, _, _, _, rule = choose_vehicle(row)
        return 'INSERT', destination, rule + (':sv_product_alias' if alias_sv else ''), 'confirmed_stable_value'
    if separate:
        if managed and name not in {'separately managed account', 'separate managed account'}:
            if re.search(r'\breserve trust\b|\bcollective\b|\bpooled\b', name):
                return 'UNRESOLVED', '', 'pooled_vehicle_in_managed_account_name', 'managed_wrapper_conflict'
            if typ in {'insurance separate account', 'insurance general account'}:
                return 'UNRESOLVED', '', 'conflicting_account_structure', 'account_conflict'
            return 'INSERT', 'plan_separately_managed_history', 'explicit_managed_menu_account', 'managed_menu_account'
        return 'UNRESOLVED', '', 'account_structure_or_menu_evidence_required', 'generic_separate_account'
    # The existing alternative keyword catalogue supplies candidates; preserve
    # its manual-review requirement rather than inventing new automatic rules.
    for pattern in alternative_patterns:
        if pattern[0] in name:
            return 'UNRESOLVED', 'plan_alternatives_history', 'alternative_keyword_requires_review', pattern[4]
    return 'OUTSIDE_SCOPE', '', 'ordinary_holding_without_target_evidence', 'scope_exclusion'


def history_index(history):
    names, values = defaultdict(list), defaultdict(list)
    for h in history:
        names[product_name(h.get('raw_entity_name'))].append(h)
        value = amount(h.get('plan_investment_amt'))
        if value is not None:
            values[value].append(h)
    return names, values


def possible_history_variants(row, history):
    # Similarity is a hold-for-review guard, never proof of existing identity.
    # It remains relevant when the amount also changed on a new extraction.
    return [h for h in history if similar_name(row.get('raw_entity_name', ''), h.get('raw_entity_name', ''))]


def decide(row, indexes, pair_identities, duplicate_of='', alternative_patterns=(), label_conflict=False,
           staging_amount_names=None, approved_rules=None):
    status, destination, reason, rule = classify(row, alternative_patterns)
    approval_key = (product_name(row.get('raw_entity_name')),
                    normalize_name(row.get('raw_sponsor_name')),
                    normalize_name(row.get('asset_type')),
                    normalize_name(row.get('asset_class')),
                    normalize_name(row.get('asset_sub_class')))
    approval = (approved_rules or {}).get(approval_key)
    if approval and reason in {'stable_value_product_evidence_required', 'account_structure_or_menu_evidence_required',
                              'alternative_keyword_requires_review', 'insurance_wrapper_without_stable_value_mandate'}:
        status, destination, reason, rule = 'INSERT', approval['destination_table'], 'approved_product_rule', approval['rule_id']
    result = dict(row, candidate_id=identity(row), outcome=status,
                  destination_table=destination, reason=reason, rule=rule,
                  matched_history_tables='', matched_history_names='',
                  matched_history_sponsors='', duplicate_of=duplicate_of,
                  normalized_entity_name=product_name(row.get('raw_entity_name')),
                  routing_version=VERSION)
    value = amount(row.get('plan_investment_amt'))
    names, values = indexes
    exact = names.get(result['normalized_entity_name'], []) if result['normalized_entity_name'] else []
    same = [h for h in exact if value is not None and amount(h.get('plan_investment_amt')) == value]
    provider = normalize_name(row.get('raw_sponsor_name'))
    compatible = [h for h in same if normalize_name(h.get('raw_sponsor_name')) == provider]
    def evidence(matches):
        result['matched_history_tables'] = '; '.join(sorted({h['source_table'] for h in matches}))
        result['matched_history_names'] = '; '.join(sorted({h.get('raw_entity_name', '') for h in matches}))
        result['matched_history_sponsors'] = '; '.join(sorted({h.get('raw_sponsor_name', '') for h in matches}))
    if not row.get('ack_id', '').strip() or value is None or not result['normalized_entity_name']:
        result.update(outcome='UNRESOLVED', reason='missing_identity_or_amount')
    elif compatible:
        evidence(compatible)
        signatures = {(h['source_table'], product_name(h.get('raw_entity_name')),
                       normalize_name(h.get('raw_sponsor_name')), str(amount(h.get('plan_investment_amt')))) for h in compatible}
        if len(compatible) == 1 and len(signatures) == 1:
            result.update(outcome='EXISTING', reason='same_ack_normalized_name_provider_amount', destination_table=compatible[0]['source_table'])
        else:
            result.update(outcome='UNRESOLVED', reason='multiple_existing_records')
    elif exact:
        evidence(exact)
        result.update(outcome='UNRESOLVED', reason='existing_name_provider_or_amount_conflict')
    elif label_conflict:
        result.update(outcome='UNRESOLVED', reason='duplicate_identity_conflicting_source_labels')
    elif status != 'OUTSIDE_SCOPE':
        if value < 0:
            result.update(outcome='UNRESOLVED', reason='negative_amount')
        elif len(pair_identities.get(result['normalized_entity_name'], set())) > 1:
            result.update(outcome='UNRESOLVED', reason='staging_name_multiple_providers_or_amounts')
        elif len((staging_amount_names or {}).get(value, set())) > 1:
            result.update(outcome='UNRESOLVED', reason='same_staging_amount_different_products')
        elif value > 0 and values.get(value):
            evidence(values[value])
            result.update(outcome='UNRESOLVED', reason='same_ack_amount_different_product')
    # Repeated identical extractions produce one proposal, never summed amounts.
    if result['outcome'] == 'INSERT':
        near = possible_history_variants(row, (h for bucket in names.values() for h in bucket))
        if near:
            evidence(near)
            result.update(outcome='UNRESOLVED', reason='possible_history_name_variant')
    if duplicate_of and result['outcome'] == 'INSERT':
        result.update(outcome='UNRESOLVED', reason='duplicate_staging_proposal')
    return result
