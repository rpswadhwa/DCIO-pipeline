"""Production adapter for the reviewed stable-value reconciliation rules."""
from collections import defaultdict

from .holding_reconciliation import classify, decide, history_index, identity, product_name
from .stable_value_routing import VERSION, amount, choose_vehicle


def eligible(row):
    # Non-stable-value managed-account proposals are outside this promotion.
    outcome, _, _, rule = classify(row)
    return rule == 'confirmed_stable_value' or ('stable_value' in rule or
        'stable_value' in classify(row)[2] or rule in {'type_or_product_only',
        'insurance_mandate_evidence_required', 'classification_conflict', 'security_conflict'})


def build_decisions(staging, history, resolver):
    source_by_ack, history_by_ack = defaultdict(list), defaultdict(list)
    for row in staging:
        source_by_ack[row['ack_id']].append(row)
    for row in history:
        history_by_ack[row['ack_id']].append(row)
    results = []
    for ack, rows in source_by_ack.items():
        pairs, labels, amounts = defaultdict(set), defaultdict(set), defaultdict(set)
        grouped = defaultdict(list)
        for row in rows:
            key = identity(row)
            grouped[key].append(row)
            pairs[product_name(row.get('raw_entity_name'))].add(key)
            labels[key].add(tuple((row.get(k) or '').strip().lower()
                                 for k in ('asset_type', 'asset_class', 'asset_sub_class')))
            value = amount(row.get('plan_investment_amt'))
            if value is not None:
                amounts[value].add(product_name(row.get('raw_entity_name')))
        indexes = history_index(history_by_ack[ack])
        for key, copies in grouped.items():
            candidates = [r for r in copies if eligible(r)]
            if not candidates:
                continue
            row = candidates[0]
            d = decide(row, indexes, pairs, label_conflict=len(labels[key]) > 1,
                       staging_amount_names=amounts)
            destination, vehicle, account, contract, rule = choose_vehicle(row)
            d.update(vehicle_type=vehicle, account_structure=account, contract_type=contract,
                     routing_rule=rule, routing_reason=d['reason'], routing_version=VERSION,
                     routing_status={'INSERT': 'READY', 'EXISTING': 'ALREADY_EXISTS'}.get(d['outcome'], 'REVIEW'),
                     staging_row_count=sum(int(r.get('staging_row_count', 1)) for r in copies),
                     matched_history_table=d['matched_history_tables'],
                     matched_history_name=d['matched_history_names'],
                     plan_investment_amt=str(amount(row.get('plan_investment_amt'))) if amount(row.get('plan_investment_amt')) is not None else None)
            d.update(resolver.resolve(row.get('raw_entity_name'), row.get('raw_sponsor_name')))
            results.append(d)
    return results
