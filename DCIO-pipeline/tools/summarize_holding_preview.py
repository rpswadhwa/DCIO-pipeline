"""Verify a completed preview and produce a focused product-level review queue."""
import argparse
from collections import Counter
import csv
import hashlib
import json
import re
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.holding_reconciliation import CONTRACT, MANAGED, PRODUCT, SEPARATE, STABLE, product_name
from src.stable_value_routing import normalize_name


def scope(row):
    name = product_name(row['raw_entity_name'])
    typ = normalize_name(row['asset_type'])
    if STABLE.search(name) or CONTRACT.search(name) or PRODUCT.search(name) or 'stable value' in typ or normalize_name(row['asset_class']) == 'stable value':
        return 'stable_value'
    if SEPARATE.search(' '.join([name, typ, normalize_name(row['asset_class']), normalize_name(row['asset_sub_class'])])):
        return 'separate_account'
    if row['destination_table'] == 'plan_alternatives_history':
        return 'alternative'
    return 'other'


def family(row, category):
    name, typ = product_name(row['raw_entity_name']), product_name(row['asset_type'])
    pattern = ('explicit_stable_value_name' if STABLE.search(name) else
               'explicit_contract_name' if CONTRACT.search(name) else
               'fixed_or_guaranteed_account_name' if PRODUCT.search(name) else
               'explicit_managed_account_name' if MANAGED.search(name) else
               'explicit_separate_account_name' if SEPARATE.search(name) else
               'investment_strategy_or_fund_name' if re.search(r'\b(fund|trust|portfolio|income|equity|growth|cap|value|bond|index)\b', name) else
               'company_style_name' if re.search(r'\b(inc|corp|corporation|company|co|ltd|plc|bancorp)\b', name) else
               'generic_or_unrecognized_name')
    type_family = ('stable_value_labeled' if 'stable value' in typ else
                   'managed_account_labeled' if MANAGED.search(typ) or typ == 'sma' else
                   'separate_account_labeled' if SEPARATE.search(typ) else
                   'pooled_fund_labeled' if re.search(r'collective|commingled', typ) else
                   'mutual_fund_labeled' if 'mutual fund' in typ else
                   'insurance_contract_labeled' if re.search(r'annuity|guaranteed.*contract|general account|\bgic\b', typ) else
                   'other_or_missing_type')
    cls = normalize_name(row['asset_class'])
    class_family = 'stable_value' if cls == 'stable value' else 'not_classified' if cls in {'', 'pending_ai'} else 'classified_other'
    return (category, row['reason'], pattern, type_family, class_family)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--preview-dir', required=True)
    args = parser.parse_args()
    output = Path(args.preview_dir)
    summary = json.loads((output / 'summary.json').read_text())
    counts, focus, reasons, destinations = Counter(), Counter(), Counter(), Counter()
    groups = {}
    families = {}
    proposal_ids = set()
    artifacts = {a['path']: a['sha256'] for a in summary['artifacts']}
    parts = sorted(output.glob('all_decisions_*.csv'))
    covered = 0
    with (output / 'target_candidates.csv').open('w', encoding='utf-8-sig', newline='') as target:
        writer = None
        for path in parts:
            digest = hashlib.sha256()
            with path.open('rb') as f:
                for chunk in iter(lambda: f.read(1048576), b''):
                    digest.update(chunk)
            if digest.hexdigest() != artifacts.get(path.name):
                raise RuntimeError('Preview artifact checksum mismatch: ' + path.name)
            with path.open(encoding='utf-8-sig', newline='') as f:
                reader = csv.DictReader(f)
                if writer is None:
                    writer = csv.DictWriter(target, ['candidate_scope', *reader.fieldnames])
                    writer.writeheader()
                for row in reader:
                    covered += 1
                    counts[row['outcome']] += 1
                    if row['outcome'] == 'INSERT':
                        if row['candidate_id'] in proposal_ids:
                            raise RuntimeError('Repeated insert proposal identity')
                        proposal_ids.add(row['candidate_id'])
                    category = scope(row)
                    focus[(category, row['outcome'])] += 1
                    if category in {'stable_value', 'separate_account', 'alternative'}:
                        writer.writerow(dict(row, candidate_scope=category))
                        if row['outcome'] == 'INSERT':
                            destinations[(category, row['destination_table'])] += 1
                        if row['outcome'] == 'UNRESOLVED':
                            reasons[(category, row['reason'])] += 1
                            # A linked repeated extraction is already handled by
                            # the primary proposal; it needs no product review.
                            if row['reason'] == 'duplicate_staging_proposal':
                                continue
                            family_key = family(row, category)
                            family_id = hashlib.sha256(json.dumps(family_key).encode()).hexdigest()
                            item = families.setdefault(family_key, dict(family_id=family_id, candidate_scope=category,
                                reason=row['reason'], name_pattern=family_key[2], source_type_family=family_key[3],
                                source_class_family=family_key[4], rows=0, products=set(), providers=set(),
                                sample_products=[], sample_ack_ids=[]))
                            item['rows'] += 1
                            item['products'].add(row['normalized_entity_name'])
                            item['providers'].add(normalize_name(row['raw_sponsor_name']))
                            if len(item['sample_products']) < 3 and row['raw_entity_name'] not in item['sample_products']:
                                item['sample_products'].append(row['raw_entity_name'])
                            if len(item['sample_ack_ids']) < 3 and row['ack_id'] not in item['sample_ack_ids']:
                                item['sample_ack_ids'].append(row['ack_id'])
                            key = (category, row['reason'], row['normalized_entity_name'],
                                   normalize_name(row['raw_sponsor_name']), normalize_name(row['asset_type']),
                                   normalize_name(row['asset_class']), normalize_name(row['asset_sub_class']))
                            group = groups.setdefault(key, dict(group_id=hashlib.sha256(json.dumps(key).encode()).hexdigest(), family_id=family_id,
                                candidate_scope=category, reason=key[1],
                                raw_entity_name=row['raw_entity_name'], raw_sponsor_name=row['raw_sponsor_name'],
                                asset_type=row['asset_type'], asset_class=row['asset_class'], asset_sub_class=row['asset_sub_class'],
                                sample_history_tables=row['matched_history_tables'],
                                sample_history_names=row['matched_history_names'][:2000],
                                sample_history_sponsors=row['matched_history_sponsors'][:2000],
                                rows=0, sample_ack_ids=[]))
                            group['rows'] += 1
                            if len(group['sample_ack_ids']) < 3 and row['ack_id'] not in group['sample_ack_ids']:
                                group['sample_ack_ids'].append(row['ack_id'])
    if covered != summary['source_rows'] or dict(counts) != summary['outcomes']:
        raise RuntimeError('Preview row/outcome totals failed independent verification')
    exported_ids, pairs = set(), set()
    for path in sorted(output.glob('proposed_inserts_*.csv')):
        with path.open(encoding='utf-8-sig', newline='') as f:
            for row in csv.DictReader(f):
                pair = (row['ack_id'], row['normalized_entity_name'])
                if row['candidate_id'] in exported_ids or pair in pairs:
                    raise RuntimeError('Insert export is not unique within ack/name')
                exported_ids.add(row['candidate_id'])
                pairs.add(pair)
    if exported_ids != proposal_ids:
        raise RuntimeError('Insert export differs from full decision report')
    fields = ['group_id', 'family_id', 'candidate_scope', 'reason', 'raw_entity_name', 'raw_sponsor_name', 'asset_type',
              'asset_class', 'asset_sub_class', 'rows', 'sample_ack_ids', 'sample_history_tables',
              'sample_history_names', 'sample_history_sponsors']
    with (output / 'priority_product_groups.csv').open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fields)
        writer.writeheader()
        for group in sorted(groups.values(), key=lambda g: (-g['rows'], g['raw_entity_name'])):
            writer.writerow(dict(group, sample_ack_ids='; '.join(group['sample_ack_ids'])))
    with (output / 'decision_families.csv').open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, ['family_id', 'candidate_scope', 'reason', 'name_pattern', 'source_type_family',
                                  'source_class_family', 'rows', 'distinct_products', 'distinct_providers', 'sample_products', 'sample_ack_ids'])
        writer.writeheader()
        for item in sorted(families.values(), key=lambda item: -item['rows']):
            record = {k:v for k,v in item.items() if k not in {'products', 'providers'}}
            record.update(distinct_products=len(item['products']), distinct_providers=len(item['providers']),
                          sample_products='; '.join(item['sample_products']), sample_ack_ids='; '.join(item['sample_ack_ids']))
            writer.writerow(record)
    result = dict(verified_source_rows=covered, verified_outcomes=dict(counts),
                  verified_unique_insert_proposals=len(exported_ids),
                  scope_outcomes=[dict(candidate_scope=c, outcome=o, rows=n) for (c,o),n in sorted(focus.items())],
                  proposed_destinations=[dict(candidate_scope=c, destination_table=t, rows=n) for (c,t),n in sorted(destinations.items())],
                  target_review_reasons=[dict(candidate_scope=c, reason=r, rows=n) for (c,r),n in reasons.most_common()],
                  automatic_duplicate_proposal_rows=sum(n for (c,r),n in reasons.items() if r == 'duplicate_staging_proposal'),
                  actionable_target_review_rows=sum(n for (c,r),n in reasons.items() if r != 'duplicate_staging_proposal'),
                  priority_product_groups=len(groups),
                  decision_families=len(families),
                  review_note='Start with decision families; product groups are drilldown detail, not a request for row-by-row review. Family grouping does not authorize a bulk load.',
                  classification_note='Scope identifies candidate evidence; it does not establish the vehicle or endorse historic classifications.')
    (output / 'focused_summary.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
