"""Apply tightened vehicle/mandate guards to an already generated v1 preview.

Only INSERT proposals are eligible; existing identity decisions and
source snapshots stay intact. This records the incremental review adjustment.
Fresh reconciliation runs already include the same guard in classify().
"""
import argparse
from collections import defaultdict
import csv
import io
import json
import os
import re
import sqlite3
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reconcile_staging_holdings import sha256
from src.holding_reconciliation import classify, possible_history_variants, product_name
from src.stable_value_routing import normalize_name


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--preview-dir', required=True)
    args = parser.parse_args()
    output = Path(args.preview_dir).resolve()
    summary = json.loads((output / 'summary.json').read_text())
    assert summary['mode'] == 'PREVIEW' and summary['business_table_writes'] == 0
    for artifact in summary['artifacts']:
        if sha256(output / artifact['path']) != artifact['sha256']:
            raise RuntimeError('Artifact changed: ' + artifact['path'])
    adjustments = []
    db = sqlite3.connect('file:' + (output / 'working.sqlite').as_posix() + '?mode=ro', uri=True)
    last_ack, history = None, []
    for path in sorted(output.glob('proposed_inserts_*.csv')):
        with path.open(encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            fields = reader.fieldnames
            rows = list(reader)
        retained = []
        for row in rows:
            result = classify(row)
            guarded = result[0] == 'UNRESOLVED' and result[2] in {'pooled_vehicle_in_managed_account_name',
                    'insurance_wrapper_without_stable_value_mandate', 'stable_value_product_evidence_required'}
            if guarded:
                updated = dict(row, outcome=result[0], destination_table=result[1], reason=result[2], rule=result[3])
                adjustments.append((row, updated, fields))
            else:
                if last_ack != row['ack_id']:
                    history = [json.loads(record[0]) for record in db.execute('SELECT payload FROM history WHERE ack=?', (row['ack_id'],))]
                    last_ack = row['ack_id']
                near = possible_history_variants(row, history)
                if near:
                    updated = dict(row, outcome='UNRESOLVED', reason='possible_history_name_variant',
                        matched_history_tables='; '.join(sorted({h['source_table'] for h in near})),
                        matched_history_names='; '.join(sorted({h['raw_entity_name'] for h in near})),
                        matched_history_sponsors='; '.join(sorted({h['raw_sponsor_name'] for h in near})))
                    adjustments.append((row, updated, fields))
                else:
                    retained.append(row)
        if len(retained) != len(rows):
            temp = path.with_suffix('.refined.tmp')
            with temp.open('w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fields)
                writer.writeheader()
                writer.writerows(retained)
            os.replace(temp, path)
    db.close()
    if not adjustments:
        print('No proposals require guard refinement')
        return
    def encoded(row, fields):
        buffer = io.StringIO(newline='')
        csv.DictWriter(buffer, fields).writerow(row)
        return buffer.getvalue().encode('utf-8')
    replacements = {old['candidate_id'].encode(): (encoded(old, fields), encoded(new, fields))
                    for old,new,fields in adjustments}
    found = defaultdict(int)
    for path in sorted(output.glob('all_decisions_*.csv')):
        data = path.read_bytes()
        ranges = []
        for match in re.finditer(rb'(?<![a-f0-9])[a-f0-9]{64}(?![a-f0-9])', data):
            cid = match.group()
            if cid not in replacements:
                continue
            old_bytes, new_bytes = replacements[cid]
            start = match.start() - old_bytes.find(cid)
            end = start + len(old_bytes)
            if data[start:end] == old_bytes:
                ranges.append((start,end,new_bytes))
                found[cid] += 1
        if ranges:
            pieces, last = [], 0
            for start,end,replacement in sorted(ranges):
                assert start >= last
                pieces.extend([data[last:start], replacement])
                last = end
            pieces.append(data[last:])
            temp = path.with_suffix('.refined.tmp')
            assert temp.resolve().is_relative_to(output) and path.resolve().is_relative_to(output)
            temp.write_bytes(b''.join(pieces))
            os.replace(temp, path)
    if any(found[cid] != 1 for cid in replacements):
        raise RuntimeError('A refined decision was missing or repeated')
    new_groups = {}
    for old, new, fields in adjustments:
        new_bytes = encoded(new, fields)
        unresolved = sorted(output.glob('unresolved_*.csv'))[-1]
        with unresolved.open('ab') as f:
            f.write(new_bytes)
        key = (new['reason'], product_name(new['raw_entity_name']), normalize_name(new['raw_sponsor_name']),
               normalize_name(new['asset_type']), normalize_name(new['asset_class']), normalize_name(new['asset_sub_class']))
        group = new_groups.setdefault(key, dict(reason=key[0], product=key[1], provider=key[2], asset_type=key[3],
            asset_class=key[4], asset_sub_class=key[5], rows=0, unique_candidates=0, sample_ack_ids=[]))
        group['rows'] += 1
        group['unique_candidates'] += 1
        if len(group['sample_ack_ids']) < 3 and new['ack_id'] not in group['sample_ack_ids']:
            group['sample_ack_ids'].append(new['ack_id'])
        for counts in (summary['outcomes'], summary['unique_outcomes']):
            counts['INSERT'] -= 1
            counts['UNRESOLVED'] += 1
        summary['proposed_destinations'][old['destination_table']] -= 1
        summary['reasons'][old['reason']] -= 1
        summary['reasons'][new['reason']] = summary['reasons'].get(new['reason'], 0) + 1
    group_fields = ['reason', 'product', 'provider', 'asset_type', 'asset_class', 'asset_sub_class', 'rows', 'unique_candidates', 'sample_ack_ids']
    group_path = output / 'exception_groups.csv'
    with group_path.open(encoding='utf-8-sig', newline='') as f:
        existing_groups = list(csv.DictReader(f))
    indexed_groups = {(g['reason'],g['product'],g['provider'],g['asset_type'],g['asset_class'],g['asset_sub_class']): g for g in existing_groups}
    for key, group in new_groups.items():
        if key in indexed_groups:
            prior = indexed_groups[key]
            prior['rows'] = int(prior['rows']) + group['rows']
            prior['unique_candidates'] = int(prior['unique_candidates']) + group['unique_candidates']
        else:
            indexed_groups[key] = dict(group, sample_ack_ids='; '.join(group['sample_ack_ids']))
    with group_path.open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, group_fields)
        writer.writeheader()
        writer.writerows(sorted(indexed_groups.values(),key=lambda g: -int(g['rows'])))
    summary['exception_groups'] = len(indexed_groups)
    summary['proposed_destinations'] = {k:v for k,v in summary['proposed_destinations'].items() if v}
    summary['reasons'] = {k:v for k,v in summary['reasons'].items() if v}
    summary.setdefault('review_adjustments', []).extend(dict(candidate_id=old['candidate_id'], old_outcome=old['outcome'],
        new_outcome=new['outcome'], reason=new['reason']) for old,new,fields in adjustments)
    summary['refinement_tool_sha256'] = sha256(Path(__file__))
    summary['code_sha256']['src/holding_reconciliation.py'] = sha256(Path('src/holding_reconciliation.py'))
    for artifact in summary['artifacts']:
        artifact['sha256'] = sha256(output / artifact['path'])
    (output / 'summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    print(json.dumps(dict(refined=len(adjustments), outcomes=summary['outcomes'], destinations=summary['proposed_destinations']), indent=2))


if __name__ == '__main__':
    main()
