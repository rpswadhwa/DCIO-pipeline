"""Read-only full/batch reconciliation. Output is a preview, never a load.

Usage: python reconcile_staging_holdings.py --output-dir outputs/holding_preview
Rerun the downloaded snapshot with --reuse-snapshot; scope with --ack-ids A,B.
Athena SELECTs write their normal result files; business tables are not mutated.
"""
import argparse
import ast
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import datetime, timezone
import hashlib
import itertools
import json
from pathlib import Path
import sqlite3
from urllib.parse import urlparse

from route_stable_value import Athena, quote
from src.holding_reconciliation import FIELDS, SOURCES, VERSION, decide, history_index, identity, product_name
from src.stable_value_manager import ManagerResolver
from src.stable_value_routing import amount, normalize_name


def approved_rules(path):
    if not path:
        return {}
    data = json.loads(Path(path).read_text(encoding='utf-8-sig'))
    if not isinstance(data, list):
        raise ValueError('Product rules must be a JSON list')
    result = {}
    for rule in data:
        for key in ('rule_id', 'raw_entity_name', 'raw_sponsor_name', 'asset_type',
                    'asset_class', 'asset_sub_class', 'destination_table', 'evidence', 'approved_by'):
            if key not in rule or not isinstance(rule[key], str):
                raise ValueError('Missing/invalid product-rule field: ' + key)
        if rule['destination_table'] not in set(SOURCES) - {'plan_mf_history_v3'}:
            raise ValueError('Unsupported product-rule destination')
        if any(not rule[k].strip() for k in ('rule_id', 'raw_entity_name', 'evidence', 'approved_by')):
            raise ValueError('Rules require named product, evidence and approver')
        key = (product_name(rule['raw_entity_name']), *[normalize_name(rule[k]) for k in
               ('raw_sponsor_name', 'asset_type', 'asset_class', 'asset_sub_class')])
        if key in result:
            raise ValueError('Duplicate product-rule scope')
        result[key] = rule
    return result


def sha256(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1048576), b''):
            h.update(chunk)
    return h.hexdigest()


def alternative_patterns():
    # Read a literal catalogue without importing the validator's AWS dependencies
    # or executing its pipeline code. Fail on catalogue drift, never silently skip.
    module = ast.parse(Path('src/post_extract_validator.py').read_text(encoding='utf-8-sig'))
    for node in module.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(isinstance(t, ast.Name) and t.id == 'ALT_FUND_PATTERNS' for t in targets):
                return ast.literal_eval(node.value)
    raise RuntimeError('Existing alternative keyword catalogue missing')


def download_query(args, sql, path):
    client = Athena(args)
    client.run(sql, fetch=False)
    query_id = client.query_ids[-1]
    execution = client.client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']
    uri = urlparse(execution['ResultConfiguration']['OutputLocation'])
    client.s3.download_file(uri.netloc, uri.path.lstrip('/'), str(path))
    return dict(query_id=query_id, sql=sql, s3_uri=uri.geturl(), sha256=sha256(path),
                completed_at=execution['Status']['CompletionDateTime'].isoformat())


def snapshot(args, output):
    provenance = output / 'snapshot.json'
    names = ['staging.source.csv', 'history.source.csv']
    if args.reuse_snapshot:
        meta = json.loads(provenance.read_text())
        if meta['database'] != args.database or meta['ack_ids'] != args.ack_ids:
            raise RuntimeError('Snapshot database/batch differs from requested scope')
        for name in names:
            if sha256(output / name) != meta['queries'][name]['sha256']:
                raise RuntimeError('Snapshot checksum mismatch: ' + name)
        return meta
    client = Athena(args)
    for table in ['plan_holdings_staging', *SOURCES]:
        schema = client.schema(table)
        required = set(FIELDS) if table == 'plan_holdings_staging' else set(FIELDS[:4])
        if schema is None or not required <= schema.keys():
            raise RuntimeError('Missing required table/identity columns: ' + table)
    scope = ' WHERE ack_id IN (' + ','.join(quote(a) for a in args.ack_ids) + ')' if args.ack_ids else ''
    stage = 'SELECT ' + ', '.join(FIELDS) + ' FROM ' + args.database + '.plan_holdings_staging' + scope
    history = '\nUNION ALL\n'.join('SELECT ' + quote(t) + ' source_table, ack_id, raw_entity_name, raw_sponsor_name, plan_investment_amt FROM ' + args.database + '.' + t + scope for t in SOURCES)
    meta = dict(database=args.database, ack_ids=args.ack_ids, mode='PREVIEW',
                started_at=datetime.now(timezone.utc).isoformat(), queries={})
    with ThreadPoolExecutor(max_workers=2) as pool:
        jobs = {name: pool.submit(download_query, args, sql, output / name)
                for name, sql in zip(names, [stage, history])}
        for name, future in jobs.items():
            meta['queries'][name] = future.result()
    # These are independently completed SELECT snapshots, not one transaction.
    meta['snapshot_consistency'] = 'independent SELECT completion times; no transactional cross-table snapshot'
    provenance.write_text(json.dumps(meta, indent=2), encoding='utf-8')
    return meta


class SplitCSV:
    def __init__(self, output, name, fields, limit=500000):
        self.output, self.name, self.fields, self.limit = output, name, fields, limit
        self.count = self.part = 0
        self.file = None
        self.paths = []

    def write(self, row):
        if self.count % self.limit == 0:
            if self.file:
                self.file.close()
            self.part += 1
            path = self.output / f'{self.name}_{self.part:02d}.csv'
            self.paths.append(path)
            self.file = path.open('w', encoding='utf-8-sig', newline='')
            self.writer = csv.DictWriter(self.file, self.fields, extrasaction='ignore')
            self.writer.writeheader()
        self.writer.writerow(row)
        self.count += 1

    def close(self):
        if self.file:
            self.file.close()


def import_snapshot(db, output, reuse=False):
    if reuse:
        counts = {}
        # Verify every cached payload and ack against the source CSV in source
        # order. A row-count-only cache check could conceal corrupted identities.
        for table, name in [('stage', 'staging.source.csv'), ('history', 'history.source.csv')]:
            count = 0
            cached = iter(db.execute(f'SELECT ack,payload FROM {table} ORDER BY rowid'))
            with (output / name).open(encoding='utf-8-sig', newline='') as f:
                for row in csv.DictReader(f):
                    stored = next(cached, None)
                    if stored != (row.get('ack_id', ''), json.dumps(row, ensure_ascii=False)):
                        raise RuntimeError('Working database differs from source snapshot: ' + table)
                    count += 1
            if next(cached, None) is not None:
                raise RuntimeError('Extra records in working database: ' + table)
            counts[table] = count
            print(f'Verified cached {table}: {count:,}', flush=True)
        return counts
    db.executescript('DROP TABLE IF EXISTS stage; DROP TABLE IF EXISTS history; CREATE TABLE stage (ack TEXT, payload TEXT); CREATE TABLE history (ack TEXT, payload TEXT);')
    counts = {}
    for table, name in [('stage', 'staging.source.csv'), ('history', 'history.source.csv')]:
        count = 0
        with (output / name).open(encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            while batch := list(itertools.islice(reader, 10000)):
                db.executemany(f'INSERT INTO {table} VALUES (?,?)', [(r.get('ack_id', ''), json.dumps(r, ensure_ascii=False)) for r in batch])
                count += len(batch)
        db.commit()
        db.execute(f'CREATE INDEX {table}_ack ON {table}(ack)')
        counts[table] = count
        print(f'Imported {table}: {count:,}', flush=True)
    return counts


def reconcile(args, output, meta):
    db = sqlite3.connect(output / 'working.sqlite')
    db.execute('PRAGMA synchronous=OFF')
    db.execute('PRAGMA temp_store=FILE')
    inputs = import_snapshot(db, output, args.reuse_working_db)
    patterns = alternative_patterns()
    rules = approved_rules(args.rules_file)
    s3 = Athena(args).s3
    canonical = json.loads(s3.get_object(Bucket=args.canon_bucket, Key=args.canon_key)['Body'].read())
    resolver = ManagerResolver(canonical)
    manager_cache = {}
    fields = [*FIELDS, 'candidate_id', 'normalized_entity_name', 'outcome', 'destination_table',
              'reason', 'rule', 'matched_history_tables', 'matched_history_names',
              'matched_history_sponsors', 'duplicate_of', 'source_multiplicity',
              'normalized_manager', 'manager_match_status', 'manager_match_method',
              'manager_match_confidence', 'needs_cleaning_flag', 'routing_version']
    writers = {name: SplitCSV(output, name, fields) for name in ['all_decisions', 'proposed_inserts', 'unresolved']}
    counts, unique_counts, reasons, destinations = Counter(), Counter(), Counter(), Counter()
    groups = {}
    processed = unique = repeated_pairs = 0
    try:
        cursor = db.execute('SELECT ack,payload FROM stage ORDER BY ack,rowid')
        for ack, records in itertools.groupby(cursor, key=lambda r: r[0]):
            rows = [json.loads(r[1]) for r in records]
            history = [json.loads(r[0]) for r in db.execute('SELECT payload FROM history WHERE ack=?', (ack,))]
            indexes = history_index(history)
            pairs = defaultdict(set)
            labels = defaultdict(set)
            amount_names = defaultdict(set)
            multiplicity = Counter(identity(r) for r in rows)
            for row in rows:
                pairs[product_name(row.get('raw_entity_name'))].add(identity(row))
                labels[identity(row)].add(tuple(row.get(k, '').strip().lower() for k in ('asset_type', 'asset_class', 'asset_sub_class')))
                if amount(row.get('plan_investment_amt')) is not None:
                    amount_names[amount(row.get('plan_investment_amt'))].add(product_name(row.get('raw_entity_name')))
            repeated_pairs += sum(len(ids) > 1 for ids in pairs.values())
            seen = set()
            for row in rows:
                key = identity(row)
                duplicate = key if key in seen else ''
                decision = decide(row, indexes, pairs, duplicate, patterns, len(labels[key]) > 1,
                                  amount_names, rules)
                decision['source_multiplicity'] = multiplicity[key]
                if decision['outcome'] in {'INSERT', 'UNRESOLVED'}:
                    mk = (row.get('raw_entity_name', ''), row.get('raw_sponsor_name', ''))
                    if mk not in manager_cache:
                        if len(manager_cache) >= 100000:
                            manager_cache.clear()
                        manager_cache[mk] = resolver.resolve(*mk)
                    decision.update(manager_cache[mk])
                counts[decision['outcome']] += 1
                reasons[decision['reason']] += 1
                if not duplicate:
                    unique += 1
                    unique_counts[decision['outcome']] += 1
                if decision['outcome'] == 'INSERT':
                    destinations[decision['destination_table']] += 1
                    writers['proposed_inserts'].write(decision)
                if decision['outcome'] == 'UNRESOLVED':
                    writers['unresolved'].write(decision)
                    # Group identical normalized products across filings. Provider
                    # remains evidence, never collapsed into an investment manager.
                    group = (decision['reason'], decision['normalized_entity_name'],
                             normalize_name(row.get('raw_sponsor_name')), normalize_name(row.get('asset_type')),
                             normalize_name(row.get('asset_class')), normalize_name(row.get('asset_sub_class')))
                    item = groups.setdefault(group, dict(reason=group[0], product=group[1], provider=group[2], asset_type=group[3], asset_class=group[4], asset_sub_class=group[5], rows=0, unique_candidates=0, sample_ack_ids=[]))
                    item['rows'] += 1
                    item['unique_candidates'] += not bool(duplicate)
                    if ack not in item['sample_ack_ids'] and len(item['sample_ack_ids']) < 3:
                        item['sample_ack_ids'].append(ack)
                writers['all_decisions'].write(decision)
                seen.add(key)
                processed += 1
                if processed % 100000 == 0:
                    print(f'Processed {processed:,}: {dict(counts)}', flush=True)
    finally:
        for writer in writers.values():
            writer.close()
        db.close()
    if processed != inputs['stage'] or sum(counts.values()) != processed:
        raise RuntimeError('Incomplete source coverage')
    with (output / 'exception_groups.csv').open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, ['reason', 'product', 'provider', 'asset_type', 'asset_class', 'asset_sub_class', 'rows', 'unique_candidates', 'sample_ack_ids'])
        writer.writeheader()
        for group in sorted(groups.values(), key=lambda g: (-g['rows'], g['reason'], g['product'])):
            writer.writerow(dict(group, sample_ack_ids='; '.join(group['sample_ack_ids'])))
    artifacts = [p for w in writers.values() for p in w.paths] + [output / 'exception_groups.csv']
    summary = dict(mode='PREVIEW', business_table_writes=0, version=VERSION,
                   completed_at=datetime.now(timezone.utc).isoformat(), input_counts=inputs,
                   source_rows=processed, unique_candidates=unique, outcomes=dict(counts),
                   unique_outcomes=dict(unique_counts), reasons=dict(reasons),
                   proposed_destinations=dict(destinations), exception_groups=len(groups),
                   repeated_ack_name_pairs=repeated_pairs, canon_version=resolver.canon_version,
                   approved_rules_count=len(rules), approved_rules_sha256=sha256(Path(args.rules_file)) if args.rules_file else None,
                   code_sha256={p: sha256(Path(p)) for p in ('reconcile_staging_holdings.py', 'src/holding_reconciliation.py', 'src/stable_value_manager.py', 'route_stable_value.py')},
                   alternative_catalogue_sha256=sha256(Path('src/post_extract_validator.py')),
                   artifacts=[dict(path=p.name, sha256=sha256(p)) for p in artifacts],
                   snapshot=meta,
                   limitations=['Provider differences require review; no fuzzy or amount-only automatic matches.',
                                'Keyword-only alternatives remain review candidates under existing manual-review policy.',
                                'OUTSIDE_SCOPE rows have no target evidence; this is not an MF/CIT catch-all loader.',
                                'Duplicate extraction identities are not summed; conflicting source labels require review.',
                                'Existing matches describe presence, not endorsement of historical classification.'])
    (output / 'summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    print(json.dumps({k: v for k, v in summary.items() if k not in {'snapshot', 'artifacts'}}, indent=2), flush=True)
    return summary


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output-dir', required=True)
    p.add_argument('--ack-ids', default='')
    p.add_argument('--reuse-snapshot', action='store_true')
    p.add_argument('--reuse-working-db', action='store_true', help='Verify and reuse local indexed source rows')
    p.add_argument('--rules-file', help='Reviewed exact product/provider/source-label decisions as JSON')
    p.add_argument('--database', default='default')
    p.add_argument('--region', default='us-east-1')
    p.add_argument('--workgroup', default='primary')
    p.add_argument('--athena-output', default='s3://retirementinsights-reference/athena-results/')
    p.add_argument('--canon-bucket', default='retirementinsights-reference')
    p.add_argument('--canon-key', default='manager_canonical/v1/manager_canonical.json')
    args = p.parse_args()
    args.ack_ids = sorted({a.strip() for a in args.ack_ids.split(',') if a.strip()})
    if not args.database.replace('_', '').isalnum():
        p.error('Invalid database identifier')
    output = Path(args.output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    # Remove only generated report parts on a rerun to prevent stale CSVs from
    # representing a larger previous run. Source snapshots stay intact.
    for pattern in ('all_decisions_*.csv', 'proposed_inserts_*.csv', 'unresolved_*.csv'):
        for path in output.glob(pattern):
            path.unlink()
    summary = output / 'summary.json'
    if summary.exists():
        summary.unlink()
    meta = snapshot(args, output)
    reconcile(args, output, meta)


if __name__ == '__main__':
    main()
