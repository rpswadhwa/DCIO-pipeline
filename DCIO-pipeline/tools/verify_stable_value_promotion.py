"""Independently verify a bounded promotion and its cross-table identities."""
import argparse
from collections import Counter, defaultdict
import csv
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from route_stable_value import Athena, quote
from src.holding_reconciliation import identity, product_name
from src.stable_value_routing import VERSION, TARGETS, amount

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--run-dir', required=True)
parser.add_argument('--approved-candidates', required=True)
args = parser.parse_args()
args.region, args.database, args.workgroup = 'us-east-1', 'default', 'primary'
args.athena_output = 's3://retirementinsights-reference/athena-results/'
out = Path(args.run_dir)
summary = json.loads((out / 'summary.json').read_text())
run_id = summary['run_id']
with open(args.approved_candidates, encoding='utf-8-sig', newline='') as f:
    approved = {r['candidate_id']: r for r in csv.DictReader(f)}
with (out / 'routing_decisions.csv').open(encoding='utf-8-sig', newline='') as f:
    decisions = list(csv.DictReader(f))
expected = {r['candidate_id']: r for r in decisions if r['routing_status'] == 'INSERTED'}
assert set(r['candidate_id'] for r in decisions) == set(approved)
assert all(identity(r) == r['candidate_id'] for r in decisions)
assert len({(r['ack_id'], product_name(r['raw_entity_name'])) for r in expected.values()}) == len(expected)
athena = Athena(args)
parts = [f"SELECT {quote(t)} source_table, ack_id, raw_entity_name, raw_sponsor_name, plan_investment_amt, candidate_id FROM default.{t} WHERE routing_run_id={quote(run_id)}" for t in TARGETS]
parts.append(f"SELECT 'plan_cit_history', ack_id, raw_entity_name, raw_sponsor_name, plan_investment_amt, substr(row_order,4) FROM default.plan_cit_history WHERE normalized_version_ai={quote(VERSION + ':' + run_id)}")
inserted_sql = ' UNION ALL '.join(parts)
written = athena.run(inserted_sql)
actual = {r['candidate_id']: r for r in written}
assert len(written) == len(actual) == len(expected)
assert set(actual) == set(expected)
for key, r in actual.items():
    assert identity(r) == key
    assert amount(r['plan_investment_amt']) == amount(expected[key]['plan_investment_amt'])
    assert r['source_table'] == expected[key]['destination_table']
# Read every history holding in the filings independently verified above.
# Supplying these IDs once avoids repeating the inserted-row query per table.
filing_ids = sorted({r['ack_id'] for r in written})
arrays = ['ARRAY[' + ','.join(quote(a) for a in filing_ids[i:i + 200]) + ']'
          for i in range(0, len(filing_ids), 200)]
scope = arrays[0] if len(arrays) == 1 else 'concat(' + ','.join(arrays) + ')'
scope_sql = 'SELECT ack_id FROM UNNEST(' + scope + ') AS filing_scope(ack_id)'
history_parts = []
for t in ['plan_cit_history', 'plan_mf_history_v3', 'plan_alternatives_history', *TARGETS]:
    history_parts.append(f"SELECT {quote(t)} source_table, h.ack_id, h.raw_entity_name, h.raw_sponsor_name, h.plan_investment_amt FROM default.{t} h JOIN filing_ids s ON h.ack_id=s.ack_id")
history = athena.run('WITH filing_ids AS (' + scope_sql + ') ' + ' UNION ALL '.join(history_parts)) if written else []
pairs = defaultdict(list)
for h in history:
    pairs[(h['ack_id'], product_name(h['raw_entity_name']))].append(h)
violations = []
for key, r in actual.items():
    matches = pairs[(r['ack_id'], product_name(r['raw_entity_name']))]
    if len(matches) != 1 or identity(matches[0]) != key:
        violations.append(key)
audit = athena.run(f"SELECT routing_status, count(*) n, count(distinct candidate_id) unique_ids FROM default.plan_stable_value_routing_audit WHERE routing_run_id={quote(run_id)} GROUP BY 1")
assert {r['routing_status']: int(r['n']) for r in audit} == dict(Counter(r['routing_status'] for r in decisions))
assert all(r['n'] == r['unique_ids'] for r in audit)
result = dict(run_id=run_id, passed=not violations, approved_count=len(approved),
              verified_inserted=len(actual), counts=summary['counts'],
              destinations=dict(Counter(r['source_table'] for r in written)),
              cross_table_ack_name_conflicts=violations, query_ids=athena.query_ids)
(out / 'independent_verification.json').write_text(json.dumps(result, indent=2))
print(json.dumps(result, indent=2), flush=True)
if violations:
    raise SystemExit('Promotion identity verification failed')
