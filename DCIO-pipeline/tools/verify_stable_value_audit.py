"""Reconcile the audit table to the saved complete routing report."""
import argparse
from collections import Counter, defaultdict
import csv
import json
from pathlib import Path
import sys

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root))
from route_stable_value import Athena, quote

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('run_id')
args = parser.parse_args()
args.region, args.database, args.workgroup = 'us-east-1', 'default', 'primary'
args.athena_output = 's3://retirementinsights-reference/athena-results/'
out = root / 'outputs' / 'stable_value_router' / args.run_id
rows = list(csv.DictReader((out / 'routing_decisions.csv').open(encoding='utf-8-sig')))
ids = defaultdict(set)
source_rows = Counter()
for row in rows:
    ids[row['routing_status']].add(row['candidate_id'])
    source_rows[row['routing_status']] += int(row['staging_row_count'])
expected = {status: dict(audit_records=len(values), unique_candidates=len(values), source_rows=source_rows[status]) for status, values in ids.items()}
athena = Athena(args)
actual_rows = athena.run(f"SELECT routing_status, count(*) audit_records, count(distinct candidate_id) unique_candidates, sum(staging_row_count) source_rows FROM default.plan_stable_value_routing_audit WHERE routing_run_id={quote(args.run_id)} GROUP BY 1")
actual = {row['routing_status']: {key: int(row[key]) for key in ['audit_records', 'unique_candidates', 'source_rows']} for row in actual_rows}
result = dict(run_id=args.run_id, passed=actual == expected, expected=expected, actual=actual, query_ids=athena.query_ids)
(out / 'audit_verification.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
print(json.dumps(result, indent=2), flush=True)
if not result['passed']:
    raise SystemExit('Audit reconciliation failed')
