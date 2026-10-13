"""Read-only verification of router-owned holdings after the backfill."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root))
from route_stable_value import Athena
from src.stable_value_routing import TARGETS

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--expected-total', type=int, required=True)
args = parser.parse_args()
args.region = 'us-east-1'
args.database = 'default'
args.workgroup = 'primary'
args.athena_output = 's3://retirementinsights-reference/athena-results/'
athena = Athena(args)
parts = [f"SELECT '{t}' source_table, ack_id, raw_entity_name, plan_investment_amt, normalized_manager FROM default.{t}" for t in TARGETS]
parts.append("SELECT 'plan_cit_history', ack_id, raw_entity_name, plan_investment_amt, proposed_manager_ai FROM default.plan_cit_history WHERE row_order LIKE 'SV:%'")
owned = ' UNION ALL '.join(parts)
counts = athena.run('WITH owned AS (' + owned + ') SELECT source_table, count(*) holdings, sum(plan_investment_amt) reported_amount, count_if(normalized_manager IS NULL) unresolved_managers FROM owned GROUP BY source_table ORDER BY source_table')
legacy = ["SELECT ack_id, raw_entity_name FROM default.plan_cit_history WHERE row_order IS NULL OR row_order NOT LIKE 'SV:%'",
          'SELECT ack_id, raw_entity_name FROM default.plan_mf_history_v3']
if athena.schema('plan_alternative_history'):
    legacy.append('SELECT ack_id, raw_entity_name FROM default.plan_alternative_history')
checks = athena.run("""WITH owned AS (""" + owned + """), legacy AS (""" + ' UNION ALL '.join(legacy) + """),
  duplicates AS (SELECT ack_id, lower(trim(regexp_replace(raw_entity_name, '\\s+', ' '))) name,
    count(*) n FROM owned GROUP BY 1,2 HAVING count(*)>1)
  SELECT 'duplicate_router_ack_name_pairs' check_name, count(*) violations FROM duplicates
  UNION ALL SELECT 'duplicate_router_positive_ack_amounts', count(*) FROM
    (SELECT ack_id, plan_investment_amt FROM owned WHERE plan_investment_amt>0
     GROUP BY 1,2 HAVING count(*)>1)
  UNION ALL SELECT 'legacy_ack_name_overlap', count(*) FROM owned o JOIN legacy l
    ON o.ack_id=l.ack_id AND lower(trim(regexp_replace(o.raw_entity_name, '\\s+', ' ')))=
    lower(trim(regexp_replace(l.raw_entity_name, '\\s+', ' ')))
  UNION ALL SELECT 'missing_or_negative_amount', count(*) FROM owned WHERE plan_investment_amt IS NULL OR plan_investment_amt<0
  UNION ALL SELECT 'router_view_count', count(*) FROM default.v_plan_stable_value_holdings WHERE record_origin='stable_value_router'
""")
total = sum(int(r['holdings']) for r in counts)
metrics = {r['check_name']: int(r['violations']) for r in checks}
passed = total == args.expected_total and metrics['router_view_count'] == total and all(v == 0 for k, v in metrics.items() if k != 'router_view_count')
result = dict(verified_at=datetime.now(timezone.utc).isoformat(), passed=passed,
              expected_total=args.expected_total, actual_total=total, counts=counts,
              checks=metrics, query_ids=athena.query_ids)
out = root / 'outputs' / 'stable_value_router' / 'backfill_verification.json'
out.write_text(json.dumps(result, indent=2), encoding='utf-8')
print(json.dumps(result, indent=2), flush=True)
if not passed:
    raise SystemExit('Backfill verification failed; inspect the saved report')
