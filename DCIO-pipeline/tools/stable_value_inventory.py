"""Read-only, reproducible stable-value inventory across history destinations."""
import csv,json
from collections import Counter,defaultdict
from datetime import datetime,timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from route_stable_value import Athena,quote
from src.holding_reconciliation import product_name
from src.stable_value_routing import normalize_name,amount

out=Path('outputs/stable_value_inventory_20261014');out.mkdir(exist_ok=True)
a=Athena(SimpleNamespace(region='us-east-1',database='default',workgroup='primary',athena_output='s3://retirementinsights-reference/athena-results/'))
schemas=json.loads((out/'schemas.json').read_text())
tables=['plan_insurance_history','plan_separately_managed_history','plan_stable_value_unresolved_history','plan_cit_history','plan_mf_history_v3','plan_alternatives_history']
pattern=r'(^|[^a-z0-9])(stable|stbl|stab|stabl)\s+(value|val)([^a-z0-9]|$)'
parts=[]
for table in tables:
    schema=schemas[table]
    def col(name):return name if name in schema else 'cast(null as varchar)'
    classified="regexp_like(lower(coalesce(asset_class,'')),"+quote(pattern)+")"
    named="regexp_like(lower(coalesce(raw_entity_name,'')),"+quote(pattern)+")"
    typed="regexp_like(lower(coalesce(asset_type,'')),"+quote(pattern)+")" if 'asset_type' in schema else 'false'
    owned="coalesce(routing_version,'') LIKE 'stable_value_router%'" if 'routing_version' in schema else "coalesce(row_order,'') LIKE 'SV:%'" if table=='plan_cit_history' else 'false'
    cols=['ack_id','raw_entity_name','raw_sponsor_name','plan_investment_amt','asset_class','asset_sub_class','asset_type','normalized_manager','manager_match_status','routing_run_id','vehicle_type','account_structure','contract_type']
    parts.append('SELECT '+quote(table)+" source_table,"+','.join(col(c)+' '+c for c in cols)+', '+owned+' router_owned, '+classified+' classified_stable_value, '+named+' named_stable_value FROM default.'+table+' WHERE ('+classified+' OR '+named+' OR '+typed+' OR '+owned+')')
source_sql=' UNION ALL '.join(parts)
(out/'inventory.sql').write_text(source_sql)
rows=a.run(source_sql)
fields=list(rows[0]) if rows else []
if rows:
    with (out/'stable_value_holdings.csv').open('w',encoding='utf-8-sig',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)
print('Inventory holdings:',len(rows),flush=True)
# A single plan mapping per ACK; ambiguity is visible rather than multiplied.
plan_sql="""WITH sv AS ("""+source_sql+"""), all_plans AS (
 SELECT ack_id,plan_id,plan_name_clean FROM default.plan_master
 WHERE nullif(trim(plan_id),'') IS NOT NULL), latest AS (
 SELECT plan_id,max(ack_id) latest_ack_id FROM all_plans GROUP BY plan_id)
 SELECT p.ack_id, min(p.plan_id) plan_id, min(p.plan_name_clean) plan_name,
 count(distinct p.plan_id) plan_id_count, min(l.latest_ack_id) latest_ack_id
 FROM all_plans p JOIN latest l ON p.plan_id=l.plan_id
 JOIN (SELECT DISTINCT ack_id FROM sv) s ON p.ack_id=s.ack_id
 GROUP BY p.ack_id"""
plan_rows=a.run(plan_sql)
(out/'plan_mapping.json').write_text(json.dumps(plan_rows,indent=2))
plans={r['ack_id']:r for r in plan_rows if r['plan_id_count']=='1'}

def metrics(items):
    return dict(holdings=len(items),filings=len({r['ack_id'] for r in items}),
      known_plans=len({plans[r['ack_id']]['plan_id'] for r in items if r['ack_id'] in plans}),
      reported_assets=str(sum((amount(r['plan_investment_amt']) or Decimal(0) for r in items),Decimal(0))),
      missing_amounts=sum(amount(r['plan_investment_amt']) is None for r in items),
      negative_amounts=sum((amount(r['plan_investment_amt']) or Decimal(0))<0 for r in items))
by_table={t:metrics([r for r in rows if r['source_table']==t]) for t in tables}
router=[r for r in rows if r['router_owned']=='true']
legacy=[r for r in rows if r['router_owned']!='true']
latest=[r for r in rows if r['ack_id'] in plans and r['ack_id']==plans[r['ack_id']]['latest_ack_id']]
pairs=defaultdict(list);identities=defaultdict(list)
for r in rows:
    pairs[(r['ack_id'],product_name(r['raw_entity_name']))].append(r)
    identities[(r['ack_id'],product_name(r['raw_entity_name']),normalize_name(r['raw_sponsor_name']),str(amount(r['plan_investment_amt'])))].append(r)
duplicate_pairs=[v for v in pairs.values() if len(v)>1]
exact_duplicates=[v for v in identities.values() if len(v)>1]
conflicting=[r for r in rows if r['classified_stable_value']!='true' and normalize_name(r['asset_class']) not in {'','pending_ai'}]
summary=dict(as_of_utc=datetime.now(timezone.utc).isoformat(),scope='Stored holdings across all filing periods; name-only legacy matches remain candidates',
 all_history=metrics(rows),by_table=by_table,router_owned=metrics(router),legacy=metrics(legacy),latest_known_plan_filings=metrics(latest),
 latest_by_table={t:metrics([r for r in latest if r['source_table']==t]) for t in tables},
 missing_plan_master_filings=len({r['ack_id'] for r in rows if r['ack_id'] not in plans}),
 normalized_ack_name_pairs=len(pairs),duplicate_ack_name_groups=len(duplicate_pairs),
 cross_table_ack_name_groups=sum(len({r['source_table'] for r in group})>1 for group in duplicate_pairs),
 exact_identity_duplicate_groups=len(exact_duplicates),exact_identity_excess_rows=sum(len(v)-1 for v in exact_duplicates),
 exact_identity_deduplicated_assets=str(sum((amount(v[0]['plan_investment_amt']) or Decimal(0) for v in identities.values()),Decimal(0))),
 conflicting_classification_records=len(conflicting),
 synthetic_gic_holdings=[{k:r[k] for k in ['source_table','ack_id','raw_entity_name','plan_investment_amt','vehicle_type','contract_type']} for r in rows if 'synthetic' in normalize_name(r['raw_entity_name'])],
 query_ids=a.query_ids)
(out/'summary.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary,indent=2),flush=True)
