"""Match the saved stable-value inventory to the current index universe by ACK."""
import csv,json,sys
from collections import defaultdict,Counter
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime,timezone
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from route_stable_value import Athena
from src.stable_value_routing import amount
root=Path('outputs/stable_value_inventory_20261014');out=root/'index_universe_match';out.mkdir(exist_ok=True)
a=Athena(SimpleNamespace(region='us-east-1',database='default',workgroup='primary',athena_output='s3://retirementinsights-reference/athena-results/'))
sql="""SELECT ack_id,plan_id,plan_name_clean,plan_year,filing_year,assets,entity_category,is_master_trust
 FROM default.plan_master_index_universe WHERE ack_id IS NOT NULL"""
(out/'universe_query.sql').write_text(sql)
if '--reuse-universe' in sys.argv:
    with (out/'universe_source.csv').open(encoding='utf-8-sig',newline='') as f:universe=list(csv.DictReader(f))
else:
    universe=a.run(sql)
print('Universe source rows:',len(universe),flush=True)
with (out/'universe_source.csv').open('w',encoding='utf-8-sig',newline='') as f:
    w=csv.DictWriter(f,fieldnames=list(universe[0]));w.writeheader();w.writerows(universe)
with (root/'stable_value_holdings.csv').open(encoding='utf-8-sig',newline='') as f:holdings=list(csv.DictReader(f))
inventory_ack_ids={r['ack_id'] for r in holdings}
universe_counts=Counter(r['ack_id'] for r in universe)
by_ack=defaultdict(list)
for r in universe:
    if r['ack_id'] in inventory_ack_ids:by_ack[r['ack_id']].append(r)
metadata={}
for ack,rs in by_ack.items():
    values={k:sorted({r[k] for r in rs if r.get(k) not in [None,'']}) for k in ['plan_id','plan_name_clean','plan_year','filing_year','assets','entity_category','is_master_trust']}
    metadata[ack]={**{k:' | '.join(v) for k,v in values.items()},'index_universe_rows':len(rs),'index_metadata_conflict':any(len(v)>1 for v in values.values())}
matched=[];unmatched=[]
for h in holdings:
    if h['ack_id'] in metadata:matched.append({**h,**metadata[h['ack_id']]})
    else:unmatched.append(h)
for name,rows in [('matched_holdings',matched),('unmatched_holdings',unmatched)]:
    with (out/(name+'.csv')).open('w',encoding='utf-8-sig',newline='') as f:
        cols=list(rows[0]) if rows else list(holdings[0]);w=csv.DictWriter(f,fieldnames=cols);w.writeheader();w.writerows(rows)
tables=list(json.loads((root/'summary.json').read_text())['by_table'])
def metrics(rows):
    return {'holdings':len(rows),'filings':len({r['ack_id'] for r in rows}),
      'reported_assets':str(sum((amount(r['plan_investment_amt']) or Decimal(0) for r in rows),Decimal(0))),
      'missing_amounts':sum(amount(r['plan_investment_amt']) is None for r in rows)}
matched_by_ack=defaultdict(list)
for r in matched:matched_by_ack[r['ack_id']].append(r)
matched_ack_ids=set(matched_by_ack)
ack_rows=[]
for ack,rs in sorted(matched_by_ack.items()):
    row={'ack_id':ack,**metadata[ack],**metrics(rs)}
    for t in tables:
        subset=[r for r in rs if r['source_table']==t];row[t+'_holdings']=len(subset);row[t+'_assets']=metrics(subset)['reported_assets']
    ack_rows.append(row)
if ack_rows:
    with (out/'matched_plans.csv').open('w',encoding='utf-8-sig',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(ack_rows[0]));w.writeheader();w.writerows(sorted(ack_rows,key=lambda r:-Decimal(r['reported_assets'])))
with (out/'unmatched_ack_ids.txt').open('w',encoding='utf-8') as f:f.write('\n'.join(sorted({r['ack_id'] for r in unmatched}))+'\n')
summary={'as_of_utc':datetime.now(timezone.utc).isoformat(),'match_key':'ack_id','inventory_source':'stable_value_inventory_20261014/stable_value_holdings.csv',
 'universe_rows':len(universe),'universe_distinct_ack_ids':len(universe_counts),
 'universe_duplicate_ack_groups':sum(n>1 for n in universe_counts.values()),
 'matched':metrics(matched),'unmatched':metrics(unmatched),
 'matched_distinct_plan_ids':len({r['plan_id'] for r in universe if r['ack_id'] in matched_ack_ids and r['plan_id']}),
 'matched_metadata_conflict_ack_ids':[r['ack_id'] for r in ack_rows if r['index_metadata_conflict']],
 'by_table':{t:{'matched':metrics([r for r in matched if r['source_table']==t]),'unmatched':metrics([r for r in unmatched if r['source_table']==t])} for t in tables},
 'router_owned_matched':metrics([r for r in matched if r['router_owned']=='true']),
 'latest_promotion_matched':metrics([r for r in matched if r['routing_run_id']=='20261011T194752Z-c18e4a48']),
 'ntess':{'ack_id':'20251014110308NAL0001274947001','in_universe':'20251014110308NAL0001274947001' in by_ack,'stable_value':metrics([r for r in matched if r['ack_id']=='20251014110308NAL0001274947001'])},
 'limitations':['Reported values retain inventory overlap and valuation issues; no reconciliation or deduplication is implied.','Universe membership does not confirm either OpenSearch index has been refreshed.','Matching is by exact filing ID; a different filing for the same plan is not treated as a match.','Universe duplicate ACK rows do not multiply source holdings.'],
 'query_ids':a.query_ids or ['da2b93e6-63f7-41f7-a75d-0bf73596050b'],'reused_universe_export':'--reuse-universe' in sys.argv}
assert len(matched)+len(unmatched)==len(holdings)
assert Decimal(summary['matched']['reported_assets'])+Decimal(summary['unmatched']['reported_assets'])==Decimal(json.loads((root/'summary.json').read_text())['all_history']['reported_assets'])
(out/'summary.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary,indent=2),flush=True)
