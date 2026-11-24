import sys,json,csv,re
from pathlib import Path
from types import SimpleNamespace
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from route_stable_value import Athena,quote
out=Path('outputs/stable_value_inventory_20261014')
schemas=json.loads((out/'schemas.json').read_text())
pattern=r'(^|[^a-z0-9])(stable|stbl|stab|stabl)[\s_-]*(value|val)([^a-z0-9]|$)'
parts=[]
for part in (out/'inventory.sql').read_text().split(' UNION ALL '):
    prefix,old=part.rsplit(' WHERE ',1)
    table=re.search(r'FROM default\.(\w+)$',prefix).group(1)
    cols=[c for c in ['raw_entity_name','asset_class','asset_sub_class','asset_type'] if c in schemas[table]]
    broader=' OR '.join("regexp_like(lower(coalesce("+c+",'')),"+quote(pattern)+")" for c in cols)
    parts.append(prefix+' WHERE ('+broader+') AND NOT '+old)
sql=' UNION ALL '.join(parts);(out/'subclass_additions.sql').write_text(sql)
a=Athena(SimpleNamespace(region='us-east-1',database='default',workgroup='primary',athena_output='s3://retirementinsights-reference/athena-results/'))
rows=a.run(sql)
(out/'subclass_additions.json').write_text(json.dumps({'rows':rows,'query_ids':a.query_ids},indent=2))
print('Additional subclass/formatting matches:',len(rows),flush=True)
