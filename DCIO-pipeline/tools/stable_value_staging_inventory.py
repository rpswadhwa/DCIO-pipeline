"""Read-only staging candidate counts; never treats candidates as new AUM."""
import json
from pathlib import Path
from types import SimpleNamespace
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from route_stable_value import Athena,quote
out=Path('outputs/stable_value_inventory_20261014')
a=Athena(SimpleNamespace(region='us-east-1',database='default',workgroup='primary',athena_output='s3://retirementinsights-reference/athena-results/'))
pattern=r'(^|[^a-z0-9])(stable|stbl|stab|stabl)\s+(value|val)([^a-z0-9]|$)'
strong="regexp_like(lower(coalesce(raw_entity_name,'')),"+quote(pattern)+") OR regexp_like(lower(coalesce(asset_type,'')),"+quote(pattern)+") OR regexp_like(lower(coalesce(asset_class,'')),"+quote(pattern)+")"
rows=a.run("""SELECT count(*) source_rows,count(distinct ack_id) filings,
 count(distinct concat(coalesce(ack_id,''),chr(31),lower(trim(regexp_replace(coalesce(raw_entity_name,''),'\\s+',' '))),chr(31),lower(trim(coalesce(raw_sponsor_name,''))),chr(31),cast(plan_investment_amt as varchar))) distinct_source_identities,
 count_if(regexp_like(lower(coalesce(raw_entity_name,'')),"""+quote(pattern)+""")) name_signal_rows,
 count_if(regexp_like(lower(coalesce(asset_type,'')),"""+quote(pattern)+""")) type_signal_rows
 FROM default.plan_holdings_staging WHERE """+strong)
result={'scope':'Staging rows with stable-value name/type/class signals; overlaps destination histories and includes repeated extractions. Not additional holdings or AUM.','counts':rows,'query_ids':a.query_ids}
(out/'staging_signal_counts.json').write_text(json.dumps(result,indent=2));print(json.dumps(result,indent=2))
