"""Read completed SELECT result CSVs and finish an interrupted/slow preview."""
import csv
import io
import json
from pathlib import Path
import sys
from urllib.parse import urlparse
import uuid
from datetime import datetime, timezone

import boto3

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root))
from route_stable_value import report, publish
from src.stable_value_manager import ManagerResolver
from src.stable_value_routing import build_decisions

client = boto3.client('athena', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')


def result(query_id):
    execution = client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']
    if execution['Status']['State'] != 'SUCCEEDED':
        raise RuntimeError(execution['Status'])
    location = urlparse(execution['ResultConfiguration']['OutputLocation'])
    stream = s3.get_object(Bucket=location.netloc, Key=location.path.lstrip('/'))['Body']
    with io.TextIOWrapper(stream, encoding='utf-8-sig', newline='') as reader:
        return list(csv.DictReader(reader))


stage, history = result(sys.argv[1]), result(sys.argv[2])
print(f'Read {len(stage)} staging groups and {len(history)} existing holdings', flush=True)
canon = json.loads(s3.get_object(Bucket='retirementinsights-reference', Key='manager_canonical/v1/manager_canonical.json')['Body'].read())
resolver = ManagerResolver(canon)
decisions = build_decisions(stage, history, resolver)
run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-preview-' + uuid.uuid4().hex[:8]
output = root / 'outputs' / 'stable_value_router' / run_id
report(decisions, output, run_id, resolver.canon_version, sys.argv[1:3], 'preview')
publish(s3, output, 'retirementinsights-reference', 'stable_value_routing/runs/' + run_id)
