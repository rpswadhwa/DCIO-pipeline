"""Independent stable value router. Preview is default; apply is explicit.

No modifications to extraction, staging, MF writers, or the live CIT cleaner.
"""
import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import io
import pathlib
import re
import time
import uuid
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
from src.stable_value_manager import ManagerResolver
from src.stable_value_routing import TARGETS, VERSION
from src.stable_value_expanded import build_decisions, eligible

COMMON_COLUMNS = {
    'candidate_id': 'string', 'ack_id': 'string', 'raw_entity_name': 'string',
    'normalized_entity_name': 'string', 'raw_sponsor_name': 'string',
    'plan_investment_amt': 'decimal(18,2)', 'asset_type': 'string',
    'asset_class': 'string', 'asset_sub_class': 'string', 'validation_status': 'string',
    'normalized_manager': 'string', 'manager_match_status': 'string',
    'manager_match_method': 'string', 'manager_match_confidence': 'double',
    'needs_cleaning_flag': 'boolean', 'vehicle_type': 'string',
    'account_structure': 'string', 'contract_type': 'string',
    'resolution_status': 'string', 'routing_rule': 'string', 'routing_version': 'string',
    'routing_reason': 'string', 'source_table': 'string', 'routing_run_id': 'string',
    'canon_version': 'string', 'created_at': 'timestamp', 'updated_at': 'timestamp',
}
AUDIT_COLUMNS = {
    'audit_id': 'string', 'routing_run_id': 'string', 'candidate_id': 'string',
    'ack_id': 'string', 'raw_entity_name': 'string', 'raw_sponsor_name': 'string',
    'plan_investment_amt': 'decimal(18,2)', 'source_asset_type': 'string',
    'validation_status': 'string', 'staging_row_count': 'bigint',
    'destination_table': 'string', 'routing_status': 'string', 'routing_reason': 'string',
    'matched_history_table': 'string', 'matched_history_name': 'string',
    'normalized_manager': 'string', 'manager_match_status': 'string',
    'manager_match_method': 'string', 'manager_match_confidence': 'double',
    'routing_version': 'string', 'canon_version': 'string', 'recorded_at': 'timestamp',
}


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def literal(value, kind):
    if value is None or value == '':
        return 'NULL'
    if kind == 'boolean':
        return 'true' if value is True or str(value).lower() == 'true' else 'false'
    if kind in ('bigint', 'int', 'double') or kind.startswith('decimal'):
        return str(Decimal(str(value)))
    if kind == 'timestamp':
        return 'TIMESTAMP ' + quote(value)
    return quote(value)


class Athena:
    def __init__(self, args):
        self.args = args
        self.client = boto3.client('athena', region_name=args.region)
        self.glue = boto3.client('glue', region_name=args.region)
        self.s3 = boto3.client('s3', region_name=args.region)
        self.query_ids = []

    def run(self, sql, fetch=True):
        query_id = self.client.start_query_execution(
            QueryString=sql, QueryExecutionContext={'Database': self.args.database},
            ResultConfiguration={'OutputLocation': self.args.athena_output},
            WorkGroup=self.args.workgroup)['QueryExecutionId']
        self.query_ids.append(query_id)
        print(f'Athena query {query_id}', flush=True)
        deadline = time.monotonic() + 1800
        while True:
            execution = self.client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']
            status = execution['Status']
            if status['State'] == 'SUCCEEDED':
                break
            if status['State'] in ('FAILED', 'CANCELLED'):
                raise RuntimeError(f'{query_id}: {status.get("StateChangeReason", status)}')
            if time.monotonic() > deadline:
                self.client.stop_query_execution(QueryExecutionId=query_id)
                raise TimeoutError(f'Athena timed out: {query_id}')
            time.sleep(2)
        if not fetch:
            return []
        # SELECT result CSVs avoid thousands of paginated API requests when
        # comparing all holdings in candidate filings. No shell subprocesses.
        location = execution.get('ResultConfiguration', {}).get('OutputLocation', '')
        if location.startswith('s3://') and location.endswith('.csv'):
            result_uri = urlparse(location)
            stream = self.s3.get_object(Bucket=result_uri.netloc, Key=result_uri.path.lstrip('/'))['Body']
            with io.TextIOWrapper(stream, encoding='utf-8-sig', newline='') as reader:
                return list(csv.DictReader(reader))
        rows, header = [], None
        for page in self.client.get_paginator('get_query_results').paginate(QueryExecutionId=query_id):
            for record in page['ResultSet']['Rows']:
                values = [c.get('VarCharValue') for c in record['Data']]
                if header is None:
                    header = values
                else:
                    rows.append(dict(zip(header, values)))
        return rows

    def schema(self, name):
        try:
            table = self.glue.get_table(DatabaseName=self.args.database, Name=name)['Table']
            return {c['Name']: c['Type'] for c in table['StorageDescriptor']['Columns']}
        except self.glue.exceptions.EntityNotFoundException:
            return None

    def table(self, name):
        return f'{self.args.database}.{name}'


def staging_query(db, ack_ids):
    scope = ' AND ack_id IN (' + ','.join(quote(a) for a in ack_ids) + ')' if ack_ids else ''
    if ack_ids:
        return f"""SELECT ack_id, raw_entity_name, raw_sponsor_name, plan_investment_amt,
          asset_class, asset_sub_class, validation_status, asset_type, count(*) staging_row_count
          FROM {db}.plan_holdings_staging WHERE ack_id IN ({','.join(quote(a) for a in ack_ids)})
          GROUP BY 1,2,3,4,5,6,7,8"""
    # Select candidate filings broadly; read every staging holding in those
    # filings so duplicate/amount guards have the same context as reconciliation.
    return f"""SELECT ack_id, raw_entity_name, raw_sponsor_name, plan_investment_amt,
      asset_class, asset_sub_class, validation_status, asset_type, count(*) staging_row_count
      FROM {db}.plan_holdings_staging
      WHERE ack_id IN (SELECT DISTINCT ack_id FROM {db}.plan_holdings_staging
        WHERE (regexp_like(lower(coalesce(raw_entity_name,'')),
          'stable|stbl|stabl|stab |fixed account|guaranteed|group annuity|insurance|benefit.?responsive|retirement savings trust|managed income|\\bmip\\b|preserv|interest income|fixed ?interest')
          OR regexp_like(lower(coalesce(asset_type,'')), 'stable value')
          OR lower(trim(coalesce(asset_class,'')))='stable value'){scope})
      GROUP BY 1,2,3,4,5,6,7,8"""


def load_inputs(athena, ack_ids):
    staging = athena.run(staging_query(athena.args.database, ack_ids))
    if not staging:
        return [], []
    selected = "SELECT DISTINCT ack_id FROM candidates"
    parts = []
    sources = ['plan_cit_history', 'plan_mf_history_v3', *TARGETS]
    if athena.schema('plan_alternatives_history'):
        sources.append('plan_alternatives_history')
    for table in sources:
        schema = athena.schema(table)
        if schema is None:
            if table in {'plan_cit_history', 'plan_mf_history_v3'}:
                raise RuntimeError(f'Missing required table {table}')
            continue
        candidate_col = 'h.candidate_id' if 'candidate_id' in schema else 'cast(null as varchar)'
        # Fetch all holdings for candidate filings; do not filter existing history
        # by class/name. This catches abbreviated and incorrectly classified rows.
        parts.append(f"""SELECT {quote(table)} source_table, h.ack_id, h.raw_entity_name,
          h.raw_sponsor_name, h.plan_investment_amt, {candidate_col} candidate_id
          FROM {athena.table(table)} h JOIN ({selected}) a ON h.ack_id=a.ack_id""")
    if ack_ids:
        # Athena array constructors accept at most 254 arguments.
        filing_ids = sorted({r['ack_id'] for r in staging})
        arrays = ['ARRAY[' + ','.join(quote(a) for a in filing_ids[i:i + 200]) + ']'
                  for i in range(0, len(filing_ids), 200)]
        expression = arrays[0] if len(arrays) == 1 else 'concat(' + ','.join(arrays) + ')'
        candidate_sql = 'SELECT ack_id FROM UNNEST(' + expression + ') AS filing_scope(ack_id)'
    else:
        candidate_sql = staging_query(athena.args.database, ack_ids)
    query = 'WITH candidates AS (' + candidate_sql + ')\n' + '\nUNION ALL\n'.join(parts)
    print('Fetching existing holdings for candidate filings', flush=True)
    return staging, athena.run(query)


def create_tables(athena):
    for table, columns in [*[(t, COMMON_COLUMNS) for t in TARGETS], ('plan_stable_value_routing_audit', AUDIT_COLUMNS)]:
        existing = athena.schema(table)
        if existing is not None:
            missing = set(columns) - existing.keys()
            mismatched = {k for k in columns if k in existing and existing[k].replace(' ', '') != columns[k].replace(' ', '')}
            if missing or mismatched:
                raise RuntimeError(f'Incompatible existing {table}: missing={missing}, types={mismatched}')
            continue
        definitions = ',\n'.join(f'  {column} {kind}' for column, kind in columns.items())
        location = athena.args.warehouse.rstrip('/') + '/' + table
        athena.run(f"CREATE TABLE {athena.table(table)} (\n{definitions}\n) LOCATION {quote(location)} TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet')", fetch=False)


def unified_view_sql(db):
    parts = []
    for table in TARGETS:
        parts.append(f"""SELECT {quote(table)} source_table, ack_id, raw_entity_name,
          raw_sponsor_name, plan_investment_amt, asset_class, asset_sub_class, asset_type,
          normalized_manager, vehicle_type, account_structure, resolution_status,
          validation_status, manager_match_status, 'stable_value_router' record_origin
          FROM {db}.{table}""")
    parts.append(f"""SELECT 'plan_cit_history', ack_id, raw_entity_name, raw_sponsor_name,
      plan_investment_amt, asset_class, asset_sub_class, cast(null as varchar),
      coalesce(proposed_manager_ai, normalized_manager),
      CASE WHEN row_order LIKE 'SV:%' THEN 'collective_investment_trust' ELSE 'legacy_vehicle_unverified' END,
      CASE WHEN row_order LIKE 'SV:%' THEN 'pooled_trust' ELSE 'unknown' END,
      CASE WHEN row_order LIKE 'SV:%' THEN 'vehicle_resolved' ELSE 'legacy_unverified' END,
      cast(null as varchar), manager_status_ai,
      CASE WHEN row_order LIKE 'SV:%' THEN 'stable_value_router' ELSE 'legacy_history' END
      FROM {db}.plan_cit_history WHERE asset_class='Stable Value'
      OR regexp_like(raw_entity_name, '(?i)(^|[^a-z0-9])stable\\s+value([^a-z0-9]|$)')""")
    parts.append(f"""SELECT 'plan_mf_history_v3', ack_id, raw_entity_name, raw_sponsor_name,
      plan_investment_amt, asset_class, asset_sub_class, cast(null as varchar),
      cast(null as varchar), 'legacy_vehicle_unverified', 'unknown', 'legacy_unverified',
      cast(null as varchar), 'UNVERIFIED', 'legacy_history'
      FROM {db}.plan_mf_history_v3 WHERE asset_class='Stable Value'
      OR regexp_like(raw_entity_name, '(?i)(^|[^a-z0-9])stable\\s+value([^a-z0-9]|$)')""")
    # Surface legacy overlap rather than dropping possibly distinct contracts.
    return f"""CREATE VIEW {db}.v_plan_stable_value_holdings AS
      SELECT *, count(*) OVER (PARTITION BY ack_id,
      lower(trim(regexp_replace(raw_entity_name, '\\s+', ' '))), plan_investment_amt)
      AS equivalent_pair_amount_record_count
      FROM ({' UNION ALL '.join(parts)})"""


def create_view(athena):
    if athena.schema('v_plan_stable_value_holdings') is None:
        athena.run(unified_view_sql(athena.args.database), fetch=False)


class RunLock:
    def __init__(self, s3, bucket, key, run_id):
        self.s3, self.bucket, self.key, self.run_id = s3, bucket, key, run_id

    def __enter__(self):
        try:
            self.s3.put_object(Bucket=self.bucket, Key=self.key,
                               Body=json.dumps({'run_id': self.run_id, 'started_at': datetime.now(timezone.utc).isoformat()}).encode(),
                               ContentType='application/json', IfNoneMatch='*')
        except ClientError as exc:
            if exc.response['Error']['Code'] in {'PreconditionFailed', 'ConditionalRequestConflict'}:
                raise RuntimeError('Another stable value apply run owns the lock; inspect it before retrying') from exc
            raise
        return self

    def __exit__(self, kind, value, traceback):
        current = json.loads(self.s3.get_object(Bucket=self.bucket, Key=self.key)['Body'].read())
        if current.get('run_id') == self.run_id:
            self.s3.delete_object(Bucket=self.bucket, Key=self.key)


def values_source(rows, columns):
    tuples = []
    for row in rows:
        tuples.append('(' + ','.join(f'CAST({literal(row.get(k), v)} AS {v.replace("string", "varchar")})' for k, v in columns.items()) + ')')
    return '(VALUES ' + ',\n'.join(tuples) + ') AS s (' + ','.join(columns) + ')'


def merge_new(athena, table, rows, columns, key, extra_guard=''):
    if len({r[key] for r in rows}) != len(rows):
        raise ValueError(f'Duplicate {key} in MERGE source for {table}')
    if len(rows) > 60:
        return merge_parquet(athena, table, rows, columns, key, extra_guard)
    for start in range(0, len(rows), 60):
        batch = rows[start:start + 60]
        source = values_source(batch, columns)
        sql = f'MERGE INTO {athena.table(table)} t USING {source} ON t.{key}=s.{key}'
        if extra_guard:
            sql += ' OR (' + extra_guard + ')'
        sql += ' WHEN NOT MATCHED THEN INSERT (' + ','.join(columns) + ') VALUES (' + ','.join('s.' + k for k in columns) + ')'
        athena.run(sql, fetch=False)


def merge_parquet(athena, table, rows, columns, key, extra_guard):
    """One set-based MERGE per destination rather than hundreds of INSERTs.

    Input data stays in a unique S3 run prefix for recovery. Only the temporary
    Glue table created by this call is dropped; existing tables are untouched.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    schema = []
    converted = []
    for name, kind in columns.items():
        typ = (pa.decimal128(18, 2) if kind.startswith('decimal') else
               pa.bool_() if kind == 'boolean' else pa.int64() if kind in {'bigint', 'int'} else
               pa.float64() if kind == 'double' else pa.timestamp('ms') if kind == 'timestamp' else pa.string())
        schema.append(pa.field(name, typ))
    for row in rows:
        result = {}
        for name, kind in columns.items():
            value = row.get(name)
            if value is None or value == '':
                value = None
            elif kind.startswith('decimal'):
                value = Decimal(str(value))
            elif kind in {'bigint', 'int'}:
                value = int(value)
            elif kind == 'double':
                value = float(value)
            elif kind == 'boolean':
                value = value is True or str(value).lower() == 'true'
            elif kind == 'timestamp':
                value = datetime.fromisoformat(value)
            else:
                value = str(value)
            result[name] = value
        converted.append(result)
    data = pa.Table.from_pylist(converted, schema=pa.schema(schema))
    buffer = io.BytesIO()
    pq.write_table(data, buffer, compression='snappy')
    temp_name = 'sv_router_input_' + uuid.uuid4().hex
    prefix = 'stable_value_routing/merge_inputs/' + temp_name + '/'
    athena.s3.put_object(Bucket=athena.args.report_bucket, Key=prefix + 'part.parquet', Body=buffer.getvalue())
    definitions = ','.join(f'{name} {kind}' for name, kind in columns.items())
    location = f's3://{athena.args.report_bucket}/{prefix}'
    athena.run(f'CREATE EXTERNAL TABLE {athena.table(temp_name)} ({definitions}) STORED AS PARQUET LOCATION {quote(location)}', fetch=False)
    try:
        sql = f'MERGE INTO {athena.table(table)} t USING {athena.table(temp_name)} s ON t.{key}=s.{key}'
        if extra_guard:
            sql += ' OR (' + extra_guard + ')'
        sql += ' WHEN NOT MATCHED THEN INSERT (' + ','.join(columns) + ') VALUES (' + ','.join('s.' + k for k in columns) + ')'
        athena.run(sql, fetch=False)
    finally:
        athena.run(f'DROP TABLE {athena.table(temp_name)}', fetch=False)


# (2026-10-21) Shared stable value sub-class vocabulary, keyed on how the money is held. Keep in sync with
# sch_a_06_load_plan_sch_a_stable_value_history.sql and the 5updatesubasset*.sql classifiers (CITs, MFs and public
# DC funds are all 'Stable Value Pooled Fund').
def sv_sub_class(d):
    if d.get('vehicle_type') == 'collective_investment_trust':
        return 'Stable Value Pooled Fund'
    if d.get('vehicle_type') == 'separately_managed_account':
        return 'Stable Value Separately Managed'
    if d.get('vehicle_type') != 'insurance_contract':
        return 'Stable Value Unclassified'
    account, contract = d.get('account_structure'), d.get('contract_type')
    if account == 'general_account':
        return 'Stable Value General Account'
    if account == 'synthetic_wrap':
        return 'Stable Value Synthetic'
    if account == 'insurance_separate_account':
        return 'Stable Value Separate Account'
    if contract in ('gic', 'guaranteed_investment_contract'):
        return 'Stable Value GIC'
    return 'Stable Value Insurance Contract'


def target_record(d, run_id, canon_version, now):
    record = {k: d.get(k) for k in COMMON_COLUMNS}
    record.update(asset_class='Stable Value', asset_sub_class=sv_sub_class(d),
                  source_table='plan_holdings_staging', routing_run_id=run_id,
                  canon_version=canon_version, created_at=now, updated_at=now,
                  resolution_status='vehicle_unresolved' if d['vehicle_type'] == 'unknown' else 'vehicle_resolved')
    return record


def write_cit(athena, decisions, run_id, now):
    # Use existing CIT fields. No alteration of CIT schema, old rows, or cleaner.
    columns = {'ack_id': 'string', 'row_order': 'string', 'raw_entity_name': 'string',
               'raw_sponsor_name': 'string', 'plan_investment_amt': 'decimal(18,2)',
               'normalized_manager': 'string', 'needs_cleaning_flag': 'boolean',
               'confidence_score': 'double', 'manager_status_ai': 'string',
               'proposed_manager_ai': 'string', 'manager_reason_ai': 'string',
               'manager_matched_by_ai': 'string', 'manager_confidence_ai': 'double',
               'normalized_version_ai': 'string', 'asset_class': 'string', 'asset_sub_class': 'string',
               'classification_method': 'string', 'classification_confidence': 'string'}
    current = athena.schema('plan_cit_history')
    if any(k not in current for k in columns):
        raise RuntimeError('CIT schema changed; refusing to write')
    records = []
    for d in decisions:
        matched = d['manager_match_status'] == 'MATCHED'
        records.append({
            **{k: d.get(k) for k in ['ack_id', 'raw_entity_name', 'raw_sponsor_name', 'plan_investment_amt', 'normalized_manager', 'needs_cleaning_flag']},
            'row_order': 'SV:' + d['candidate_id'],
            'confidence_score': d['manager_match_confidence'],
            'manager_status_ai': 'MATCHED_CANONICAL' if matched else 'NEEDS_REVIEW',
            'proposed_manager_ai': d['normalized_manager'],
            'manager_reason_ai': 'Deterministic existing CIT rules; no AI call',
            'manager_matched_by_ai': d['manager_match_method'],
            'manager_confidence_ai': d['manager_match_confidence'],
            'normalized_version_ai': VERSION + ':' + run_id, 'asset_class': 'Stable Value',
            'asset_sub_class': sv_sub_class(d),
            'classification_method': VERSION + ':' + d['routing_rule'],
            'classification_confidence': 'HIGH',
        })
    guard = "t.ack_id=s.ack_id AND lower(trim(regexp_replace(t.raw_entity_name, '\\s+', ' ')))=lower(trim(regexp_replace(s.raw_entity_name, '\\s+', ' ')))"
    merge_new(athena, 'plan_cit_history', records, columns, 'row_order', guard)
    # Populate filing metadata from an unambiguous plan_master match only.
    if records:
        ids = ','.join(quote(r['ack_id']) for r in records)
        athena.run(f"""MERGE INTO {athena.table('plan_cit_history')} t USING (
          SELECT ack_id, max(plan_id) plan_id, max(filing_year) filing_year
          FROM {athena.table('plan_master')} WHERE ack_id IN ({ids}) GROUP BY ack_id
          HAVING count(distinct plan_id)=1 AND count(distinct filing_year)=1
        ) s ON t.ack_id=s.ack_id AND t.row_order LIKE 'SV:%'
        WHEN MATCHED THEN UPDATE SET plan_id=s.plan_id, filing_year=s.filing_year""", fetch=False)


def audit_records(decisions, run_id, canon_version, now):
    # Multiple staging representations of the same candidate stay in CSV; audit
    # preserves their combined multiplicity in one unique run/candidate record.
    records = {}
    for d in decisions:
        record = {k: d.get(k) for k in AUDIT_COLUMNS}
        record.update(audit_id=run_id + ':' + d['candidate_id'], routing_run_id=run_id,
                      source_asset_type=d.get('asset_type'), canon_version=canon_version,
                      recorded_at=now, staging_row_count=int(d['staging_row_count']))
        if record['audit_id'] in records:
            records[record['audit_id']]['staging_row_count'] += record['staging_row_count']
        else:
            records[record['audit_id']] = record
    return list(records.values())


def report(decisions, output, run_id, canon_version, queries, mode):
    output.mkdir(parents=True, exist_ok=True)
    fields = sorted({k for d in decisions for k in d})
    with (output / 'routing_decisions.csv').open('w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(decisions)
    counts = Counter(d['routing_status'] for d in decisions)
    destinations = Counter(d['destination_table'] for d in decisions if d['routing_status'] in {'READY', 'INSERTED'})
    amounts = defaultdict(Decimal)
    for d in decisions:
        if d['plan_investment_amt'] is not None:
            amounts[d['routing_status']] += Decimal(d['plan_investment_amt']) * int(d['staging_row_count'])
    summary = {'run_id': run_id, 'mode': mode, 'routing_version': VERSION,
               'canon_version': canon_version, 'grouped_candidates': len(decisions),
               'staging_rows': sum(int(d['staging_row_count']) for d in decisions),
               'counts': dict(counts), 'accepted_destinations': dict(destinations),
               'reported_amounts_by_status': {k: str(v) for k, v in amounts.items()},
               'review_reasons': dict(Counter(d['routing_reason'] for d in decisions if d['routing_status'] == 'REVIEW')),
               'query_ids': queries}
    (output / 'summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    print(json.dumps(summary, indent=2), flush=True)
    return summary


def publish(s3, output, bucket, prefix):
    for file in output.iterdir():
        if file.is_file():
            s3.upload_file(str(file), bucket, prefix.rstrip('/') + '/' + file.name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--ack-ids', default='')
    parser.add_argument('--approved-candidates', help='Reviewed proposal CSV; limits promotion to these exact identities')
    parser.add_argument('--product-rules', help='Reviewed product-rule JSON (evidence + approver per product); '
                        'promotes holdings held only for missing stable value / structure evidence. Requires --approved-candidates')
    parser.add_argument('--region', default='us-east-1')
    parser.add_argument('--database', default='default')
    parser.add_argument('--workgroup', default='primary')
    parser.add_argument('--athena-output', default='s3://retirementinsights-reference/athena-results/')
    parser.add_argument('--warehouse', default='s3://retirementinsights-silver/tables/default.db')
    parser.add_argument('--canon-bucket', default='retirementinsights-reference')
    parser.add_argument('--canon-key', default='manager_canonical/v1/manager_canonical.json')
    parser.add_argument('--output-dir', default='outputs/stable_value_router')
    parser.add_argument('--report-bucket', default='retirementinsights-reference')
    parser.add_argument('--report-prefix', default='stable_value_routing/runs')
    args = parser.parse_args()
    for identifier in [args.database, args.workgroup]:
        if not re.fullmatch(r'[a-zA-Z0-9_-]+', identifier):
            parser.error('Invalid database/workgroup identifier')
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + str(uuid.uuid4())[:8]
    output = pathlib.Path(args.output_dir) / run_id
    s3 = boto3.client('s3', region_name=args.region)
    canon = json.loads(s3.get_object(Bucket=args.canon_bucket, Key=args.canon_key)['Body'].read())
    resolver = ManagerResolver(canon)
    athena = Athena(args)
    ack_ids = [a.strip() for a in args.ack_ids.split(',') if a.strip()]
    approved = None
    product_rules = {}
    if args.product_rules:
        if not args.approved_candidates:
            parser.error('--product-rules requires --approved-candidates')
        from src.holding_reconciliation import load_product_rules
        product_rules = load_product_rules(args.product_rules)
        if any(r['destination_table'] not in {'plan_cit_history', *TARGETS} for r in product_rules.values()):
            raise ValueError('Product-rule destination is not a stable value router target')
    if args.approved_candidates:
        with open(args.approved_candidates, encoding='utf-8-sig', newline='') as f:
            proposal_rows = list(csv.DictReader(f))
        from src.holding_reconciliation import identity
        approved = {r['candidate_id'] for r in proposal_rows}
        if not approved or len(approved) != len(proposal_rows) or any(identity(r) != r['candidate_id'] for r in proposal_rows):
            raise ValueError('Invalid or duplicate proposal identities')
        if any(r['outcome'] != 'INSERT' for r in proposal_rows):
            raise ValueError('Only reviewed INSERT proposals can be promoted')
        proposal_scope = sorted({r['ack_id'] for r in proposal_rows})
        if ack_ids and set(ack_ids) != set(proposal_scope):
            raise ValueError('Explicit scope differs from approved proposals')
        ack_ids = proposal_scope

    def scoped_decisions(staging, history):
        decisions = build_decisions(staging, history, resolver, product_rules)
        if approved is not None:
            decisions = [d for d in decisions if d['candidate_id'] in approved]
            if {d['candidate_id'] for d in decisions} != approved:
                raise RuntimeError('Approved holding missing or changed in live staging; refusing promotion')
        return decisions

    def execute():
        if args.apply:
            create_tables(athena)
        staging, history = load_inputs(athena, ack_ids)
        decisions = scoped_decisions(staging, history)
        report(decisions, output, run_id, resolver.canon_version, athena.query_ids, 'apply_planned' if args.apply else 'preview')
        publish(s3, output, args.report_bucket, args.report_prefix + '/' + run_id)
        if not args.apply:
            return
        now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        ready = [d for d in decisions if d['routing_status'] == 'READY']
        # Each target receives only unambiguous unique pairs. Check all sources
        # again before the first write under the global router lock.
        if ready:
            fresh_staging, fresh_history = load_inputs(athena, ack_ids)
            fresh = {d['candidate_id']: d for d in scoped_decisions(fresh_staging, fresh_history)}
            for d in ready:
                current = fresh.get(d['candidate_id'])
                if current is None or current['routing_status'] != 'READY' or current['destination_table'] != d['destination_table']:
                    d.update(routing_status='REVIEW', routing_reason='candidate_changed_during_apply')
            ready = [d for d in ready if d['routing_status'] == 'READY']
        try:
            for table in ['plan_cit_history', *TARGETS]:
                batch = [d for d in ready if d['destination_table'] == table]
                if not batch:
                    continue
                print(f'Writing {len(batch)} holdings to {table}', flush=True)
                if table == 'plan_cit_history':
                    write_cit(athena, batch, run_id, now)
                else:
                    records = [target_record(d, run_id, resolver.canon_version, now) for d in batch]
                    guard = "t.ack_id=s.ack_id AND t.normalized_entity_name=s.normalized_entity_name"
                    merge_new(athena, table, records, COMMON_COLUMNS, 'candidate_id', guard)
                # Verify every intended holding is present exactly once with the
                # intended amount, including the legacy CIT destination.
                if table == 'plan_cit_history':
                    verified = athena.run(f"SELECT row_order id, plan_investment_amt FROM {athena.table(table)} WHERE normalized_version_ai={quote(VERSION + ':' + run_id)}")
                    expected = {'SV:' + d['candidate_id']: d['plan_investment_amt'] for d in batch}
                else:
                    verified = athena.run(f"SELECT candidate_id id, plan_investment_amt FROM {athena.table(table)} WHERE routing_run_id={quote(run_id)}")
                    expected = {d['candidate_id']: d['plan_investment_amt'] for d in batch}
                actual = {r['id']: r['plan_investment_amt'] for r in verified}
                if len(verified) != len(expected) or actual != expected:
                    raise RuntimeError(f'Destination verification failed for {table}; inspect report before rerun')
                for d in batch:
                    d['routing_status'] = 'INSERTED'
        finally:
            for d in decisions:
                if d['routing_status'] == 'READY':
                    d.update(routing_status='WRITE_NOT_CONFIRMED', routing_reason='write_or_verification_failed')
            try:
                merge_new(athena, 'plan_stable_value_routing_audit', audit_records(decisions, run_id, resolver.canon_version, now), AUDIT_COLUMNS, 'audit_id')
            finally:
                report(decisions, output, run_id, resolver.canon_version, athena.query_ids, 'apply')
                publish(s3, output, args.report_bucket, args.report_prefix + '/' + run_id)
        create_view(athena)

    if args.apply:
        with RunLock(s3, args.report_bucket, 'stable_value_routing/locks/apply.json', run_id):
            execute()
    else:
        execute()


if __name__ == '__main__':
    main()
