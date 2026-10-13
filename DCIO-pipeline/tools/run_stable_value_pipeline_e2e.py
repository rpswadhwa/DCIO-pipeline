"""Exercise the deployed pipeline with synthetic PDFs and isolated AWS tables."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import uuid

project = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project))
from route_stable_value import Athena, quote

token = os.environ.get('STABLE_VALUE_TEST_TOKEN') or uuid.uuid4().hex[:8]
if len(token) != 8 or any(c not in '0123456789abcdef' for c in token):
    raise ValueError('Test token must be eight lowercase hexadecimal characters')
database = 'sv_pipeline_e2e_20261010_' + token
ack_id = 'SV_PIPELINE_TEST_' + token
workspace = project / ('stable_value_pipeline_e2e_' + token)
inputs = workspace / 'inputs'
inputs.mkdir(parents=True)
warehouse = f's3://retirementinsights-silver/stable_value_routing/integration_tests/{token}'
args = SimpleNamespace(database=database, region='us-east-1', workgroup='primary',
                       athena_output='s3://retirementinsights-reference/athena-results/')
athena = Athena(args)
# The instance role cannot create databases. Provision this isolated namespace
# through the configured operator credentials before invoking this test.
athena.glue.get_database(Name=database)
manifest = dict(database=database, ack_id=ack_id, warehouse=warehouse, workspace=str(workspace),
                started_at=datetime.now(timezone.utc).isoformat(), synthetic=True)
(workspace / 'manifest.json').write_text(json.dumps(manifest, indent=2))
print(json.dumps(manifest, indent=2), flush=True)


def create_table(name, columns):
    definitions = ','.join(f'{name} {kind}' for name, kind in columns.items())
    athena.run(f"CREATE TABLE {database}.{name} ({definitions}) LOCATION {quote(warehouse+'/'+name)} TBLPROPERTIES ('table_type'='ICEBERG','format'='parquet')", fetch=False)


for name in ['plan_holdings_staging', 'plan_mf_history_v3', 'plan_cit_history',
             'plan_mf_history_validation_summary', 'plan_mf_history_validation_errors']:
    source = athena.glue.get_table(DatabaseName='default', Name=name)['Table']
    create_table(name, {c['Name']: c['Type'] for c in source['StorageDescriptor']['Columns']})
create_table('plan_master_index_universe', dict(ack_id='string', plan_id='string', amt_mutual_funds='decimal(18,2)', amt_cit='decimal(18,2)'))
create_table('plan_master', dict(ack_id='string', plan_id='string', filing_year='int'))
athena.run(f"INSERT INTO {database}.plan_master_index_universe VALUES ({quote(ack_id)},'SYNTHETIC_PLAN',0.00,0.00)", fetch=False)
athena.run(f"INSERT INTO {database}.plan_master VALUES ({quote(ack_id)},'SYNTHETIC_PLAN',2025)", fetch=False)

# Minimal standards-compliant, one-page PDF. Clearly marked synthetic and never
# placed in production input folders or the production filings universe.
def text(x, y, value, size=9):
    escaped = value.replace('\\', '\\\\').replace('(', '\\(').replace(')', '\\)')
    return f'BT /F1 {size} Tf {x} {y} Td ({escaped}) Tj ET\n'

content = text(36, 575, 'SYNTHETIC TEST FIXTURE - NOT A REAL PLAN OR INVESTMENT', 11)
content += text(36, 548, 'Schedule of Assets (Held at End of Year)', 12)
content += text(36, 530, 'Schedule H, Line 4i', 10)
content += text(36, 511, 'Identity of Issuer') + text(410, 511, 'Description of Investment') + text(667, 511, 'Current Value')
fixture = [('Putnam Stable Value Collective Investment Trust', 'Common/Collective Trust Fund', '$100,001'),
           ('Transamerica Stable Value Core Account', 'Insurance company general account', '$200,002'),
           ('Synthetic Stable Value Portfolio', 'Stable Value Fund', '$400,004')]
for y, row in zip([478, 450, 422], fixture):
    content += text(36, y, row[0]) + text(410, y, row[1]) + text(677, y, row[2])
content += text(36, 394, 'Total Investments') + text(677, 394, '$700,007')
for y in [500, 487, 459, 431, 403, 381]:
    content += f'36 {y} m 756 {y} l S\n'
data = content.encode('ascii')
objects = [b'<< /Type /Catalog /Pages 2 0 R >>', b'<< /Type /Pages /Kids [3 0 R] /Count 1 >>',
           b'<< /Type /Page /Parent 2 0 R /MediaBox [0 0 792 612] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>',
           b'<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>',
           b'<< /Length '+str(len(data)).encode()+b' >>\nstream\n'+data+b'endstream']
pdf = bytearray(b'%PDF-1.4\n')
offsets = [0]
for i, obj in enumerate(objects, 1):
    offsets.append(len(pdf))
    pdf.extend(f'{i} 0 obj\n'.encode()+obj+b'\nendobj\n')
start = len(pdf)
pdf.extend(f'xref\n0 {len(objects)+1}\n0000000000 65535 f \n'.encode())
for offset in offsets[1:]:
    pdf.extend(f'{offset:010d} 00000 n \n'.encode())
pdf.extend(f'trailer\n<< /Size {len(objects)+1} /Root 1 0 R >>\nstartxref\n{start}\n%%EOF\n'.encode())
(inputs / (ack_id+'.pdf')).write_bytes(pdf)
(workspace / 'no_overrides.json').write_text('{}')
env = {**os.environ, 'INPUT_DIR': str(inputs), 'OUTPUT_DIR': str(workspace/'output'),
       'SYNC_S3_INPUTS': '0', 'S3_BUCKET_PATH': '', 'USE_LLM': '0', 'USE_POST_LLM': '0',
       'USE_OCR': '0', 'LLM_FULL_EXTRACT': '0', 'OCR_FALLBACK_ENABLED': '0', 'SECTION_TYPING': '0',
       'LLM_PAGE_OVERRIDES_FILE': str(workspace/'no_overrides.json'), 'VALIDATION_ENABLED': '1',
       'VALIDATION_REF_DB': database, 'VALIDATION_REF_TABLE': 'plan_master_index_universe',
       'VALIDATED_GLUE_DB': database, 'VALIDATION_SUMMARY_GLUE_DB': database,
       'VALIDATED_TABLE': 'plan_mf_history_v3', 'VALIDATION_SUMMARY_TABLE': 'plan_mf_history_validation_summary',
       'VALIDATION_ERROR_TABLE': 'plan_mf_history_validation_errors',
       'HOLDINGS_STAGING_TABLE': 'plan_holdings_staging', 'ALTERNATIVES_TABLE': '',
       'ENRICH_MF_ENABLED': '0', 'STAGE_REPORT_ENABLED': '0', 'AWS_REGION': 'us-east-1',
       'ATHENA_WORKGROUP': 'primary', 'ATHENA_STAGING_S3': args.athena_output,
       'STABLE_VALUE_WAREHOUSE': warehouse, 'PLAN_YEAR': '2025',
       'VALIDATED_S3_PATH': warehouse+'/plan_mf_history_v3/',
       'VALIDATION_ERROR_S3_PATH': warehouse+'/plan_mf_history_validation_errors/'}
for attempt in [1, 2]:
    log_path = workspace / f'pipeline_{attempt}.log'
    with log_path.open('w') as log:
        result = subprocess.run([sys.executable, '-u', '-m', 'src.run_pipeline'], cwd=project, env=env,
                                stdout=log, stderr=subprocess.STDOUT, check=False)
    print('Pipeline attempt', attempt, 'exit', result.returncode, flush=True)
    if result.returncode:
        print(log_path.read_text()[-10000:], flush=True)
        raise SystemExit('Isolated end-to-end pipeline failed')
    report = json.loads((workspace/'output/stable_value_routing_pipeline.json').read_text())
    (workspace / f'pipeline_report_{attempt}.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2), flush=True)
    expected = {'inserted': 3, 'already_existing': 0, 'review': 0} if attempt == 1 else {'inserted': 0, 'already_existing': 3, 'review': 0}
    if report['status'] != 'COMPLETED' or report['counts'] != expected or report['ack_ids'] != [ack_id]:
        raise SystemExit('End-to-end routing counts or scope did not match the fixture')
verification = athena.run(f"SELECT source_table, count(*) holdings, sum(plan_investment_amt) amount FROM {database}.v_plan_stable_value_holdings WHERE record_origin='stable_value_router' GROUP BY 1")
if {r['source_table']: (int(r['holdings']), r['amount']) for r in verification} != {
    'plan_cit_history': (1, '100001.00'), 'plan_insurance_history': (1, '200002.00'),
    'plan_stable_value_unresolved_history': (1, '400004.00')}:
    raise SystemExit('Isolated destination verification failed')
manifest.update(passed=True, verification=verification, query_ids=athena.query_ids,
                completed_at=datetime.now(timezone.utc).isoformat())
(workspace / 'manifest.json').write_text(json.dumps(manifest, indent=2))
athena.s3.upload_file(str(workspace/'manifest.json'), 'retirementinsights-reference',
                      f'stable_value_routing/integration/20261010/{token}/e2e_verification.json')
print('END-TO-END EXTRACTION, AUTOMATIC ROUTING, AND ZERO-INSERT RERUN PASSED', flush=True)
