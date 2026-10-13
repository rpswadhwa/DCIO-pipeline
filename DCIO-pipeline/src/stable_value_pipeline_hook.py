"""Mandatory, batch-scoped stable value routing after staging completes."""
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import uuid


def batch_ack_ids(rows):
    return sorted({str(row.get('pdf_stem') or '').strip() for row in rows
                   if str(row.get('pdf_stem') or '').strip()})


def validated_batch_scope(ack_ids, settings, validation_start, validation_end):
    # Use the existing summary to exclude filings whose rows were entirely
    # filtered out before staging. Do not filter by MF validation status.
    from route_stable_value import Athena, quote
    import re
    database, table = settings['summary_database'], settings['summary_table']
    if not all(re.fullmatch(r'[A-Za-z0-9_]+', value) for value in [database, table]):
        raise ValueError('Invalid validation-summary identifier')
    athena = Athena(SimpleNamespace(**settings))
    rows = athena.run(f"""SELECT DISTINCT ack_id FROM {database}.{table}
      WHERE ack_id IN ({','.join(quote(a) for a in ack_ids)})
      AND from_iso8601_timestamp(cast(run_ts AS varchar)) >= from_iso8601_timestamp({quote(validation_start)})
      AND from_iso8601_timestamp(cast(run_ts AS varchar)) <= from_iso8601_timestamp({quote(validation_end)})""")
    return sorted({row['ack_id'] for row in rows})


def run_stable_value_pipeline_step(*, ack_ids, staging_completed, output_dir,
                                   database='default', workgroup='primary',
                                   athena_output='s3://retirementinsights-reference/athena-results/',
                                   region='us-east-1', summary_database='default',
                                   summary_table='plan_mf_history_validation_summary',
                                   validation_start=None, validation_end=None,
                                   validation_count=0, upstream_errors=(),
                                   router_path=None, warehouse=None,
                                   report_bucket='retirementinsights-reference',
                                   runner=None, scope_reader=None):
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    report_path = output / 'stable_value_routing_pipeline.json'
    scope = sorted({str(a).strip() for a in ack_ids if a is not None and str(a).strip()})
    report = dict(step='stable_value_routing', mandatory=True,
                  started_at=datetime.now(timezone.utc).isoformat(),
                  requested_ack_ids=scope, ack_ids=[], status='INCOMPLETE',
                  counts=dict(inserted=0, already_existing=0, review=0),
                  upstream_errors=list(upstream_errors), report_path=str(report_path))
    try:
        if any(',' in a or '\n' in a or '\r' in a for a in scope):
            raise ValueError('Invalid filing ID in batch scope')
        if not scope:
            report.update(status='COMPLETED', reason='empty_extraction_batch')
        elif not staging_completed:
            raise RuntimeError('Staging did not complete; mandatory stable value routing cannot run')
        elif validation_count == 0:
            report.update(status='COMPLETED', reason='no_holdings_survived_validation_filters')
        else:
            if not validation_start or not validation_end:
                raise ValueError('Current validation time window is required')
            settings = dict(database=database, workgroup=workgroup,
                            athena_output=athena_output, region=region,
                            summary_database=summary_database, summary_table=summary_table)
            scope = (scope_reader or validated_batch_scope)(scope, settings, validation_start, validation_end)
            if not scope or not set(scope) <= set(report['requested_ack_ids']):
                raise RuntimeError('No trustworthy current-batch validation scope; refusing an unscoped router run')
            if len(scope) != validation_count:
                raise RuntimeError('Current validation scope does not match the number of validated filings')
            report['ack_ids'] = scope
            router = Path(router_path) if router_path else Path(__file__).resolve().parents[1] / 'route_stable_value.py'
            if not router.is_file():
                raise FileNotFoundError(f'Stable value router not deployed: {router}')
            run_output = output / 'stable_value_routing' / uuid.uuid4().hex
            run_output.mkdir(parents=True)
            command = [sys.executable, '-u', str(router.resolve()), '--apply',
                       '--ack-ids', ','.join(scope), '--database', database,
                       '--workgroup', workgroup, '--region', region,
                       '--athena-output', athena_output, '--output-dir', str(run_output),
                       '--warehouse', warehouse or f's3://retirementinsights-silver/tables/{database}.db',
                       '--report-bucket', report_bucket]
            report['command'] = command
            report['router_output_dir'] = str(run_output)
            result = (runner or subprocess.run)(command, cwd=str(router.resolve().parent), check=False)
            summaries = list(run_output.glob('*/summary.json'))
            if len(summaries) == 1:
                summary = json.loads(summaries[0].read_text(encoding='utf-8'))
                report['router_summary'] = summary
                report['router_summary_path'] = str(summaries[0])
                report['counts'] = dict(inserted=summary['counts'].get('INSERTED', 0),
                    already_existing=summary['counts'].get('ALREADY_EXISTS', 0),
                    review=summary['counts'].get('REVIEW', 0))
            if result.returncode != 0:
                raise RuntimeError(f'Stable value router failed with exit code {result.returncode}; inspect its reports before retrying')
            if len(summaries) != 1 or summary.get('mode') != 'apply':
                raise RuntimeError('Router did not produce a confirmed apply summary')
            if set(summary['counts']) - {'INSERTED', 'ALREADY_EXISTS', 'REVIEW'}:
                raise RuntimeError('Router has unconfirmed writes or unresolved execution states')
            if sum(summary['counts'].values()) != summary['grouped_candidates']:
                raise RuntimeError('Router decision counts do not reconcile')
            report.update(status='COMPLETED', reason='router_apply_verified')
        if upstream_errors:
            report.update(status='INCOMPLETE', error='Existing downstream processing failed')
    except Exception as exc:
        report.update(status='INCOMPLETE', error=str(exc))
    report['completed_at'] = datetime.now(timezone.utc).isoformat()
    report_path.write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(f"  Stable value routing: {report['status']} - {report['counts']}", flush=True)
    if report.get('error'):
        print(f"  {report['error']}", flush=True)
    print(f'  Routing pipeline report: {report_path}', flush=True)
    return report
