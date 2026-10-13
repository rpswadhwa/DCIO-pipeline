import ast
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from src.stable_value_pipeline_hook import batch_ack_ids, run_stable_value_pipeline_step, validated_batch_scope


class PipelineHookTests(unittest.TestCase):
    def invoke(self, **overrides):
        with tempfile.TemporaryDirectory() as directory:
            router = Path(directory) / 'route_stable_value.py'
            router.write_text('# test router', encoding='utf-8')
            args = dict(ack_ids=['B', 'A', 'A'], staging_completed=True,
                output_dir=directory, router_path=router, validation_count=2,
                validation_start='2026-10-10T00:00:00+00:00',
                validation_end='2026-10-10T00:01:00+00:00',
                scope_reader=lambda ids, *rest: ids)
            args.update(overrides)
            result = run_stable_value_pipeline_step(**args)
            saved = json.loads((Path(directory) / 'stable_value_routing_pipeline.json').read_text())
            self.assertEqual(result, saved)
            return result

    def successful_runner(self, command, **kwargs):
        self.command = command
        out = Path(command[command.index('--output-dir') + 1]) / 'run'
        out.mkdir()
        (out / 'summary.json').write_text(json.dumps(dict(mode='apply',
            grouped_candidates=6, counts=dict(INSERTED=2, ALREADY_EXISTS=3, REVIEW=1))))
        return Mock(returncode=0)

    def test_current_batch_identity_ignores_override_and_empty_names(self):
        self.assertEqual(batch_ack_ids([{'pdf_stem': ' A '}, {'pdf_stem': 'A'}, {'pdf_stem': None}, {}]), ['A'])

    def test_scoped_apply_and_output_settings_and_counts(self):
        result = self.invoke(runner=self.successful_runner, database='sandbox', workgroup='test',
                             athena_output='s3://test/results/', region='us-west-2')
        self.assertEqual(result['status'], 'COMPLETED')
        self.assertEqual(result['counts'], dict(inserted=2, already_existing=3, review=1))
        self.assertEqual(self.command[self.command.index('--ack-ids')+1], 'A,B')
        self.assertEqual(self.command[self.command.index('--warehouse')+1], 's3://retirementinsights-silver/tables/sandbox.db')
        self.assertEqual(self.command[self.command.index('--athena-output')+1], 's3://test/results/')

    def test_empty_batch_never_launches_all_history(self):
        runner = Mock()
        result = self.invoke(ack_ids=[], runner=runner)
        self.assertEqual(result['status'], 'COMPLETED')
        runner.assert_not_called()

    def test_missing_staging_is_incomplete_even_if_disable_variable_exists(self):
        runner = Mock()
        with patch.dict('os.environ', {'STABLE_VALUE_ROUTING_ENABLED': '0'}):
            result = self.invoke(staging_completed=False, runner=runner)
        self.assertEqual(result['status'], 'INCOMPLETE')
        runner.assert_not_called()

    def test_stale_or_mismatched_validation_scope_blocks_launch(self):
        for scope in [[], ['A'], ['OTHER']]:
            runner = Mock()
            result = self.invoke(scope_reader=lambda *args: scope, runner=runner)
            self.assertEqual(result['status'], 'INCOMPLETE')
            runner.assert_not_called()

    def test_filtered_out_filings_do_not_route_old_staging(self):
        runner = Mock()
        result = self.invoke(validation_count=0, runner=runner)
        self.assertEqual(result['reason'], 'no_holdings_survived_validation_filters')
        runner.assert_not_called()

    def test_partial_router_failure_preserves_counts_and_is_incomplete(self):
        def fail(command, **kwargs):
            self.successful_runner(command, **kwargs)
            return Mock(returncode=1)
        result = self.invoke(runner=fail)
        self.assertEqual(result['status'], 'INCOMPLETE')
        self.assertEqual(result['counts']['inserted'], 2)

    def test_missing_confirmation_is_not_pipeline_success(self):
        result = self.invoke(runner=Mock(return_value=Mock(returncode=0)))
        self.assertEqual(result['status'], 'INCOMPLETE')

    def test_existing_processing_failure_keeps_pipeline_incomplete(self):
        result = self.invoke(runner=self.successful_runner, upstream_errors=['MF enrichment failed'])
        self.assertEqual(result['status'], 'INCOMPLETE')
        self.assertEqual(result['counts']['inserted'], 2)

    def test_validation_scope_uses_time_window_without_pass_gate(self):
        with patch('route_stable_value.Athena') as athena:
            athena.return_value.run.return_value = [{'ack_id': 'A'}]
            ids = validated_batch_scope(['A'], dict(summary_database='default', summary_table='summary'),
                                       '2026-10-10T00:00:00+00:00', '2026-10-10T00:01:00+00:00')
            sql = athena.return_value.run.call_args.args[0]
            self.assertEqual(ids, ['A'])
            self.assertIn('run_ts', sql)
            self.assertNotIn('validation_status', sql)

    def test_pipeline_orders_hook_after_staging_and_classification(self):
        path = Path(__file__).resolve().parents[1] / 'src' / 'run_pipeline.py'
        tree = ast.parse(path.read_text(encoding='utf-8'))
        main = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'main')
        final_call = next(n for n in main.body if isinstance(n, ast.Assign) and
                          any(isinstance(t, ast.Name) and t.id == 'routing_report' for t in n.targets))
        staging_line = min(n.lineno for n in ast.walk(main) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == 'run_post_extract_validation')
        classification_line = max(n.lineno for n in ast.walk(main) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and isinstance(n.func.value, ast.Name) and n.func.value.id == 'subprocess' and n.func.attr == 'run')
        self.assertGreater(final_call.lineno, staging_line)
        self.assertGreater(final_call.lineno, classification_line)
        # Execute the actual pipeline's completion branch without OCR imports.
        branch = next(n for n in main.body if isinstance(n, ast.If) and
                      any(isinstance(x, ast.Raise) and isinstance(x.exc, ast.Call) and isinstance(x.exc.func, ast.Name) and x.exc.func.id == 'SystemExit' for x in n.body))
        with self.assertRaises(SystemExit) as caught:
            exec(compile(ast.Module(body=[branch], type_ignores=[]), str(path), 'exec'),
                 {'routing_report': {'status': 'INCOMPLETE'}, 'print': Mock()})
        self.assertEqual(caught.exception.code, 1)


if __name__ == '__main__':
    unittest.main()
