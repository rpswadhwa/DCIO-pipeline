import unittest
from unittest.mock import Mock
from route_stable_value import merge_new, COMMON_COLUMNS, audit_records, target_record, RunLock
from src.stable_value_routing import build_decisions
from test_stable_value_routing import holding, NoManager


class WriterTests(unittest.TestCase):
    def test_bulk_parquet_preserves_amounts_nulls_and_timestamps(self):
        import io
        from decimal import Decimal
        import pyarrow.parquet as pq
        client = Mock()
        client.args.report_bucket = 'test-bucket'
        client.table.side_effect = lambda name: 'default.' + name
        rows = [dict(candidate_id=str(i), plan_investment_amt='1234567890.12',
                     normalized_manager=None, needs_cleaning_flag=True,
                     created_at='2026-10-10 01:02:03.456') for i in range(61)]
        columns = dict(candidate_id='string', plan_investment_amt='decimal(18,2)',
                       normalized_manager='string', needs_cleaning_flag='boolean',
                       created_at='timestamp')
        merge_new(client, 'plan_insurance_history', rows, columns, 'candidate_id')
        body = client.s3.put_object.call_args.kwargs['Body']
        restored = pq.ParquetFile(io.BytesIO(body)).read(use_threads=False).to_pylist()
        self.assertEqual(len(restored), 61)
        self.assertEqual(restored[0]['plan_investment_amt'], Decimal('1234567890.12'))
        self.assertIsNone(restored[0]['normalized_manager'])
        self.assertTrue(restored[0]['needs_cleaning_flag'])
        self.assertEqual(restored[0]['created_at'].microsecond, 456000)
        self.assertIn('WHEN NOT MATCHED THEN INSERT', client.run.call_args_list[1].args[0])
        self.assertTrue(client.run.call_args_list[-1].args[0].startswith('DROP TABLE default.sv_router_input_'))

    def test_sql_quotes_names_and_preserves_decimals(self):
        client = Mock()
        client.table.return_value = 'default.plan_insurance_history'
        merge_new(client, 'plan_insurance_history', [dict(candidate_id='x', raw_entity_name="O'Brien", plan_investment_amt='123.45')],
                  dict(candidate_id='string', raw_entity_name='string', plan_investment_amt='decimal(18,2)'), 'candidate_id')
        query = client.run.call_args.args[0]
        self.assertIn("O''Brien", query)
        self.assertIn('CAST(123.45 AS decimal(18,2))', query)
        self.assertNotIn('DELETE', query)

    def test_rejects_nonunique_merge_source(self):
        with self.assertRaises(ValueError):
            merge_new(Mock(), 'x', [dict(candidate_id='same'), dict(candidate_id='same')], {'candidate_id': 'string'}, 'candidate_id')

    def test_audit_combines_representations_without_losing_multiplicity(self):
        row = holding()
        decisions = build_decisions([row, row], [], NoManager())
        records = audit_records(decisions, 'run', 'canon', '2026-10-10 00:00:00')
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['staging_row_count'], 2)
        self.assertEqual(records[0]['routing_status'], 'REVIEW')

    def test_target_keeps_validation_and_unknown_vehicle(self):
        decision = build_decisions([holding()], [], NoManager())[0]
        record = target_record(decision, 'run', 'canon', '2026-10-10 00:00:00')
        self.assertEqual(record['validation_status'], 'FAIL')
        self.assertEqual(record['resolution_status'], 'vehicle_unresolved')

    def test_lock_is_conditional_and_releases_only_own_lock(self):
        import io
        s3 = Mock()
        s3.get_object.return_value = {'Body': io.BytesIO(b'{"run_id":"another"}')}
        with RunLock(s3, 'bucket', 'key', 'mine'):
            self.assertEqual(s3.put_object.call_args.kwargs['IfNoneMatch'], '*')
        s3.delete_object.assert_not_called()


if __name__ == '__main__':
    unittest.main()
