import unittest
from unittest.mock import Mock
from src.stable_value_expanded import build_decisions
from test_stable_value_routing import NoManager
from test_holding_reconciliation import row


class ExpandedRouterTests(unittest.TestCase):
    def test_scoped_queries_fetch_full_staging_context_and_history_without_repeating_staging(self):
        from route_stable_value import staging_query, load_inputs
        query = staging_query('default', ['A'])
        self.assertIn("WHERE ack_id IN ('A')", query)
        self.assertNotIn('regexp_like', query)
        client = Mock()
        client.args.database = 'default'
        client.schema.return_value = {'ack_id': 'string'}
        client.table.side_effect = lambda t: 'default.' + t
        client.run.side_effect = [[row()], []]
        load_inputs(client, ['A'])
        history_sql = client.run.call_args.args[0]
        self.assertIn('UNNEST(ARRAY[', history_sql)
        self.assertNotIn('plan_holdings_staging', history_sql)
        self.assertIn('plan_alternatives_history', history_sql)

    def decisions(self, rows, history=()):
        return build_decisions(rows, history, NoManager())

    def test_reviewed_product_rule_promotes_evidence_hold_but_keeps_duplicate_guards(self):
        from src.holding_reconciliation import approval_key
        r = row(raw_entity_name='Prudential Guaranteed Income', asset_type='stable value fund')
        self.assertEqual(self.decisions([r])[0]['routing_reason'], 'stable_value_product_evidence_required')
        rules = {approval_key(r): dict(rule_id='reviewed-pru-gi', destination_table='plan_insurance_history')}
        d = build_decisions([r], [], NoManager(), rules)[0]
        self.assertEqual((d['routing_status'], d['destination_table'], d['routing_reason']),
                         ('READY', 'plan_insurance_history', 'approved_product_rule'))
        same_amount = row(raw_entity_name='Some Other Contract', source_table='plan_insurance_history')
        d = build_decisions([r], [same_amount], NoManager(), rules)[0]
        self.assertEqual(d['routing_status'], 'REVIEW')

    def test_large_scope_splits_athena_array_constructors(self):
        from route_stable_value import load_inputs
        client = Mock()
        client.args.database = 'default'
        client.schema.return_value = {'ack_id': 'string'}
        client.table.side_effect = lambda t: 'default.' + t
        client.run.side_effect = [[row(ack_id=str(i)) for i in range(255)], []]
        load_inputs(client, [str(i) for i in range(255)])
        query = client.run.call_args.args[0]
        self.assertIn('UNNEST(concat(ARRAY[', query)
        self.assertEqual(query.count('ARRAY['), 2)

    def test_ei_series_vi_has_verified_insurance_structure(self):
        d = self.decisions([row(raw_entity_name='EI Fixed Account - Series Class VI',
                               asset_type='stable value fund')])[0]
        self.assertEqual(d['routing_status'], 'READY')
        self.assertEqual(d['destination_table'], 'plan_insurance_history')
        self.assertEqual(d['account_structure'], 'general_account')
        self.assertEqual(d['contract_type'], 'group_annuity')

    def test_unverified_fixed_account_requires_stable_value_evidence(self):
        d = self.decisions([row(raw_entity_name='Other Fixed Account')])[0]
        self.assertEqual(d['routing_status'], 'REVIEW')

    def test_guaranteed_contract_and_abbreviation_are_selected(self):
        for name in ['Guaranteed investment contract', 'Example Stbl Val Fund']:
            self.assertEqual(self.decisions([row(raw_entity_name=name)])[0]['routing_status'], 'READY')

    def test_ordinary_staging_holding_supplies_amount_collision_context(self):
        ds = self.decisions([row(), row(raw_entity_name='Other Bond Fund')])
        self.assertEqual(len(ds), 1)
        self.assertEqual(ds[0]['routing_status'], 'REVIEW')
        self.assertEqual(ds[0]['routing_reason'], 'same_staging_amount_different_products')

    def test_identical_extractions_produce_one_write_with_multiplicity(self):
        r = row(staging_row_count=3)
        ds = self.decisions([r, r])
        self.assertEqual(len(ds), 1)
        self.assertEqual(ds[0]['routing_status'], 'READY')
        self.assertEqual(ds[0]['staging_row_count'], 6)

    def test_conflicting_source_labels_prevent_write(self):
        ds = self.decisions([row(), row(asset_type='group annuity contract')])
        self.assertEqual(ds[0]['routing_status'], 'REVIEW')

    def test_existing_history_and_provider_conflicts_checked(self):
        h = row(source_table='plan_alternatives_history')
        self.assertEqual(self.decisions([row()], [h])[0]['routing_status'], 'ALREADY_EXISTS')
        h['raw_sponsor_name'] = 'Other Provider'
        self.assertEqual(self.decisions([row()], [h])[0]['routing_status'], 'REVIEW')

    def test_conflicting_cit_label_does_not_override_known_insurance(self):
        d = self.decisions([row(raw_entity_name='EI Fixed Account Series Class VI',
                               asset_type='collective investment trust')])[0]
        self.assertEqual(d['routing_status'], 'REVIEW')

    def test_non_stable_value_sma_not_promoted_by_this_router(self):
        self.assertEqual(self.decisions([row(raw_entity_name='Brand Separately Managed Account')]), [])


class ProductAliasTests(unittest.TestCase):
    # (2026-10-17) Stable value products named without the phrase "stable value".
    def status(self, name, **kw):
        return build_decisions([row(raw_entity_name=name, **kw)], [], NoManager())

    def test_named_products_are_ready(self):
        for name in ['Vanguard Retirement Savings Trust III', 'Fidelity Managed Income Portfolio II Class 4',
                     'MIP CL 2', 'FID MIP II, CL', 'Empower Guaranteed Income Fund', 'Key Guaranteed Portfolio Fund',
                     'Principal Fixed Income Guaranteed Option', 'Guaranteed Interest Account 680',
                     'VALIC FixedInterest Option', 'Federated Hermes Capital Preservation Fund R6P',
                     'Aramark Interest Income Fund', 'Invesco Stable Asset Fund', 'Galliard Stable Return Fund C',
                     # (2026-10-26) issuer-named synthetic GIC / wrap contract lines
                     'Voya wrap contract', 'SYNTHETIC GIC PRUDENTIAL INS CO CONTRACT']:
            ds = self.status(name)
            self.assertEqual(ds[0]['routing_status'], 'READY', name)
            self.assertIn('sv_product_alias', ds[0]['routing_reason'], name)

    def test_lookalikes_are_not_promoted(self):
        for name in ['PIMCO Stable Income Fund', 'Galliard Short Core Fund F', 'MM FIXED INTEREST',
                     'Synthetic GIC', 'Wrap Contracts', 'Traditional Non Benefit Responsive',
                     'Capital Preservation Equity Fund']:
            ds = self.status(name)
            self.assertTrue(not ds or ds[0]['routing_status'] != 'READY', name)

    def test_alias_still_guarded_by_existing_history(self):
        h = row(raw_entity_name='Vanguard Retirement Savings Trust III', source_table='plan_cit_history')
        ds = build_decisions([row(raw_entity_name='Vanguard Retirement Savings Trust III')], [h], NoManager())
        self.assertEqual(ds[0]['routing_status'], 'ALREADY_EXISTS')
