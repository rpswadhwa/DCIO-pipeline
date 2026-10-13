import unittest
from src.stable_value_routing import build_decisions, choose_vehicle, amount
from src.stable_value_manager import ManagerResolver


class NoManager:
    def resolve(self, entity, sponsor):
        return dict(normalized_manager=None, manager_match_status='UNMATCHED',
                    manager_match_method=None, manager_match_confidence=None, needs_cleaning_flag=True)


def holding(**kwargs):
    return dict(ack_id='A', raw_entity_name='Example Stable Value Fund',
                raw_sponsor_name='Example', plan_investment_amt='123.45',
                asset_type='', asset_class='PENDING_AI', asset_sub_class='PENDING_AI',
                validation_status='FAIL', staging_row_count='1', **kwargs)


class RoutingTests(unittest.TestCase):
    def row(self, **changes):
        row = holding()
        row.update(changes)
        return row

    def decide(self, row=None, history=None):
        return build_decisions([row or self.row()], history or [], NoManager())[0]

    def test_missing_vehicle_and_failed_mf_total_are_captured(self):
        d = self.decide()
        self.assertEqual(d['routing_status'], 'READY')
        self.assertEqual(d['destination_table'], 'plan_stable_value_unresolved_history')
        self.assertEqual(d['validation_status'], 'FAIL')

    def test_insurance_separate_is_not_managed_account(self):
        row = self.row(raw_entity_name='Insurance Company Separate Account Stable Value', asset_type='separately managed account')
        self.assertEqual(choose_vehicle(row)[0], 'plan_insurance_history')
        self.assertEqual(choose_vehicle(row)[2], 'insurance_separate_account')

    def test_unsubstantiated_legacy_sma_label_is_unresolved(self):
        self.assertEqual(choose_vehicle(self.row(asset_type='separately managed account'))[0], 'plan_stable_value_unresolved_history')

    def test_explicit_sma(self):
        self.assertEqual(choose_vehicle(self.row(raw_entity_name='Stable Value Separately Managed Account'))[0], 'plan_separately_managed_history')

    def test_known_insurance_product_conflicts_with_cit_label(self):
        row = self.row(raw_entity_name='Transamerica Stable Value Core Account', asset_type='common/collective trust fund')
        self.assertEqual(choose_vehicle(row)[0], 'plan_stable_value_unresolved_history')
        row['asset_type'] = ''
        self.assertEqual(choose_vehicle(row)[:4], ('plan_insurance_history', 'insurance_contract', 'general_account', 'group_annuity'))

    def test_conflict_is_captured_in_unresolved(self):
        d = self.decide(self.row(asset_type='group annuity contract', raw_entity_name='Stable Value Collective Investment Trust'))
        self.assertEqual(d['destination_table'], 'plan_stable_value_unresolved_history')
        self.assertEqual(d['routing_status'], 'READY')

    def test_target_date_name_with_trailing_stable_value_label_needs_review(self):
        d = self.decide(self.row(raw_entity_name='Yourpath 2010 Moderate, stable value fund, fully benefit-responsive', asset_type='group annuity contract'))
        self.assertEqual(d['routing_status'], 'REVIEW')

    def test_existing_cit_ignores_case_and_internal_whitespace(self):
        h = self.row(raw_entity_name='EXAMPLE  STABLE VALUE FUND', source_table='plan_cit_history')
        self.assertEqual(self.decide(history=[h])['routing_status'], 'ALREADY_EXISTS')

    def test_existing_amount_conflict_is_not_inserted(self):
        h = self.row(plan_investment_amt='456', source_table='plan_cit_history')
        self.assertEqual(self.decide(history=[h])['routing_status'], 'REVIEW')

    def test_putnam_shortened_name_is_possible_duplicate(self):
        row = self.row(raw_entity_name='Putnam Stable Value Fund')
        h = self.row(raw_entity_name='PUTNAM STABLE VALUE', source_table='plan_cit_history')
        self.assertEqual(self.decide(row, [h])['routing_status'], 'REVIEW')

    def test_same_manager_different_strategy_is_not_duplicate(self):
        row = self.row(raw_entity_name='Putnam Stable Value Fund')
        h = self.row(raw_entity_name='Putnam Growth Fund', plan_investment_amt='999.99', source_table='plan_mf_history_v3')
        self.assertEqual(self.decide(row, [h])['routing_status'], 'READY')

    def test_duplicate_staging_pair_is_not_collapsed(self):
        self.assertEqual(self.decide(self.row(staging_row_count='1059'))['routing_status'], 'REVIEW')

    def test_same_amount_staging_variants_are_not_both_inserted(self):
        rows = [self.row(raw_entity_name='TIAA Stable Value'), self.row(raw_entity_name='TIAA Stable Value General Account')]
        decisions = build_decisions(rows, [], NoManager())
        self.assertTrue(all(d['routing_status'] == 'REVIEW' for d in decisions))
        self.assertTrue(all(d['routing_reason'] == 'same_staging_ack_amount_different_names' for d in decisions))

    def test_same_name_across_filings_is_not_duplicate(self):
        h = self.row(ack_id='B', source_table='plan_cit_history')
        self.assertEqual(self.decide(history=[h])['routing_status'], 'READY')

    def test_repeated_history_match_is_review(self):
        h = self.row(source_table='plan_cit_history')
        self.assertEqual(self.decide(history=[h, h])['routing_status'], 'REVIEW')

    def test_decimal_is_exact(self):
        self.assertEqual(str(amount('1234567890.12')), '1234567890.12')
        self.assertIsNone(amount('NaN'))

    def test_new_table_rerun(self):
        d = self.decide()
        h = self.row(source_table='plan_stable_value_unresolved_history', candidate_id=d['candidate_id'])
        self.assertEqual(self.decide(history=[h])['routing_status'], 'ALREADY_EXISTS')


class ManagerTests(unittest.TestCase):
    def setUp(self):
        self.resolver = ManagerResolver([
            {'canonical': 'Fidelity Investments', 'tokens': ['FIDELITY'], 'aliases': []},
            {'canonical': 'Putnam Investments', 'tokens': ['PUTNAM'], 'aliases': []},
            {'canonical': 'Principal', 'tokens': ['PRINCIPAL'], 'aliases': []},
        ])

    def test_entity_wins_over_platform(self):
        result = self.resolver.resolve('Putnam Stable Value Fund', 'Fidelity Trust Company')
        self.assertEqual(result['normalized_manager'], 'Putnam Investments')

    def test_unknown_stays_unmatched(self):
        self.assertTrue(self.resolver.resolve('Unknown Stable Value Fund', '')['needs_cleaning_flag'])

    def test_insurer_secondary_does_not_invent_manager(self):
        result = self.resolver.resolve('Generic Stable Value Fund', 'Principal Life Insurance Company')
        self.assertIsNone(result['normalized_manager'])


if __name__ == '__main__':
    unittest.main()
