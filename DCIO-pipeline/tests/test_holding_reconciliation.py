import unittest
from unittest.mock import Mock
from src.holding_reconciliation import classify, decide, history_index, identity, product_name


def row(**updates):
    data = dict(ack_id='A', raw_entity_name='Example Stable Value Fund',
                raw_sponsor_name='Provider', plan_investment_amt='100.00',
                asset_type='', asset_class='PENDING_AI', asset_sub_class='', validation_status='FAIL')
    data.update(updates)
    return data


class ReconciliationTests(unittest.TestCase):
    def decision(self, source=None, histories=(), duplicate='', conflict=False):
        source = source or row()
        return decide(source, history_index(histories),
                      {product_name(source['raw_entity_name']): {identity(source)}},
                      duplicate, label_conflict=conflict)

    def test_generic_stable_without_vehicle_or_pass_is_captured(self):
        d = self.decision()
        self.assertEqual(d['outcome'], 'INSERT')
        self.assertEqual(d['destination_table'], 'plan_stable_value_unresolved_history')
        self.assertEqual(d['validation_status'], 'FAIL')

    def test_abbreviation_match_in_alternatives(self):
        h = row(raw_entity_name='Example Stab Val Fund', source_table='plan_alternatives_history')
        self.assertEqual(self.decision(histories=[h])['outcome'], 'EXISTING')

    def test_provider_difference_not_silently_collapsed(self):
        h = row(raw_sponsor_name='Other Provider', source_table='plan_cit_history')
        self.assertEqual(self.decision(histories=[h])['reason'], 'existing_name_provider_or_amount_conflict')

    def test_amount_difference_is_conflict(self):
        h = row(plan_investment_amt='200.00', source_table='plan_cit_history')
        self.assertEqual(self.decision(histories=[h])['outcome'], 'UNRESOLVED')

    def test_same_amount_unrelated_name_is_never_existing(self):
        h = row(raw_entity_name='Example Equity Fund', source_table='plan_cit_history')
        self.assertEqual(self.decision(histories=[h])['reason'], 'same_ack_amount_different_product')

    def test_existing_name_variant_with_changed_amount_is_not_new_insert(self):
        r = row(raw_entity_name='Principal Life Insurance Company Principal Stable Value Z Fund')
        h = row(raw_entity_name='PRINCIPAL STABLE VALUE Z FUND', plan_investment_amt='90.00', source_table='plan_cit_history')
        self.assertEqual(self.decision(r, [h])['reason'], 'possible_history_name_variant')

    def test_different_share_class_is_never_exact(self):
        a = row(raw_entity_name='Example Stable Value Class A')
        h = row(raw_entity_name='Example Stable Value Class B', source_table='plan_cit_history')
        self.assertEqual(self.decision(a, [h])['outcome'], 'UNRESOLVED')

    def test_duplicate_history_is_not_unique_match(self):
        h = row(source_table='plan_cit_history')
        self.assertEqual(self.decision(histories=[h, h])['reason'], 'multiple_existing_records')

    def test_only_one_insert_proposal_for_repeat(self):
        self.assertEqual(self.decision(duplicate='ID')['reason'], 'duplicate_staging_proposal')

    def test_conflicting_repeat_labels_are_not_proposed(self):
        self.assertEqual(self.decision(conflict=True)['reason'], 'duplicate_identity_conflicting_source_labels')

    def test_stable_type_on_small_cap_is_conflict(self):
        self.assertEqual(classify(row(raw_entity_name='Small Cap Index Fund', asset_type='Stable Value Fund'))[0], 'UNRESOLVED')

    def test_type_only_generic_brand_requires_evidence(self):
        self.assertEqual(classify(row(raw_entity_name='Fidelity', asset_type='Stable Value Fund'))[0], 'UNRESOLVED')

    def test_fixed_account_corroborated_by_stable_type(self):
        d = self.decision(row(raw_entity_name='Voya Fixed Account 4450', asset_type='Stable Value Fund'))
        self.assertEqual(d['outcome'], 'INSERT')
        self.assertEqual(d['destination_table'], 'plan_stable_value_unresolved_history')

    def test_insurance_separate_does_not_route_to_sma(self):
        d = self.decision(row(raw_entity_name='Insurance Separate Account Stable Value'))
        self.assertEqual(d['destination_table'], 'plan_insurance_history')

    def test_generic_annuity_wrapper_does_not_establish_stable_value(self):
        d = self.decision(row(raw_entity_name='Single Group Annuity Contract', asset_type='group annuity contract'))
        self.assertEqual(d['outcome'], 'UNRESOLVED')
        self.assertEqual(d['reason'], 'insurance_wrapper_without_stable_value_mandate')

    def test_fixed_account_annuity_has_corroborating_mandate(self):
        d = self.decision(row(raw_entity_name='EI Fixed Account Series Class I Group Annuity Contract', asset_type='group annuity contract'))
        self.assertEqual(d['outcome'], 'INSERT')
        self.assertEqual(d['destination_table'], 'plan_insurance_history')

    def test_generic_separate_requires_menu_evidence(self):
        self.assertEqual(classify(row(raw_entity_name='Equity', asset_type='Separate Account'))[0], 'UNRESOLVED')

    def test_plural_pooled_separate_accounts_not_missed(self):
        d = classify(row(raw_entity_name='American Funds Growth Funds', asset_type='Pooled Separate Accounts'))
        self.assertEqual(d[2], 'account_structure_or_menu_evidence_required')

    def test_account_evidence_in_subclass_is_not_missed(self):
        d = classify(row(raw_entity_name='Fund', asset_sub_class='Separate Account'))
        self.assertEqual(d[0], 'UNRESOLVED')

    def test_sma_underlying_security_excluded(self):
        d = classify(row(raw_entity_name='SEPARATELY MANAGED ACCOUNT - COMMON STOCK: Las Vegas Sands', asset_type='Separately Managed Account'))
        self.assertEqual(d[0], 'OUTSIDE_SCOPE')

    def test_explicit_managed_menu_proposal(self):
        d = self.decision(row(raw_entity_name='International Equity Separately Managed Account', asset_type='Separately Managed Account'))
        self.assertEqual(d['destination_table'], 'plan_separately_managed_history')

    def test_reserve_trust_name_does_not_prove_plan_specific_sma(self):
        d = self.decision(row(raw_entity_name='BRANDES SEPARATELY MANAGED ACCOUNT RESERVE TRUST', asset_type='separately managed account'))
        self.assertEqual(d['outcome'], 'UNRESOLVED')
        self.assertEqual(d['reason'], 'pooled_vehicle_in_managed_account_name')

    def test_target_date_stable_suffix_not_blindly_captured(self):
        self.assertEqual(classify(row(raw_entity_name='Yourpath 2045 Stable Value Fund'))[0], 'UNRESOLVED')

    def test_sv_names_found_in_mf_on_2026_10_26_are_confirmed(self):
        # Typed mutual fund by the old extraction, so the name alone has to carry it.
        for name in ('MetlifeGAC StableValueFund', 'Principal Life Fixed Income Guaranteed Acc',
                     'Principal Life Fixed Inc Guaranteed Option', 'Principal Life Guaranteed Option',
                     'SYNTHETIC GIC PRUDENTIAL INS CO OF AMERICA CONTRACT',
                     'Prudential Ins. Co. of America Wrapper Contract', 'Voya Financial Wrap Contract'):
            with self.subTest(name=name):
                d = classify(row(raw_entity_name=name, asset_type='mutual fund', asset_class=''))
                self.assertEqual(d[3], 'confirmed_stable_value')

    def test_similar_non_sv_names_not_captured(self):
        for name in ('Lincoln Guaranteed Income Builder', 'Wrapped holdings', 'Vanguard Value Index'):
            with self.subTest(name=name):
                self.assertEqual(classify(row(raw_entity_name=name, asset_type='mutual fund', asset_class=''))[0],
                                 'OUTSIDE_SCOPE')

    def test_pointer_excluded(self):
        self.assertEqual(classify(row(raw_entity_name='Stable Value Fund See Appendix'))[0], 'OUTSIDE_SCOPE')

    def test_years_preserved(self):
        self.assertNotEqual(product_name('Fund 2045'), product_name('Fund 2050'))

    def test_cit_evidence_routes_to_cit(self):
        d = self.decision(row(asset_type='Collective Investment Trust'))
        self.assertEqual(d['destination_table'], 'plan_cit_history')

    def test_same_amount_in_staging_blocks_multiple_inserts(self):
        r = row()
        d = decide(r, history_index([]), {product_name(r['raw_entity_name']): {identity(r)}},
                   staging_amount_names={__import__('decimal').Decimal('100.00'): {'fund a', 'fund b'}})
        self.assertEqual(d['reason'], 'same_staging_amount_different_products')

    def test_reviewed_product_rule_reused_without_ignoring_duplicate_guard(self):
        r = row(raw_entity_name='Example Fixed Account')
        key = (product_name(r['raw_entity_name']), 'provider', '', 'pending_ai', '')
        rules = {key: dict(rule_id='reviewed-fixed-account', destination_table='plan_insurance_history')}
        d = decide(r, history_index([]), {product_name(r['raw_entity_name']): {identity(r)}}, approved_rules=rules)
        self.assertEqual(d['outcome'], 'INSERT')
        h = row(raw_entity_name=r['raw_entity_name'], source_table='plan_cit_history')
        d = decide(r, history_index([h]), {}, approved_rules=rules)
        self.assertEqual(d['outcome'], 'EXISTING')

    def test_reviewed_rule_cannot_override_equity_conflict(self):
        r = row(raw_entity_name='Small Cap Index Fund', asset_type='Stable Value Fund')
        key = (product_name(r['raw_entity_name']), 'provider', 'stable value fund', 'pending_ai', '')
        d = decide(r, history_index([]), {}, approved_rules={key: dict(rule_id='unsafe', destination_table='plan_insurance_history')})
        self.assertEqual(d['outcome'], 'UNRESOLVED')

    def test_reviewed_named_insurance_product_supplies_mandate_evidence(self):
        r = row(raw_entity_name='Example Named Income Product Group Annuity Contract', asset_type='group annuity contract')
        key = (product_name(r['raw_entity_name']), 'provider', 'group annuity contract', 'pending_ai', '')
        d = decide(r, history_index([]), {}, approved_rules={key: dict(rule_id='documented-product', destination_table='plan_insurance_history')})
        self.assertEqual(d['outcome'], 'INSERT')
        self.assertEqual(d['reason'], 'approved_product_rule')

    def test_router_checks_actual_plural_alternatives_table(self):
        from route_stable_value import load_inputs
        athena = Mock()
        athena.args.database = 'default'
        athena.schema.side_effect = lambda table: {'ack_id': 'string'} if table != 'plan_alternative_history' else None
        athena.table.side_effect = lambda table: 'default.' + table
        athena.run.side_effect = [[row()], []]
        load_inputs(athena, ['A'])
        query = athena.run.call_args.args[0]
        self.assertIn('default.plan_alternatives_history', query)
        self.assertNotIn('default.plan_alternative_history ', query)

    def test_rules_require_evidence_and_reject_duplicate_scopes(self):
        import json
        import tempfile
        from pathlib import Path
        from reconcile_staging_holdings import approved_rules
        entry = dict(rule_id='r', raw_entity_name='Fixed Account', raw_sponsor_name='',
                     asset_type='', asset_class='', asset_sub_class='',
                     destination_table='plan_insurance_history', evidence='Reviewed contract', approved_by='Reviewer')
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'rules.json'
            path.write_text(json.dumps([dict(entry, evidence='')]))
            with self.assertRaises(ValueError):
                approved_rules(path)
            path.write_text(json.dumps([entry, entry]))
            with self.assertRaises(ValueError):
                approved_rules(path)

    def test_reused_working_database_is_checked_against_source_content(self):
        import csv
        import json
        import sqlite3
        import tempfile
        from pathlib import Path
        from reconcile_staging_holdings import import_snapshot
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            db = sqlite3.connect(':memory:')
            for table, name in [('stage', 'staging.source.csv'), ('history', 'history.source.csv')]:
                db.execute(f'CREATE TABLE {table}(ack TEXT,payload TEXT)')
                r = row()
                with (output / name).open('w', newline='') as f:
                    w = csv.DictWriter(f, r.keys())
                    w.writeheader()
                    w.writerow(r)
                db.execute(f'INSERT INTO {table} VALUES (?,?)', (r['ack_id'], json.dumps(r, ensure_ascii=False)))
            self.assertEqual(import_snapshot(db, output, True), {'stage': 1, 'history': 1})
            db.execute("UPDATE history SET ack='OTHER'")
            with self.assertRaises(RuntimeError):
                import_snapshot(db, output, True)
            db.close()


if __name__ == '__main__':
    unittest.main()
