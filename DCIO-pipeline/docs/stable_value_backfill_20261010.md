# Stable value backfill: October 10, 2026

Implemented, deployed, and successfully run in `default` on AWS `us-east-1`.
The independent verification completed at 17:51:30 UTC.

| Destination | New holdings, including pilot |
|---|---:|
| plan_cit_history | 515 |
| plan_insurance_history | 735 |
| plan_stable_value_unresolved_history | 2,589 |
| plan_separately_managed_history | 0 |
| Total | 3,839 |

Unknown vehicle types and conflicting vehicle labels are captured in the
unresolved history table. The managed-account table exists; no current candidate
had sufficiently explicit plan-specific managed-account evidence.

The full run processed 14,685 grouped candidates representing 14,687 source rows.
It inserted 3,836 holdings after the three-row pilot, recognized 1,350 existing
holdings, and recorded 9,499 review decisions representing 9,501 source rows.
All source rows reconcile to the audit table and saved CSV. Review decisions are
possible duplicates, repeated pairs, missing/negative amounts, or conflicting
strategy evidence; missing vehicle type alone does not cause review or exclusion.

## Validation

- All 26 tests passed locally and on EC2 with a clean exit.
- The pilot inserted one CIT, one insurance, and one unresolved holding.
- Pilot reruns inserted zero additional holdings.
- Every accepted destination ID and exact decimal amount was verified by the job.
- Independent verification found zero duplicate router filing/name pairs, zero
  duplicate positive filing/amount pairs, zero exact filing/name overlaps with
  legacy CIT/MF/alternative holdings, and zero missing/negative accepted amounts.
- `v_plan_stable_value_holdings` includes all 3,839 router-owned holdings.
- Audit reconciliation accounts for all 14,687 staging source rows.

The reported amounts are historical filing holdings, not current AUM. Manager
cleaning resolved 2,731 canonical managers; 1,108 holdings retain a manager-review
flag without preventing capture.

## Exact code scope

No existing extraction, staging writer, MF writer, validation function, or live
CIT cleaning script was modified. `write_iceberg_via_athena()` and
`run_post_extract_validation()` remain unchanged. MF-total validation status is
preserved and is not used as a stable value acceptance gate.

New runtime files:

- `route_stable_value.py`: independent Athena reads, table creation, insert-only
  MERGEs, CIT metadata population for router-owned rows, audit, verification,
  locking, reports, and unified view creation.
- `src/stable_value_routing.py`: duplicate checks and destination selection.
- `src/stable_value_manager.py`: existing canonical manager rules and safeguards.
- `src/stable_value_legacy_manager_rules.py`: pure-function snapshot of the active
  CIT cleaner; no import or execution of its live job or secret-loading code.

Tests, generated SQL, documentation, packaging, snapshot, status, and independent
verification helpers were added under `tests/`, `sql/stable_value/`, `docs/`, and
`tools/`. See [technical design and operation](stable_value_router.md).

The initial backfill deployment was standalone. A subsequent, always-enabled
pipeline integration now invokes routing automatically; see
[integration deployment](stable_value_pipeline_integration_20261010.md).
Staging rows remain available, and existing history holdings were not deleted or
overwritten.

## Reproducibility and reports

- EC2 instance: `i-0eaee37f64dfe7195`.
- Runtime directory:
  `/home/ec2-user/DCIO-pipeline/DCIO-pipeline/stable_value_router_runtime_20261010_01`.
- Python: `/home/ec2-user/DCIO-pipeline/venv/bin/python`.
- Successful full run: `20261010T174121Z-8e66fe77`.
- Full SSM command: `a7d5128e-7719-46d6-bc2b-70b81ab7560c`.
- Pilot run: `20261010T172900Z-e96f82c4`.
- Final pilot rerun: `20261010T174013Z-418e0a5d`.
- Code bundle SHA256:
  `646cc78a92f6bf850878b06101a30785ccc9ca0f356230d6332ff5717b851f3e`.
- Canonical manager dictionary SHA256:
  `aa0110b44a32eef53356bcebe9cef7a7e0792f43f913a010807128bbb516c600`.
- S3 report prefix:
  `s3://retirementinsights-reference/stable_value_routing/runs/20261010T174121Z-8e66fe77/`.
- Local decisions and audit verification:
  `outputs/stable_value_router/20261010T174121Z-8e66fe77/`.
- Local cross-table verification:
  `outputs/stable_value_router/backfill_verification.json`.

The independent read-only queries are recorded in the verification JSON files.
Full routing query IDs are recorded in the run summary.
