# Always-enabled stable value pipeline integration

The main EC2 pipeline now invokes stable value routing after the existing staging,
MF enrichment, and manual-classification steps. There is no enable/disable flag.
The original extractor, staging writer, and `run_post_extract_validation()` remain
unchanged. Staging validation status still represents MF-total validation and is
not used as a stable value acceptance gate.

## Changes

- `src/run_pipeline.py`: collect current extracted filing IDs, record the existing
  validation invocation's time window, invoke the mandatory downstream helper,
  and fail the pipeline with exit code 1 when downstream processing is incomplete.
- `src/stable_value_pipeline_hook.py`: confirm current staging scope through the
  existing validation summary, run the router with `--apply`, pass current batch
  ack_ids/database/workgroup/region/output settings, and reconcile its report.
- `tests/test_stable_value_pipeline_hook.py`: batch scope, empty/filtered batches,
  settings, count reconciliation, partial failure, missing staging evidence,
  upstream failure, and mandatory invocation order.
- The previously built router and pure manager helpers are now installed in the
  main EC2 project so the pipeline can invoke them directly.

Rows entirely filtered before staging are excluded using the existing summary's
ack_ids and timestamp window. PASS, FAIL, and SKIP statuses are all eligible.
Empty batches do not invoke the standalone router without ack_ids. Scope/count
disagreement is reported as INCOMPLETE rather than routing old historical rows.

The router compares existing holdings within each ack_id across CIT, MF, and the
other history destinations. Confirmed stable value without reliable vehicle
information goes to `plan_stable_value_unresolved_history`. Ambiguous duplicates
and invalid amounts remain in review. Classification and manager-cleaning rules
are unchanged from the completed backfill.

## Reports and failure behavior

`<OUTPUT_DIR>/stable_value_routing_pipeline.json` contains status, current scope,
inserted/already-existing/review counts, router report location, and errors.
Individual router reports use a fresh subdirectory on each invocation and are
published to `s3://retirementinsights-reference/stable_value_routing/runs/<run_id>/`.
Counts describe grouped candidate holdings, not raw staging-row counts.

A failed router, absent apply summary, unreconciled counts, missing staging, or
existing downstream failure produces an INCOMPLETE report and pipeline exit 1.
Verified partial counts are retained when available. Review decisions are normal
routing outcomes and do not alone make the step incomplete. Reruns compare
destination holdings and recognize completed writes.

Production configuration already enables validation and holdings staging. These
are prerequisites for a nonempty batch; disabling either cannot silently bypass
the mandatory step. Optional `STABLE_VALUE_WAREHOUSE` and
`STABLE_VALUE_REPORT_BUCKET` configure storage only.

## Deployment evidence

- EC2: `i-0eaee37f64dfe7195`, region `us-east-1`.
- Main project: `/home/ec2-user/DCIO-pipeline/DCIO-pipeline`.
- Python: `/home/ec2-user/DCIO-pipeline/venv/bin/python`.
- Successful deployment SSM command: `44d4f0ed-e0fd-4456-81a8-301b4e37b5e1`.
- Pipeline baseline SHA256:
  `f466a7889c243eb1925f06ce1989d953d9496ed6ab209813a9bd77af15bdad52`.
- Deployed pipeline SHA256:
  `05d00c602612503d812941673c69f9f9bd618c757c32558961c24c5c5014cf6d`.
- Original backup:
  `stable_value_integration_backups/<baseline-sha>/run_pipeline.py` in the EC2 project.
- S3 baseline: `s3://retirementinsights-reference/stable_value_routing/integration/20261010/baseline/run_pipeline.py`.
- Bundle: `s3://retirementinsights-reference/stable_value_routing/code/stable_value_integration_20261010.zip`.
- Existing validation module SHA256 remained unchanged:
  `bbec795ebf88d9611429db0c52dafc75148237f90b66191065b6b94da98d9dce`.

Deployment checked the exact inspected live baseline, refused concurrent pipeline
processes, preserved CRLF, and atomically replaced only the pipeline entry point.
Unrelated local changes and the old isolated backfill runtime were preserved.
All 37 stable value tests passed locally and on EC2.

The full pipeline test uses a synthetic PDF and an isolated database, warehouse,
inputs, and outputs. No production input folder or production holdings are used.
The EC2 role lacks CreateDatabase; the configured operator credentials provision
only the test namespace before the EC2 test runs.

The initial fixture used $100,001, $200,002, and $300,003. The unchanged junk filter
correctly identified the third amount as the sum of the other two and removed it
as a subtotal. The final fixture uses $400,004 for that holding. The regex remains
scoped to existing staging raw_entity_name; it cannot recover rows dropped before
staging or names that no longer contain the stable value phrase.

## End-to-end result

The actual deployed `python -m src.run_pipeline` completed twice with the synthetic
PDF in database `sv_pipeline_e2e_20261010_10a5be03`. First run: 3 inserted,
0 already-existing, 0 review. Second run: 0 inserted, 3 already-existing, 0 review.
Both pipeline processes exited 0. MF-total validation was SKIP on both runs;
staging status was UNVALIDATED, proving the downstream router does not require PASS.

Independent view queries verified one CIT holding at $100,001, one insurance
holding at $200,002, and one unresolved holding at $400,004, totaling $700,007.
No production histories were written by these tests. Test namespaces are retained
for inspection and are explicitly synthetic.

- Successful test SSM command: `d4071cd9-b31a-42fb-b1c2-9fe22a13371f`.
- Initial router run: `20261010T235703Z-42868650`.
- Rerun: `20261010T235810Z-91007de9`.
- Test workspace: `/home/ec2-user/DCIO-pipeline/DCIO-pipeline/stable_value_pipeline_e2e_10a5be03`.
- Verification: `s3://retirementinsights-reference/stable_value_routing/integration/20261010/10a5be03/e2e_verification.json`.
- Local evidence: `outputs/stable_value_router/integration/e2e_verification.json`
  and `e2e_ssm_result.json` (contains both pipeline reports).


## Expanded selection deployed October 11, 2026

Router v2 is installed in the same production checkout. The existing mandatory
pipeline hook invokes the updated router automatically; no second orchestration
step or feature flag is needed. Extraction and MF-total validation are unchanged.

`route_stable_value.py` selects candidate filings using stable-value labels,
fixed accounts, guaranteed contracts, and bounded name abbreviations. It fetches
all staging holdings in those filings to preserve same-amount and repeated-name
context, and compares all six history destinations (including
`plan_alternatives_history`). `src/stable_value_expanded.py` adapts the reviewed
`src/holding_reconciliation.py` rules to the existing locked, idempotent writer.
Only stable-value candidates are promoted by this adapter; non-stable-value SMAs
and alternative keyword candidates retain their separate processes.

`src/stable_value_routing.py` identifies the verified EI Fixed Account Series
Class VI product as an insurance general-account group annuity, based on the
Empower factsheet linked in its source comment. Contradictory vehicle evidence
is retained as unresolved or review, rather than forced into insurance.

Historical promotion is bounded by `--approved-candidates <proposal.csv>`.
The manifest must contain unique identities recomputed from ack_id, normalized
name, raw provider, and amount. Live staging and history are read again under the
existing global apply lock. Missing/changed candidates block promotion; duplicate
or conflicting holdings remain review. Fresh decisions are checked immediately
before writing. Every write is verified for unique identity and exact amount.

Deployment command: `416d4477-f14d-4a0d-85e7-69f318dcb347` (Success).
Installed files and hashes are in
`outputs/stable_value_promotion_20261011/deployment_manifest.json`.
Backup: `stable_value_integration_backups/expanded_router_20261011_v2` on EC2.
The production pipeline entry point and mandatory hook were checked against their
existing hashes and did not need replacement. Nine expanded-routing tests and
11 integration tests passed on EC2; 79 related tests passed locally.


The scoped-query optimization was installed by successful SSM command
`4d225d03-9fbc-47cf-9da3-d437799cc047`. It reads the current batch's complete
staging context directly, then supplies distinct filing IDs once to the six-table
history query. Large scopes use concatenated arrays of at most 200 IDs to comply
with Athena's array argument limit. The final router SHA256 is
`56e21a0304a602fb5180451cc876b1fd56348dc882a8b96f9ba9812fa68085ad`;
see `scoped_deployment_manifest.json` beside the initial manifest.
Eleven expanded-router tests passed on EC2 after this optimization, bringing
related local coverage to 81 tests. An earlier preview query was cancelled when
its repeated staging work was identified; a preview already running older code
subsequently hit the array argument limit. Both were read-only and inserted zero
records. The final apply uses the tested, deployed scoped-query implementation.
