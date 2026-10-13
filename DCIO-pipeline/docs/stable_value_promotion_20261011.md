# Stable-value promotion and production router update — October 11, 2026

The approved full-staging proposal set contained 3,222 holding identities in
2,880 filings. All 3,222 were inserted into production after two live staging
and history checks under the existing global router lock. No proposals were
already existing or held for review during this apply.

| Destination | New holdings |
| --- | ---: |
| plan_insurance_history | 583 |
| plan_cit_history | 56 |
| plan_stable_value_unresolved_history | 2,583 |
| plan_separately_managed_history | 0 |
| Total | 3,222 |

The verified EI Fixed Account Series Class VI rule moved 26 proposed holdings
from unknown vehicle to insurance compared with the original preview. Other
unknown-vehicle stable-value holdings remain captured in the unresolved-history
table. This is an unresolved legal structure, not an unmatched staging holding.

Run: `20261011T194752Z-c18e4a48`, routing version `stable_value_router_v2`.
The apply exited 0, verified each inserted amount and unique destination key,
and wrote the routing audit. Reports are under
`outputs/stable_value_promotion_20261011/apply/20261011T194752Z-c18e4a48/`
and the existing production report prefix
`s3://retirementinsights-reference/stable_value_routing/runs/20261011T194752Z-c18e4a48/`.

## Production code

Local checkout: `C:/Users/User/Documents/GitHub/DCIO-pipeline/DCIO-pipeline`.
EC2: `i-0eaee37f64dfe7195`, `us-east-1`.
Production checkout: `/home/ec2-user/DCIO-pipeline/DCIO-pipeline`.

- `route_stable_value.py`: broader candidate selection, complete filing staging
  context, scoped six-table history comparison, optional exact approved-proposal
  scope for historical promotion. Existing lock/MERGE/reporting is reused.
- `src/stable_value_expanded.py`: adapts the reviewed reconciliation rules to
  production write decisions, retaining source-label/provider/amount conflicts,
  repeated-name checks, name-variant guards, and source multiplicity.
- `src/holding_reconciliation.py`: the reviewed pure classification/matching
  implementation is now installed as a production dependency.
- `src/stable_value_routing.py`: version v2 and verified EI Series Class VI
  general-account group-annuity routing.

The existing mandatory `src/stable_value_pipeline_hook.py` already invokes this
router after staging and existing downstream processing. Its production hash and
`src/run_pipeline.py` hash were verified and their files were preserved. Future
batches automatically use v2 with current batch ack_ids, database, workgroup,
region, output settings, and existing inserted/already-existing/review reporting.
Extraction and MF-total validation were preserved; PASS is never a stable-value
acceptance gate. Existing deterministic manager-cleaning rules are reused, and
ambiguous manager roles retain their review flags.

Deployment manifests and SSM results are in
`outputs/stable_value_promotion_20261011/`. Both code-only installations succeeded.
EC2 backups are in `stable_value_integration_backups/expanded_router_20261011_v2`
and `stable_value_integration_backups/expanded_router_20261011_v2_scoped`.
81 related local tests passed; the final 11 expanded-routing tests and existing
11 pipeline integration tests passed on EC2 across the deployment checks.

Automatic review rejected the initial deployment upload because its package
included the approved-proposals CSV without explicit approval for that upload.
The successful deployments used code-only bundles. The original proposal CSV
was not uploaded as part of either deployment package.


## Independent verification completed

`independent_verification.json` reports PASS: all 3,222 approved identities
were found exactly once at their intended destination with their exact amounts.
All six history tables were checked in affected filings; zero cross-table
ack_id plus bounded-normalized-name conflicts were found. Audit status counts
and unique candidate IDs matched the completed routing report. Local runtime
files also match the final deployed manifest hashes. `completion.json` records
the completed release and proposal checksum.
