# Downstream stable value routing

The independent `route_stable_value.py` job reads the existing eight staging
columns. It does not modify extraction, staging, upstream validation, MF routing,
or the live CIT manager-cleaning job. It selects the case-insensitive phrase
`stable\s+value` with word boundaries. Broader abbreviated stable value names are
outside this initial backfill scope.

Run `python route_stable_value.py` for a preview. Use `--ack-ids ACK1,ACK2` to
scope a pilot. Run `python route_stable_value.py --apply` to create missing
destinations and insert accepted holdings. The pipeline now also invokes apply
automatically after staging and existing processing, scoped to the current batch.

## Destinations

- `plan_cit_history`: explicit collective-trust evidence.
- `plan_insurance_history`: explicit insurance-contract/account evidence.
- `plan_separately_managed_history`: explicit plan-specific managed-account evidence.
- `plan_stable_value_unresolved_history`: stable value names without a reliable
  vehicle classification, including conflicting vehicle evidence.

An uncorroborated separately managed asset-type label is unresolved because the
existing upstream pattern maps insurance-company separate accounts to that label.
Unknown vehicle information never excludes a confirmed stable value candidate.
Invalid amounts, repeated holding pairs, inconsistent strategy classifications,
and suspected existing holdings are retained in the review report instead.

The specific Transamerica Stable Value Core Account/Option names have provider
evidence of insurance general-account group annuities. A contradictory CIT label
is captured as unresolved. The provider source is
https://www.ta-retirement.com/funds/FundFootnotes.aspx?cat=60.
Target-date strategy names with appended stable value text require review; the
phrase alone does not prove the whole holding is a stable value fund.

## Duplicate checks

Fetch all holdings in candidate filings from CIT, MF, the new destinations, and
alternatives if present. Compare lowercase/whitespace-normalized filing/name pairs
and exact decimal amounts. Clear existing matches are skipped. Name variants,
same-amount/different-name matches, conflicting amounts, and repeated pairs are
reviewed. The router never sums multiple source rows into one holding.
Different staging names with the same positive amount within a filing also go to
review, even when neither name already exists in history.

Accepted pairs use deterministic candidate hashes of existing filing/name/sponsor/
amount fields. These are job-generated comparison fingerprints, not new extracted
source-row identities. New targets use insert-only MERGE with both candidate and
filing/name guards. CIT uses a `SV:` row_order plus a filing/name guard. No existing
history holdings are deleted or overwritten. The only update fills plan metadata
for router-owned CIT rows from an unambiguous plan_master match.

## Managers and validation

`stable_value_legacy_manager_rules.py` is a pure-function snapshot of the active
ai-cit deterministic rules, with a source SHA256. Generate it using
`tools/snapshot_stable_value_manager_rules.py`; the Glue script is never imported.
Load the same S3 canonical manager JSON at run time. Entity-first overrides and
dictionary matches are reused. Sponsor-secondary platform/insurer matches remain
unresolved rather than silently equating insurer with investment manager.

The original staging validation_status remains unchanged and is preserved in the
new tables. It reflects upstream MF-total validation, not a row-level stable value
acceptance gate. Failed MF-total reconciliation alone does not exclude a holding.

## Execution and recovery

Apply acquires `s3://retirementinsights-reference/stable_value_routing/locks/apply.json`
with a conditional put. An existing lock blocks another router. A crash can leave
a lock: inspect its run ID and confirm that process is no longer running before
removing it. Do not run competing routers or external history writers during apply;
the lock does not control pre-existing pipeline jobs.

Recheck live staging and history immediately before writing. Verify every accepted
destination record and its amount. Each Iceberg MERGE is transactional, but writes
across targets and audit are not one transaction. Reports retain partial results;
reruns recognize completed destination rows. CSV and JSON reports are published to
`s3://retirementinsights-reference/stable_value_routing/runs/<run_id>/`.

`plan_stable_value_routing_audit` records all decisions, including review/existing
matches. It does not contain newly extracted metadata. The audit and reports must
be consulted before rerunning a partially failed job.

## Checks

Run `python -m unittest discover -s tests -p 'test_stable_value_*.py' -v`.
Validate a scoped pilot, verify counts/amounts, and rerun that pilot to demonstrate
zero new inserts before the full apply.

The initial deployment runs in an isolated directory on EC2 instance
`i-0eaee37f64dfe7195` in `us-east-1`:
`/home/ec2-user/DCIO-pipeline/DCIO-pipeline/stable_value_router_runtime_20261010_01`.
Use `/home/ec2-user/DCIO-pipeline/venv/bin/python` from that directory.
The code bundle lives at
`s3://retirementinsights-reference/stable_value_routing/code/stable_value_router_v1.zip`.
The subsequent integration deploys the router into the main project directory.
`src/run_pipeline.py` invokes `src/stable_value_pipeline_hook.py` on every run;
there is no enable/disable flag. The helper confirms current-batch staging through
validation-summary rows within this invocation's time window, without requiring
MF-total validation PASS. Empty batches never become an unscoped historical run.
The helper passes the database, workgroup, Athena output, region, and batch ack_ids
to the router. Existing extraction, staging writing, and validation functions
remain unchanged.

`<OUTPUT_DIR>/stable_value_routing_pipeline.json` records inserted,
already-existing, and review counts. Routing errors or missing staging evidence
mark the downstream step INCOMPLETE and make the pipeline exit nonzero. Partial
write counts are retained when available; reruns check destination holdings again.
See [deployment and integration verification](stable_value_pipeline_integration_20261010.md).

`tools/stable_value_ec2_status.py` reads the initial full-apply log through SSM.
`tools/verify_stable_value_backfill.py --expected-total N` independently checks
router-owned counts, duplicate filing/name pairs, overlaps with legacy CIT/MF/
alternative holdings, nonnegative amounts, and the combined view. `N` includes
the pilot and all subsequent accepted inserts. The router also verifies every
intended holding and exact amount during each destination write.

The generated SQL is saved under `sql/stable_value/`. It creates the three new
history tables, the audit table, and the view; it does not ALTER the legacy tables.

## Full staging reconciliation preview

`reconcile_staging_holdings.py` is a separate read-only job. It compares full
staging (or `--ack-ids A,B`) against CIT, MF, **plan_alternatives_history**, and
all three new holding tables. The audit table is not a holding destination.
The local existing router's optional alternatives lookup now uses the correct
plural table name. This preview does not replace the deployed pipeline router.

Run `python reconcile_staging_holdings.py --output-dir outputs/holding_preview`.
Use `--reuse-snapshot` to rerun downloaded, checksum-verified source CSVs without
rescanning Athena. Database, workgroup and output options match the router.
The snapshots are independent SELECTs, not a cross-table transaction.
Add `--reuse-working-db` to reuse the local indexed rows; every cached ack and
payload is first compared against its source CSV, including missing/extra rows.

Each staging row receives EXISTING, INSERT, UNRESOLVED, or OUTSIDE_SCOPE.
Reports preserve all eight source fields. Normalized name, provider and exact
decimal amount must match within an ack_id for an automatic existing match.
Bounded abbreviations are supported; share classes and years remain distinct.
Same amount alone, provider changes, repeated history rows and conflicting
extractions require review. Identical repeated source identities receive one
insert proposal; their amounts are never added together. Source MF validation
PASS is not required. Existing presence does not endorse historical taxonomy.
Possible history name variants also require review when the amount differs;
similarity never creates an automatic EXISTING match.

Literal stable value and corroborated fixed-account candidates can be proposed
for the unresolved stable-value table when the wrapper remains unknown.
An insurance wrapper alone does not establish the stable-value mandate;
generic annuity contracts remain evidence-required candidates. A named reserve
trust also does not prove a plan-specific separately managed account.
Generic separate-account labels need investment-menu evidence; individual
stocks listed inside an account are excluded from automatic account proposals.
Alternative keywords reuse the existing catalogue and retain manual review.
An INSERT outcome is a **proposal**, never a business-table write.

`exception_groups.csv` groups the same product/provider/source classification
across filings. A reviewed group can become one reusable `--rules-file` JSON
entry containing rule_id, raw_entity_name, raw_sponsor_name, asset_type,
asset_class, asset_sub_class, destination_table, evidence and approved_by.
Rules use exact normalized product/provider/source-label scope and cannot
override security/classification conflicts or history/duplicate safeguards.
Do not approve a rule solely from an extracted label. Provider identity is
retained separately from the manager; the existing manager cleaner is reused.

`summary.json` contains coverage totals, destination proposals, reason counts,
canonical/rule versions, source query IDs and artifact checksums. CSVs split at
500,000 rows. The CLI has no apply option. Production loads require a fresh
history recheck, reviewed rules and the existing writer's locking/verification.

After completion, run `python tools/summarize_holding_preview.py --preview-dir
outputs/holding_preview` (on one command line). This verifies report checksums
and row/outcome totals, then creates `target_candidates.csv`,
`priority_product_groups.csv`, and `focused_summary.json`. Candidate scope is
evidence for selection, not a determination of the investment's mandate.
The priority queue focuses on stable value, separate accounts (including pooled
and plural labels), and alternative destinations. Ordinary MF/CIT identity
discrepancies remain visible in the complete exception and decision exports.
Start with `decision_families.csv`, which aggregates the target exceptions by
evidence pattern, source classification and matching problem. Detailed product
groups provide drilldown examples; they are not a request to manually review
every name. Families organize rule development and do not automatically resolve
uncertain products or authorize a bulk load.

The October 11 preview records incremental proposal refinements in summary.json.
`tools/refine_sma_trust_preview.py` applies the tightened pooled-wrapper and
insurance-mandate guards to existing previews without rescanning sources.
Fresh reconciliation runs use these guards directly. Refined reports must be
verified again with the summary tool before use.


### Production router v2 (October 11, 2026)

The main EC2 router now uses `src/stable_value_expanded.py`, backed by the same
pure reconciliation rules used for the reviewed full-staging preview. Candidate
selection accepts stable-value abbreviations and corroborated fixed/guaranteed
products, then reads the entire candidate filing's staging context. Matching
uses ack_id, bounded-normalized product name, raw provider, and exact amount;
conflicting names, providers, amounts, source labels, or possible history name
variants stay in review. Generic confirmed stable value is retained in the
unknown-vehicle table. The source MF-total validation status is preserved and
never treated as proof of stable-value validity.

A historical promotion can be restricted to an approved proposal export:

```powershell
python route_stable_value.py --apply --approved-candidates <approved-proposals.csv> --output-dir <report-directory>
```

This restriction is for reviewed backfills. Normal production batches continue
through the mandatory pipeline hook with their current ack_ids. The code-only
EC2 deployment package excludes any holding/proposal CSV.
