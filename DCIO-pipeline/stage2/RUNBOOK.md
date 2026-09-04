# Stage-2 Over-Capture Re-Extraction — RUNBOOK (exception path)

Repeatable, git-committed process for correcting the **over-capture quarantine**
(bonds / stocks / treasuries / collective trusts leaked into `plan_mf_history_v3`).

## Model: it is an EXCEPTION, not a full-universe reprocess
The section-typing correction (`src/section_typing.py::apply_section_typing_stage`)
is now a real stage inside `run_pipeline` — but **default OFF** (`SECTION_TYPING=0`).
The normal full run over the ~44K good plans never applies it. It is switched on
(`SECTION_TYPING=1`) ONLY for the targeted re-run over flagged plans, whose results
then replace the bad rows per-ACK. Good plans are never touched.

Where it runs: `run_pipeline.py`, per-PDF, AFTER `extract_tables_and_map` and the
attachment/detail handling, BEFORE `supplemental_pages.extend(page_data)` — i.e.
after extraction, before the final validator (`post_extract_validator`). It only
DOWNGRADES non-MF rows / removes junk; it never turns a real fund into a non-fund.

## What the stage does (in order, in place)
1. `retype_result` — re-type each row by its PDF section via word geometry + a full
   MF/non-MF section-header map + cross-page carry (CONSERVATIVE: unlocated rows keep
   the extractor's own type — no carry-bleed).
2. name-based CIT catch — CITs are captured via a separate source, so force CIT type
   (dropped by the validator). Verified 0 false positives on real MFs.
3. drop section-subtotal rows ("Total Corporate Debt Instruments" ...).
4. de-dup identical (issuer, description, value) rows (table+text double-pass).
The validator then drops non-MF-typed rows, garbage values (>= $20B, FIX 21),
total-lines (FIX 19), CUSIP artifacts (FIX 20), and bad names (FIX 15-18).

## Procedure
```bash
# 1. Pull the flagged ACK list (over-capture / quarantine) from the scorecard:
#    quality_flag IN ('OVER_CAPTURE','QUARANTINE_IMPOSSIBLE')  OR
#    validation_status = 'OVER_CAPTURE_QUARANTINE'
bash stage2/reextract_overcapture.sh <ack_list_file> <workdir>

# 2. reextract_overcapture.sh:
#    - downloads those ACK PDFs from bronze to <workdir>/inputs
#    - runs `SECTION_TYPING=1 python -m src.run_pipeline` (USE_LLM=0)
#    - the stage fires inline; corrected rows land in data/outputs/investments_clean.csv

# 3. VALIDATE before swapping (never blind-swap): compare per-ACK new MF total to
#    certified amt_mutual_funds. Only swap ACKs that RECONCILE (OK). Hold UNDER
#    (over-stripped) / NO_CERT for review. See stage2/pilot_and_size.py for the harness.

# 4. SWAP per-ACK (backup first!):
#    - back up current rows:  SELECT * ... WHERE ack_id IN (winners) -> CSV
#    - DELETE ... WHERE ack_id IN (winners)
#    - INSERT corrected rows, validation_status='STAGE2_RECON'
#    - rebuild plan_mf_scorecard (DELETE + INSERT, definition in DCIO_CONTEXT.md §0)
```

## Safety / reversibility
- Every swap/cleanup writes a CSV backup FIRST to
  `C:\Users\User\Documents\DCIO_review\overcapture_investigation\`.
- Table is Iceberg + S3 versioning ON (double recovery).
- The stage is conservative + validated (Amex/Google/Intel unchanged; carry-bleed fixed).

## Coverage (2026-07-05 sizing, random 250 of quarantine)
Section-typing fully resolves ~14% of quarantine plans (~950) + the high-$ named-section
whales (done: 19 winners incl. Google/Intel/Goldman). Residual ~5,850 plans need other
work: ~3,100 single-table layouts (no sections) + ~2,770 value/dup/scramble on top of
sections. This exception path is for the section-resolvable set; it is NOT a full fix.
