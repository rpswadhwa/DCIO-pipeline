# Stage 2 — Over-Capture Re-Extraction (isolated path)

**Purpose.** Fix the ~$2.6T over-capture quarantine (6,816 plans) WITHOUT touching the
core extraction path that already works for ~44K clean plans.

## Why a separate path
The core `text_extract.extract_tables_and_map` extracts ~44K plans well. The over-capture
plans leak non-MF holdings (bonds, treasuries, individual stocks, CITs) into the MF table
because their section asset-type isn't assigned (blank type -> passes the load gate). The
correct type only exists in the PDF's **section context**, so the fix is at extraction —
but we refuse to risk the working core. Hence a **4-layer isolation**:

1. **Code** — lives only on branch `overcapture-reextract`; `master` (EC2 runtime of record) is untouched.
2. **Runtime** — `extract_tables_and_map` is NOT modified. `section_typing.retype_result()` is an
   ADDITIVE post-pass, invoked only when `OVERCAPTURE_MODE=1`. Default run == today, bit-for-bit.
3. **Data** — Stage 2 writes to staging table `plan_mf_history_v3_stage2`, never production.
4. **Scope** — the Stage-2 driver processes ONLY the over-capture ACK list (~6,816). Clean plans are never opened.

## Flow
```
quarantine ACK list  (validation_status='OVER_CAPTURE_QUARANTINE')
   -> download those PDFs from bronze
   -> run_pipeline with OVERCAPTURE_MODE=1  (core extract + section_typing post-pass)
   -> load to plan_mf_history_v3_stage2
   -> compare per-ACK: new_ext vs certified vs net_assets  (rebuild a staging scorecard)
   -> STAGE 3 (gated, per-ACK): for ACKs that now reconcile, DELETE their quarantine rows in
      production + INSERT the staged rows with un-quarantined status. Non-reconcilers stay quarantined.
```

## The fix (section_typing.py)
Root cause (Mode 3): the text-path parser's `SECTION_HEADING_MAP` lists only MF-ish section
labels, so entering a non-MF section (Common Stocks, Corporate Debt, Asset-Backed, Govt
Securities, Preferred Stock, CDs) does NOT reset `current_section_type` — the stale MF type
bleeds onto bonds/stocks. `section_typing` re-derives each page's section boundaries from the
page text (a FULL map incl. non-MF headers), then re-assigns every row's `asset_type` by the
section it falls under, resetting per page. Non-MF rows then get a non-MF type and the existing
load gate (`asset_type not in mf_types`) drops them. It never *adds* rows and never changes MF
rows -> can only reduce over-capture.

## Rollout gate
Pilot the top ~100 over-capture plans (45% of the $) first; only scale to all 6,816 if the
pilot reconciles a strong majority. Residual plans still > 1.5x net_assets after re-extraction
are unrecoverable (lost names / column scramble) -> permanent quarantine.
