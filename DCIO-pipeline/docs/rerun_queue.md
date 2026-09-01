# Rerun Queue — Plans Needing a Prod Reload After a Code Fix

Plans below were extracted/classified with a bug that has since been fixed and
deployed to EC2, but the corrected data has **not yet been loaded into prod**
(`plan_mf_history_v3` / `plan_holdings_staging` still hold the old, wrong
values for these ack_ids). Each needs a full pipeline + classification rerun,
scoped to just its ack_id, once queued up for that pass.

Do not run these prod reloads without explicit go-ahead — add here, then wait.

---

## Pfizer Inc.
- ack_id: `20251002084241NAL0000160195001`
- Bug: cross-page duplicate collapse in `remove_cross_page_duplicates()`
  (`src/data_cleaner.py`) dropped 2 legitimate funds that happened to share a
  dedup key with rows on another page.
- Fix status: committed `a82456ed`, deployed to EC2 (2026-08-26), verified via
  isolated scratch pipeline run (both funds recovered, $293,759,000 gap closed).
- Rerun status: **not yet run** — user said "Hold off" on the prod reload.

## Pending sponsor (undercapture_47 tracker row 24)
- ack_id: `20250924113851NAL0002990851001`
- Bug: `extract_text_based_investments()` (`src/text_extract.py`) matched the
  first `**`/`*` footnote marker in a line instead of the line's trailing
  value — for rows where a fund name itself carries a `**` footnote suffix
  (e.g. `FID 500 INDEX**`), this captured the share/unit count instead of the
  dollar value. Affected 6 Fidelity fund rows.
- Fix status: committed `c93a76b3`, deployed to EC2 (2026-08-26), verified
  directly against the PDF (all 6 rows now correct).
- Rerun status: **not yet run** — plan review for this ack_id is paused
  pending this rerun; no verdict recorded yet in
  `undercapture_47_verdicts.csv` row 24.

## Pending sponsor (v3 shows only 3 of 15 MF rows)
- ack_id: `20251002095709NAL0000502128001`
- Bug: no current code bug found. Live `plan_mf_history_v3` only has 3 rows
  ($835,466,787) and was evidently loaded by an older, pre-fix run of the
  pipeline — 2 surviving Fidelity rows have mangled names
  (`raw_entity_name = "; 18777731.868shares"`, missing issuer + description
  entirely) and 12 legitimate Vanguard/T. Rowe Price/American Funds rows are
  missing outright.
- Fix status: N/A — isolated scratch rerun on current deployed code
  (commit `c93a76b3`) correctly extracts all 15 mutual fund rows,
  $2,398,495,431 total, matching the PDF exactly line-for-line. No new code
  change needed, just a reload with current code.
- Note: one cosmetic issue survives even in the current code — the
  "Vanguard Intermediate-Term Investment-Grade Fund" row doesn't split
  issuer/description (description repeats the full original string instead
  of "Mutual Funds; ... shares" like its siblings). Value and asset_type are
  still correct. Likely the same root cause as the ongoing
  `cleanup_investment_names.py` manager-prefix-parsing investigation
  (todo: "Check how widespread this bug is across the 88-plan batch") —
  treat as a data point there, not a separate fix.
- Rerun status: **not yet run** — user chose to queue only, not reload yet.
  Expected recovery if run: $835,466,787 → $2,398,495,431
  (+$1,563,028,644).

## American Cancer Society, Inc. 403(b) Plan
- ack_id: `20251007171612NAL0002843619001`
- Bug: `extract_text_based_investments()` (`src/text_extract.py`) had no
  mechanism to backfill `asset_type` from a TRAILING "Total <category>"
  subtotal line (e.g. "TOTAL FIXED ANNUITY CONTRACTS", "TOTAL VARIABLE
  ANNUITY ACCOUNTS", "TOTAL MUTUAL FUNDS") when the schedule (page 19) has
  no leading section headings at all — rows stayed blank-typed, and the
  grand-total "TOTAL ASSETS (HELD AT END OF YEAR)" line leaked through as a
  phantom row with a wrong asset_type. Same mechanism also added to the
  camelot table-based loop in `extract_tables_and_map()` for other plans
  with this shape.
- Fix status: implemented and verified locally (not yet committed) — all 32
  typed rows now get correct asset_type (2 Group Annuity Contract, 8
  Variable Annuity Contract, 20 Mutual Fund, plus 2 already-typed Money
  Market Fund rows), grand-total phantom row no longer leaks, sum of
  extracted values = $425,134,662 matching the plan's certified total
  exactly. Participant Loans row correctly stays blank (no type signal
  anywhere on this page for that row).
- Rerun status: **not yet run** — needs commit + EC2 deploy first, then a
  full pipeline + classification rerun scoped to this ack_id. Queued here
  per user's instruction, will run later.

## Flowers Foods, Inc. 401(k) Retirement Savings Plan
- ack_id: `20260623130926NAL0009114320001`
- Bug: manager-prefix truncation in `parse_issuer_and_investment()`
  (`cleanup_investment_names.py`) — the ~13 hardcoded manager-prefix rules
  (Vanguard, Fidelity, PIMCO, BlackRock, etc.) discard the specific fund
  name from `issuer_name`, relying on a fallback that copies the full name
  into `investment_description` first when that description is a pure
  asset-type label. That fallback's label-recognition regex
  (`_LABEL_TRAILING_RE`) only handled comma/hyphen-separated trailing share
  counts, not Camelot's actual semicolon-separated format (e.g.
  `"Collective Trust Fund; 264,734 shares"`) — so the fallback never fired,
  the manager-prefix rules discarded the real fund name with nothing to
  recover it from, and `build_mf_rows_df`'s name-quality gate silently
  dropped the corrupted rows. Confirmed via direct repro: 1 of 8 Mutual
  Fund rows survived before the fix, 8 of 8 after.
- Fix status: committed `4b48eeb9`, pushed to `origin/master` — **not yet
  deployed to EC2**.
- Rerun status: **not yet run** — needs EC2 deploy first, then a full
  pipeline + classification rerun scoped to this ack_id.

## Wilbur-Ellis 401(k) Plan
- ack_id: `20251015152759NAL0002551539001`
- Bug (two-part, both in `classify_pages_text()`, `src/text_extract.py`):
  1. Page 22 (the real Schedule H, Line 4i table) prints a routine
     "(See Independent Auditors' Report)" citation in its own header, which
     tripped the `negative_keywords` entry `"INDEPENDENT AUDITOR"` and forced
     `is_supplemental=0` despite the page also matching the positive keyword
     "SCHEDULE H, LINE 4i" — the entire $391.8M / 27-row schedule (25 mutual
     funds + 1 pooled separate account + notes receivable) was never
     extracted. Certified `amt_mutual_funds` = $379,570,871;
     `plan_holdings_staging` had zero rows; `plan_mf_history_v3` had exactly
     1 garbage row.
  2. Separately, page 6 (auditor's own "Other Matter — Supplemental
     Schedules" boilerplate paragraph) legitimately matches a keyword on its
     own and started a false continuation run that swept pages 7-13
     (narrative "Notes to the Financial Statements") into the supplemental
     range.
- Fix status: committed `a3feecb8`, deployed to EC2 (MD5-verified,
  2026-08-30). Verified locally against the PDF: (1) added a structural override
  so a negative-keyword hit doesn't veto a page that also carries the real
  schedule's column headers (identity-of-issue + current-value) — page 22
  now correctly extracts all 27 rows, $398,034,989 total, exact match to
  the schedule's own "Total Investments" line. (2) gated the continuation
  run so it can't start from a keyword match unless backed by a structural
  schedule signal or a dense, table-like page — pages 7-13 no longer swept
  in (page 14 still independently re-triggers a shorter false sweep through
  15-19, but those narrative pages yield zero extracted rows either way, so
  no new garbage results).
- Known residual issue (not fixed, separate root cause): the single garbage
  row in `plan_mf_history_v3` (`raw_entity_name="October 14"`,
  `plan_investment_amt=2025.00`) persists — page 6 legitimately matches the
  schedule keyword in its own right (it's the auditor's own paragraph about
  the schedule), and `extract_text_based_investments()`
  (`src/text_extract.py:1502`) misreads its sign-off line "October 14,
  2025" as an entity/value pair. This is a distinct bug in that row-parsing
  function, not the page classifier — deferred to a separate investigation
  since it's low-harm (already caught downstream as `UNDER_CAPTURE`) and a
  blanket date-rejection guard risks affecting other plans' legitimate
  text-based extraction without broader testing first.
- Rerun status: **not yet run** — needs EC2 deploy first, then a full
  pipeline + classification rerun scoped to this ack_id.
  Expected recovery: 1 garbage row → 27 rows + garbage row unchanged,
  reconciling to $398,034,989 total investments ($391,792,566 mutual
  funds + pooled separate account, $6,242,423 notes receivable).

## Lee Health System 403(b) Retirement Plan
- ack_id: `20250623132312NAL0003563811002`
- Bug: `extract_text_based_investments()`'s trailing-total backfill (added
  for American Cancer Society, above) only recognized "Total <category>"
  (leading), not this filer's "<category> Total" (trailing) shape — so it
  never fired at all. All 35 real mutual fund rows stayed blank-typed and
  got dropped downstream; both category-total lines leaked in as fake rows.
  Fixing this also surfaced 2 more bugs on the same page: a slash
  ("Company/General") broke the Insurance General Account pattern, and an
  abbreviated "Retment" spelling broke the Personal Choice Retirement
  Account (Schwab SDBA) pattern. See `docs/plan_notes.md` for full detail.
- Fix status: committed `c054722a`, deployed to EC2 (MD5-verified, 2026-08-27)
  — 35 MF rows ($1,127,268,967, matches PDF to within $4), 2 Insurance
  General Account rows ($131,919,328 exact), 1 Self-Directed Brokerage
  Account row ($10,423,480) all now resolve correctly.
- Rerun status: **not yet run** — code is live on EC2 but the pipeline
  hasn't been rerun against prod for this ack_id yet.
  Expected recovery if run: $137,752,780 (1 row) → ~$1,280,404,013 (all
  Schedule H rows, minus the still-open "Other Funds Total" residual).

## Oracle Corporation 401(k) Savings and Investment Plan
- ack_id: `20260427164405NAL0013754176001`
- Bug: `has_schedule_marker` in `extract_text_based_investments()`
  (`src/text_extract.py`) requires the literal phrase "Schedule H, Line
  4(i)" / "Schedule of Assets" somewhere on the page; this filer's schedule
  first page (page 14) is titled only "Notes to Financial Statements" and
  never states that phrase, so the whole page (5 Mutual Fund rows,
  ~$3.27B) was silently skipped.
- Fix status: implemented and verified locally — narrow Oracle-only
  exception added (page text contains both "ORACLE" and "NOTES TO
  FINANCIAL STATEMENTS"). Not yet committed/deployed.
- Rerun status: **not yet run** — needs commit + EC2 deploy first, then a
  full pipeline + classification rerun scoped to this ack_id. Note: this
  plan also has a separate, unrelated `PENDING_AI` classification backlog
  (~90 rows) that this fix does not address.

## American Institutes for Research in the Behavioral Sciences
- ack_id: `20251015105028NAL0002275363002`
- Bug: `extract_text_based_investments()` (`src/text_extract.py`) only
  looked for a per-row category label (e.g. "Registered Investment
  Company") when the row also contained a "shares"/"units" count; this
  filer reports "N/A" instead of a share count (participant-directed
  investments, footnoted) and puts the label as a PREFIX of the identity-
  of-issue field on every row, so the label was never matched and 41 of
  42 rows lost their asset_type and got dropped downstream.
- Fix status: committed, deployed to EC2 not yet done — added a
  line-start-anchored prefix match against the same lookup table. Full
  page now sums to $322,953,560, exact match to certified total assets.
- Rerun status: **not yet run** — needs EC2 deploy first, then a full
  pipeline + classification rerun scoped to this ack_id. Note: the 13
  TIAA/CREF rows on this same page remain unresolved (separate, unfixed
  issue) even after this fix and after a rerun.

## Brown University Defined Contribution Deferred Vesting Retirement Plan
- ack_id: `20251008130222NAL0009448880001`
- Bug: `extract_tables_and_map()` (`src/text_extract.py`) — the
  `_HEADING_OFFERED_BY_RE` branch (added for this filer's "Mutual funds
  offered by Fidelity:" heading) unconditionally dropped the whole row
  whenever it matched, but Camelot fuses that heading directly onto the
  first real data row of its own section on this layout, so 2 real Mutual
  Fund rows ($5,038,300 "BrokerageLink Fidelity Fund" + $10,985,609 "John
  Hancock Funds III Disciplined Value Fd Cl R6") were silently discarded
  along with the heading text.
- Fix status: implemented and verified locally — new
  `_HEADING_OFFERED_BY_STRIP_RE` strips just the heading clause and keeps
  the row if real data remains. Mutual Fund total recovered from $758.2M to
  $774.3M (certified target: $875.0M). Committed, not yet deployed to EC2.
  Known residuals not fixed: 1 duplicate Freedom Index 2055 row from an
  overlapping Camelot section split, and garbled `�`-value rows in the
  page 19 TIAA/CREF section.
- Rerun status: **not yet run** — needs EC2 deploy first, then a full
  pipeline + classification rerun scoped to this ack_id.

## Bill & Melinda Gates Foundation Employee Retirement Plan
- ack_id: `20250909140130NAL0012629043001`
- Bug: none — staged `plan_mf_history_v3` (1 row, $15.3M) predates the
  trailing-total asset-type backfill (commit `f48a2e64`, already
  committed/deployed from an earlier session).
- Fix status: N/A — verified via isolated local rerun on current code:
  all 17 rows recover correctly (16 Registered Investment Company Funds
  backfilled via the trailing "Registered investment company funds total"
  line + 1 Schwab SDBA row), summing to exactly $330,185,065, matching
  certified `amt_mutual_funds`.
- Rerun status: **not yet run** — just needs a pipeline + classification
  reload scoped to this ack_id, no code change required.
  Expected recovery if run: $15,322,852 → $330,185,065 (+$314,862,213).

## Board of Trustees of the Building Service 32BJ Supplemental Retirement Savings Plan
- ack_id: `20260415123903NAL0017000545001`
- Bug: manager-prefix truncation in `parse_issuer_and_investment()`
  (`cleanup_investment_names.py`) — same bug class as Flowers Foods
  (`4b48eeb9`), different trigger shape. All 18 real "Mutual Fund" rows on
  page 21 have empty Collateral/Rate/Interest/Maturity-Date columns that
  Camelot concatenates into the description as `"Mutual Fund N/A N/A"`.
  `_is_asset_type_label()`'s trailing-content stripper only handled
  trailing share/unit counts, not trailing `N/A` placeholder tokens, so it
  failed to recognize this as a pure label — the manager-prefix rules
  (Vanguard/T. Rowe Price/American Funds) then truncated `issuer_name` down
  to the bare manager name with the real fund name recovered nowhere, and
  `build_mf_rows_df`'s name-quality gate silently dropped the row.
  Confirmed via direct repro: 0 of 18 Mutual Fund rows survived before the
  fix (all fell through the name-quality gate); fix verified to recover the
  Vanguard Wellesley Income Adm row (issuer="Vanguard",
  desc="Vanguard Wellesley Income Adm") plus regression-checked against
  "Target Date Fund 2035" (still correctly NOT treated as a pure label) and
  the existing Flowers Foods semicolon-share-count case (unaffected).
- Fix status: committed `10d8ea59`, deployed to EC2 (MD5-verified,
  2026-08-29).
- Rerun status: **not yet run** — needs a full pipeline + classification
  rerun scoped to this ack_id.
  Expected recovery if run: certified `amt_mutual_funds` $2,524,373,761 (18
  rows, PDF subtotal current value); currently 0 rows in
  `plan_mf_history_v3` for this ack_id.

## Nouryon Chemicals LLC (Retirement Savings Plan / Hourly Savings Plan Master Trust)
- ack_id: `20251015102624NAL0002240291001`
- Bug: two issues on this filer's Schedule H, 4i page. (1) Column (E)'s
  header text is corrupted in the source PDF (literally reads "18"),
  defeating normal header-text matching — `column_map` ended up completely
  empty and the whole page (0/24 meaningful rows) fell back to text-based
  extraction, which only recovered 1 row. (2) Separately, a general
  asset-type bug: `table_section_asset_type` (coarse, per-Camelot-table-area
  default) unconditionally overrode the more specific, freshly-detected
  in-row `current_section_type`, so every row showed asset_type "Mutual
  Fund" regardless of its real category; two heading wordings ("MANAGED
  SEPARATE ACCOUNT", "INTEREST BEARING CASH") also had no matching entry in
  `ASSET_TYPE_PATTERNS` at all.
- Fix status: committed `87f05afc`, deployed to EC2 (MD5-verified,
  2026-08-30). Added a plan-specific (Nouryon-only) fallback that bootstraps
  `current_value`/`issuer_name` column mapping positionally when header-text
  matching finds neither; fixed the general `current_section_type` vs
  `table_section_asset_type` priority bug; added the two missing
  `ASSET_TYPE_PATTERNS` entries; fixed a regression that surfaced from that
  last change (`_detect_section_heading_text` was substring-matching instead
  of fullmatching, causing a false section-area split). Verified against the
  real PDF: all 23 rows extract with correct values and correct asset_types
  (9 Common/Collective Trust Fund, Mutual Fund rows, 1 Separate Account, 1
  Cash), summing to exactly $555,439,476 (certified total, exact match).
- Rerun status: **not yet run** — needs a full pipeline + classification
  rerun scoped to this ack_id.
  Expected recovery if run: currently ~1 row recovered via text-fallback
  extraction → 23 rows, $555,439,476.

## Kaman Corporation 401(k) Plan
- ack_id: `20251006123629NAL0009268594001`
- Bug: no extraction defect for this plan — investigated a latent bug it
  surfaced instead. Schedule H, 4i content is duplicated verbatim across
  two physical pages (16 and 18) in the source PDF; `current_section_type`
  in `src/text_extract.py` persists across pages by design (needed for
  genuine multi-page continuations), so page 16's trailing "Participant
  Loan" heading leaks forward and mistypes every fund row on page 18's
  (duplicate) table as `'Participant Loan'`. Confirmed this self-heals for
  this plan: `remove_cross_page_duplicates` (`src/data_cleaner.py`) drops
  the mistyped page-18 duplicate on an exact-value tie, keeping page 16's
  correctly-typed row. Verified by running the real dedup function against
  both pages' raw output: 27 rows survive, summing to exactly the
  certified $447,641,558 investments + $4,978,622 loans = $452,620,180.
  The underlying `current_section_type` cross-table-persistence bug
  remains unfixed (deferred per user decision, 2026-08-30) — could bite a
  future plan where a duplicate page's dedup tie-break doesn't hold.
- Fix status: no code change made this pass; no fix needed for this plan's
  numbers to reconcile.
- Rerun status: **not yet run** — numbers already reconcile from raw
  extraction + existing dedup logic, so a rerun would only refresh the
  load, not change any result.

## Pacific Coast Benefits Trust Fund
- ack_id: `20260330192503NAL0015004466001`
- Bug: two issues on this filer's Schedule H, 4i "Mutual and
  Exchange-Traded Funds" section (`src/text_extract.py`,
  `extract_text_based_investments()`). (1) `SECTION_HEADING_MAP` had no
  entry for this exact heading wording, so the section inherited the
  prior section's asset_type ('Real Estate') and was dropped from
  `plan_mf_history_v3` entirely; the wrong rows that landed in v3 instead
  (blank-typed common-stock holdings) got spuriously sponsor-matched by
  name coincidence. (2) This filer prints its two dollar columns as Fair
  Value then Cost (reversed from the IRS standard order), and the
  trailing-number parser always captured the line's last number — silently
  storing Cost instead of Fair Value for every row on the page.
- Fix status: committed `8e65ed2e`, deployed to EC2 (MD5-verified,
  2026-08-30) — added the missing `SECTION_HEADING_MAP` entry, plus a
  header-gated dual-trailing-number check that captures Fair Value instead
  of Cost when the page's own header reads "Value Cost". Verified locally:
  Mutual/ETF section total $313,904,150 exact match to certified
  `amt_mutual_funds`.
- Rerun status: **not yet run** — needs a full pipeline + classification
  rerun scoped to this ack_id.
  Expected recovery if run: 0 correctly-typed Mutual Fund rows currently in
  `plan_mf_history_v3` for this ack_id → 6 rows + TOTALS reconciling to
  $313,904,150.

## Kelley Drye & Warren LLP Retirement Savings Plan
- ack_id: `20251015111048NAL0002192867001`
- Bug: two compounding issues in `src/text_extract.py`. (1) This filing's
  Schedule H, Line 4i table (pages 15-16) has table-body characters flagged
  `upright: False` by pdfplumber even though their transform matrices are
  near-identity (only ~1e-8/1e-9 floating-point noise off the diagonal, not
  real rotation) — `page.extract_text()` mis-groups these into badly garbled
  lines (e.g. `"InC Id e n tity o f Is s u e r..."`), so the real schedule
  pages classified `is_supplemental=0` and captured 0 rows
  (`classify_pages_text()`, `extract_text_based_investments()`). (2) Page 16
  is a byte-for-byte duplicate of page 15 in the source PDF (confirmed:
  identical char stream, same positions/text) — without an explicit guard,
  both pages would classify as supplemental and double-count every holding.
- Fix status: committed `c47f6e4d`, deployed to EC2 (MD5-verified,
  2026-08-30). Added `_extract_text_robust()`, a top/x0-clustering fallback
  extractor, gated behind `_GARBLED_UPRIGHT_ACK_IDS` scoped to this ack_id
  only (not a general-purpose change — other filers are unaffected and this
  has not been checked for regressions elsewhere); plus a post-continuation-
  run override forcing page 16 to `is_supplemental=0`. Verified locally: page
  15 now extracts 27 Mutual Fund rows totaling $276,554,169 vs. certified
  `amt_mutual_funds` of $286,907,715 (~4% off, consistent with other plans'
  loose ties between the certified summary field and detail-schedule totals).
- Rerun status: **not yet run** — needs a full pipeline + classification
  rerun scoped to this ack_id.
  Expected recovery if run: 0 rows currently in `plan_holdings_staging` and
  `plan_mf_history_v3` (only the pre-existing "October 14" garbage row) for
  this ack_id → ~27 Mutual Fund rows reconciling to roughly $276.5M-$286.9M.
  Known residual issue, out of scope for this fix: page 5's auditor
  narrative still produces the "October 14"/2025 garbage row, same mechanism
  as the pre-fix Wilbur-Ellis bug — not addressed here.

## TD 401(k) Retirement Plan
- ack_id: `20251010121541NAL0004227267001`
- Bug: in `extract_text_based_investments()` (`src/text_extract.py`), when a
  fund's own name+description line (e.g. "Dodge & Cox Stock X Registered
  investment company") has no trailing value — because this filer wraps the
  dollar value onto a later, separate line instead of trailing it on the
  same row — the line fell into the "no value found" branch and was matched
  against `SECTION_HEADING_MAP` as if it were a bare category heading (e.g.
  "Mutual Funds:"), discarding the fund's real name entirely. The exact same
  problem (a name-bearing row with no value on its own line) is already
  solved for the camelot/table-based extraction path via
  `pending_single_cell_fragments`, just not for this text-based path. User
  confirmed via review that the fix logic already existed elsewhere in the
  codebase before any code was written.
- Fix status: committed `50ee3b49`, deployed to EC2 (MD5-verified,
  2026-08-31). Added `pending_issuer_name`, mirroring the existing
  `pending_untyped_rows`/`pending_single_cell_fragments` stash-and-reclaim
  pattern: a name+description line with no value is now distinguished from a
  bare heading by checking whether real text precedes the matched
  `SECTION_HEADING_MAP` phrase (`_before` length), and if so the name is
  stashed and reclaimed by the next value-bearing line that has no name of
  its own. Purely additive, scoped to minimize risk of interfering with
  other filers' extraction: verified via a regression sweep across all 68
  cached plan PDFs (pages 1-30), diffing row counts/exceptions against a
  pre-fix baseline — exactly one line differs (this plan's row count,
  26→27), zero changes anywhere else. Verified locally against the target
  case: Dodge & Cox Stock X now extracts as `Mutual Fund` / $461,376,276,
  matching the certified value.
- Rerun status: **not yet run** — needs a full pipeline + classification
  rerun scoped to this ack_id.
  Expected recovery if run: Dodge & Cox Stock X row currently missing/
  mistyped in `plan_mf_history_v3` for this ack_id → recovered as a
  correctly-named, correctly-typed Mutual Fund row at $461,376,276.
  Known residual issues, out of scope for this fix: (1) the same page's
  Toronto-Dominion Bank common-stock row has a separate wrong-value bug,
  explicitly deprioritized by the user ("mutual funds is the main issue");
  (2) Vanguard Treasury Money Market's issuer_name is mangled downstream to
  just "Investment" — this fix does not correct it, since its value line
  already carries non-empty leading text so the stashed name is dropped
  rather than substituted (only applies to lines where the value line's
  `issuer_description` is fully empty).

## Molson Coors Employees' Retirement and Savings Plan
- ack_id: `20250919104203NAL0002173521001`
- Bug: `_page_values_are_in_thousands()` (`src/text_extract.py`) is anchored on
  the word "thousands" or a literal `$000s`; this filer's Schedule H, 4i
  header instead reads "(amounts in 000's)", and the apostrophe extracts as a
  garbled/curly character rather than a plain `'` — the regex never matched,
  `_page_value_scale_factor()` returned 1 instead of 1000, and every
  current_value on pages 25-26 (Stable Value, Common Stock, and Mutual Fund
  rows) was captured 1000x too low (e.g. Dodge & Cox Stock staged as $149,252
  instead of $149,252,000). User caught this directly by comparing a pasted
  PDF excerpt against staged values that matched the PDF's raw printed digits
  exactly.
- Fix status: committed `0bddfc9c`, deployed to EC2 (MD5-verified,
  2026-09-01). Added a new alternative to the thousands-detection regex
  matching `in\s+000\W{0,2}s` (tolerant of any short non-word run between
  "000" and "s"), so it also fires on this wording regardless of which
  garbled character the apostrophe extracts as. Verified locally against the
  PDF: all page 25-26 rows now scale correctly (e.g. Dodge & Cox Stock →
  $149,252,000).
- Rerun status: **not yet run** — needs a full pipeline + classification
  rerun scoped to this ack_id.
  Expected recovery if run: all Schedule H,4i current_value figures for this
  ack_id currently understated 1000x in `plan_holdings_staging` /
  `plan_mf_history_v3` → corrected to their true (x1000) values.

## ALLETE and Affiliated Companies Retirement Savings and Stock Ownership Plan
- ack_id: `20250729111547NAL0001546291001`
- Bug (two, compounding on the same page):
  1. Schedule H, 4i header (page 16) prints a bare standalone `"Thousands"`
     label instead of "in thousands" / "(amounts in 000's)" wording.
     `_page_values_are_in_thousands()` (`src/text_extract.py`) didn't match
     it, so `_page_value_scale_factor()` returned 1 instead of 1000 and every
     current_value on the page (Mutual Fund, Collective Fund, Employer Stock
     rows) was captured 1000x too low (e.g. Fidelity 500 Index staged as
     $100,610 instead of $100,610,000).
  2. Every row's "Description of Investment" column under the "Mutual Fund
     Securities" heading literally reads `"Mutual Fund - N Shares"` (category
     label + share count, not a real description).
     `_score_as_fund_name()` (`src/post_extract_validator.py`) scored that
     placeholder text higher than several real issuer names (any name
     without a recognized fund/share-class keyword — e.g. `FIDELITY
     CONTRAFUND`, `FIDELITY EMERGING MARKETS K`, `VANGUARD
     INFLATION-PROTECTED SECS ADM`) purely because the placeholder contains
     the word "fund" and is a plausible sentence length. Once the
     placeholder won, `_normalize_mf_name()` stripped the leading "Mutual
     Fund" label and was left with digits only, returning `""` — the
     name-quality gate then silently dropped the row. Net effect before the
     fix: only 2 of 13 real mutual funds were landing in `plan_mf_history_v3`
     for this ack_id, both also under-scaled 1000x.
- Fix status: committed `2e05e5d2`, deployed to EC2 (MD5-verified,
  2026-09-01).
  - Added a per-line `^\s*thousands\s*$` match to
    `_page_values_are_in_thousands()` for the bare-label case.
  - Added a trailing-count strip in `_score_as_fund_name()` so
    `"mutual fund - 738 shares"` re-checks as the generic category
    `"mutual fund"` (score -50) instead of out-scoring the real name.
  - Verified locally end-to-end (extraction → cleanup →
    `build_mf_rows_df()`): all 13 Mutual Fund Securities rows now land with
    correct names and correctly scaled values (e.g. Fidelity 500 Index →
    $100,610,000).
- Rerun status: **not yet run** — needs a full pipeline + classification
  rerun scoped to this ack_id.
  Expected recovery if run: `plan_mf_history_v3` gains the 11 currently
  missing Mutual Fund Securities rows for this ack_id, and all current
  current_value figures for this page (Mutual Fund, Collective Fund,
  Employer Stock) correct from 1000x-understated to their true values.

---

Template for a new entry:

```
## <Sponsor / identifying label>
- ack_id: `<ack_id>`
- Bug: <what was wrong, which function/file>
- Fix status: <committed/deployed commit hash, verification done>
- Rerun status: <not yet run / run on <date>, results>
```
