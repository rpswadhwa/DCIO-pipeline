# Plan Notes

Running log of per-plan diagnostic notes from manual review (PDF opened and
inspected, pipeline log/output cross-checked). One entry per plan, keyed by
ack_id. Append new entries below as plans are investigated — this is a
running notebook, not a prioritized action list (see `parking_lot.md` for
that).

Template for a new entry:

```
## <Sponsor Name>
- ack_id: `<ack_id>`
- Certified: $<amt> — Staged: $<amt> (<pct>% capture)
- Note: <finding>
- Status: <open / parking-lot / fixed>
```

---

## Brown University Defined Contribution Legacy Retirement Plan
- ack_id: `20251008130018NAL0005876449001`
- EIN 22-2768204, Plan No. 001. Certified `amt_mutual_funds`: $1,002,976,594.
  Total plan assets: $1,487,677,264.
- Current `plan_mf_history_v3` state: only 47 rows / $121,806,101 staged
  (~12% of certified) — well below what the plan's true schedule supports.
- Four distinct issues found (raw extraction pulled directly, bypassing
  cleanup/dedup, to isolate where each one happens):
  1. **Fidelity-prefix name truncation** (previously known/tracked bug,
     reconfirmed here). Raw extraction for the "Mutual funds offered by
     Fidelity" section (page 18) is clean — every fund name (Fidelity
     Contrafund K6, Fidelity 500 Index Fund, all 13 Freedom Index funds,
     etc.) comes through intact with the correct value. The name gets
     destroyed downstream in `cleanup_investment_names.py`'s
     `parse_issuer_and_investment()` — value survives, identity is lost
     (issuer_name collapses to bare "Fidelity", asset_class/asset_sub_class
     end up `PENDING_AI`).
  2. **TIAA section + Nuveen Lifecycle section: wrong column mapped at
     extraction time (NOT a downstream name-collision as first suspected)**.
     For "Mutual Funds offered by Teachers Insurance and Annuity
     Association" (~$237M) and "Teachers Insurance Lifecycle Funds"
     (Nuveen, ~$31.4M), the raw extractor grabs the generic
     investment-TYPE label ("Domestic equities", "Global equities", "Fixed
     income", "Multi-strategy funds") into `investment_description` instead
     of the real fund name, and leaves `issuer_name` blank entirely. Many
     rows end up with an identical blank-name/generic-description
     signature, so the downstream same-description dedup pass can't tell
     them apart and collapses them — this is why almost the entire $237M
     TIAA section and $31.4M Nuveen Lifecycle section are missing from v3.
  3. **Apparent duplicate re-extraction on page 18** (needs closer look, not
     confirmed harmful yet). Logs show page 18 splitting into 4 section
     table areas, with the Transamerica rows (Money Market Fund/Large Value
     Fund/Government Fixed Fund) extracted twice — once cleanly inline,
     once again via a remapped column reuse producing duplicate `row_id`s.
     Checked v3: not double-counted today, so whatever dedup catches it
     works in this case, but the re-splitting behavior itself looks
     fragile.
  4. **Page 19 (TIAA Traditional annuity, CREF Stock, CREF Annuities Other,
     Real Estate): correct values, blank asset_type**. All 17 rows extract
     with correct dollar values, but 15 of 17 come out with
     `asset_type=""` — nothing in `ROW_TYPE_PATTERNS`/`ASSET_TYPE_PATTERNS`
     recognizes "CREF Stock Fund," "CREF Annuities Other," "TIAA
     Traditional," etc. as row-level type declarations. Separately: the
     certified `amt_mutual_funds` figure only reconciles against the PDF if
     CREF Stock + CREF Annuities Other (~$480.6M combined) are counted as
     part of the mutual-fund bucket — but `ASSET_TYPE_PATTERNS` currently
     maps bare "CREF Accounts?" to `'Group Annuity Contract'`, a different
     bucket. This is a taxonomy/policy question, not just a blank-type bug,
     and needs a decision before fixing.
- Status: open — needs further investigation before any fix is written.
  Come back to this plan; do not fix inline yet.

---

## (Plan pending sponsor-name confirmation)
- ack_id: `20251007150830NAL0002566323001`
- Note: no asset type present in this plan's schedule (per user's own PDF
  review).
- Status: open

---

## (Plan pending sponsor-name confirmation)
- ack_id: `20250922133318NAL0005296289001`
- Note: no asset type present in this plan's schedule (per user's own PDF
  review).
- Status: open

---

## Navicent Health 403(b) Retirement Savings Plan
- ack_id: `20251009115215NAL0003540883001`
- Certified mutual fund total: $502,577,205 (VALIC block, page 17)
- Note: page 17/18 has two separate-annuity blocks: 29 real VALIC rows
  (correctly typed 'Mutual Fund' via "Total mutual funds" trailing line —
  confirmed 29, NOT 40 as the pipeline's own backfill log line first
  suggested) and 10 real "Lincoln VIP" variable-annuity sub-account rows on
  page 17 plus 9 more on page 18 (20 pages 17-18 total minus 1 "Guaranteed
  Investment Option" row = 19 Lincoln VIP rows).
- Issue 1 (open, NOT to be worked on per user instruction — user does not
  believe this is the right assessment): extraction also produces 10 phantom
  rows on page 17 with blank issuer/description and the literal string
  "Lincoln" sitting in `current_value` — duplicates alongside the real VALIC
  rows, not present in the source PDF text. Root cause not yet correctly
  diagnosed. Skip for now.
- Issue 2 (FIXED, verified locally, not yet deployed): the 19 real Lincoln
  VIP sub-account rows were staying blank-typed because no
  `ASSET_TYPE_PATTERNS` entry matched the trailing "Total separate accounts"
  line (only "pooled separate accounts" variants existed). Added a new bare
  `Separate\s+Accounts?` -> 'Separate Account' pattern in
  `src/asset_type_patterns.py` (ordered after the "pooled separate accounts"
  patterns so those still win when present). Re-ran
  `extract_tables_and_map()` locally against this PDF: all 19 Lincoln VIP
  rows now backfill correctly to `asset_type='Separate Account'`; the
  "Guaranteed Investment Option" row still correctly resolves to
  `Stable Value Fund` on its own.
- Fix committed (`f48a2e64`) and deployed to EC2 (MD5-verified, 2026-08-27).
- Rerun status: **not yet run** — code is live on EC2 but the pipeline hasn't
  been rerun against prod for this ack_id yet, so `plan_mf_history_v3`/
  `plan_holdings_staging` still hold the old data. Queued here per user's
  instruction, will run later.
- Status: open (fix ready, rerun pending)

---

## (Plan pending sponsor-name confirmation)
- ack_id: `20251010160314NAL0019037938001`
- Note: no asset type present in this plan's schedule (per user's own PDF
  review).
- Status: open

---

## Horizon Blue Cross Blue Shield of New Jersey Employees' Savings and Investment Plan
- ack_id: `20251009103902NAL0003502899001`
- Certified: $1,517,698,551 — Staged: $1,517,698,551 (100% capture, raw extraction sums exactly)
- Note: Clean extraction, all 37 rows recovered (Fidelity-heavy: FID 500 INDEX,
  FID CONTRAFUND K, FID FREEDOM K 2010-2070 series, FIMM GOVT INST, MIP II CL 1,
  plus non-Fidelity managers Franklin/Invesco/Loomis Sayles/MFS/New York Life/
  Neuberger Berman/North Square/American Beacon, plus self-directed brokerage).
  Schedule uses issuer=manager / description=specific fund name layout (opposite
  of Flowers Foods), which is the case `parse_issuer_and_investment()`'s
  manager-prefix rules were designed for — verified all Fidelity rows pass
  through correctly (issuer -> 'Fidelity', desc preserved as the real fund
  name), not affected by the semicolon-label bug just fixed.
  **However**: raw extraction returns `asset_type` blank for every row in this
  plan — the schedule is one flat table with no per-section subheadings
  (Mutual Fund / CIT / Money Market / Self-Directed Brokerage aren't split
  into separate table sections), so the section-heading-based asset_type
  inference in `text_extract.py` has nothing to key off. Not yet confirmed
  whether a later pipeline stage fills this in (e.g. via
  `_infer_schedule_of_title_asset_type` or a downstream classifier pass) or
  whether this plan would land in staging with blank/wrong asset_type per row.
- Status: open — asset_type blank-fill needs checking before this plan is
  considered fully clean.

## The Board of Trustees of the Leland Stanford Junior University
- ack_id: `20251010175027NAL0008070449001`
- Certified: $9,682,393,396 — Staged: $0 (0% capture)
- Note: Pages 18–22 have zero extractable text (chars=0 via pdfplumber),
  including page 21 where the "Registered Investment Company" column is
  visible on screen. Vector-flattened text or unOCR'd scan. `USE_OCR=0`
  was not enabled for this rerun.
- Status: parking-lot (see `parking_lot.md` #1)

## Duke Energy Corporation
- ack_id: `20251010135251NAL0018754754001`
- Certified: $9,569,696,000 — Staged: $0 (0% capture)
- Note: Extraction succeeded (24 rows, $11.26B, matches PDF total) but the
  "Institutional Funds" section (~$5.58B, 16 rows) isn't in
  `SECTION_HEADING_MAP` and got absorbed into the prior "Common Stock
  Funds" section, tagged `Employer Stock` instead of `Mutual Fund`. Zero
  rows tagged Mutual Fund.
- Status: parking-lot (see `parking_lot.md` #2, action item added)

## CVS Health Corporation
- ack_id: `20251006141123NAL0003741777001`
- Certified: $9,446,229,019 — Staged: $0 (0% capture)
- Note: Whole-document font/glyph corruption. Pages 4–86 of 86 extract as
  unmapped `(cid:NN)` glyph codes, not real text (chars extracted but all
  garbage). Only front boilerplate pages (1-3, 8) have readable text, which
  is why the classifier picked pages 3/8 (auditor's opinion letter, no
  data) instead of the real schedule. Distinct failure mode from Stanford
  (no text at all) and Duke (readable text, wrong heading map).
- Status: parking-lot (see `parking_lot.md` #3)

## Comcast Corporation
- ack_id: `20251007174512NAL0008660608003`
- Certified: $5,152,855,131 — Staged: $0 (0% capture)
- Note: NOT a parsing bug — this PDF is only 3 pages and is a **Master
  Trust / DFE filing** (Form 5500 Part I box "DFE" is checked: "Comcast
  Corporation Employee Savings Plans Master Trust", pooling 3 plan numbers
  12458/28024/28050, net assets $20.84B). It contains a "SUMMARY OF NET
  TRUST ASSETS" table, not a standard Schedule H Line 4i asset schedule —
  no "Schedule H"/"4i"/"Supplemental Schedule" language anywhere, so
  `Supplemental pages: []` is the classifier working correctly on a
  document type it was never built to parse. Comcast's $5.15B certified
  figure is presumably its allocated share of this pooled trust, reported
  via Schedule D on its own plan's Schedule H — not extractable from this
  PDF at all.
- Status: open (scope gap, not a bug — needs a decision on whether Master
  Trust/DFE filings are in scope, and if so a dedicated parser)

## Massachusetts Mutual Life Insurance Company
- ack_id: `20250919133721NAL0002366369001`
- Certified: — Staged: — (not yet cross-checked against comparison report)
- Note: Same failure mode as Comcast — PDF has a "SUMMARY OF NET TRUST
  ASSETS" heading, not a standard Schedule H Line 4i asset schedule. Likely
  another Master Trust/DFE filing, so no asset-type identifier / schedule
  language for the pipeline to key off. Second confirmed instance of the
  Master Trust scope gap (see `parking_lot.md` #4).
- Status: open (Master Trust/DFE scope gap, same as Comcast)

## GA WellSpan 403(b) Retirement Savings Plan (WellSpan Health)
- ack_id: `20251015130854NAL0006186912001`
- Certified: $2,494,530,966 — Staged: $0 (0% capture)
- Note: **Two separate problems**, not one.
  (1) **Genuine OCR gap** on pages 1-18 (e.g. page 17: 0 chars, 0 images,
  but 176 vector `rects` — text/table drawn as vector paths, same failure
  signature as Stanford). This is very likely where the fuller/audited
  schedule with a "Description of Investment" column (containing values
  like "Mutual Fund", per user's visual inspection) actually lives —
  invisible to text extraction, would need `USE_OCR=1`.
  (2) Separately, pages 19-20 contain a different, readable, abbreviated
  ticker-based "SCHEDULE OF ASSETS (HELD AT END OF YEAR)" schedule (44
  rows, ticker + maturity/rate/cost/value only, no description/asset-type
  column at all — resolved via a ticker legend on page 20 instead). Its
  total ($2,618,851,797) is only roughly close to certified ($2,494,530,966,
  ~5% off), suggesting it's a secondary/partial record, not the
  authoritative source. This page's header text ("SCHEDULE OF ASSETS (HELD
  AT END OF YEAR)" / "Schedule H, Part 4, Item I") also isn't in
  `config/keywords.yml`'s `supplemental_schedule_keywords` list, so the
  primary classifier missed it (`Supplemental pages: []`); a secondary
  "Simple-format fallback" caught it but mis-mapped columns, extracting 44
  rows with blank asset_type and a nonsensical $1.56 total value.
- Status: open (primary issue is likely the OCR gap on pages 1-18; the
  pages 19-20 bugs are secondary/lower-value given the total mismatch)

## Delta Air Lines, Inc.
- ack_id: `20251014143400NAL0006349954001`
- Certified: — Staged: near-zero (<0.2% captured; flagged as likely parse
  failure — stray line items only, from the 100-plan comparison report)
- Note: PDF opened for visual review; root cause not yet diagnosed.
- Status: open

## American Airlines, Inc. - Retirement
- ack_id: `20251013090403NAL0002149570001`
- Certified: $2,023,744,079 — Staged: $53,862 (0.003% capture)
- Note: **Not an extraction failure — classification bug.** PDF is a
  Master Trust for DC Plans of American Airlines and Affiliates, real
  Schedule H 4i format (TOTAL $27,388,265,073, pooled trust — AA's
  certified figure is presumably its allocated share). All 21 line items
  on page 1 were correctly extracted into `investments` with correct
  issuer names and values matching the PDF. But 19/21 rows got
  `asset_type = 'Employer Stock'` (wrong), 1 `'Bond'`, and only row 1 has
  a blank asset_type — none tagged `Mutual Fund`, despite most rows'
  descriptions literally containing "Registered Investment Company." The
  $53,862 staged total exactly equals row 1's value (the only blank-type
  row) — downstream MF-total logic appears to only pick up
  blank/unclassified rows, not misclassified ones. Root cause: composite
  "Separately managed account which includes: ... Registered Investment
  Company ..." descriptions don't match any section-heading pattern (full
  sentences, not clean headings), so the classifier defaults to
  `Employer Stock` instead of scanning the description text for RIC/CCT
  keywords.
- Status: parking-lot (see `parking_lot.md` #7)

## Johnson and Johnson
- ack_id: `20251015121015NAL0002381763001`
- Certified: — Staged: — (not yet cross-checked against comparison report)
- Note: **Correction**: despite being sourced from a different S3
  batch_date folder (`2026-07-26/`, not the `2026-06-28/` used for the
  original 100-plan rerun inputs), JNJ IS present in `run_100rerun.log`
  (line 2503) — it was processed. **New failure mode, distinct from
  WellSpan/Macy's/Gundersen.** 241 of this 243-page PDF's pages were
  classified as `Supplemental pages` (nearly the entire filing). Camelot
  found no usable table on virtually every page (`No tables found` x430),
  and the text-based fallback extracted only **2 investments total** from
  all 241 pages (`[OK] Extracted 1 investments from text` x2). Not an OCR
  issue — page 225's text is fully readable and clean (e.g.
  `PIMCO FDS SHORT TERM FLTG NAV ... 1,736,900.920 17,400,023.51
  17,398,536.52`, numbers correctly formatted, no stray spaces like
  Gundersen). Root cause looks structural: each holding is a **3-line
  stacked record** — fund/description name on its own line, then a
  quantity/cost/current-value line, then a fund-code+CUSIP line that
  repeats the same three numbers — rather than a single row with columns.
  Neither the table extractor nor the text-based fallback is built to
  parse a 3-line-per-record vertical layout, so almost nothing matches
  across the whole 241-page range.
- Status: open (large-scope: near-total extraction failure across a
  241-page document; needs a dedicated multi-line-record parser, not a
  small regex fix)

## Gundersen Lutheran Administrative Services Inc
- ack_id: `20251014161612NAL0003380401001`
- Certified: $2,101,982,929 — Staged: $0 (0% capture)
- Note: **New failure mode.** Page 23's "VALUE OF INTEREST IN REGISTERED
  INVESTMENT COMPANIES" section totals $2,101,982,928.92 — matches
  certified almost exactly, so this single page/section IS the whole
  schedule, and it's unambiguously Mutual Fund data (30+ funds: Vanguard
  Target Retirement series, Vanguard Institutional Index, DFA, Baird
  Aggregate Bond, etc.). Text is fully readable, not an OCR gap. But
  `current_value` numbers have a **stray space injected mid-digits** — e.g.
  `1 ,549,552.40` instead of `1,549,552.40`, `8 2,736,077.39` instead of
  `82,736,077.39` — a PDF character-kerning artifact that makes
  `pdfplumber` tokenize one number as two separate "words". Confirmed via
  full log block (lines 1997-2080): table extraction found 92-103 row
  candidates per page (17-24) but **0/N meaningful** on every page, and the
  text-based fallback ALSO found zero investments on all 8 pages
  (`[!] No investments found in text format either` x8). Root cause: every
  numeric-value regex in the pipeline (`\$?\s*\(?\s*[0-9][0-9,]*(?:\.[0-9]+)?\)?`)
  expects one contiguous token, so a value with an embedded space never
  matches as a value cell — every row gets rejected as non-data on both
  extraction paths, for the whole document. `pipeline.db` confirms 0
  investment rows and a `MISSING_EIN` synthetic key (EIN extraction also
  failed) for this plan.
- Status: parking-lot (see `parking_lot.md` #6)

## Froedtert ThedaCare Health, Inc.
- ack_id: `20250914082113NAL0000341680001`
- Certified: $1,966,990,552 — Staged: $0 (0% capture)
- Note: Real, nonzero character counts on nearly every page (e.g. page 4:
  21,919 chars) but text is 100% unmapped `(cid:NN)` glyph codes — same
  failure mode as CVS Health (#3), not a scanned-image OCR gap like
  Stanford/Avago. Font subset has no usable ToUnicode CMap.
- Status: parking-lot (see `parking_lot.md` #8)

## Avago Technologies U.S. Inc.
- ack_id: `20251009135917NAL0011609856001`
- Certified: $1,895,742,465 — Staged: $0 (0% capture)
- Note: Classic OCR gap, same pattern as Stanford (#1). All 19 pages: 0
  extractable chars, 1 full-page image per page, 0 rects, 0 lines. No text
  layer at all.
- Status: parking-lot (see `parking_lot.md` #9)

## TWDC Enterprises 18 Corp. (Disney Retirement Plan Master Trust)
- ack_id: `20251008155841NAL0006101921001`
- Certified: $1,692,043,384 — Staged: $0 (0% capture)
- Note: Text extracts cleanly and is fully readable (not OCR/cid
  corruption), but `find_tables()` returns 0 tables — same no-column-ruling
  symptom as Macy's (#5). Unlike Macy's, this is a 522-page composite
  report built from many short per-plan-number sub-reports (Disney Master
  Pension Trust plans BDVC etc.), so fund-code legends and dollar values
  likely live on different page ranges within each sub-report rather than
  one flat schedule. Needs its own parser design, not a direct copy of the
  Macy's fix.
- Status: parking-lot (see `parking_lot.md` #10)

## Bristol-Myers Squibb Company
- ack_id: `20260622142505NAL0006418033001`
- Certified: $1,588,326,065 — Staged: $0 (0% capture)
- Note: **Not a bug** — third confirmed Master Trust/DFE instance (see
  Comcast/MassMutual above). Actual PDF on EC2 (MD5-verified identical to
  a fresh S3 download, ruling out a batch_date file-mismatch) is only 3
  pages, 60KB: "Master Trust: Bristol-Myers Squibb Company" /
  "SUMMARY OF NET TRUST ASSETS" — no "Schedule H"/"4i"/"Schedule of
  Assets" language anywhere in the document, so `Supplemental pages: []`
  is correct behavior, not a failure. Net trust assets $12,940,489,916.01
  pooled across 3 participating plan numbers (38100, 38101, 77999); BMS's
  $1.59B certified figure is its allocated share, reported elsewhere via
  Schedule D — not present in this PDF. No asset-type column exists in the
  source document at all, which is why none was extracted.
- Status: open (Master Trust/DFE scope gap, same as Comcast/MassMutual —
  see `parking_lot.md` #4)

## Eaton Corporation
- ack_id: `20251014082229NAL0001120627001`
- Certified: $1,591,771,779 — Staged: $0 (0% capture)
- Note: Page 2 of 5 is a genuine, readable Schedule H Line 4i schedule
  ("EATON SAVINGS TRUST" / "SCHEDULE H - LINE 4I - SCHEDULE OF ASSETS
  HELD", chars=1598) — `find_tables()` correctly detects a 44-row table
  there (page 1 also has a 40-row table). Source table has only two
  columns, **Fund Name** and **Total Market Value** — no asset-type/
  description column exists in the document itself; category labels like
  "EATON FIXED INCOME" and "EATON SHARES FUND" appear as informal
  in-list section headers, not real columns, similar to Duke Energy's
  `SECTION_HEADING_MAP` gap. However, `pipeline.db`'s `investments` table
  for this plan holds only 2 rows total, both garbage EIN-header artifacts
  ("EIN: 47?" / "FORM" / "5500") — not the ~40+ real fund rows visible in
  the raw text/table extract. This is unreconciled: the 44-row table is
  detected by `find_tables()` but is not making it into `investments` at
  all (not merely missing asset_type). Root cause not yet isolated —
  needs a direct dump of `table.extract()` contents plus a
  `plan_holdings_staging` check for this ack_id.
- Status: open — under investigation (unresolved discrepancy between
  `find_tables()` output and loaded `investments` rows; not yet in
  `parking_lot.md`)

## Saint-Gobain Corporation
- ack_id: `20251015094802NAL0004496641001`
- Certified: — Staged: — (not yet cross-checked against comparison report)
- Note: PDF opened for visual review; root cause not yet diagnosed.
- Status: open

## Macy's, Inc.
- ack_id: `20251015182817NAL0011027730001`
- Certified: $2,252,908,000 — Staged: $0 (0% capture)
- Note: **New failure mode, distinct from all others found so far.** Text
  extraction is perfect — page 3's raw text is a clean, fully readable
  "SCHEDULE OF ASSETS HELD FOR INVESTMENT PURPOSES" (1,846 chars, real
  text, not cid garbage), including a legible $1.5B "VALUE OF INTEREST IN
  COMMON/COLLECTIVE TRUSTS" section (Northern Trust CCTs) and a "VALUE OF
  INTEREST IN REGISTERED INVESTMENT COMPANIES" section (JPMorgan MMKT,
  Macy's Mutual Fund Window, 8x Vanguard Target Retirement trusts). The
  problem is table geometry: this PDF has **no visible column-ruling
  lines**, so `pdfplumber.find_tables()` / Camelot returns just ONE table
  per page with the entire multi-line schedule crammed into a single giant
  cell per section, instead of one row per holding. Confirmed directly
  against `investments` table in `pipeline.db`: Macy's `sponsor_ein`
  (`MISSING_EIN::20251015182817NAL0011027730001`, itself notable — EIN
  extraction also failed) has **zero** investment rows, not just zero rows
  surviving a downstream filter/join.
  The pipeline log shows apparent progress ("Splitting page 3 into 2
  section table areas", "Reusing same-page column map for section table on
  page 3") because the custom section-splitter did detect 2 section
  boundaries within the blob, but since each "table" is really 1-2
  undivided mega-cells (not real columns), the per-row loop never finds
  header/value-shaped cells to populate `row_data`, so every row is
  silently dropped — no error, no "[OK] Extracted N" line, nothing.
  Likely fix: since the text layer is clean, `extract_text_based_investments`
  (the text-fallback path already used for WellSpan's ticker schedule)
  should work here — but it never triggers because pdfplumber DID return
  "a table" for this page, so the poor-quality-retry check at
  `text_extract.py:2145` never fires. May need a geometry-aware retry
  trigger (e.g. "a table with 1-2 giant rows and a huge single-cell text
  blob" should count as a failed table extraction, not a successful one).
- Status: open

## 2026-08-17 batch: Schedule H 4i "no section header" plans + Chubb re-check

User visually reviewed these PDFs and flagged them over two messages; investigated
here against the specific hints given. See cross-plan synthesis at the bottom of
this file for the enhancement recommendation that comes out of this batch.

## Thomas Jefferson University
- ack_id: `20251015122510NAL0002403219001`
- Certified: $2,371,811,436 — Staged: $230,937,196 (9.7% capture)
- Note: User: "has no asset type, but should be extracted." Confirmed —
  local re-extraction gets 92 rows but 84/92 (91%) have blank `asset_type`,
  and raw dollar sum is $11.0B (464% of certified, pre-dedup). This is the
  same over-capture root cause as finding #22 in `parking_lot.md` ("Mode 3"
  section-type bleed) — `apply_section_typing_stage` finds `retyped_non_mf: 0`
  on this PDF, i.e. the current `_SECTION_MAP` pattern list doesn't match
  Jefferson's section-header wording at all, so the fix doesn't help here.
- Status: open — needs `_SECTION_MAP` extended for this doc's specific
  header language (not yet identified via raw-text dump).

## The Progressive Corporation & Its Participating Subsidiaries
- ack_id: `20260513074811NAL0006610787001`
- Certified: $2,043,916,868 — Staged: $144,544,066 (7.1% capture)
- Note: User: "should have extracted, clear data and asset type." Same
  mechanism as Jefferson — 93 rows, 66/93 (71%) blank asset_type, raw sum
  $24.8B (1213% of certified). `apply_section_typing_stage` gets
  `retyped_non_mf: 0` here too. Confirmed via master gap list (2026-08-16)
  that ack_id `20260513074811NAL0006610787001` is the correct one for this
  batch (a different ack_id seen in `pipeline_stage_compare.csv` is from an
  unrelated/older batch — non-issue).
- Status: open — same fix needed as Jefferson.

## Toyota Motor North America, Inc. Retirement Savings Plan
- ack_id: `20251008114247NAL0009333472001`
- Certified: $2,018,329,458 — Staged: $138,530,923 (6.9% capture)
- Note: User: "has a clear column with mutual fund. not sure how we missed
  it." **Confirmed directly from the raw PDF text** — page 21 (and page 33,
  a second copy of the same schedule) lists each fund with an explicit
  inline type token right next to the row, e.g. `FIDELITY BALANCED FUND
  CLASS K   Mutual fund   678,306,426`, `VANGUARD INST INDEX   Commingled
  fund   1,893,295,276`, `TOYOTA ADR FUND   Company Stock fund
  152,636,321`. This is a genuinely different pattern from the section-
  header approach the pipeline uses: there is no section heading at all for
  most of this table — the type is a **per-row inline column**, not a
  section boundary. Local extraction gets 1,222 rows but 610/1222 (50%)
  blank asset_type, meaning the mapper is not reading this inline type
  column into `asset_type` even though it's right there in the text.
- Status: open — needs a new "inline per-row type column" detector,
  distinct from the section-heading detector `section_typing.py` already
  has. See cross-plan synthesis below — this is the same underlying gap as
  the 5 Schedule H plans below, just manifesting as a column instead of
  embedded description text.

## Presbyterian Healthcare Services
- ack_id: `20250715090357NAL0001184387001`
- Certified: $904,086,154 — Staged: $81,636,103 (9% capture)
- Note: User: "Presbyterian has a clear asset type heading. not sure why we
  miss it." The actual Schedule H 4i table is on pages 19-20, columns `(a)
  Party in Interest / (b) Identity of Issuer / (c) Description / (d) Cost /
  (e) Current Value` — a standard DOL schedule with **no section headers
  separating asset types at all**; the "clear heading" the user saw is
  likely the column header row itself ("Description"), not a type-per-
  section heading `classify_section_line()` can match. After section-typing,
  30/31 rows are still blank asset_type (3% of certified typed as MF).
- Status: open.

## University Hospitals Health System
- ack_id: `20251014172558NAL0001702339001`
- Certified: $2,378,021,777 — Staged: $250,579,629 (10.5% capture)
- Note: User: "has no asset type but has investments as a heading."
  Confirmed — the real Schedule H 4i table (columns `(b)(c)(e)`) is on
  pages 15-16 with no per-section type headers. Note: page 12 does contain
  the literal text "Mutual funds" but it's from the **fair-value hierarchy
  footnote table** (Level 1/2/3 disclosure), not the actual investment
  schedule — a red herring if matched naively. After section-typing, 44/45
  rows still blank (2% of certified typed as MF).
- Status: open.

## Cedars-Sinai Medical Center
- ack_id: `20251015113153NAL0002330259001`
- Certified: $2,234,176,687 — Staged: $344,306,145 (15.4% capture)
- Note: User: "no asset type." Confirmed — same Schedule H 4i shape, no
  section headers. After section-typing: 33/35 rows blank, only 3% typed MF.
- Status: open.

## The Carnegie Mellon University Faculty and Staff Retirement Plan
- ack_id: `20251015121322NAL0004812305001`
- Certified: $2,340,772,588 — Staged: $373,342,210 (15.9% capture)
- Note: User: "no asset type." Confirmed — same shape. Section-typing does
  catch a "Participant Loan" section heading here (2 rows), but the bulk of
  the schedule (31/39 rows) stays blank; only 2% typed MF.
- Status: open.

## Memorial Hermann Health System 403(b) Employee Retirement Savings Plan
- ack_id: `20251013141455NAL0001402353001`
- Certified: $3,100,785,291 — Staged: $477,524,253 (15.4% capture)
- Note: User: "no asset type." Confirmed — small 5-page schedule, same
  Schedule H 4i shape, 19/22 rows blank, 5% typed MF.
- Status: open.

## HonorHealth 403(b) Retirement Security Plan
- ack_id: `20251009185745NAL0007452353001`
- Certified: $973,239,381 — Staged: $146,614,816 (15.1% capture)
- Note: User: "no asset type." Confirmed — same shape; page 18's schedule
  literally has row text like `"EAIC Evergreen Annuity Contract"` embedded
  in the description column, which is exactly the kind of inline type text
  a description-parser could catch. After section-typing: 21/23 rows blank,
  0% typed MF (the one annuity-type row present doesn't map to Mutual Fund,
  correctly, but nothing gets typed as MF either since there isn't any MF
  content captured).
- Status: open.

## Mizuho Americas 401(k) Plan
- ack_id: `20251011072951NAL0005041267001`
- Certified: $979,077,559 — Staged: $245,372,964 (25.1% capture)
- Note: User hypothesized "the page might have got completely left out"
  since it has asset type but no useful heading. **This hypothesis does not
  hold up** — pages 13-20 ARE classified supplemental and DO extract (this
  document actually has a clean "Mutual Fund" section heading, correctly
  detected). The real issue was over-capture from the schedule appearing
  twice in the PDF (once on pages ~13-16, again ~17-20): before dedup, 227%
  of certified; after `apply_section_typing_stage`'s dedup step, 106% of
  certified with 27/28 rows correctly typed Mutual Fund — i.e. this plan is
  in good shape once the existing Stage 2 dedup fix runs. Not a
  "left-out page" problem at all; a duplicate-schedule dedup problem, now
  resolved by tooling that already exists (just not deployed/default-on).
- Status: parking-lot (existing `SECTION_TYPING=1` fix already resolves
  this one; just needs to be enabled for this plan/cohort).

## Chubb US 401(k) Plan (Chubb INA Holdings Inc.)
- ack_id: `20251013102609NAL0001106817004`
- Certified: $1,477,658,783 — Staged: $75,153,324 (5.1% capture)
- Note: User (mid-session follow-up): "should be a clean extraction, not
  sure if we've fixed the issue there." **Checked directly — it is NOT
  fixed.** Local extraction actually does reasonably well at *typing* rows
  (asset_type breakdown is populated: Separately Managed Account, Mutual
  Fund, Common/Collective Trust Fund, etc., not mostly blank like the
  plans above) — the problem here is a severe **over-capture / duplication**
  issue instead. Raw extraction: 379 rows, $27.9B (1886% of certified).
  After `apply_section_typing_stage` (dedup + retyping): still 253 rows,
  $19.5B all-rows / $3.78B MF-only (256% of certified) — a 2.5x overcount
  even after the existing fix runs. Cause: pages 19-20 and pages 24-25 each
  independently extract ~64 and ~60 investments respectively — nearly
  identical counts, strongly suggesting the same schedule (likely
  current-year vs. prior-year comparison columns, a common Schedule H
  pattern) is present twice in the PDF and being summed as if both copies
  were distinct holdings, and the existing Stage 2 dedup doesn't fully
  catch it because these dedup keys apparently differ enough between the
  two copies to survive as separate rows.
- Status: open — distinct root cause from the "no asset type" plans above;
  needs investigation into why Stage 2 dedup isn't collapsing the two
  near-identical page ranges (19-20 vs 24-25).

## Pending sponsor (large filing, only 1 MF row captured)
- ack_id: `20251209141357NAL0003820562001`
- Certified: — Staged: $1,237,729,022 (1 row: Fidelity 500 Index,
  `validation_status=MANUAL_REVIEW`)
- Note: User: "no asset type." Large filing (14MB PDF, many pages). Same
  shape as the Presbyterian/University Hospitals/Cedars-Sinai/Carnegie
  Mellon/Memorial Hermann/HonorHealth batch above — real schedule likely has
  no section-heading structure the classifier can key off, so only 1 row got
  typed at all. Not yet root-caused in detail (no local re-extraction run
  yet) — come back to this.
- Status: open — come back to later.

## Pending sponsor (only 1 MF row captured, Vanguard Inst Idx)
- ack_id: `20251007153158NAL0002578931001`
- Certified: — Staged: $106,075,241 (1 row: Vanguard Inst Idx Inst Plus,
  `validation_status=MANUAL_REVIEW`)
- Note: User: "no asset type." Same shape as the other "no asset type"
  plans above/below — likely a Schedule H 4i table with no section-heading
  structure the classifier can key off, so only 1 row got typed. Not yet
  root-caused in detail (no local re-extraction run yet) — come back to
  this.
- Status: open — come back to later.

## Pending sponsor (only 1 MF row captured, Vanguard Inst Idx Inst Plus)
- ack_id: `20251015150753NAL0009946722001`
- Certified: — Staged: $490,473,003 (1 row: Vanguard Institutional Index Fund
  Institutional Plus Shares, `validation_status=MANUAL_REVIEW`)
- Note: User: "no asset type." Same shape as the 3 plans directly above —
  root cause now confirmed (see synthesis entry below): headerless documents
  lose every row except one from a big-4 manager (Vanguard/Fidelity/
  BlackRock/PIMCO) whose exact fund name happens to already be cached in the
  mf_mapping_enrichment crosswalk.
- Status: open — root cause identified, fix not yet designed.

## ROOT CAUSE FOUND: "single row survives" pattern on headerless plans
- Applies to: `20251209141357NAL0003820562001`, `20251007153158NAL0002578931001`,
  `20251015150753NAL0009946722001`, and likely others with the same shape
  (1 row in v3, always a well-known Vanguard/Fidelity/BlackRock/PIMCO fund,
  always `validation_status=MANUAL_REVIEW`).
- Mechanism (two stacked filters, not one "backup asset type" rule):
  1. **`asset_type` gate**: when a document has no section-heading structure,
     `text_extract.py` can't set `asset_type` from headings. LLM enhancement's
     `infer_asset_type()` fallback (`llm_enhance_investments.py` lines 74-76)
     then types a row 'Mutual Fund' ONLY if its name contains "vanguard",
     "fidelity", "blackrock", or "pimco" — everything else falls to 'Other'.
     Prod load (`build_mf_rows_df(..., mf_only=True)`) only inserts rows
     tagged `asset_type='Mutual Fund'` into `plan_mf_history_v3` — so every
     non-big-4-manager fund on a headerless document is silently dropped,
     not just miscategorized.
  2. **`asset_class`/`asset_sub_class` gate**: `src/mf_mapping_enrichment.py`
     copies `asset_class`/`asset_sub_class` onto a row only if its
     `raw_entity_name` exact-matches (case/whitespace-insensitive) a name
     already classified in a prior plan; names it's never seen become
     `PENDING_AI`. "Vanguard Institutional Index Fund Institutional Plus
     Shares" / "Fidelity 500 Index" are common enough to already be cached
     with real values (Equity / US Total Market, etc.), which is why the
     surviving row looks fully classified.
- Net effect: these are NOT "1-fund plans" — they are real under-capture
  bugs. Every other genuine MF holding on a headerless document (State
  Street, T. Rowe Price, American Funds, etc.) gets dropped by the
  `infer_asset_type()` 4-manager whitelist, not surfaced anywhere for review.
- Fix not yet designed — candidate approaches: (a) widen the
  `infer_asset_type()` fallback beyond 4 manager names (risky — bare "fund"
  keyword match already exists above it; need to check why that doesn't
  already catch these), (b) add a row-level fallback ahead of LLM enhancement
  when zero rows in a plan got a section-heading asset_type (treat the whole
  plan as MF by default, matching Schedule H 4i table conventions), or
  (c) flag these plans for manual review instead of silently dropping
  non-whitelisted rows. Needs discussion before implementing.
- Status: open — root cause confirmed, fix approach not yet decided.

## Pending sponsor (only 1 MF row captured, Spartan 500 Index)
- ack_id: `20251010153535NAL0004377779003`
- Certified: — Staged: $243,251,067 (1 row: Spartan 500 Index Fund,
  `raw_sponsor_name` blank, `normalized_sponsor_name=Fidelity Investments`,
  `validation_status=MANUAL_REVIEW`)
- Note: User: "same issue...no asset type." Same shape as the "ROOT CAUSE
  FOUND" entry above — headerless document, only the Fidelity-named row
  survived the `infer_asset_type()` 4-manager whitelist + the
  `mf_mapping_enrichment` name-cache. 6th confirmed instance of this bug.
- Status: open — root cause confirmed, fix approach not yet decided.

## Pending sponsor (only 1 MF row captured, Vanguard Inst Idx)
- ack_id: `20251209102447NAL0003446401001`
- Certified: — Staged: $158,541,938 (1 row: Vanguard Institutional Index
  Fund, `validation_status=MANUAL_REVIEW`)
- Note: User: "same issue...no asset type." Same shape as the "ROOT CAUSE
  FOUND" entry above. 7th confirmed instance of this bug.
- Status: open — root cause confirmed, fix approach not yet decided.

## Pending sponsor (only 1 MF row captured, Vanguard Instl Index NR)
- ack_id: `20251014153055NAL0003290785001`
- Certified: — Staged: $219,783,664 (1 row: The Vanguard Group Vanguard
  Instl Index Fund NR, `validation_status=MANUAL_REVIEW`)
- Note: User: "in parallel we have to fix this... lets discuss." Same shape
  as the "ROOT CAUSE FOUND" entry above. 8th confirmed instance of this bug.
  Fix design discussion started this session (see rerun_queue.md / commit
  history once a fix lands).
- Status: open — root cause confirmed, fix design in progress.

## Pending sponsor (4 MF rows, mixed managers — GOOD PLAN, not the bug pattern)
- ack_id: `20250924132044NAL0017172210001`
- Certified: — Staged: $240,014,606 (4 rows: Vanguard Cash Rsrv Federal
  $79,394,558; DFA Global Allocation 6040 Port Instl $30,358,800; PGIM
  Global Total Return $54,664,071; Vanguard Short-Term Investment-Grade I
  $75,597,177 — all `validation_status=MANUAL_REVIEW`)
- Note: User: "bad source data. this is a good plan. 100% covered." Does
  NOT match the "1-row survivor" pattern (finding #23 in parking_lot.md) —
  multiple non-big-4 managers (DFA, PGIM) are present and correctly
  classified, so this plan's small row count is genuine, not a symptom of
  the headerless-doc bug.
- Status: closed — confirmed good, no action needed.

## Pending sponsor (only 1 MF row captured, Vanguard Inst Idx Inst Plus)
- ack_id: `20260107152947NAL0009140480001`
- Certified: — Staged: $376,222,572 (1 row: "VANGUARD Vanguard Inst Idx Inst
  Plus -", `validation_status=MANUAL_REVIEW`)
- Note: User: "same issue." Same shape as the "ROOT CAUSE FOUND" entry
  above. 9th confirmed instance of this bug.
- Status: open — root cause confirmed, fix design in progress.

## Pending sponsor (3 MF rows, PENDING_AI row among them — Capital Group/PIMCO)
- ack_id: `20260415141735NAL0019944529001`
- Certified: — Staged: $163,920,160 (3 rows: American Funds Washington
  $32,615,966 `PENDING_AI`/`PENDING_AI`; PIMCO Income Institutional Fund
  $3,285,877 Fixed Income/Strategic-Unconstrained Income; American Funds
  Growth Fund $128,018,317 Equity/US Large Cap — all `validation_status=
  MANUAL_REVIEW`)
- Note: User: "again...no asset type." Same underlying gap as the "ROOT
  CAUSE FOUND" entry above (finding #23 in parking_lot.md) — the American
  Funds Washington row has no `mf_mapping_enrichment` cache hit yet, so it's
  stuck at `PENDING_AI`/`PENDING_AI` even though the other two non-big-4
  rows (Capital Group Growth Fund, PIMCO) did get real classifications. 10th
  confirmed instance of this bug family.
- Status: open — root cause confirmed, fix design deferred (parking_lot.md #23).

## Pending sponsor (only 1 MF row captured, Vanguard Inst Idx Inst Plus)
- ack_id: `20260107152644NAL0008199489001`
- Certified: — Staged: $666,936,303 (1 row: "VANGUARD Vanguard Inst Idx Inst
  Plus -", `validation_status=MANUAL_REVIEW`)
- Note: User: "same story no asset type." Same shape as the "ROOT CAUSE
  FOUND" entry above. 11th confirmed instance of this bug.
- Status: open — root cause confirmed, fix design deferred (parking_lot.md #23).

## Pending sponsor (only 1 MF row captured, Fidelity 500 Index Fund)
- ack_id: `20251015151614NAL0004962433001`
- Certified: — Staged: $418,269,838 (1 row: "Fidelity Management & Research
  Company LLC 500 Index Fund", `validation_status=MANUAL_REVIEW`)
- Note: User: "same story." Same shape as the "ROOT CAUSE FOUND" entry
  above. 12th confirmed instance of this bug.
- Status: open — root cause confirmed, fix design deferred (parking_lot.md #23).

## Fish & Richardson Profit Sharing 401(k) Plan
- ack_id: `20251027143530NAL0005871121001`
- Certified: — Staged: $105,675,596 (2 rows: "Vanguard Institutional Index
  Fund" $95,671,375 and "Artisan Partners Artisan International
  Institutional Fund" $10,004,221, both `MATCHED_CANONICAL`/`MANUAL_REVIEW`)
- Note: User: "no asset type." Same shape as the "ROOT CAUSE FOUND" entry
  above — 13th confirmed instance of this bug.
- Status: open — root cause confirmed, fix design deferred (parking_lot.md #23).

## Live Nation Entertainment, Inc. 401(k) Savings Plan
- ack_id: `20251014105114NAL0005497426001`
- Certified: — Staged: $182,209,216 (1 row: "Growth Company Fund",
  `validation_status=MANUAL_REVIEW`, blank sponsor match)
- Note: User: "no asset type." Same shape as the "ROOT CAUSE FOUND" entry
  above — 14th confirmed instance of this bug.
- Status: open — root cause confirmed, fix design deferred (parking_lot.md #23).

## Jones Day (Defined Contribution Master Trust)
- ack_id: `20251013182017NAL0000895075001`
- Certified: — Staged: 2 rows — "ABRDN Physical Silver Shares ETF" $25,833
  and "American Funds" $155,993,017, both `asset_class`/`asset_sub_class =
  PENDING_AI`, `MATCHED_CANONICAL`/`MANUAL_REVIEW`
- Note: User: "same issue, no asset type." Same shape as the "ROOT CAUSE
  FOUND" entry above — 15th confirmed instance of this bug.
- Status: open — root cause confirmed, fix design deferred (parking_lot.md #23).

## Kohls Corporation Master Trust
- ack_id: `20251210095920NAL0005401200001`
- Certified: — Staged: 3 rows only ($37,771,466.13 Baird Core Plus Bond,
  $71,231,879.50 American EuroPacific Growth R6 — both `asset_type='mutual
  fund'`, `PENDING_AI`/`PENDING_AI` — plus $7,776,445.57 State Street US
  Bond Index CCT)
- Note: Northern Trust composite participation schedule (region-subgrouped,
  `MFO` prefix, `Total <Region>` rollups) — same format as Howmet (#12) /
  Lumen (#15). PDF's real "Value of Interest in Registered Investment
  Companies" (Mutual Fund) section has 9 rows totaling ~$583.2M; only 2 of
  9 (~$109M, 19%) reached staging. Common/Collective Trust section similarly
  gutted (1 of ~11 rows captured). Each surviving row's raw_entity_name is
  glued to the section/region heading that precedes it, and specifically to
  whichever row sits at a heading boundary or page break — same
  wrapped-heading/continuation-line mechanism as Lumen (#15), confirmed on a
  second filer/custodian (Northern Trust vs. Lumen's). Not the headerless
  "1-row survivor" bug (#23) — this plan has full section-heading structure.
- Status: parking-lot — see `parking_lot.md` #15 (new confirming instance);
  user decision (2026-08-26): build a dedicated parser for this
  composite-participation-schedule format, since multiple plans share this
  exact structure.

## VillageMD 401(k) Retirement Savings Plan
- ack_id: `20251015124623NAL0004579633001`
- Certified: — Staged: $233,711,173 (1 row: "Fidelity 500 Index Fund",
  `MATCHED_CANONICAL`/`MANUAL_REVIEW`)
- Note: User: "no asset type." Same shape as the "ROOT CAUSE FOUND" entry
  above — 16th confirmed instance of this bug.
- Status: open — root cause confirmed, fix design deferred (parking_lot.md #23).

## William Marsh Rice University Supplemental 403(b) Plan
- ack_id: `20251014195131NAL0003665441001`
- Certified: — Staged: $110,992,854 (1 row: "College Retirement Equities
  Fund CREF", `asset_class`/`asset_sub_class = PENDING_AI`,
  `MATCHED_CANONICAL`/`MANUAL_REVIEW`)
- Note: User: "same issue no asset type." Same shape as the "ROOT CAUSE
  FOUND" entry above — 17th confirmed instance of this bug.
- Status: open — root cause confirmed, fix design deferred (parking_lot.md #23).

## Southwest Research Institute Retirement Plan
- ack_id: `20250930092302NAL0005410659001`
- Certified: $1,246,743,370 (amt_mutual_funds, `Plan by MF.csv`; total plan
  assets $1,698,290,111, certified MF% = 73%) — Staged: $200,581,791 (1 row:
  "CREF Stock R3 Equities", `MATCHED_CANONICAL`/`MANUAL_REVIEW`) — 16.1%
  capture
- Note: User supplied the full raw PDF holdings list — 16 TIAA-CREF equity
  rows totaling $856,508,041 (CREF Stock R3, CREF Growth R3, CREF Global
  Equities R3, TIAA-CREF S&P 500 Index, TIAA-CREF Equity Index, TIAA-CREF
  Social Choice Equity, TIAA-CREF International Equity Index, TIAA-CREF
  Small-Cap Equity, CREF Equity Index R3, TIAA-CREF Real Estate Securities,
  TIAA-CREF Large-Cap Value, TIAA-CREF Mid Cap Value, TIAA-CREF Growth and
  Income, TIAA-CREF Mid-Cap Growth, TIAA-CREF Emerging Market Equity Index,
  TIAA-CREF International Equity) — only 1 of 16 reached staging. Same
  shape as the "ROOT CAUSE FOUND" entry above — "CREF Stock R3 Equities" is
  a well-known fund name already cached in the `mf_mapping_enrichment`
  crosswalk, while every other TIAA-CREF fund name (never seen before) got
  dropped by the same mechanism. 18th confirmed instance of this bug.
  **Open question (parking item):** all 16 rows are tagged `asset_class =
  'Equity'` in the source, not an explicit "Mutual Fund" label — need to
  decide whether `asset_class = 'Equity'` (TIAA-CREF variable annuity /
  equity accounts) should map into the certified `amt_mutual_funds` total
  at all, or if these are a different certified bucket entirely. Affects
  how "capture %" should even be computed for this plan.
- Status: open — root cause confirmed, fix design deferred (parking_lot.md
  #23); PLUS a separate open question on Equity-asset_class-to-MF mapping
  (see parking_lot.md action items).

## Twin City Ironworkers Defined Contribution Fund
- ack_id: `20250819144412NAL0002298337001`
- Certified: $365,045,161 (amt_mutual_funds, `Plan by MF.csv`; total plan
  assets $379,551,424, certified MF% = 96%) — Staged: $59,275,286 (1 row:
  "Nuveen International Equity Index R6", `MATCHED_CANONICAL`/
  `MANUAL_REVIEW`) — 16.2% capture
- Note: User: "no asset type." Same shape as the "ROOT CAUSE FOUND" entry
  above — 19th confirmed instance of this bug.
- Status: open — root cause confirmed, fix design deferred (parking_lot.md #23).

## Jefferson Health-Northeast 403(b) Plan (f/k/a Aria Health 403(b) Plan)
- ack_id: `20251015132524NAL0009462242001`
- Certified: $397,056,575 (amt_mutual_funds, `Plan by MF.csv`; total plan
  assets $428,397,658, certified MF% = 93%) — Staged: $58,145,551 (1 row:
  "Vanguard Inst Idx Inst Plus", `MATCHED_CANONICAL`/`MANUAL_REVIEW`) —
  14.6% capture
- Note: User: "no asset type." Same shape as the "ROOT CAUSE FOUND" entry
  above — 20th confirmed instance of this bug.
- Status: open — root cause confirmed, fix design deferred (parking_lot.md #23).

## University Hospitals Health System
- ack_id: `20251014172558NAL0001702339001`
- Certified: $2,378,021,777 (amt_mutual_funds, `plan_master_index_universe`;
  total plan assets $2,463,799,725) — Staged: $250,579,629 (1 row:
  "Vang Target Retirement 2030", asset_class `Target Date` / asset_sub_class
  `Target Date 2030`) — 10.5% capture
- Note: User: "no asset type identified." Same shape as the "ROOT CAUSE
  FOUND" entry above — 21st confirmed instance of this bug.

## Pacific Coast Benefits Trust Fund
- ack_id: `20260330192503NAL0015004466001`
- Certified `amt_mutual_funds`: $313,904,150 (`plan_master_index_universe`).
- User visually confirmed the source PDF's real "Mutual and Exchange-Traded
  Funds" section (physical page 17 — this filer's own printed/footer page
  number is "15", a +2 offset from the physical PDF page index) and pasted
  its content directly, flagging that the fund rows had been mistyped and
  the values looked wrong.
- **Bug 1 (heading gap)**: `SECTION_HEADING_MAP` in
  `extract_text_based_investments()` (`src/text_extract.py`) had no entry
  for this filer's exact heading wording, "Mutual and Exchange-Traded
  Funds" — so `current_section_type` stayed stuck at 'Real Estate' (carried
  over from the immediately preceding "Real Estate Investment Trusts"
  heading) for the entire section. Fund rows got typed 'Real Estate' and
  were silently dropped from `plan_mf_history_v3` (`build_mf_rows_df`'s
  `_non_mf` set excludes real-estate-typed rows entirely), which is why
  none of the 6 real fund rows (EUPAC Fund, SPDR S&P 500 ETF, PIMCO Income
  Fund Ins, Goldman Sachs FS Treas Oblig, Allspring Treasury Money Market,
  Dodge and Cox International Stock Fd) ever reached v3 — instead, blank-
  typed individual common-stock rows from elsewhere on the page (Netflix,
  Hartford, BlackRock, Goldman Sachs Group, Schwab, JPMorgan Chase) made it
  into v3 and got spuriously sponsor-matched to canonical asset-manager
  brand names via `run_classification.py`'s dictionary matching, since
  those *company names* happen to match asset-manager names in the
  sponsor dictionary.
- **Bug 2 (Fair Value / Cost column swap, separately discovered)**: this
  filer's own printed header reads "... Value Cost" — Fair Value column
  BEFORE Cost column, reversed from the IRS's standard Cost-then-Value
  order. The existing trailing-number value parser always takes the LAST
  number on a data line as `current_value`, which is correct for the
  standard order but was silently capturing Cost instead of Fair Value for
  every row on this page (confirmed on both the Mutual/ETF section and the
  Common Stocks section — plan-wide for this filer, not section-specific).
- Fix: added `'mutual and exchange-traded fund': 'Mutual Fund'` to
  `SECTION_HEADING_MAP`; added a narrow, header-gated `_value_before_cost`
  check (only fires when the page's own header literally contains "value
  cost") plus a `_dual_trailing_pattern` that captures the FIRST of two
  trailing numbers (Fair Value) instead of the last (Cost) when that
  condition holds — single-value lines and standard-order filers are
  unaffected.
- Verified: all 6 fund rows plus the TOTALS row now show
  `asset_type='Mutual Fund'` with correct Fair Value figures (e.g. EUPAC
  Fund $36,074,618, not its $29,899,242 Cost); computed section total
  $313,904,150 — exact match to certified `amt_mutual_funds`.
- Not yet investigated (flagged, out of scope): certified `amt_real_estate
  = 0.00` doesn't match the extracted $1,561,810 REIT section total on the
  same page — possibly a certified-bucket labeling quirk, pre-existing
  behavior unrelated to either bug above.
- Fix committed (`8e65ed2e`), not yet deployed to EC2.
- Status: fixed locally, pending deploy + rerun (see `rerun_queue.md`).
- Status: open — root cause confirmed, fix design deferred (parking_lot.md #23).

## Wake Forest University Baptist Medical Center
- ack_id: `20251014114613NAL0001328003001`
- Certified: $3,160,533,459 (amt_mutual_funds, `plan_master_index_universe`;
  total plan assets $3,642,720,552) — Staged: $335,924,701 (1 row:
  "Vanguard Instl Index Instl Plus", asset_class `Equity` / asset_sub_class
  `Diversified Equity`) — 10.6% capture
- Note: User: "no asset type." Same shape as the "ROOT CAUSE FOUND" entry
  above — 22nd confirmed instance of this bug.
- Status: open — root cause confirmed, fix design deferred (parking_lot.md #23).

## Oracle Corporation 401(k) Savings and Investment Plan
- ack_id: `20260427164405NAL0013754176001`
- Staged (`plan_mf_history_v3`, pre-fix): 3 rows only — RBC Emerging Markets
  Equity Fund Class I ($107,456,000), DFA US Targeted Value I
  ($234,980,000), DFA Emerging Markets Core Equity Portfolio Institutional
  Class ($107,457,000). `plan_holdings_staging` had ~90+ additional rows
  (Vanguard, Fidelity, Galliard, Allspring, PIMCO CT, William Blair, etc.)
  sitting at `asset_class = PENDING_AI` — a separate, still-open
  classification/promotion issue, not an extraction bug.
- Bug found: page 14 of the PDF (the schedule's own first page) is titled
  only "Notes to Financial Statements" — this filer never prints "Schedule
  H, Line 4(i)" or "Schedule of Assets" anywhere on that page, even though
  it has the full standard column-header row (Identity of Issue /
  Description of Investment / Current Value / Maturity Value) and starts
  the "Registered Investment Companies:" section. `has_schedule_marker` in
  `extract_text_based_investments()` (`src/text_extract.py`) requires that
  literal phrase and returned `False`, so the whole page — 5 real mutual
  fund rows — was silently skipped: Dodge & Cox International Stock Fund
  ($423,736,000), Dodge & Cox Stock Fund ($1,141,288,000), Fidelity
  Balanced Fund—Class K ($870,639,000), Fidelity Worldwide Fund
  ($410,529,000), PIMCO Inflation Response Multi-Asset Fund Institutional
  ($423,736,000). Confirmed via word-level (x/y) coordinates that the
  Dodge & Cox International and PIMCO rows are two independently-printed
  rows that coincidentally carry the identical share count and dollar
  value — not a column-bleed/duplicate-row artifact.
- Fix: added a narrow, Oracle-only exception at the `has_schedule_marker`
  gate — treat the page as a valid schedule page if the text contains both
  "ORACLE" and "NOTES TO FINANCIAL STATEMENTS", per user's explicit
  instruction to scope this to Oracle only rather than widen the general
  marker. Verified locally: page 14 now returns all 5 rows, correctly
  typed Mutual Fund, values scaled correctly (×1000 for the page's "in
  thousands" units).
- Status: fix implemented and locally verified; not yet committed/deployed.
  The separate `PENDING_AI` classification backlog for the ~90 other funds
  on this plan remains open and untouched.

## Thomas Jefferson University
- ack_id: `20251015122510NAL0002403219001`
- Certified: $2,371,811,436 (amt_mutual_funds, `plan_master_index_universe`;
  total plan assets $2,792,479,411) — Staged: $230,937,196 (1 row: "Inst
  Idx Inst Plus", asset_class `Equity` / asset_sub_class `US Total Market`)
  — 9.7% capture
- Note: User: "no asset type." Same shape as the "ROOT CAUSE FOUND" entry
  above — 23rd confirmed instance of this bug.
- Status: open — root cause confirmed, fix design deferred (parking_lot.md #23).

## Thomas Jefferson University (2)
- ack_id: `20250730144153NAL0002010355001`
- Certified: $2,371,811,436 (amt_mutual_funds, `plan_master_index_universe`;
  total plan assets $2,792,479,411) — Staged: $208,334,076 (1 row:
  "VANGUARD TARGET RETIREMENT 2040", asset_class `Target Date` /
  asset_sub_class `Target Date 2040`) — 8.8% capture
- Note: User: "same issue." Same shape as the "ROOT CAUSE FOUND" entry
  above — 24th confirmed instance of this bug. User also flagged that the
  section ends with a trailing "Total investments, at fair value" line —
  unlike the trailing-total lines fixed for Lee Health System/American
  Cancer Society, this one is ambiguous: it's the grand total across
  *all* asset types on the page, not a single-category subtotal, so it
  cannot resolve to one canonical asset_type the way "Total Mutual Funds"
  can. Flagged for awareness, not treated as a backfill opportunity.
- Status: open — root cause confirmed, fix design deferred (parking_lot.md #23).

## Texas Children's
- ack_id: `20251015122503NAL0002285555001`
- Certified: $2,030,623,209 (amt_mutual_funds, `plan_master_index_universe`;
  total plan assets $2,141,432,234) — Staged: $33,179,467 (1 row: "Vanguard
  Vanguard Institutional Index Inst Plus", asset_class `Equity` /
  asset_sub_class `Diversified Equity`) — 1.6% capture
- Note: User: "same...see Total investments at fair value in the end but
  nothing else." Same shape as the "ROOT CAUSE FOUND" entry above — 25th
  confirmed instance of this bug. Same grand-total-only trailing line
  pattern noted on the Thomas Jefferson University (2) entry above — no
  per-category subtotal available to backfill from.
- Status: open — root cause confirmed, fix design deferred (parking_lot.md #23).

## American Institutes for Research in the Behavioral Sciences
- ack_id: `20251015105028NAL0002275363002`
- Certified: $272,352,258 (amt_mutual_funds, `plan_master_index_universe`;
  total plan assets $322,953,560) — Staged (pre-fix): $22,867,853 (1 row:
  "College Retirement Equities Fund CREF Stock N/A", `PENDING_AI`/
  `PENDING_AI`) — 8.4% capture.
- Bug found: this filer puts the category label literally in column (a),
  Identity of Issue, on every row (e.g. `"Registered Investment Company
  Nuveen International Equity Index N/A"`, `"* Registered Investment
  Company T.Rowe Price Retirement 2040 N/A"`), instead of as a section
  heading or a trailing "Total <category>" line — and this plan reports
  "N/A" instead of a share count for its participant-directed investments
  (footnoted), so the existing per-row inline-keyword check in
  `extract_text_based_investments()` (`src/text_extract.py`), which was
  gated behind finding a `shares`/`units` count in the row text, never
  fired. All 42 rows on the page were extracted with correct dollar
  values (confirmed directly), but 41 of them had a blank `asset_type` and
  got dropped downstream — only the CREF Stock row survived into staging.
  Third distinct root-cause pattern from the "Cross-plan enhancement
  synthesis" section below (inline per-row type embedded in the identity-
  of-issue column), now with a concrete instance and fix.
- Fix: added a second, narrower trigger to the same per-row check —
  if the row's identity-of-issue text (after stripping a leading `*`)
  *starts with* one of the known category-label keys (already in the
  existing lookup table, e.g. `REGISTERED INVESTMENT COMPANY` → `Mutual
  Fund`), strip the label as a prefix and use the remainder as the fund
  name. Anchored to line-start so it can't misfire on an unrelated fund
  name that merely contains one of these phrases elsewhere in its text.
  Verified locally: 27 "Registered Investment Company"-labeled rows (Nuveen,
  T.Rowe Price Retirement 2005–2065, American Funds, Metropolitan West,
  Vanguard) now resolve to Mutual Fund with clean fund names; full-page
  42-row sum = $322,953,560, an exact match to the plan's certified total
  assets. The 13 TIAA/CREF rows above (their labels are literal company
  names, not category labels) are untouched — still no asset_type, a
  separate open issue.
- Status: fix implemented and locally verified; committed, not yet
  deployed to EC2 (see `docs/rerun_queue.md`).

## The Progressive Corporation & Its Participating Subsidiaries
- ack_id: `20260513074811NAL0006610787001`
- Certified: $2,043,916,868 (amt_mutual_funds, `plan_master_index_universe`;
  total plan assets $12,393,607,991) — Staged: $144,544,066 (1 row:
  "Various Mutual Funds", `PENDING_AI`/`PENDING_AI`) — 7.1% capture
- Note: User: "no asset type." Same shape as the "ROOT CAUSE FOUND" entry
  above — 26th confirmed instance of this bug.
- Status: open — root cause confirmed, fix design deferred (parking_lot.md #23).

## Midwestern University
- ack_id: `20251013140532NAL0001464049001`
- Certified: $405,049,902 (amt_mutual_funds, `plan_master_index_universe`;
  total plan assets $507,684,771) — Staged: $30,056,032 (1 row: "CREF
  Equity Index", asset_class `Equity` / asset_sub_class `US Total Market`)
  — 7.4% capture
- Note: User: "no asset type." Same shape as the "ROOT CAUSE FOUND" entry
  above — 27th confirmed instance of this bug.
- Status: open — root cause confirmed, fix design deferred (parking_lot.md #23).

## Ballad Health
- ack_id: `20260415164121NAL0020128513001`
- Certified: $654,419,864 (amt_mutual_funds, `plan_master_index_universe`;
  total plan assets $731,254,873) — Staged: $40,407,353 (3 rows: "Dodge
  Cox Income Fund Class X" $15,368,385 `PENDING_AI`/`PENDING_AI`,
  "Goldman Sachs GQG Partners International Opportunities Class R6"
  $14,883,868 Equity/Diversified Equity, "Dodge Cox International Stock
  Class X" $10,155,100 Equity/Intl Equity) — 6.2% capture
- Note: User: "same issue no asset type." Same shape as the "ROOT CAUSE
  FOUND" entry above — 28th confirmed instance of this bug.
- Status: open — root cause confirmed, fix design deferred (parking_lot.md #23).

## Ogletree, Deakins, Nash, Smoak & Stewart, P.C.
- ack_id: `20251014210105NAL0003605745001`
- Certified: $379,945,350 (amt_mutual_funds, `plan_master_index_universe`;
  total plan assets $398,410,041) — Staged: $21,621,103 (1 row:
  "Appreciation Institutional Fund", asset_class `Equity` / asset_sub_class
  `Diversified Equity`) — 5.7% capture
- Note: User: "nothing...just 'Investments at fair value:' at the top. no
  asset type." Same shape as the "ROOT CAUSE FOUND" entry above — 29th
  confirmed instance of this bug.
- Status: open — root cause confirmed, fix design deferred (parking_lot.md #23).

## Brown University Defined Contribution Deferred Vesting Retirement Plan
- ack_id: `20251008130222NAL0009448880001`
- Certified: $875,046,877 (amt_mutual_funds, `plan_master_index_universe`;
  total plan assets $974,489,255) — Staged: $53,813,787 (14 rows, all
  `PENDING_AI`/unclassified) — 6.1% capture. Page 18 of the PDF has two
  "Mutual funds offered by <provider>:" section headings ("...Fidelity:"
  and "...Teachers Insurance and Annuity Association:").
- User flagged that our earlier Fidelity-heading fix should already cover
  this file, and asked to rerun locally to confirm. First rerun (existing
  code) did recover the Fidelity section correctly ($758.2M in Mutual Fund
  rows), but user then spotted the *second* heading on the same page
  ("Mutual funds offered by Teachers Insurance and Annuity Association:")
  was still losing data.
- Root cause (`src/text_extract.py`, `extract_tables_and_map()`): the
  existing `_HEADING_OFFERED_BY_RE` branch recognizes an "X offered by
  <provider>:" heading and unconditionally `continue`s past the row to
  avoid it leaking in as fake data. But Camelot fuses this heading directly
  onto the FIRST real data row of its own section on this filer's layout —
  e.g. `issuer_name = "Mutual funds offered by Fidelity: BrokerageLink
  Fidelity Fund"` with `current_value = "5,038,300.00"` already attached.
  The unconditional `continue` was silently discarding that real row along
  with the heading text. Confirmed via direct instrumentation: two rows
  were being dropped this way —
  "BrokerageLink Fidelity Fund" ($5,038,300) and "John Hancock Funds III
  Disciplined Value Fd Cl R6" ($10,985,609).
- Fix: added `_HEADING_OFFERED_BY_STRIP_RE`, which strips only the heading
  clause through its trailing colon. If real content remains after
  stripping, the row is now kept and processed normally (with
  `current_section_type` still set to `Mutual Fund`); if nothing remains
  (a genuine standalone heading line, the common case), it's still dropped
  exactly as before. Same trigger condition as before — only fires on rows
  that already matched the existing narrow "offered by" pattern.
- Verified locally (direct `extract_tables_and_map()` call against this
  PDF): both previously-dropped rows now come through correctly, typed
  `Mutual Fund`. Mutual Fund total went from $758.2M (before this fix,
  same-session baseline) to $774.3M (certified target: $875.0M).
- Known residual, not fixed here: a duplicate "Fidelity Freedom Index 2055
  Fund" row ($26,577,597, appears twice) from an overlapping Camelot
  section-table-area split; and page 19 (TIAA/CREF/annuity section) has
  several rows with garbled `�` values and blank issuer names — both
  flagged for awareness, not addressed this pass.
- Status: fix committed, not yet deployed to EC2. Rerun queued (see
  `docs/rerun_queue.md`).

## Iowa Independent Higher Education Research Foundation
- ack_id: `20251006151116NAL0003908849001`
- Certified: $428,612,765 (amt_mutual_funds, `plan_master_index_universe`;
  total plan assets $621,446,067) — Staged: $19,784,993 (1 row: "CREF
  Equity Index R2", asset_class `Equity` / asset_sub_class `US Total
  Market`) — 4.6% capture.
- Note: User: "no asset type." Same shape as the recurring "ROOT CAUSE
  FOUND" bug — 30th confirmed instance.
- Status: open — root cause confirmed, fix design deferred (parking_lot.md #23).

## Bill & Melinda Gates Foundation Employee Retirement Plan
- ack_id: `20250909140130NAL0012629043001`
- Certified: $330,185,065 (amt_mutual_funds, `plan_master_index_universe`;
  total plan assets $336,289,927) — Staged: $15,322,852 (1 row: "Registered
  Investment Company Funds: Special Small Cap Value") — 4.6% capture.
- Note: User: "interesting..i see Registered investment company funds
  total in the end." PDF page 13/14 has a leading "Registered Investment
  Company Funds:" heading (not itself recognized) followed by 16 fund
  rows, then a trailing "Registered investment company funds total
  330,185,065" line.
- NOT a new bug — a local rerun with the currently-committed code (the
  trailing-total backfill added in an earlier session, commit `f48a2e64`)
  already recovers all 17 rows correctly: 16 RIC funds backfilled to
  `Mutual Fund`/`Money Market Fund` via the trailing-total line, plus the
  Charles Schwab & Co. Self-Directed Brokerage Account row. Sum = exactly
  $330,185,065, matching certified `amt_mutual_funds` to the dollar.
- Status: no fix needed — staged data is just stale, predates this
  backfill fix's rollout. Rerun-only, queued in `docs/rerun_queue.md`.

## Cross-plan enhancement synthesis (as of 2026-08-17)

Across this batch and the prior 2026-08-16 batch (Molson Coors, Emory
Clinic, 32BJ, Brown, BNY Mellon, Cleveland Clinic, Texas Children's, Chubb),
three distinct, non-overlapping failure patterns keep recurring. All three
point toward the same conclusion: **the pipeline's asset-type classifier is
built almost entirely around detecting section *headers*, and that's only
one of (at least) three ways these DOL Schedule H 4i documents encode
investment type.** The enhancement to build is a unified per-row asset-type
classifier that layers in the two missing signal sources, not just more
header patterns:

1. **Section-header detection (existing, `section_typing.py`)** — works
   when a schedule groups holdings under a bold/short heading line
   ("Mutual Fund", "Common/Collective Trust Fund"). Already built; needs
   its `_SECTION_MAP` pattern list extended per-document (Jefferson,
   Progressive both return `retyped_non_mf: 0`, meaning their header
   wording isn't covered yet) and to be turned on by default rather than
   gated behind `SECTION_TYPING=1`.
2. **Inline per-row type column (new, not built anywhere)** — some
   schedules (Toyota) put the type directly next to each fund name as its
   own token/column ("Mutual fund", "Commingled fund", "Company Stock
   fund") with no section grouping at all. Needs a new extraction path
   that recognizes and maps this column during table parsing, not a
   post-hoc section-typing pass.
3. **Type embedded in free-text description (new, not built anywhere)** —
   the majority of this batch (Presbyterian, University Hospitals,
   Cedars-Sinai, Carnegie Mellon, Memorial Hermann, HonorHealth) are
   Schedule H 4i tables with a `(c) Description of Investment` column and
   *no* section headers or type column at all — the type has to be
   inferred from the free text itself (fund family + share-class name →
   Mutual Fund; "Evergreen Annuity Contract" → Group/Annuity Contract;
   "Institutional Shares" language, etc.). This is the largest bucket by
   plan count in both batches and has no existing tooling at all — it's a
   net-new classifier, essentially a keyword/pattern matcher against the
   description text, similar in spirit to `_SECTION_MAP` but applied
   per-row instead of per-section.

Separately, **duplication/over-capture (Mizuho, Chubb, and finding #22's
Jefferson/Progressive)** is a related but distinct problem from missing
asset_type — existing Stage 2 dedup fixes it for some cases (Mizuho: fixed)
but not others (Chubb: two near-identical page ranges survive dedup as
separate rows) — worth a follow-up look at why the dedup key doesn't catch
the Chubb case once the above classifier work is scoped.

## Lee Health System 403(b) Retirement Plan
- ack_id: `20250623132312NAL0003563811002`
- EIN 99-2646504, Plan No. 001, Plan Year Ending 12/31/2024. Total plan
  assets (PDF) $1,280,404,013. PDF Schedule H, 4i totals: Insurance
  Company/General Accounts Total $131,919,328; Mutual Funds Total
  $1,127,268,971; Other Funds Total (Charles Schwab SDBA) $10,423,480;
  Participant Notes Receivable $10,792,234.
- Before fix: `plan_mf_history_v3` had only **1 row** ($137,752,780,
  "Vanguard Instl Index Instl Plus") — 12% capture of the certified MF
  total.
- Root cause: this filer puts "Total" at the END of the category label
  ("Mutual Funds Total", "Insurance Company/General Accounts Total")
  instead of the front ("Total Mutual Funds"). The trailing-total
  backfill/drop mechanism added in an earlier session (`pending_untyped_rows`
  in `src/text_extract.py`) only recognized the leading-"Total" shape, so on
  this plan it never fired at all: none of the 35 real mutual fund rows
  ever got an `asset_type` backfilled, and both total lines leaked in as
  fake duplicate rows instead of being dropped.
- Two more bugs surfaced while fixing this one, same page:
  1. Even after widening the trailing-total detection, "Insurance
     Company/General Accounts Total" still didn't resolve to a canonical
     type — `Insurance General Account`'s regex required whitespace
     between "Company" and "General", but this PDF uses a slash
     ("Company/General") with no space.
  2. The lone Schwab SDBA row ("Charles Schwab Instl Personal Choice
     Retment Account") was getting swept into the Mutual Fund backfill
     because it carries no independent type signal of its own — the
     existing `personal choice retirement account` pattern didn't match
     because this filer abbreviates it "Retment" (no second "i").
- Fix (all in `src/text_extract.py` / `src/asset_type_patterns.py`, not yet
  committed):
  1. New `_is_total_line_shape()` helper recognizes total lines with
     "Total"/"Subtotal"/"Grand total" at either the front OR the end;
     wired into all three backfill/drop gates (two in the text-based
     extraction path, one in the camelot table-based path).
  2. Widened `Insurance General Account` pattern (both the standalone
     `ASSET_TYPE_PATTERNS` entry and `ROW_TYPE_PATTERNS`) to accept `/` as
     well as whitespace between "Company" and "General".
  3. Widened `personal choice ret(?:ire)?ment account` pattern
     (`ROW_TYPE_PATTERNS`) plus added matching literal keys to
     `text_extract.py`'s local per-row `asset_type_patterns` dict, so the
     Schwab SDBA row resolves to `Self-Directed Brokerage Account`
     directly off its own description, independent of any section/total
     backfill.
- Verified locally (direct `extract_text_based_investments()` call against
  this PDF, page 2): 35 Mutual Fund rows now backfill correctly, summing to
  $1,127,268,967 (matches PDF's $1,127,268,971 to within $4 rounding); 2
  Insurance General Account rows backfill correctly, summing exactly to
  $131,919,328; Schwab SDBA row correctly types as `Self-Directed Brokerage
  Account` ($10,423,480); both total lines no longer leak in as fake rows.
- Known residual, not fixed: "Other Funds Total" itself still leaks through
  as a duplicate blank-`asset_type` row (same $10,423,480 as the real
  Schwab row) — "Other Funds" isn't a recognized DOL category so it never
  resolves via `_detect_section_heading_text`, and it's deliberately
  excluded from the provider-total drop logic (`_TOTAL_PROVIDER_EXCLUDE_WORDS`
  contains "funds", by design, to avoid dropping real fund names ending in
  "... Total ... Fund"). Low materiality (~0.8% of plan assets) and likely
  harmless in practice — blank-`asset_type` rows are the same shape that
  caused the original under-capture, so this row will likely just get
  dropped downstream in classification the same way, not double-count.
  Flagged for awareness, not blocking.
- Status: fix committed (`c054722a`) and deployed to EC2 (MD5-verified,
  2026-08-27). Prod rerun not yet run — code is live on EC2 but the
  pipeline hasn't been rerun against prod for this ack_id yet (see
  `docs/rerun_queue.md`).

---

## Pomona Valley Hospital Medical Center
- ack_id: `20250730121541NAL0002334083001`
- Certified: $450,959,099 (amt_mutual_funds) / $594,197,158 total plan
  assets — Staged (`plan_mf_history_v3`): $21,581,729, 17 rows (~4.8%
  capture).
- Note: another confirmed instance of the recurring "no asset type" bug
  pattern (root cause conceptually understood, fix deferred —
  `parking_lot.md` #23). User's verdict on the PDF: "no asset type."
  `plan_holdings_staging` (pre-classification) shows the same signature —
  208 rows / $563,273,163 total, but 181 of those 208 rows (87%) carry a
  blank `asset_type`, accounting for $447,681,408 (79.5%) of the staged
  dollar total. Only 27 rows resolved a type at extraction time (13 typed
  `mutual fund`, 2 `real estate`, 2 `etf`, 1 `money market fund`, 1 `bond`,
  1 `stable value fund`, plus the raw-sponsor-name variants), and every row
  — typed or not — sits at `asset_class`/`asset_sub_class` = `PENDING_AI`,
  `validation_status` = `MANUAL_REVIEW`. This is the standard shape: real,
  correctly-valued rows losing their `asset_type` at extraction and getting
  dropped or stalled downstream, not a value-extraction or shares-vs-value
  problem.
- `plan_mf_history_v3` currently holds only these 17 rows (the ones that
  did resolve a type and made it through classification):
  Fid Low Priced Stk $827,426 (Equity/blank, PENDING_AI); Fid Asset Mgr 85%
  $1,364,159 (PENDING_AI); Fid New Millen $840,236 (PENDING_AI); Fid Freedom
  2040 K $872,518 (Target Date/Target Date 2040); Fid Freedom 2025 K
  $1,100,556 (Target Date/Target Date 2025); Fid Freedom 2030 K $922,162
  (Target Date/Target Date 2030); Lincoln National Life Ins. Co. LVIP
  Dimensional U.S. Core Equity I ** $3,123,636 (Equity/Diversified Equity);
  Lincoln National Life Ins. Co. American Funds Growth ** $2,644,137
  (Equity/US Large Cap); Fid 500 Index $2,728,796 (Equity/US Large Cap);
  Lincoln National Life Ins. Co. American Funds Global Growth ** $978,313
  (Equity/Global Equity); Lincoln National Life Ins. Co. LVIP Vanguard
  International Equity ETF ** $375 (Equity/Intl Equity); Lincoln National
  Life Ins. Co. Fidelity VIP Contrafund ** $1,135,421 (Equity/US Large Cap);
  Lincoln National Life Ins. Co. LVIP Vanguard Domestic Equity ETF ** $1,705
  (Equity/Diversified Equity); Lincoln National Life Ins. Co. LVIP SSGA S&P
  500 Index $792,543 (Equity/US Large Cap); Fid Nasdaq Comp Indx $741,656
  (Equity/US Total Market); Fid Extd Mkt Idx $1,236,296 (Equity/US Total
  Market); Fid Blue Chip Growth $2,271,794 (Equity/US Large Cap) — total
  $21,581,729.
  The other ~191 real holdings sitting in `plan_holdings_staging`
  (T. Rowe Price Retirement series, Vanguard 500/Mid/Small Cap Index,
  American Funds series, PIMCO Total Return, Lincoln Stable Value Account
  $82,506,611, Schwab SDBA $3,697,662, dozens of Fidelity Select/sector
  funds, etc.) never made it into `plan_mf_history_v3` — most of these
  carry a blank `asset_type` in staging, consistent with the pattern above.
- Status: open, no fix built yet — same underlying bug class as
  `parking_lot.md` #23 ("1-row survivor" / no-asset-type on headerless
  plans). Not added to `docs/rerun_queue.md` — no fix exists yet to queue a
  reload against.

---

## Independent School Collaborative
- ack_id: `20251014150531NAL0006493298001`
- Certified: $340,863,585 (amt_mutual_funds) / $448,998,868 total plan
  assets — Staged (`plan_mf_history_v3`): $12,908,564, **1 row** (~3.8%
  capture).
- Note: another confirmed instance of the recurring "no asset type" bug
  pattern (`parking_lot.md` #23). `plan_holdings_staging` shows the same
  signature — 603 rows / $445,827,721 total, but 511 of those 603 rows
  (85%) carry a blank `asset_type`, accounting for $307,179,603 (69%) of
  the staged dollar total. Only the 1 row that resolved a type survived
  into `plan_mf_history_v3`; the rest (real holdings, correctly valued)
  never made it through.
- Status: open, no fix built yet — same underlying bug class as
  `parking_lot.md` #23. Not added to `docs/rerun_queue.md` — no fix exists
  yet to queue a reload against.

---

## HP Inc. 401(k) Plan
- ack_id: `20260622163704NAL0006526769001`
- EIN 94-1081436, PN 004, PYE 12/31/2025. `plan_master_index_universe`
  reports `amt_mutual_funds` = $494,489,776 / total assets
  $10,868,029,615. `plan_mf_history_v3` currently has 1 row, $13,097,347
  (~2.6% "capture" against the certified number).
- **Not the recurring no-asset-type bug — the certified number itself is
  wrong for this ack_id.** Per the user's direct read of the PDF's Schedule
  H, Part IV, Line 4i, the plan's schedule only lists ONE real "Mutual
  Fund:" section: "* Dreyfus Government Cash Management Fund $13,097,347"
  — which is exactly the 1 row already sitting in `plan_mf_history_v3`.
  Extraction/classification got this one right. The $494,489,776 pulled
  into `amt_mutual_funds` on `plan_master_index_universe` is not a mutual
  fund total at all — it's an exact match to the PDF's separate
  "Self-Directed Brokerage Account: * Fidelity Self-Directed Brokerage
  Account $494,489,776" line. So the certified benchmark itself has the
  wrong PDF line mapped to `amt_mutual_funds` (likely a Form 5500 filing
  data/summary-schedule issue upstream of our pipeline, not a bug in our
  extraction code) — the ~2.6% "gap" is an artifact of comparing against a
  mislabeled certified figure, not a real under-capture.
- Other sections on this schedule not evaluated here (not requested):
  Short-Term Investments (Vanguard Federal Money Market $416,616,883),
  Common Collective Trust Funds ($3,190,241,363, 4 BlackRock funds),
  Collective Investment Trust Funds ($6,571,269,516, 22 SEI Trust Company
  funds), Common Stock (HP Inc. $84,314,070), Participant Loans
  ($32,182,682). None of these are "Mutual Fund" per the PDF's own section
  labels, so their absence from `plan_mf_history_v3` is expected/correct,
  not a bug.
- Status: no fix needed on our side — flag the certified
  `amt_mutual_funds` value for this ack_id as unreliable/mismapped upstream
  if certified-vs-staged comparisons are used to drive prioritization.

---

## The Governing Committee of the Section 403(b) Defined Contribution
- ack_id: `20250922150650NAL0002203571001`
- Certified: $944,964,649 (amt_mutual_funds) / $1,283,763,729 total plan
  assets — Staged (`plan_mf_history_v3`): $36,018,350, **2 rows** (~3.8%
  capture).
- Note: another confirmed instance of the recurring "no asset type" bug
  pattern (`parking_lot.md` #23). `plan_holdings_staging` shows the same
  signature — 230 rows / $520,945,932 total, but 212 of those 230 rows
  (92%) carry a blank `asset_type`, accounting for $419,865,500 (81%) of
  the staged dollar total. Only the 2 rows that resolved a type survived
  into `plan_mf_history_v3`; the rest (real holdings, correctly valued)
  never made it through.
- Status: open, no fix built yet — same underlying bug class as
  `parking_lot.md` #23. Not added to `docs/rerun_queue.md` — no fix exists
  yet to queue a reload against.

---

## Board of Trustees of the Building Service 32BJ Supplemental Retirement Savings Plan
- ack_id: `20260415123903NAL0017000545001`
- Certified: $2,524,373,761 current value / $2,439,253,481 cost (18
  "Mutual Fund" section rows, per the user's direct PDF read of page 21) —
  Staged (`plan_mf_history_v3`): 0 rows (0%).
- **Not the recurring no-asset-type bug** — `asset_type` is present and
  correct on every row (both the section heading AND the per-row column
  say "Mutual Fund" in the PDF; Camelot/`extract_tables_and_map()` extract
  and map all 18 rows correctly with `asset_type='Mutual Fund'`). This is a
  distinct, newly root-caused bug: all 18 rows have empty
  Collateral/Rate/Interest/Maturity-Date columns, which Camelot
  concatenates into the description field as `"Mutual Fund N/A N/A"`.
  `cleanup_investment_names.py`'s `_is_asset_type_label()` only stripped
  trailing share/unit counts before checking for a pure asset-type label,
  not trailing `N/A` placeholder tokens — so it failed to recognize
  `"Mutual Fund N/A N/A"` as a label, the real fund name (e.g. "Vanguard
  Wellesley Income Adm", $1,762,125,480) was never recovered from
  `issuer_name` into the description, the manager-prefix rules truncated
  `issuer_name` down to a bare manager name ("Vanguard"/"T. Rowe
  Price"/"American Funds"), and `post_extract_validator.py`'s
  `build_mf_rows_df()` name-quality gate silently dropped every row since
  neither field held a usable fund name. Same bug class as the Flowers
  Foods fix (`4b48eeb9`), different trigger shape (trailing `N/A` tokens
  vs. semicolon-separated share counts).
- Fix: added `_NA_TRAILING_RE` and a second strip pass inside
  `_is_asset_type_label()`. Verified locally: `"Mutual Fund N/A N/A"` now
  correctly recognized as a label, recovering "Vanguard Wellesley Income
  Adm" as the description; regression-checked "Target Date Fund 2035"
  (correctly still NOT a pure label) and the existing Flowers Foods
  semicolon-share-count case (unaffected).
- Status: fix committed `10d8ea59`, deployed to EC2 (MD5-verified,
  2026-08-29). Added to `docs/rerun_queue.md` — queued, prod reload not
  yet run.

---

## Nouryon Chemicals LLC (Retirement Savings Plan / Hourly Savings Plan Master Trust)
- ack_id: `20251015102624NAL0002240291001`
- Certified: $555,439,476 (assets/net_assets) — Staged (`plan_mf_history_v3`
  prior to fix): effectively 0 real rows (only ~1 row recovered via a
  text-based fallback extraction).
- Two distinct bugs on this filer's Schedule H, 4i page. (1) Column (E)'s
  header text is corrupted in the source PDF (literally reads "18" instead
  of "Current Value"), which defeats normal header-text column matching —
  `column_map` ended up completely empty (0/24 meaningful rows mapped), and
  the page fell through to text-based extraction, which only recovered 1
  row. (2) A general, filer-independent bug: whenever both signals were
  present on a row, `table_section_asset_type` (a coarse, per-Camelot-table
  default) unconditionally overrode the more specific, freshly-detected
  in-row `current_section_type`, so every row showed asset_type "Mutual
  Fund" regardless of its actual category (Common/Collective Trust Fund,
  Separate Account, Cash, etc.). Two heading wordings on this page —
  "MANAGED SEPARATE ACCOUNT" and "INTEREST BEARING CASH" — also had no
  matching entry in `ASSET_TYPE_PATTERNS` at all, a gap likely affecting
  other filers using the same wording.
- Fix: (1) added a plan-specific (Nouryon-only, matched via
  "NOURYON CHEMICALS" on the supplemental page text) fallback that bootstraps
  `current_value`/`issuer_name` column mapping positionally — picks the
  rightmost numeric-majority column as `current_value`, then restricts the
  issuer-column search to rows where that value column is populated (to
  avoid picking the interleaved asset-type-label column instead of the real
  issuer column) — only fires when normal header-text matching mapped
  neither field, so it can never override a correctly-mapped table. (2)
  Fixed the general `current_section_type` vs `table_section_asset_type`
  priority bug via a new per-table `section_type_seen_in_table` flag. (3)
  Added `ASSET_TYPE_PATTERNS` entries for "Managed Separate Account" and
  "Interest Bearing Cash". (4) Fixing (3) surfaced a regression — the new
  "Interest Bearing Cash" pattern falsely substring-matched inside an
  unrelated multi-line preamble sentence ("...MUTUAL FUNDS, INTEREST
  BEARING CASH, NONINTEREST-BEARING CASH AND OTHER LIABILITIES)"),
  producing a false extra section-area split. Root cause:
  `_detect_section_heading_text` used `re.search` (substring) instead of
  `re.fullmatch` against `ASSET_TYPE_PATTERNS`/`_HEADING_ONLY_PATTERNS`,
  despite being documented to detect "label-only" full-line headings —
  switched to `re.fullmatch`.
- Verified against the real PDF: all 23 rows extract with correct values
  and correct asset_types (9 Common/Collective Trust Fund, 12 Mutual Fund,
  1 Separate Account [Galliard, exact match to certified
  `amt_pooled_sep_acct` $64,413,123], 1 Cash), summing to exactly
  $555,439,476 (certified total, exact match). Sub-bucket splits
  (`amt_mutual_funds` $262,276,593 vs. computed, `amt_interest_cash`
  $5,898,507 vs. computed) are off by exactly $3,504 each — a filer-side
  certified-data categorization quirk between two buckets, not an
  extraction defect.
- Status: fix committed `87f05afc`, deployed to EC2 (MD5-verified,
  2026-08-30). Added to `docs/rerun_queue.md` — queued, prod reload not
  yet run.

---

## Kaman Corporation 401(k) Plan
- ack_id: `20251006123629NAL0009268594001`
- Certified: $452,620,180 assets = $447,641,558 investments + $4,978,622
  participant loans (`plan_master_index_universe`). Note: certified
  `amt_pooled_sep_acct` ($4,978,622) is actually Participant Loans, not a
  separate account — a filer-side bucket-labeling quirk, not an extraction
  defect (same pattern seen on Nouryon).
- No extraction defect requiring a fix — the plan's Schedule H, 4i content
  is duplicated verbatim across two physical pages in the source PDF
  (pages 16 and 18, with a blank page 17 between), both flagged
  `is_supplemental` and both fed to `extract_tables_and_map` together by
  `src/run_pipeline.py`, exactly as production does.
- Investigated a real but latent bug this surfaced: `current_section_type`
  in `src/text_extract.py` (~line 2671) is intentionally persisted *across
  pages* within one extraction call (needed for genuine multi-page
  continuations where a heading appears once and the table carries on
  without repeating it). Page 16 ends by detecting the "Participant Loan"
  heading on its last row, setting `current_section_type = 'Participant
  Loan'`; that state is never reset before page 18's table starts, so page
  18's earlier fund rows fall through to the stale `elif
  current_section_type:` fallback (~line 3077) and get wrongly stamped
  `'Participant Loan'`.
- Confirmed this self-heals for Kaman: pages 16/18 are byte-identical, so
  every fund produces two rows with identical `(pdf, description,
  current_value)`. `remove_cross_page_duplicates` in `src/data_cleaner.py`
  groups by `(pdf, description)` and drops all but the first-seen
  occurrence on an exact-value tie — page 16's (correctly typed) row wins,
  page 18's (mistyped) row is dropped. Verified directly by running the
  real dedup function against both pages' raw extraction output: 27 rows
  survive, all carrying page 16's correct asset_types, summing to exactly
  the certified $447,641,558 investments + $4,978,622 loans =
  $452,620,180.
- Also noticed (cosmetic, not value-affecting): the Participant Loans
  row's `investment_description` is truncated to a trailing fragment,
  "with maturity dates ranging through 2036", instead of the full loan
  description.
- User visually confirmed against the source PDF (2026-08-30): the
  Schedule H, 4i page genuinely has no asset-type section headings or
  labels of any kind — a flat, unlabeled list of holdings. So blank
  `asset_type` on the fund rows is correct, expected extraction behavior
  for this plan, not a gap to fix.
- Status: no code change made — per user decision (2026-08-30), logging
  only. The latent `current_section_type` cross-table-persistence bug
  remains unfixed and could bite a future plan where a duplicated page has
  any formatting difference that breaks the exact-tie dedup, or where a
  duplicate page's row count/values differ from the original. Added to
  `docs/rerun_queue.md` — queued, prod reload not yet run (numbers already
  reconcile; rerun would just refresh the load, not fix anything).
