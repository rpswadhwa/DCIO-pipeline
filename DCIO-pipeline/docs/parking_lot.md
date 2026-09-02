# Parking Lot — Extraction Gaps from 100-Plan Rerun

Target revisit date: **2026-08-15**

Findings below are root-caused but intentionally deferred (not fixed yet) per
2026-08-12 decision to come back to these after other priorities. Source: the
100-plan under-capture rerun (`run_100rerun.log`, `investments.csv` on EC2 at
`/home/ec2-user/run_100rerun_out/`).

## Action Items

- [ ] Add `'institutional fund'` / `'institutional funds'` → `'Mutual Fund'`
      to `SECTION_HEADING_MAP` in `src/text_extract.py` (~line 1104). See
      finding #2 (Duke Energy) below for root cause detail.
- [ ] Add a poor-quality-retry trigger in `text_extract.py` (~line 2117) for
      tables with very few rows but an abnormally large single-cell text
      block (no column-ruling lines), so they fall through to
      `extract_text_based_investments` instead of silently producing zero
      rows. See finding #5 (Macy's) below for root cause detail.
- [ ] Make the numeric-value regex (`\$?\s*\(?\s*[0-9][0-9,]*(?:\.[0-9]+)?\)?`,
      used across both the table and text-based extraction paths) tolerant
      of a stray internal space in long numbers (e.g. `1 ,549,552.40`), or
      strip embedded whitespace from numeric-looking tokens before matching.
      See finding #6 (Gundersen Lutheran) below for root cause detail.
- [ ] Teach the asset-type classifier to parse "Registered Investment
      Company" / "Common/Collective Trust" language embedded mid-description
      in composite "Separately managed account which includes: ..." rows,
      instead of defaulting the whole row to `Employer Stock` when no
      section-heading pattern matches. See finding #7 (American Airlines)
      below for root cause detail.
- [ ] Extend `classify_pages_text` keywords to catch "SUMMARY OF NET TRUST
      ASSETS" / "Master Trust" statement headers, or route master-trust-summary
      pages through the same detection path as finding #4. See finding #21
      (Arconic) below.
- [ ] Investigate double-counting of subtotal + detail rows on wide/multi-fund
      holdings tables (Progressive Corp, Thomas Jefferson Univ. — local
      extraction sums to 5-12x certified value). See finding #22 below.
- [ ] Asset_type is still blank on 50-91% of rows on large multi-fund holdings
      tables even after this session's fixes (Progressive, Toyota, Thomas
      Jefferson). See finding #22 below — root cause not yet isolated.
- [ ] Enable `USE_OCR=1` (or a cid-to-unicode remap fallback) for pages
      where extracted text is overwhelmingly `(cid:NN)` glyph codes despite
      a nonzero char count — same fix path as CVS Health (#3). See finding
      #8 (Froedtert) below.
- [ ] Enable `USE_OCR=1` for pages with zero extractable chars and a
      full-page image (same as Stanford #1). See finding #9 (Avago) below.
- [ ] Build a non-table-based parser for TWDC's composite multi-sub-report
      format (fund-code list + separate value section, no ruled table
      structure) — related to but distinct from the Macy's #5 fix since
      TWDC is a 522-page composite, not a single schedule. See finding #10
      (TWDC) below.
- [ ] Enable `USE_OCR=1` for Con Edison — same zero-extractable-chars OCR
      gap as Stanford (#1), confirmed 0 chars across all 33 pages. See
      finding #11 below.
- [x] **DONE (2026-08-14)** Build a Master-Trust-participation-schedule
      parser (nonstandard multi-section report with "Security ID / Security
      Description / Shares / Cost / Market Value" columns and category
      subtotal rows, instead of the standard Schedule H columns (a)-(e)) —
      Howmet's real holdings, including the unrecognized-until-now
      "REGISTERED INVESTMENT COMPANIES" section, live entirely in this
      alternate format on a page our extractor currently skips. See finding
      #12 below for the implementation, and finding #15 (Lumen Technologies)
      for confirmation the same detector generalizes to a second filer.
- [ ] Extend the composite participation-schedule parser to handle rows
      where the CUSIP/asset-ID token wraps onto its own line, separate
      from the fund-name description that follows it — confirmed gap on
      Lumen Technologies. See finding #15 below.
- [ ] Scope (likely defer/exclude) J&J — its Master Trust reports via a
      289-page "Composite" direct-securities schedule (individual bond/
      MBS pool holdings under internal fund codes, no ruled table
      structure) rather than a mutual-fund menu; this may not fit the
      DCIO pipeline's model at all. See finding #13 below. A second J&J
      ack_id (`20251015121015NAL0002381763001`) shows the same JJ-coded
      fund fingerprint in `plan_holdings_staging` — likely the same format,
      not yet opened/confirmed. See finding #26.
- [ ] Teach asset-type classification to fall back to a per-fund-name/
      per-issuer heuristic (or flag for manual review) when a schedule has
      NO section headings at all and the "Identity of Issue" column is a
      recordkeeper/trustee name (e.g. "Empower Trust Company, LLC") rather
      than the fund family — currently the row-list extracts cleanly but
      every row's `asset_type` stays blank. See finding #14 (Advocate
      Aurora) below.
- [ ] DISCUSS BEFORE BUILDING: fix the "1-row survivor" bug on headerless
      plans (finding #23 below) — same underlying class as #14, but with
      the exact mechanism now root-caused (2026-08-26). Decision needed on
      scope (auto-load all blank-type rows as MF/MANUAL_REVIEW vs. a
      stricter fund-like-signal gate vs. routing to a separate manual
      review table) before touching `post_extract_validator.py` /
      `llm_enhance_investments.py`.
- [ ] TBD: build an OCR-based extraction path for schedule pages whose
      table body is embedded as a raster image inside an otherwise
      text-native filing (finding #24, Fermi National Accelerator
      Laboratory, root-caused 2026-08-26). `src/ocr_passes.py::run_ocr`
      already exists but is only wired into the separate rendered-page-
      image branch of `run_pipeline.py` (~line 150-160); it needs to be
      invoked from `extract_tables_and_map`'s text/Camelot path
      (`text_extract.py`) as a per-page fallback whenever a page has
      real title/keyword text but the table body itself yields ~0
      extractable rows (image present, `page.lines`/`page.chars` far
      below what the visible content implies).
- [ ] PARKED (2026-08-28), needs careful before/after testing before
      building: cross-check Camelot's captured value-lines against the
      full page's value-lines (via pdfplumber) and trigger the text-based
      retry when the table misses any, so a second section below a
      subtotal line on the same page isn't silently dropped. See finding
      #25 (Fox Corporation) below — user is specifically concerned about
      regressing plans that already extract correctly via this same
      shared quality-gate code path.
- [ ] DECIDE: whether `asset_class = 'Equity'` (TIAA-CREF variable annuity /
      equity account rows, e.g. "CREF Stock R3 Equities") should map into
      the certified `amt_mutual_funds` total at all, or belongs to a
      different certified bucket entirely. Raised on Southwest Research
      Institute Retirement Plan (`20250930092302NAL0005410659001`,
      2026-08-26) — see `plan_notes.md`. Affects how "capture %" should be
      computed for TIAA-CREF-style plans generally, independent of the
      1-row-survivor bug (#23) also present on that plan.

---

## 1. Stanford — no extractable text layer (OCR gap)

- **Plan**: The Board of Trustees of the Leland Stanford Junior University
- **ack_id**: `20251010175027NAL0008070449001`
- **Certified**: $9,682,393,396 — **Staged**: $0 (0% capture)

Pages 18–22 of the PDF (including page 21, where the "Registered Investment
Company" column is visible on screen) have **zero extractable characters**
via `pdfplumber` — confirmed with a per-page `chars`/`images`/`lines`/`rects`
scan. Page 21 specifically has 0 chars, 1 image (on page 20), and 249 vector
`rects` (table gridlines drawn as vector paths, not text). This is either
flattened/vector-outlined text or a scanned page with no text layer — both
defeat `pdfplumber.extract_text()` and Camelot's stream parser.

**Root cause**: `USE_OCR` defaults to `"0"` (`run_pipeline.py:220`) and was
not enabled for this rerun. `src/ocr_passes.py` (`run_ocr`) already exists
and is wired into the pipeline via `run_pipeline.py:25`, but is unused.

**Fix candidate**: enable `USE_OCR=1` for plans/pages that fail the
zero-chars check, or unconditionally for the worst-undercapture cohort.

---

## 2. Duke Energy — "Institutional Funds" section heading not recognized

- **Plan**: Duke Energy Retirement Savings Plan
- **ack_id**: `20251010135251NAL0018754754001`
- **Certified**: $9,569,696,000 — **Staged**: $0 (0% capture)

Extraction DID succeed (24 rows, $11.26B total extracted — matches the
PDF's own "Total" line), but all rows are misclassified: 17 tagged
`Employer Stock`, 3 `Commingled Fund`, 3 `Common/Collective Trust Fund`,
1 `Self-Directed Brokerage Account`. **Zero rows tagged `Mutual Fund`.**

Confirmed via raw page text (page 15/154): the PDF has a distinct
**"Institutional Funds"** section (16 line items, "Total Institutional
Funds 5,575,809" thousand = ~$5.58B) sitting directly after "Common Stock
Funds". The row-count math confirms the bug exactly: 17 = 1 (Common Stock
Fund row) + 16 (Institutional Funds rows) — the Institutional Funds section
never got recognized as a new section boundary, so its 16 rows inherited
the prior section's type (`Employer Stock`).

**Root cause**: two independent, non-shared section-heading detectors exist
in `src/text_extract.py`:
- `ASSET_TYPE_PATTERNS` (`src/asset_type_patterns.py`, used by
  `_detect_section_heading_text`) — **does** include
  `(r'Institutional\s+Funds?', 'Mutual Fund')` at line 67.
- `SECTION_HEADING_MAP` (`text_extract.py:1104`, used by the "simple
  two-column format" extraction path) — has **no** "institutional" key at
  all.

Duke's PDF was processed via the `SECTION_HEADING_MAP` path, which doesn't
know "Institutional Funds" is a heading, so ~$5.58B of legitimate MF-type
holdings never reached the MF load gate.

**Fix candidate**: add `'institutional fund': 'Mutual Fund'` (and
`'institutional funds'`) to `SECTION_HEADING_MAP`, or unify the two heading
detectors so they share one source of truth.

---

## 3. CVS Health — font has no usable character mapping (whole-document glyph corruption)

- **Plan**: CVS Health Corporation
- **ack_id**: `20251006141123NAL0003741777001`
- **Certified**: $9,446,229,019 — **Staged**: $0 (0% capture)
- **86 pages total**

Supplemental-page detection picked `[3, 8]`, but those are just the
**auditor's boilerplate opinion letter** ("Our audits were conducted for
the purpose of forming an opinion..." — identical text on both pages,
likely once per each of two plans bundled in this filing). They matched the
`SUPPLEMENTAL SCHEDULE` keyword but contain no investment data.

Full-document scan found the real cause: **pages 4–86 (i.e. nearly the
entire filing) extract as unmapped `(cid:NN)` glyph codes, not real text**
— on almost every page, `cid_occurrences == chars`, meaning every single
character `pdfplumber` pulls out is an unresolved glyph ID. Page 18 (which
visibly contains real dollar figures when viewed) extracts as 4,072
characters, all of them `(cid:...)` garbage. This is why the actual 4i
asset schedule — wherever it sits among pages 9–86 — was never even a
candidate for supplemental-page classification: its keyword text isn't
readable as text at all.

**Root cause**: this PDF's embedded font subset has no usable ToUnicode
CMap, so `pdfplumber`'s text extraction can't resolve glyph IDs to
characters across nearly the whole document (only the front boilerplate
pages 1–3/8 use a different, correctly-mapped font). This is a distinct
failure mode from both Stanford (zero extractable chars) and Duke Energy
(text extracts fine, wrong section-heading list) — here the text technically
"extracts" but is semantically garbage.

**Fix candidate**: `USE_OCR=1` should work around this (OCR reads the
rendered page image, not the font's character map) — same fix as Stanford,
worth testing on both together. A pure-text fix is not really possible here
without re-deriving the font's glyph mapping.

---

## 4. Master Trust/DFE filings — out of current parser scope (3 confirmed instances)

Confirmed on **three** plans so far, all lacking a standard Schedule H Line
4i schedule — this looks like a systemic gap affecting any Master
Trust/DFE filing in the under-capture cohort, not a one-off.

- **Bristol-Myers Squibb Company** — ack_id `20260622142505NAL0006418033001`
  (note: this ack_id/filename also exists under a different S3 batch_date
  folder — same duplicate-batch_date pattern seen with JNJ/TWDC — but the
  copy actually on EC2, MD5-confirmed identical to the copy downloaded
  fresh from S3, is a tiny 3-page, 60KB file).
- **Certified**: $1,588,326,065 — **Staged**: $0 (0% capture)

3-page PDF, first line "Master Trust: Bristol-Myers Squibb Company PAGE:
N". `chars` per page: 2375, 1079, 0 (page 3 is a single image, likely a
signature/cover page) — real readable text, not OCR/glyph corruption.
`Supplemental pages: []` fired correctly: no "Schedule H"/"4i"/"Schedule
of Assets" language anywhere in the document, because this is a Master
Trust net-assets summary, not a standard 4i schedule. Zero rows in
`investments`, zero rows in `default.plan_holdings_staging` — both
correct given the document type, not a bug. This is why there's no
asset-type column to see: the filing itself was never built with one.

- **Massachusetts Mutual Life Insurance Company** — ack_id
  `20250919133721NAL0002366369001`. Same "SUMMARY OF NET TRUST ASSETS"
  heading confirmed, no asset-type identifier / schedule language present.
  Not yet cross-checked against the comparison report for certified/staged
  amounts.

- **Comcast Corporation** — ack_id `20251007174512NAL0008660608003`
- **ack_id**: `20251007174512NAL0008660608003`
- **Certified**: $5,152,855,131 — **Staged**: $0 (0% capture)

Not a parsing bug. This 3-page PDF is a **Master Trust/DFE filing** (Form
5500 Part I box "DFE" checked), pooling 3 participating plan numbers
(12458, 28024, 28050) with net trust assets of $20.84B. It contains a
"SUMMARY OF NET TRUST ASSETS" table — a different layout entirely, with no
"Schedule H"/"4i"/"Supplemental Schedule" language. `Supplemental pages:
[]` is correct behavior for this document type; the pipeline was simply
never built to parse Master Trust summaries. Comcast's certified $5.15B is
its allocated share of the pooled trust, reported via Schedule D on its own
plan's Schedule H — not present in this PDF.

**Decision needed**: is Master Trust/DFE parsing in scope? If yes, needs a
dedicated parser for the "SUMMARY OF NET TRUST ASSETS" format (see
`docs/plan_notes.md` for full page-by-page detail).

---

## 5. Macy's — table has no column-ruling lines, whole section collapses into one cell

- **Plan**: Macy's, Inc.
- **ack_id**: `20251015182817NAL0011027730001`
- **Certified**: $2,252,908,000 — **Staged**: $0 (0% capture)

Text extraction is perfect — page 3's raw text is a clean, fully readable
"SCHEDULE OF ASSETS HELD FOR INVESTMENT PURPOSES" (1,846 chars, real text,
not cid garbage), including a legible $1.5B "VALUE OF INTEREST IN
COMMON/COLLECTIVE TRUSTS" section (Northern Trust CCTs) and a "VALUE OF
INTEREST IN REGISTERED INVESTMENT COMPANIES" section (JPMorgan MMKT, Macy's
Mutual Fund Window, 8x Vanguard Target Retirement trusts).

**Root cause**: this PDF has **no visible column-ruling lines**, so
`pdfplumber.find_tables()` / Camelot returns just **one table per page with
the entire multi-line schedule crammed into a single giant cell per
section**, instead of one row per holding. The pipeline's custom
section-splitter still detects 2 section boundaries within that blob
("Splitting page 3 into 2 section table areas", "Reusing same-page column
map for section table on page 3" in the log — looks like progress), but
since each "table" is really 1-2 undivided mega-cells with no real column
boundaries, the per-row loop (`text_extract.py` ~line 1990 onward) never
finds header/value-shaped cells to populate `row_data`. Every row is
silently dropped — no error, no `[OK] Extracted N` line, nothing. Confirmed
directly against `investments` table in `pipeline.db`: Macy's `sponsor_ein`
has **zero** investment rows, not just zero rows lost to a downstream
filter/join.

The existing poor-quality-retry check (`text_extract.py:2145`,
`meaningful_rows / len(rows) < 0.1`) never fires here because `rows` itself
ends up empty for this page — so the text-based fallback path
(`extract_text_based_investments`, already proven to work on WellSpan's
ticker schedule) never gets a chance to run, even though the clean text
layer means it likely would work fine.

**Fix candidate**: add a geometry-aware retry trigger — detect when a
"table" returned by Camelot/pdfplumber consists of 1-2 rows with an
oversized multi-line single-cell block (character count far exceeding a
normal cell) and treat that as a failed table extraction, routing the page
to `extract_text_based_investments` the same way pages with `not rows` or
low `meaningful_rows` ratio already are.

**Action item**:
- [ ] Add a check in `text_extract.py`'s pages-to-retry logic (~line 2117)
      that flags a page as poor-quality when its table has very few rows
      but an abnormally large single-cell text block (column-ruling-less
      table), so it falls through to `extract_text_based_investments`
      instead of silently producing zero rows. See finding #5 (Macy's)
      above for root cause detail.

---

## 6. Gundersen Lutheran — stray mid-number space breaks the value regex everywhere

- **Plan**: Gundersen Lutheran Administrative Services Inc
- **ack_id**: `20251014161612NAL0003380401001`
- **Certified**: $2,101,982,929 — **Staged**: $0 (0% capture)

Page 23's "VALUE OF INTEREST IN REGISTERED INVESTMENT COMPANIES" section
totals $2,101,982,928.92 — matches certified almost exactly, so this single
page/section is the whole missing schedule, and it's unambiguously Mutual
Fund data (30+ funds: Vanguard Target Retirement series, Vanguard
Institutional Index, DFA, Baird Aggregate Bond, etc.). Text is fully
readable — not an OCR gap.

**Root cause**: `current_value` numbers have a stray space injected
mid-digits — e.g. `1 ,549,552.40` instead of `1,549,552.40`,
`8 2,736,077.39` instead of `82,736,077.39` — a PDF character-kerning
artifact that makes `pdfplumber` tokenize one number as two separate
"words." Every numeric-value regex in the pipeline
(`\$?\s*\(?\s*[0-9][0-9,]*(?:\.[0-9]+)?\)?`) expects one contiguous token,
so a value with an embedded space never matches as a value cell. Confirmed
via the full log block: table extraction found 92-103 row candidates per
page (17-24) but **0/N meaningful** on every page, and the text-based
fallback also came back empty on all 8 pages
(`[!] No investments found in text format either` x8) — both paths use the
same broken regex, so both fail identically across the whole document.

**Fix candidate**: make the numeric-value regex tolerant of a stray
internal space in long numbers, or strip embedded whitespace from
numeric-looking tokens before matching (e.g.
`re.sub(r'(?<=\d)\s+(?=\d)', '', token)` before applying the value regex).

---

## 7. American Airlines — composite account descriptions default to wrong asset_type

- **Plan**: American Airlines, Inc. - Retirement (Master Trust for DC Plans
  of American Airlines, Inc. and Affiliates)
- **ack_id**: `20251013090403NAL0002149570001`
- **Certified**: $2,023,744,079 — **Staged**: $53,862 (0.003% capture)

Extraction itself succeeded — this is purely a classification bug. All 21
line items on the PDF's page 1 are present in `investments` with correct
issuer names and values matching the source document exactly (e.g.
`TARGET DATE 2030` → `2,291,902,852`, matches the PDF). But **19 of the 21
rows are tagged `asset_type = 'Employer Stock'`**, one (`BROKERAGELINK`) is
tagged `'Bond'`, and only the first row (`FID GOV CASH RESERVE`) has a
blank asset_type — none are tagged `Mutual Fund`, despite most rows
literally containing "Registered Investment Company" (= Mutual Fund) in
their description, e.g. `"Separately managed account which includes:
Corporate Common Stocks, Registered Investment Company, Common/Collective
Trust and Interest Bearing Cash, etc."`.

The exact staged figure ($53,862) is not a coincidence — it equals row 1's
value precisely, the only row with a **blank** asset_type rather than the
wrongly-assigned `Employer Stock`. Whatever downstream logic builds the
staged MF-scoped total appears to only pick up blank/unclassified rows,
excluding everything explicitly (but wrongly) tagged `Employer Stock`.

**Root cause**: none of these composite "Separately managed account which
includes: ... Registered Investment Company ..." descriptions match any
existing section-heading pattern (they're not clean single-word headings
like "Institutional Funds" — they're full sentences describing a blended
account), so the classifier falls back to a default asset_type of
`Employer Stock` instead of parsing the RIC/CCT language embedded
mid-description.

**Fix candidate**: add a fallback classification rule that scans the full
`investment_description` text for keywords like "Registered Investment
Company" / "Common/Collective Trust" when no section heading matched, and
tags `Mutual Fund` accordingly, instead of defaulting to `Employer Stock`.

**Note**: this PDF is also a pooled Master Trust document (total
$27,388,265,073 vs. AA's certified $2.02B), so even a full classification
fix would only close part of the gap — see finding #4 for the related
allocated-share scope question. But per user decision (2026-08-13), that
distinction doesn't matter for prioritization here since the PDF maps
1:1 to this ack_id regardless of the underlying Master Trust structure.

---

## 8. Froedtert ThedaCare Health — whole-document glyph corruption (same pattern as CVS Health)

- **Plan**: Froedtert ThedaCare Health, Inc.
- **ack_id**: `20250914082113NAL0000341680001`
- **Certified**: $1,966,990,552 — **Staged**: $0 (0% capture)

Not OCR in the "no text layer" sense — `pdfplumber` finds real, nonzero
character counts on nearly every page (e.g. page 4: 21,919 chars, page 5:
23,707 chars). But the text is 100% unmapped `(cid:NN)` glyph codes, e.g.
page 4 opens `'(cid:0)(cid:2)(cid:3)(cid:4)(cid:5)(cid:6)(cid:7)(cid:8)...'`
— identical failure mode to CVS Health (#3): the embedded font subset has
no usable ToUnicode CMap, so glyph IDs never resolve to real characters.
This is a different mechanism from Stanford (#1)/Avago (#9), which have
near-zero chars and no text layer at all — Froedtert's text "extracts"
but is semantically garbage.

**Fix candidate**: same as CVS Health — `USE_OCR=1` should work around this
since OCR reads the rendered page image rather than the font's broken
character map.

---

## 9. Avago Technologies — no extractable text layer (true OCR gap)

- **Plan**: Avago Technologies U.S. Inc.
- **ack_id**: `20251009135917NAL0011609856001`
- **Certified**: $1,895,742,465 — **Staged**: $0 (0% capture)

Confirmed classic OCR gap, same pattern as Stanford (#1): all 19 pages show
**0 extractable chars**, 1 full-page image per page, 0 rects, 0 lines. This
is a scanned document with no text layer at all — not a font-mapping issue
like Froedtert (#8)/CVS Health (#3).

**Fix candidate**: `USE_OCR=1`.

---

## 10. TWDC Enterprises (Disney) — composite multi-report format, no ruled table structure

- **Plan**: TWDC Enterprises 18 Corp. (Disney Retirement Plan Master Trust)
- **ack_id**: `20251008155841NAL0006101921001`
- **Certified**: $1,692,043,384 — **Staged**: $0 (0% capture)

Text extracts cleanly and is fully readable (not OCR/cid corruption) — page
9, for example, is a clean list of ~50 fund names/codes (`BDAU MAKENA
GLOBAL EQUITIES`, `BDVX DISNEY COMMON STOCK`, etc.). But
`page.find_tables()` returns **0 tables** — same no-column-ruling symptom
as Macy's (#5). Unlike Macy's, this is a 522-page **composite report**
built from many short "BDVC ... PAGE: N" sub-reports (one per participating
plan number within the Disney Master Pension Trust), so the fund-code
legend and the actual dollar values likely live on different page ranges
within each sub-report rather than in one flat schedule — this needs
verification before building a fix.

**Fix candidate**: needs a dedicated non-table parser for this composite
format — likely keying off the fund-code list plus a separate
values/holdings section per sub-report, rather than assuming one flat
per-page schedule like Macy's. Requires deeper page-range investigation
(which pages within each ~15-page sub-report carry the $ figures) before a
fix can be scoped precisely.

---

## 11. Consolidated Edison of New York — no extractable text layer (OCR gap)

- **Plan**: Consolidated Edison Company of New York, Inc.
- **ack_id**: `20250930082946NAL0011438529001`
- **Certified**: $1,484,429,237 — **Staged**: $0 (0% capture)

Confirmed via `pdfplumber`: **zero extractable characters across all 33
pages** of the PDF. This is a fully scanned/image-based filing with no text
layer at all — same category as Stanford (#1), not a parser bug.

**Fix candidate**: same as #1 — enable `USE_OCR=1` for this plan (or the
same zero-chars-detection cohort generally).

---

## 12. Howmet Aerospace — real holdings live in an unrecognized Master Trust participation schedule

- **Plan**: Howmet Aerospace Savings Plan (via ALCOA COMBINED MGRS Master Trust)
- **ack_id**: `20251015060139NAL0001924163002`
- **Certified**: $1,509,507,305 — **Staged**: $0 (0% capture)

The PDF contains **two structurally different schedules**. Page 3 has the
standard Schedule H columns (a)-(e) format, but it only lists a single line
item: a $52.2M Schwab Self-Directed Brokerage Account. The actual bulk of
the plan's holdings — corporate stock, unallocated insurance contracts,
Common/Collective Trust, and a **"REGISTERED INVESTMENT COMPANIES"**
section (individually named funds like `AMER FND INV CO OF AM-R6`,
`AMERICAN BALANCED FUND-R6`) — are reported on page 1 in a completely
different Master Trust participation report layout: columns "Security ID /
Security Description / Shares / Cost / Market Value / Unrealized Gain-Loss"
with category subtotal rows ("TOTAL CORPORATE STOCK - COMMON", "TOTAL
COMMON/COLLECTIVE TRUST", etc.), no ruled table lines. This format isn't
recognized by the current schedule extractor, so it's silently skipped —
the $1.3B+ in real holdings on page 1 is never read at all.

Note: `Registered\s+Investment\s+Compan(?:y|ies)` → `Mutual Fund` **is
already present** in `src/asset_type_patterns.py:59` — the classification
pattern itself is fine. The gap is purely that the page carrying it is
never parsed as a table/schedule in the first place.

**Fix candidate**: build a parser for this Master Trust participation
report layout (recognize the "Security ID / Security Description / Shares
/ Cost / Market Value" header row and per-category subtotal lines as a
distinct schedule type, separate from the standard (a)-(e) format).

**RESOLVED (2026-08-14)**: Built a filer-agnostic detector/parser in
`src/text_extract.py` (`_composite_participation_schedule_pages`,
`_extract_composite_participation_rows_for_pdf`, plus supporting helpers
`_match_asset_category_text`, `_infer_participation_column_order`,
`_split_trailing_numeric_tokens`). Deliberately keyed off structure, not
Howmet-specific wording: a flat, unruled page qualifies if it has (a) ≥1
recognized bare section-heading line, (b) ≥2 recognized `TOTAL <category>`
subtotal lines (reusing the shared `ASSET_TYPE_PATTERNS` vocabulary via a
new `_match_asset_category_text` matcher, not new hardcoded strings), and
(c) enough comma-formatted numeric-row density. Column semantics
(shares/cost/value/gain-loss) are inferred per page from the header row's
own wording rather than assumed fixed order, so the same parser handles
Howmet's 4-column layout without filer-specific code.

Detection and row extraction are both scoped to the exact matching page
numbers (not "does this PDF match" as a whole) — necessary because Howmet's
own filing bundles this schedule (page 1) alongside an unrelated Form 5500
page and a transaction schedule; without page-scoping those leaked in as
garbage rows.

Local test against the Howmet PDF: 27 rows extracted, section subtotals
match the PDF's own printed subtotals exactly, Mutual Fund (Registered
Investment Companies) total = $1,509,507,305.37 vs. certified
$1,509,507,305 (matches to the penny, rounding aside).

EC2 scoped live-pipeline verification (command
`8e90ef1c-1a42-4966-bbaf-65f70d2000f7`, 2026-08-14): Status=Success, same
27 rows, same classification breakdown (9 Insurance General Account, rest
Common/Collective Trust Fund incl. the 3-way "EB TEMP INV FD" split),
clean run through cleanup/QA/export/validation/MF-backfill with no errors.
Confirms the local result holds under the real pipeline, not just the
standalone parser test.

Deployed as part of commit `c0ebaf62f48f` (also includes an incidental
`deploy_to_ec2.sh` portability fix for a Windows Git-Bash/`aws.exe` path
bug — see below — and one new `asset_type_patterns.py` entry:
`Unallocated Insurance Contracts?` → `Insurance General Account`).

Confirmed NOT a false-positive risk: tested against both J&J PDF variants
(finding #13) and the detector correctly fires on neither — J&J's format
is structurally different (no TOTAL-category subtotals, no section
headings, direct-securities pool codes) and still needs its own bespoke
work if ever in scope.

---

## 13. Johnson & Johnson — 289-page composite Master Trust direct-securities schedule, not a mutual-fund menu

- **Plan**: Johnson and Johnson (Composite Master Trust)
- **ack_id**: `20251015121024NAL0002265923001`
- **Certified**: $1,407,259,857 — **Staged**: $0 (0% capture)

Text extracts cleanly (552,768 chars across 289 pages, not OCR/cid
corruption), but every page has **zero ruling-line edges** and no
"Schedule of Assets" / "Identity of Issue" heading — instead each page is
headed `JJ3K PAGE: N — COMPOSITE PLAN YEAR ENDING: 12/31/24 — SCHEDULE H,
LINE 4I - SCHEDULE OF ASSETS`, followed by hundreds of individually listed
direct securities (FNMA/FHLMC mortgage pools, Treasury TBAs, corporate
bonds) under internal fund codes (`JJDV`, `JJ2H`, `JJ2I`, etc.), two text
lines per holding, no columnar/ruled structure at all. This is a Master
Trust investing directly in fixed-income securities, not a menu of named
mutual funds — structurally a different kind of filing than what this
pipeline is built to extract.

**Fix candidate**: needs its own bespoke parser if in scope at all — worth
a scoping decision (defer/exclude vs. build) before investing further,
since the content isn't DCIO/mutual-fund-shaped and a fix would only
attribute participants' record-keeping-level fund elections rather than
these composite trust-level securities.

---

## 14. Advocate Aurora Health — rows extract cleanly but asset_type never populates (no section headings)

- **Plan**: Advocate Aurora Health 401(k) Plan
- **ack_id**: `20251007064524NAL0010856370001`
- **Certified**: $1,316,458,216 — **Staged**: $0 (0% capture)

Schedule H, Line 4(i) (page 17) is a completely standard columns (a)-(e)
table and extracts fine structurally: every row's "Identity of Issue"
column is **the same recordkeeper/trustee name, "Empower Trust Company,
LLC"**, for all 24 rows — the actual fund names (`Vanguard Target
Retirement 2020 Trust A`, `Dodge & Cox International Stock X`, `T. Rowe
Price Large Cap Growth Trust D`, etc.) live entirely in column (c),
"Description of Investment". Asset-type classification in this pipeline is
driven by **section headings** (`_detect_section_heading` /
`ASSET_TYPE_PATTERNS` in `src/asset_type_patterns.py`) — this schedule has
none, just the standard column labels — so `asset_type` never gets set for
any row despite the row data itself being fully correct.

**Fix candidate**: when no section heading is present, fall back to
per-row classification off column (c)'s fund-name text (many names
self-identify: "... Trust A" / "... CIT" typically CCT, share-class
suffixes like "-R6"/"Inst" typically mutual fund) rather than leaving
`asset_type` blank — or flag these rows for manual/AI review rather than
silently zeroing the plan's capture.

---

## 15. Lumen Technologies — same composite-schedule format as Howmet, confirms generic detector, but a wrapped-CUSIP parsing gap remains

- **Plan**: Lumen Technologies (via CenturyLink DC Master Trust, Northern
  Trust "Assets Held for Investment Purposes" report)
- **ack_id**: `20250923164353NAL0002676707001`

244-page PDF: pages 1-204 are Form 5500 + a 202-page "5% Report - Part C"
transactions schedule (irrelevant, same shape as noise pages in other
filings); pages 205-244 (40 pages) are the real asset schedule, using the
**same structural pattern as Howmet** — category headings, `TOTAL
<category>` subtotal lines, no ruling lines — but with an added layer of
country/region sub-groupings under each category (e.g. "United States -
USD", "International Region - USD"), each rolling up via its own `Total
<Country> - <Currency>` line into the category `TOTAL`. Header wording:
"Security Description / Asset ID  Shares/Par Value  Cost  Current Value" —
3 trailing numeric columns (shares, cost, value), one fewer than Howmet's 4
(no gain/loss column).

**Good news — validates the generic approach**: the new
`_composite_participation_schedule_pages` detector (built for Howmet, no
Lumen-specific code) fires correctly on pages 205-244, and Lumen's
certified total ($1,141,874,062) matches the PDF's own "Total Value of
Interest in Registered Investment Companies" line (page 242) almost
exactly. This is real evidence the structural-signature design generalizes
beyond the one filer it was built against, per the "figure out how to use
it on other plans" ask.

**Gap found — not yet fixed**: when a fund-name description is long, some
of Lumen's rows wrap the leading CUSIP/Asset-ID token onto its own line,
separate from the description line that follows. The current row parser
(`_split_trailing_numeric_tokens` + `_parse_composite_participation_row`)
is line-by-line, so it extracts the correct dollar values (still
attributed to the right row) but loses the fund name — `issuer_name` ends
up as the bare CUSIP instead of the actual fund name. Needs a small
multi-line-join enhancement: when a line has no trailing numeric tokens and
matches the ID-token shape alone (nothing else on the line), treat it as a
continuation prefix for the next data line rather than a heading candidate.

**Not yet run on EC2** — this was local-only PDF inspection so far, no
scoped pipeline test attempted (unlike Howmet, which is fully verified).

**Fix candidate**: extend `_extract_composite_participation_rows_for_pdf`'s
line loop to buffer a lone ID-token line and prepend it to the next row's
`lead_text` before parsing, instead of only stripping ID tokens that are
already inline).

**Second confirming instance (2026-08-26): Kohls Corporation Master Trust**
(`20251210095920NAL0005401200001`, Northern Trust custodian, not Lumen's).
Same composite-participation-schedule shape (region-subgrouped `MFO`-prefix
rows, `Total <Region>` rollups feeding `Total <Category>`). Raw
`plan_holdings_staging` has only 3 rows for this plan total: the PDF's real
"Value of Interest in Registered Investment Companies" (Mutual Fund)
section has 9 rows (~$583.2M) but only 2 survived (~$109M, 19%); the
Common/Collective Trust section is similarly gutted (1 of ~11 rows). Each
surviving row's `raw_entity_name` is glued to the section/region heading
that precedes it, and specifically to whichever row sits at a heading
boundary or page break (page 8→9) — the same wrapped-heading/continuation
mechanism as Lumen, now confirmed on a second filer/custodian. **User
decision (2026-08-26): this needs a dedicated parser fix, not just a
one-off patch — multiple plans share this exact structure.**

---

## TWDC (#10) — worth re-checking against the new composite-schedule detector

Not yet done. TWDC's described symptom (clean text, 0 tables found, no
column-ruling) sounds adjacent to the Howmet/Lumen composite pattern, but
its structure was previously described as "fund-code list + separate value
section" across a 522-page composite of per-plan-number sub-reports — a
different shape from Howmet/Lumen's single flat category-subtotal
schedule. Should be tested against `_composite_participation_schedule_pages`
directly before assuming it needs a fully separate parser; if it doesn't
match, the sub-report structure genuinely does need its own work as
originally scoped in finding #10 above.

---

## 16. Syracuse University — column-mapping bug, wrapped-header cell (FIXED, not yet deployed)

`Syracuse_University_20251015102447NAL0002237763001` — 34 of 39 investment
rows had blank `asset_type` and $0 staged to `plan_mf_history_v3`, despite
the PDF self-labeling every row's investment vehicle type ("Mutual Fund",
"Variable Annuities", "Pooled separate account", "Deposit administration
contract", etc.) in column (c).

**Root cause**: not a classification bug — `ROW_TYPE_PATTERNS` /
`parse_investment_row` already handle this filer's exact wording correctly.
The bug is one step earlier: Camelot's `stream` flavor infers a *wider*
column boundary for the wrapped header cell "(c) Description of investment
including maturity date, rate of interest, collateral, par, or maturity
value" than the actual data rows use underneath it. The header cell lands
at column index 3; every data row's real description text ("Deposit
administration contract", "Mutual Fund", etc.) is actually in column 2.
Column 3 is empty in every data row, so `investment_description` — and
everything downstream that depends on it — came back blank for all 39
rows, even though the classification patterns were fully correct.

**Fix**: new `_verify_or_remap_description_column()` in `text_extract.py`
(same shape as the existing `_verify_or_remap_value_column` numeric check).
After the column map is built, verify the column mapped to
`investment_description` actually has text in the sampled data rows; if
it's empty, retarget to whichever immediately adjacent unmapped column has
description-like (non-numeric) text in most sampled rows. This is a
**general structural check, not a per-plan exception** — unlike the
existing `_PLAN_SPECIFIC_ISSUER_COLUMN_SHIFT_BY_NAME` dict (which is
gated on a regex match for one plan's name), this fix has no plan-name
gating at all; it runs on every table for every plan and only acts when
the mapped description column is verifiably empty.

**Regression-tested locally** across all 81 PDFs in `data/` and
`data/inputs/` (via a before/after scan using `extract_tables_and_map`
with `use_llm=False`): only 1 of 81 PDFs triggered the new remap logic at
all (`20220708124640NAL0018324257001.pdf`, 721 rows, blank-description
count dropped from higher to 99 after the fix); the other 80 were
completely unaffected — confirming the check is a no-op unless the
description column is genuinely empty, not a broad behavior change.

**Not yet committed or deployed** — local uncommitted change to
`src/text_extract.py` only. EC2 is still on commit `c0ebaf62` (Howmet fix),
which does not include this.

---

## Citadel Enterprise Americas LLC — CONFIRMED: OCR gap, not a parsing bug

`20250717153557NAL0000980354001` — near-zero capture. Confirmed root cause:
this filing's Schedule H pages are scanned/image-based, no extractable text
layer. Needs OCR (not a Camelot/column-mapping issue at all — nothing to
parse without OCR first). Moves out of "not yet root-caused" below into
this confirmed-cause, needs-OCR bucket alongside PSEG (see next finding).

---

## 17. Four-plan batch (Lyondell/Equistar/Houston Refining, Woodward, PSEG) — three distinct, unrelated root causes

Opened as a batch of 4 (Citadel, Lyondell, Woodward, PSEG); Citadel's cause
is OCR (above). The other three each have a **different** root cause —
none of them is a repeat of the Syracuse bug:

**LYONDELL CHEMICAL CO, EQUISTAR CHEMICALS LP, & HOUSTON REFINING LP**
(`20250926150056NAL0004064531001`) — page-classification gap, not a
column-mapping or asset-type bug. The source PDF is only 2 pages and is not
a standard Schedule H filing at all — it's a Fidelity-generated "Master
Trust — Summary of Net Trust Assets" report (Fund Name / Share Balance /
Historical Cost / Price / Total Market Value columns; top rows are
aggregated asset-category totals like "COMMON STOCK $180,095,759.46",
"CASH", "UNIT", etc., with no per-row type column in the source document at
all). `classify_pages_text` requires at least one positive keyword hit
(`SCHEDULE OF INVESTMENTS`, `4i`, etc.) from `config/keywords.yml`, and this
document's header text matches none of them, so `supplemental_pages` comes
back empty and the page is never extracted. Also checked against the
Howmet/Lumen `_composite_participation_schedule_pages` detector — doesn't
match either; this is a third, distinct document format. Needs its own
structural detector (by column headers "Fund Name"/"Share Balance"/"Total
Market Value" plus the "SUMMARY OF NET TRUST ASSETS" title, since
"SUMMARY" is also in `negative_keywords` and would need an explicit carve
out) and its own row parser (category-total rows vs. named-fund rows are
structurally different).

**WOODWARD, INC.** (`20250926131507NAL0010895024001`) — same class of gap
as Advocate Aurora (finding #14), not the Syracuse bug. Page 23 is a
standard Schedule H, line 4i table and extracts cleanly (issuer names, cost,
current value all correct) — but the *source PDF itself* leaves column (c)
"Description of investment" blank for every single fund row (no "Mutual
Fund" text anywhere per-row, unlike Syracuse where the PDF did self-label
each row). This is a real absence of data in the filing, not an extraction
bug — column-mapping fixes can't help here. Needs a fund-name-pattern
fallback classifier (e.g. "... Fund" / "... Index" suffix → Mutual Fund)
for filers that never write per-row type labels at all.

**PUBLIC SERVICE ENTERPRISE GROUP INCORPORATED**
(`20251013135637NAL0000680483001`) — corrupted/non-standard font encoding,
same bucket as Citadel (needs OCR, not a parsing fix). Confirmed via direct
`pdfplumber.extract_text()`: every page sampled (14 through 71) returns
literal `(cid:XX)` glyph codes instead of readable text — the PDF's font
has no usable ToUnicode mapping. No keyword match, no column-mapping, no
classification fix can work here since there is no real text layer to
read; this needs OCR fallback (same as Citadel).

---

## Not yet root-caused (flagged only, from the 100-plan comparison report)

- **Loyola University of Chicago** (`20251003142017NAL0002469824001`) —
  188.2% overshoot (certified $985M, staged $1.85B) — likely duplicate/
  double-counted rows.
- **Near-zero-nonzero plans** (<0.2% captured, stray line items only —
  likely parse failures, not true $0): Delta Air Lines
  (`20251014143400NAL0006349954001`), American Airlines
  (`20251013090403NAL0002149570001`), Stifel Financial
  (`20260615123345NAL0000011248001`), The Emory Clinic
  (`20251013083002NAL0000402803005`), Molson Coors
  (`20250919104203NAL0002173521001`). (Citadel Enterprise Americas moved
  above — confirmed OCR gap, not unroot-caused.)

---

## 18. Tower Health — CONFIRMED: OCR gap

`20251013104203NAL0002466066001` — 0 characters extracted via
`pdfplumber` on every sampled page across the 23-page filing. No text
layer at all. Same bucket as Citadel and PSEG — needs OCR, not a parsing
fix.

---

## 19. Quad/Graphics & Walmart — SAME ROOT CAUSE: `classify_pages_text` doesn't propagate "supplemental" across continuation pages

Both plans looked at together because they share one bug, not two:

**QUAD/GRAPHICS, INC.** (`20251010094830NAL0007889745001`) — 13-page
Schedule H, line 4i filing (~650 individual equity holdings). Only page 1
carries the header text with a keyword match
(`SCHEDULE OF ASSETS HELD AT END OF YEAR`); pages 2–13 are pure
continuation — just issuer/description/value rows with no repeated
header — so `classify_pages_text` never flags them as supplemental and
~92% of the holdings are silently dropped before extraction even starts.
The one page that *does* get extracted maps its columns correctly
(issuer, description, value all correct), so this is not a column-mapping
bug. Also noticed in passing: every "COMMON STOCK" row on page 1 gets
tagged `asset_type: Employer Stock`, which looks wrong for holdings like
3M, Abbott Labs, etc. that aren't the plan sponsor's own stock — worth a
separate look, but secondary to the missing-pages issue.

**WALMART INC.** (`20250814105617NAL0005370979001`) — same bug, bigger
scale. 64-page filing; only pages 12 and 40 carry the full repeated
Schedule H header text (oddly, both pages have near-identical content —
possibly a duplicated section in the source PDF, not yet explained).
Pages 13–39 and 41–64 — including page 48, which holds the entire
"Mutual Funds" section (2 line items, $1,032,322,277, matching the
certified figure exactly) — have no keyword match and are dropped
entirely. Confirmed via `classify_pages_text`: `supplemental_pages == [12,
40]` only.

**Fix needed**: `classify_pages_text` (`text_extract.py`) currently
requires a fresh positive keyword hit on *every* page independently. It
needs to propagate supplemental status across a contiguous run of pages
following a matched page — e.g. once page N matches, keep including
page N+1, N+2, ... as supplemental until a page looks like a clear
section break (new statement, signature page, etc.), similar in spirit to
the continuation-page reuse logic `extract_tables_and_map` already has at
the table level (`_looks_like_headerless_continuation`,
`previous_column_map` reuse) — but that logic only helps pages that are
*already* in `supplemental_pages`; it never runs on pages excluded at the
classification stage. This is likely a widespread gap, not unique to
these two plans — any multi-page brokerage-style holdings schedule where
only the first page(s) repeat the Schedule H boilerplate would hit the
same failure.

---

## 20. Annuity Board of the NFL Player Annuity Program — new format, same class as unbuilt TWDC (#10), not a "recent enhancement" case

`20250909173448NAL0020456369001` — 343-page filing. Certified figure
$1,018,292,830 matches the "Registered investment companies" total on the
balance-sheet summary (page 6) exactly.

The Schedule H, line 4(i) section (pages 19–171, ~150 pages) is **not** in
standard Form 5500 `(a)/(b)/(c)/(d)/(e)` column format at all — it's a
trading-system export: "Security ID / Security Description / Shares /
Cost / Market Value / Unrealized Gain/Loss" rows grouped under
"COMBINED PLAN – NFNGCALL1000" / "COMBINED PLAN – NFAGCALL1000"
sub-report headers (different plan codes for different sub-plans within
the Program). This is structurally the same shape already flagged and
scoped — but never built — for **TWDC (finding #10)**: per-plan-number
sub-reports, not the composite category-subtotal schedule that the
Howmet/Lumen detector handles.

Checked directly: `classify_pages_text` flags 309 of 343 pages as
supplemental (plausibly over-broad, since the running header repeats
"Schedule H, Line 4(i)" across the whole 150-page section — worth
sanity-checking for false positives separately), but the
Howmet/Lumen `_composite_participation_schedule_pages` detector fires 0
pages on it — confirming this genuinely needs its own new parser for the
trading-system-export row shape, not a reuse of the existing composite
detector. Not yet built. Should likely be scoped together with TWDC (#10)
and Lumen's wrapped-CUSIP gap (#15) as one "per-plan sub-report /
non-standard schedule format" body of work, since all three are variations
on the same theme: filers whose custodian exports a different table shape
than the standard Schedule H layout.

---

## 21. Four-plan verification (Saint Luke's, ZF North America, Marsh & McLennan, Arconic) — confirms 2, nuances 1, contradicts 1

Follow-up on an informal assessment that these four had a shared root cause
("St. Luke's and ZF are OCR, Marsh & McLennan needs a dedicated parser,
Arconic has all the data extracted but missing asset_type"). Verified each
independently against the actual filing PDFs (downloaded from
`s3://retirementinsights-bronze/filings_5500_pdf/...`) rather than against
certified/staged totals alone.

**ZF NORTH AMERICA, INC.** (`20251015090950NAL0002064595001`, certified
$866,218,062) — **confirmed clean OCR gap.** All sampled pages: `chars=0,
images=1`. No text layer at all; the page content is a rasterized image.
Staged total: $0.

**SAINT LUKE'S HEALTH SYSTEM** (`20251012112733NAL0000092819001`, certified
$919,158,882) — **confirmed OCR gap, but more specific than it first looks.**
The PDF's own table of contents points to the real Schedule H schedule on
pages 18-20; those pages show `chars=0, images=0` but nonzero vector
`rects`/`curves` (e.g. page 18: `rects=10, curves=2768`) — i.e. the schedule
is rendered as vector-drawn/outlined glyphs rather than an embedded font or a
raster image, the same mechanism already documented for Stanford (finding
#1). pdfplumber can't extract text from either a raster scan or
vector-outlined text, so "needs OCR" is the right practical fix in both
cases, but the underlying cause here is glyph-outlining, not scanning.
**Separate bug found along the way**: `classify_pages_text` false-positively
flags pages 7 and 17 as supplemental — page 7 is generic auditor-opinion
boilerplate ("In our opinion: The form and content of the supplemental
schedule...") matching the `SUPPLEMENTAL SCHEDULE` keyword without containing
any data (same false-positive mechanism as CVS Health, finding #3); page 17
is a bare 39-character section-divider page. Neither of the two flagged
pages is the real schedule, and the real schedule pages (18-20) are never
flagged at all — a second, independent bug worth its own fix, not just an
OCR gap.

**MARSH & McLENNAN COMPANIES, INC.** (`20251006164519NAL0006985280001`,
certified $915,751,537) — **not a total failure; produces garbled output,
needs dedicated work.** `classify_pages_text` correctly flags pages 5-7 (no
false positive here). Table extraction fails ("poor results, 0 meaningful
rows") on pages 6-7, but the text-fallback path still pulls 8 rows worth
$5.14B. The rows are garbled: one has `issuer_name` = a mis-parsed header
line ("5500 Supplemental Schedules EIN: 13-2854946 Plan Number: 001 Year
End: 12/31/") with `current_value` = "2024"; others have `issuer_name` =
"CUSIP:" with blank descriptions. Root cause is mixed: pages 1-3 have heavy
font/ToUnicode (cid) corruption (955-3949 `(cid:` occurrences per page),
while pages 4-7 (the actual schedule) are image-heavy with low real text
(`images=306-599, chars=941-1919`). Confirms the "needs a dedicated parser"
assessment, though the mechanism is a mix of cid-corruption and
low-density/image-heavy content rather than a single new-format story.

**ARCONIC CORPORATION** (`20251001123631NAL0013215265001`, certified
$907,105,485) — **contradicts the original assessment.** `classify_pages_text`
matches **zero** of the 2 pages, so extraction never starts at all — there
is no missing-asset_type problem downstream because there is no extraction
to begin with. Inspected the raw text directly: both pages are a "SUMMARY OF
NET TRUST ASSETS" master-trust report (fund name / share balance /
historical cost / price / market value, grouped under category headers like
"COMMON STOCK", "CASH", "UNIT", "GOVERNMENT BOND"), not a standard Schedule H
4(i) schedule and not containing any of the current keyword set (no
"SUPPLEMENTAL SCHEDULE", no "Schedule H" boilerplate). This is a
page-classification miss on a master-trust-summary format, the same general
family as finding #4 (Comcast/Bristol-Myers/MassMutual), not the
missing-asset_type bug originally suspected. Fix candidate: extend
`classify_pages_text` keywords to catch "SUMMARY OF NET TRUST ASSETS" /
"Master Trust" statement headers, or route master-trust-summary pages
through the same detection path as finding #4.

---

## 22. Ten-plan "no asset_type" list (Jefferson, Progressive, Toyota, etc.) — mostly stale-staging, but real double-counting and asset_type bugs remain

Follow-up on an earlier informal list of ~10 sponsor names suspected of
missing `asset_type`. The names were too generic to identify a single plan
each (e.g. "Jefferson" and "Presbyterian" each match 60+ unrelated sponsors
in `plan_master_index_universe`); picked the largest/most specific match per
name. Certified vs. currently-staged (production `plan_mf_history_v3`)
comparison:

| Sponsor | Certified | Staged | Rows |
|---|---|---|---|
| Progressive Corporation | $2.04B | $144.5M (7%) | 1 |
| University Hospitals Health System | $2.38B | $250.6M (11%) | 1 |
| HonorHealth | $973.2M | $146.6M (15%) | 1 |
| Mizuho Americas Services | $979.1M | $245.4M (25%) | 7 |
| Memorial Hermann Health System | $3.10B | $477.5M (15%) | 1 |
| New York-Presbyterian Hospital | $4.35B | $4.38B (101%) | 279 |
| Toyota Motor North America | $2.02B | $138.5M (7%) | 2 |
| Thomas Jefferson University | $2.37B | $230.9M (10%) | 1 |
| Cedars-Sinai Medical Center | $2.23B | $344.3M (15%) | 1 |
| Carnegie Mellon University | $2.34B | $373.3M (16%) | 1 |

9 of 10 show the same shape (exactly 1-2 staged rows, 7-25% capture); only
NY-Presbyterian (279 rows) is fully captured, proving the pipeline can do
this correctly on this document family.

Re-ran Progressive, Toyota, and Thomas Jefferson through the **current**
local codebase (all this session's fixes applied) instead of relying on what
is staged in production:

| Sponsor | Local rows | Blank asset_type | Local sum vs. certified |
|---|---|---|---|
| Progressive Corporation | 93 | 66 (71%) | $24.79B (12.1x over) |
| Toyota Motor North America | 1222 | 610 (50%) | $1.40B (69%, reasonable) |
| Thomas Jefferson University | 92 | 84 (91%) | $11.01B (4.6x over) |

**Two separate takeaways, not one clean story:**

1. The 1-row-in-Athena pattern is largely a **stale-staging** artifact, not a
   live bug — local re-extraction with current code produces 92-1222 rows,
   not 1. This should self-resolve once the deferred regression run + EC2
   redeploy happens (see Action Items above), for the row-count dimension.
2. But local extraction is **not clean either**. Asset_type is blank on
   50-91% of rows across all three, and two of the three (Progressive,
   Thomas Jefferson) sum to 4.6x-12x their certified value.

**Correction (2026-08-17): this is NOT a new/unscoped bug — it's the
already-root-caused over-capture problem, and a fix already exists, just
gated off.** Should have checked `docs/stage2_overcapture.md` and
`src/section_typing.py` before writing this up as unexplored. Root cause
(documented there as "Mode 3"): audited 4i schedules group holdings under
section headers (Common Stocks, Corporate Debt, Mutual Funds, ...); the core
text parser's `SECTION_HEADING_MAP` only lists MF-ish labels, so entering a
non-MF section doesn't reset the running section type — the stale MF type
bleeds onto bonds/stocks, which then all pass the MF load gate and get
summed together. This is exactly Progressive's 12x and Jefferson's 4.6x
overcount. A post-pass fix (`apply_section_typing_stage`, branch
`overcapture-reextract`, gated by `SECTION_TYPING=1` env var, default OFF)
already exists on `master` (`src/section_typing.py`) for a separate, larger
effort: reconciling a **~6,816-plan `OVER_CAPTURE_QUARANTINE` list** in
production (see `stage2_overcapture.md`).

Tested `apply_section_typing_stage` directly against Progressive and
Jefferson's extracted rows:

| Sponsor | Before | After (all rows) | After (MF-only) |
|---|---|---|---|
| Progressive Corp | 93 rows, $24.79B (1213%) | 38 rows, $12.39B | 2 rows, $295.8M (14%) |
| Thomas Jefferson Univ | 92 rows, $11.01B (464%) | 45 rows, $5.51B | 2 rows, $47.2M (2%) |

The stage substantially helps (roughly halves the row count/total via dedup
+ CIT exclusion) but does **not** fully fix either plan — `retyped_non_mf: 0`
for both, meaning the section-header geometric detector isn't matching
anything in these two specific PDFs, so most rows stay blank/uncategorized
and the MF-only total is still far short of certified. So the existing Stage
2 tooling is a real head start, not a complete fix, for this table shape.
Also worth flagging: I picked "Progressive Corporation" (ack_id
`20260513074811NAL0006610787001`) via a fresh broad Athena substring search,
but the actual **100-plan-rerun** set (the one `docs/plan_notes.md` and this
whole parking lot are built from) uses a *different* Progressive Corporation
ack_id (`20250520131349NAL0001242561001`, per
`data/outputs/pipeline_stage_compare.csv`) — worth re-checking against that
specific ack_id, since the two may be different filing years/entities.

**Next step**: check `docs/plan_notes.md` and the `OVER_CAPTURE_QUARANTINE`
list for whether Progressive/Jefferson (and the other 8 from this list) are
already tracked there under their correct 100-plan-rerun ack_ids, before
scoping this as new work.

---

## 23. "1-row survivor" bug — headerless plans lose every non-big-4-manager fund (9 confirmed instances, 2026-08-26)

Same class of bug as finding #14 (Advocate Aurora), but root-caused in
detail this session across 9 plans reviewed back-to-back (all "no asset
type" per user's own PDF inspection): `20251209141357NAL0003820562001`,
`20251007153158NAL0002578931001`, `20251015150753NAL0009946722001`,
`20251010153535NAL0004377779003`, `20251209102447NAL0003446401001`,
`20251014153055NAL0003290785001`, `20260107152947NAL0009140480001`, plus
the earlier `20251002095709NAL0000502128001` and Duke-style precursors.
Full per-plan detail logged in `docs/plan_notes.md`.

**Mechanism (two filters stacking, not one "backup asset type" rule):**

1. **`asset_type` gate**: on a document with no section-heading structure,
   `text_extract.py` can't set `asset_type` from headings. Blank-type rows
   with a real value are NOT dropped at `build_mf_rows_df`
   (`src/post_extract_validator.py:582-650`, see line 623) — they're
   deliberately kept for "classification in post-processing." That
   post-processing is `infer_asset_type()`
   (`llm_enhance_investments.py:48-78`): it tries generic keyword patterns
   against `issuer + description` first (bond/stock/fund/etc.), and only if
   none match does it fall to a hardcoded check for
   "vanguard"/"fidelity"/"blackrock"/"pimco" → `'Mutual Fund'`; everything
   else → `'Other'`. Prod load (`mf_only=True`) then drops every `'Other'`
   row. On headerless docs, `text_extract.py` often can't cleanly split
   issuer/description without a heading to anchor the columns, so many
   rows end up with short/mangled/truncated names that hit none of the
   keyword patterns — non-big-4 funds vanish; big-4 funds survive only via
   the hardcoded safety net.
2. **`asset_class`/`asset_sub_class` gate**: `src/mf_mapping_enrichment.py`
   copies these fields onto a row only via an exact
   case/whitespace-insensitive match of `raw_entity_name` against a name
   already classified in some prior plan; unmatched names become
   `PENDING_AI`. Common big-4 fund names (e.g. "Vanguard Institutional
   Index Fund Institutional Plus Shares", "Fidelity 500 Index") are cached
   from appearing in hundreds of other (properly-headered) plans, so the
   lone survivor looks fully classified while everything else silently
   disappears.

**Net effect**: these are not "1-fund plans" — every other genuine MF
holding on the document (State Street, T. Rowe Price, American Funds,
etc.) is being dropped as `'Other'`, not merely miscategorized.

**Fix candidates discussed (none chosen yet — user wants to revisit
separately)**:
(a) When a whole plan has **zero** rows with a heading-derived
`asset_type` (i.e. the plan is fully headerless), keep all blank-type rows
with a real value as `Mutual Fund`/`MANUAL_REVIEW` by default, excluding
only rows that hit an explicit non-MF `ROW_TYPE_PATTERNS` match (stable
value, GIC, annuity, etc.) — surfaces the whole schedule for review instead
of dropping most of it.
(b) Same as (a) but additionally require some fund-like signal in the
name/description (share count, "shares", a manager name) before keeping a
row — more conservative, may still drop some genuine funds with sparse
row text.
(c) Don't auto-load into `plan_mf_history_v3` at all; write headerless-plan
blank-type rows to a separate manual-review queue/table instead.

**Status**: root cause fully confirmed; fix design deferred — revisit as
its own discussion.

---

## 24. Fermi National Accelerator Laboratory — Mutual Funds table body embedded as a raster image (needs OCR fallback in the text-extraction path)

- **Plan**: Fermi National Accelerator Laboratory Tax Sheltered Annuity Plan
- **ack_id**: `20251013153754NAL0000784979001`
- **Certified/schedule total**: $745,408,649 — Mutual Funds section alone:
  $291,852,905 (~35 fund rows) + $48,847,326 (Fidelity Self-Directed
  Brokerage Accounts) = ~$340.7M missing
- **Staged**: only 18 rows in `plan_holdings_staging`, none of which are
  the ~35 Mutual Fund line items

This is a **third, distinct** failure mode — not the "1-row survivor"
classification bug (#23) and not the Quad/Graphics-style continuation-page
classification gap (#19). Page classification itself is correct here.

**Root cause**: PDF pages 18 and 20 (the filing's own footer page numbers
"15." and "14." — this filing contains the Mutual Funds section **twice**,
in reversed footer order, an apparent filer/scan artifact) hold almost the
entire "Mutual Funds" table as a **single embedded raster image** (confirmed
via `pdfplumber`: `chars=303, images=1, lines=0, rects=5` — the 303 chars
are only the letterhead: sponsor name, EIN, "(Continued)", page number; the
~35 fund rows + dollar values live inside one 736×887px embedded picture).
Compare to the very next page (19/21), which is normal native text and
extracted fine (BNY Mellon tail rows, CREF, totals).

`extract_tables_and_map()` in `src/text_extract.py` is entirely
text/Camelot-driven. Its own title-match regex (`has_line_4ij`) correctly
sees "SCHEDULE H, LINE 4I" in the native letterhead text on page 18/20, so
the page **is** correctly flagged as part of the target schedule —
classification is not the bug. But when it then tries to pull rows off
that page, there is nothing to parse (no `page.lines`, no real table text,
just a picture); the only native text fragment on the page (the EIN
string) is exactly what got picked up as a stray single-cell "row" — the
garbage EIN row visible in the 18-row staging dump.

An OCR fallback already exists in this codebase (`src/ocr_passes.py::
run_ocr`), but it is only wired into the separate rendered-page-image
branch of `run_pipeline.py` (~line 150-160, used when a whole document is
scanned images from the start). It is never invoked from the text-based
`extract_tables_and_map` path, so a page that's text-native everywhere
except one embedded picture gets zero OCR fallback — the picture's content
is simply invisible to the pipeline.

**Fix candidate (TBD, not yet built)**: wire an OCR-based fallback into the
text-extraction path — when a page passes title/keyword classification as
part of the target schedule but yields ~0 real table rows (has an embedded
image and/or `page.chars` far below what a populated schedule page should
have), rasterize that page and run it through `ocr_passes.run_ocr` (or an
equivalent OCR pass) the same way the image-only pipeline branch already
does, then feed the OCR'd cells into the same row-mapping/classification
logic used for text-extracted rows.

**Status**: root cause fully confirmed (2026-08-26); OCR-based fix is a
TBD action item (see Action Items above) — not yet designed or built.

---

## 25. Fox Corporation — Camelot table boundary silently truncates at a subtotal line, dropping a second section on the same physical page

- **Plan**: Fox Savings Plan (Fox Corporation)
- **ack_id**: `20250902150819NAL0014415937001`
- **Certified**: $734,636,346 (amt_mutual_funds) — **Staged**: $37,897,529
  (8 rows, ~5.2% capture)

Not a shares-vs-value bug (user's initial hypothesis, checked and ruled
out) and not a missed section-heading bug either. The 8 rows currently in
`plan_mf_history_v3` are real, correctly-valued dollar amounts — Blackrock/
Prudential bond-index funds sitting *inside* the Synthetic GICs/Stable
Value Fund wrapper contracts on page 17 of the PDF. They're legitimately
extracted, just not mutual funds. The actual "Mutual Funds" section (7
real funds — Dodge & Cox, MFS, 5 Fidelity index funds — summing to exactly
$734,636,346, matching certified to the dollar) sits on **page 19, directly
below a "Total Synthetic GICs $85,657,764" subtotal line**, and was never
extracted at all.

**Root cause**: confirmed by calling Camelot directly on page 19 (`flavor=
'stream'`) — it returns exactly one table, 27 rows (1 header + 26 data
rows), and stops dead right after the `16,233,894` subtotal line, never
reaching "Total Synthetic GICs," the "Mutual Funds" heading, or the 7 real
fund rows beneath it. Also confirmed via direct call to
`extract_text_based_investments(pdf_path, 19)`: it correctly extracts all
31 rows on the page, including the 7 Mutual Fund rows with the right
values — so the text-based fallback path already handles this page's
content perfectly *if it ever ran*. It doesn't, because the truncated
23/26-row Camelot table still clears the pipeline's per-page "is this
table good enough" quality gate (`meaningful_rows / len(rows) >= 0.1`),
so `extract_tables_and_map()` never falls through to the text-based retry
for this page.

This is a new bug class for this session: **a second, differently
laid-out section sitting below a subtotal line on the same physical page
is silently dropped, because Camelot's table-boundary detector treats the
subtotal as the end of the table, and the truncated table alone still
looks "good enough" to the existing per-page quality gate.**

**Fix candidate (TBD, not yet built — parked per user request 2026-08-28)**:
after Camelot returns a table for a page, cross-check its captured
trailing-numeric-value lines against ALL of the page's own value-lines (via
`pdfplumber.extract_text()`); if the page has value lines the table never
captured, treat the page as needing the text-based retry too (merging in
only the missed rows, not replacing the good ones). Deliberately NOT
proposed as a change to the shared `meaningful_rows/len(rows) < 0.1`
threshold itself — that check is load-bearing for every plan in the
pipeline, and the user specifically flagged concern about a change here
regressing plans that already extract correctly. Needs before/after
verification against several already-working plans (not just Fox Corp)
before being built, to confirm the new check is a true no-op on pages
where Camelot already captured everything.

**Status**: root cause fully confirmed (2026-08-28); parked, not yet
designed/built, per explicit user decision to hold off given the shared
code path this would touch. `docs/rerun_queue.md` not updated — no fix
exists yet to queue a reload against.

---

## 26. Johnson & Johnson (second ack_id) — same composite Master Trust direct-securities format as finding #13, not yet root-caused in detail

- **Plan/Sponsor**: JOHNSON AND JOHNSON
- **ack_id**: `20251015121015NAL0002381763001`
- **Certified**: `amt_mutual_funds` = $2,227,385,856

Not yet opened/inspected in depth — parked per user request (2026-08-30) to
come back later, "very different format." Quick look at
`plan_holdings_staging` (58 rows) shows the same fingerprint as finding #13
(J&J, ack_id `20251015121024NAL0002265923001` — near-identical ack_id
timestamp, same sponsor): raw entity names are bare internal fund codes
(`JJDE`, `JJBE`, `JJCA`, `JJDF`, `JJDB`, `JJ7F`, repeated dozens of times
across rows ranging ~$85K–$88M, mostly typed `common stock`), plus ~15 rows
with real private-fund names (`WHITEHORSE LIQUIDITY PART V`, `PENNYBACKER
VI LP`, `AG REALTY VALUE FUND XI`, `MC CREDIT FUND IV SM LP`, etc.) also
typed `common stock` — reads like a Master Trust composite schedule mixing
direct fixed-income/PE/real-estate positions under internal fund codes,
the same general shape as #13, not a standard mutual-fund menu. One
`"ASSET CATEGORY NOT FOUND"` / $0.00 garbage row and one State Street Short
Term Investment Fund row whose value (`2030.00`) looks like a
misparsed date/unit fragment, not a dollar amount — same pattern seen
elsewhere (Wilbur-Ellis `docs/rerun_queue.md` entry, finding-#6-style
mid-number corruption).

`plan_mf_history_v3` has only **1 row** for this ack_id — the same garbage
`"ASSET CATEGORY NOT FOUND"` row — nothing else graduated from staging.

**Not yet root-caused**: haven't opened the source PDF or compared its
actual page layout against #13's "Security ID / Security Description /
Shares / Cost / Market Value" composite format, or against the
Howmet/Lumen `_composite_participation_schedule_pages` detector. Given the
shared "JJ"-prefixed fund codes and near-identical ack_id timestamp to
#13, this is very likely the same underlying filing family/format —
worth investigating both together rather than as separate one-offs.

---

## 27. North American Stainless — OCR gap, and a live instance of the "1-row survivor" pattern with a mis-scaled value

- **Plan/Sponsor**: NORTH AMERICAN STAINLESS
- **ack_id**: `20251014072759NAL0004948082001`
- **Certified**: `amt_mutual_funds` = $288,021,535

Confirmed via `pdfplumber`: all 16 pages of this filing have **0 embedded
characters** (checked every page, not just a sample) but 6-10 images per
page — a fully scanned/image-only PDF, same bucket as Citadel, Tower
Health (#18), Consolidated Edison (#11), and Fermi National (#24). This
one went through the OCR path (not `text_extract.py`'s pdfplumber path),
and only survived as **1 row** in both `plan_holdings_staging` and
`plan_mf_history_v3`: a real, correctly-named fund ("JP Morgan US Large
Cap Core Plus Fund Common") but with `plan_investment_amt = $808` against
a certified total of $288M — looks like a share/unit count captured in
place of the dollar value, not a garbage/date row. This is a live,
concrete instance of the long-pending backlog item "extracted_row_count=1
pattern" — a real multi-fund schedule that the OCR path is only
recovering a single, badly-scaled row from.

**Not yet root-caused**: haven't traced which OCR pass/stage
(`ocr_passes.py`, `normalize_images.py`, `detect_tables.py`) is responsible
for only surfacing one row here, or whether the $808 value comes from a
units/shares column being misread as the dollar column. User's direction
(2026-08-30): park this specific plan here, but treat "build out the OCR
path" as real, needed work rather than a one-off — the growing list of
OCR-gap plans (#11, #18, #24, this one) suggests the OCR path is
significantly less mature than the pdfplumber text path and needs
dedicated investment, not just per-plan patches.

---

## 28. American Airlines Master Trust — composite Master Trust format + a separate dropped-page-2 bug

- **Plan/Sponsor**: MASTER TRUST FOR DC PLANS OF AMERICAN AIRLINES, INC. AND AFFILIATES
- **ack_id**: `20251013090403NAL0002149570001`
- **Certified**: `amt_mutual_funds` = $2,023,744,079

Small 2-page filing (59KB), but a genuinely tough one — two independent
problems stacked on top of each other:

**Problem 1 — composite Master Trust format (same class as #12 Howmet, #13/#26
J&J, #15 Lumen)**: every row on the schedule is a "Separately managed account
which includes: Corporate Common Stocks, Registered Investment Company,
Common/Collective Trust and Interest Bearing Cash, etc." — a blended
account, not a discrete single-asset-type fund (e.g. `TARGET DATE 2030`,
value $2,291,902,852; `US LG CAP STK IDX`, value $3,879,553,419). The
schedule totals $27,388,265,073 across ~33 rows, but certified
`amt_mutual_funds` ($2.02B) is a small fraction of that and doesn't map
cleanly onto any row or subset of rows — it's presumably the RIC/mutual-fund
slice embedded inside many composite accounts, with no allocation logic
anywhere in the pipeline to split it out. Every row except the one trivial
"FID GOV CASH RESERVE INTEREST BEARING CASH" line ($53,862) gets typed
`common stock` by the asset-type classifier and is filtered out before
reaching `plan_mf_history_v3`.

**Problem 2 — page 2 silently dropped, independent of problem 1**:
`classify_pages_text()` correctly flags both pages `is_supplemental=1`. But
`extract_text_based_investments()` does its own separate schedule-marker
check per page, and page 2 is a bare continuation with no repeated
"Schedule H"/"Schedule of Assets" title — so `has_schedule_marker` is
False. The headerless-continuation fallback
(`_looks_like_investment_continuation_page`) only covers a narrow
`parser_profile == "inline_mutual_fund_units_value"` case that doesn't match
this composite-account row format, so it doesn't rescue the page either.
Confirmed directly: `extract_text_based_investments(pdf, page_num=2)`
returns **0 rows**, even though the page's raw text is perfectly readable
and contains 13 real rows (`AA FED CREDIT UNION` $955,937,873, `STABLE VALUE
FUND` $607,900,770, `HIGH YIELD BOND FUND` $224,866,212, `INTERNATIONAL
STOCK` $433,621,387, `TARGET DATE 2065` $43,304,092, etc. — roughly $5.6B
missing entirely).

**Net effect in the DB**: 20 of ~33 rows reached `plan_holdings_staging`
(page 1 only); only **1 row** survived into `plan_mf_history_v3` (the
trivial cash line, $53,862) — everything else typed `common stock` and
filtered out.

**Not yet fixed**: parked per user request (2026-08-31). Problem 2 (dropped
page 2) looks mechanically fixable — widen the continuation fallback beyond
the single `inline_mutual_fund_units_value` profile — but problem 1
(composite-account value allocation) doesn't have an obvious fix without new
allocation logic, and both would need to be solved together to actually

## 29. Blue Cross Blue Shield Of Michigan 401(k) Master Trust — unmatched section heading blanks asset_type for an entire page, plus a design gap in the fallback logic

- **Plan/Sponsor**: Blue Cross Blue Shield Of Michigan 401(k) Master Trust
- **ack_id**: `20251015142629NAL0004829057001`
- **EIN/PN**: 38-2069753 / 096
- **Certified net assets**: $2,213,219,330.77 (single-page schedule, ~46
  real holdings)

Extraction itself is not broken: current EC2 code (unchanged since the
ALLETE deploy, commit `2e05e5d2`) pulls all 46 rows locally with correct
dollar values, matching the PDF line-for-line. The problem is
**`asset_type` is blank for every single row on the page**.

**Root cause**: this PDF has no ruled table lines, so Camelot falls back to
"stream" (whitespace-guessing) mode. In that mode
(`src/text_extract.py:3025-3085`), a table row with exactly one non-empty
cell is checked against `ASSET_TYPE_PATTERNS`
(`src/asset_type_patterns.py`) via fullmatch; if nothing matches, the code
assumes the cell must be a fund name that Camelot split across two rows,
and glues it onto the next row's issuer-name column
(`text_extract.py:3078-3084`). This page's one section heading, `"FIXED
INCOME FUND"`, doesn't fullmatch any of the ~90 patterns in that list
(closest are `Fixed Income Securities` → Bond, `Insurance Company General
Account`, `Group Annuity Contract` — none match this filer's literal
phrase), so it got glued onto the next row (`JP MORGAN CHASE`) as
`'FIXED INCOME FUND JP MORGAN CHASE'`, and — since this page has no other
heading anywhere — `current_section_type` never gets set at all. Every one
of the 46 rows on the page inherits blank asset_type as a result, not just
the 9 insurance-contract rows under that heading.

**Design gap (separate from the missing pattern)**: the fallback code has
no third option between "recognized heading" and "must be a split fund
name" — an unrecognized bare category label is silently assumed to be the
latter, permanently discarding the type signal with no log/signal
distinguishing the two cases. Worth revisiting the fallback logic itself,
not just adding the missing pattern.

**Downstream effect verified**: staging (`plan_holdings_staging`) is fine —
traced `build_mf_rows_df(mf_only=False)` / `HOLDINGS_STAGING_TABLE` write
path and confirmed it is permissive enough (keeps blank-typed rows, only
drops junk names/no-value rows) that a rerun on current EC2 code would load
all 46 rows into staging regardless of this bug. But `_route_mf_from_staging`
only promotes rows whose asset_type is literally `mutual fund` / `index
fund` / `etf` / `target date fund` into `plan_mf_history_v3` — blank
doesn't qualify, so none of the real mutual-fund-like holdings here
(Harbor Cap App Ret, Glenmede, Putnam, Parnassus, UBS, MFS, ABF, JH DSCPL,
and the full Fidelity/Freedom Blend lineup) would be promoted until this is
fixed.

**Not yet fixed**: parked per user request (2026-09-01). Open question
before writing a fix: what canonical `asset_type` should `"FIXED INCOME
FUND"` map to? The 9 line items under it (JP Morgan Chase, Prudential,
MassMutual, Transamerica, Nationwide, Pacific Life, American General,
MetLife, State Bank & Trust Boston) read like a synthetic-GIC/stable-value
wrap structure, not a literal bond fund — candidates are `Stable Value
Fund` or `Insurance General Account` (both already canonical), needs a
call before adding the pattern. No rerun queued — not a code fix yet.
reconcile this plan's `plan_mf_history_v3` total to the certified figure.
