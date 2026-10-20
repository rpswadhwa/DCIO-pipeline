"""Deterministic text parser for Schedule H, Line 4i "Schedule of Assets
(Held at End of Year)" pages laid out as fixed columns:

    Asset Identifier | Security Title | Interest Rate | Maturity Date |
    Face Amount or Number of Shares | Cost | Market Value

with holdings grouped under section-heading lines ("Mutual funds:",
"Absolute return:", "Private Investments:", ...) and each section closed by a
numbers-only subtotal line. First seen on Mayo Pension Plan (ack
20251006115620NAL0003598321001, pp.45-52), where LLM_FULL_EXTRACT kept losing
whole pages to Gemini read timeouts on these dense pages (2026-10-12).

No LLM involved: asset_type comes from the section heading the row sits
under, every section that both starts and ends inside the parsed pages is
reconciled against its printed subtotal, and the row total is checked
against the schedule's printed grand total.

A second layout, "issue_par_rate" (2026-10-14), covers filers that print

    Identity of Issue | Par Value | Rate (%) | Maturity Date | Cost | Current Value

with ALL-CAPS section headings ("SHORT-TERM INVESTMENTS", "PARTNERSHIPS")
closed by "TOTAL <HEADING> <cost> <value>" lines and no asset identifier
column. First seen on Shell Pension Plan (ack 20251009145059NAL0006755009001,
pp.90-128), where the generic extractor dropped ~$3.0B of $9.49B in whole
blocks (all commingled funds, 30 partnerships, entire bond pages). Those
pages pad right-aligned numbers with space glyphs that overlap the first
digit, so pdfplumber reads "739,223" as "7 39,223"; _drop_overlapping_spaces
removes them before text extraction.

A third layout, "detail_report" (2026-10-14), is the custodian "5500 Assets
and Liabilities Detail Report" (BNY, report ID NA101E): the whole Schedule H
balance sheet, one line per holding as "<title> <beginning of year> <end of
year>", nested under the Schedule H line items themselves -- "C. GENERAL
INVESTMENTS" > "(4) CORPORATE STOCKS" > "(B) COMMON" -- with each leaf closed by
"TOTAL <leaf heading> <boy> <eoy>" and "F. TOTAL ASSETS" as the grand total.
First seen on RTX Master Retirement Trust (ack 20251014172506NAL0006971138001,
203 pp.), whose $11.9B "(5) PARTNERSHIP/JOINT VENTURE INTERESTS" section had
never been extracted. asset_type comes from the line-item path, so "(A)
PREFERRED" is a Bond under corporate debt but Preferred Stock under corporate
stocks. Cash and receivables (A., B.) are parsed only to reconcile against
TOTAL ASSETS; no rows are emitted for them, nor for holdings sold during the
year (end-of-year value 0).

A fourth layout, "soi_revalued_cost" (2026-10-14), is the custodian "Schedule
of Investments at End of Plan Year at Revalued Cost" (BNY, report ID M1102E):
one line per holding as "<account> <security id> <description> <shares>
<cost> <market value> <gain/loss>", long descriptions wrapping onto bare
continuation lines, ALL-CAPS Schedule H section headings closed by "TOTAL
<heading> <cost> <value> <gain>", and a numbers-only line before "GRAND TOTAL".
A continuation line can look like a heading ("INDEX FUND", "PARTNERS"), so a
line counts as a heading only if the schedule also prints "TOTAL <line>".
First seen on UPS Group Trust (ack 20251015204755NAL0011191170001, pp.5-236),
never extracted, whose JV section totals $10.22B.

A fifth layout, "jpm_held" (2026-10-14), is the JPMorgan Investor Services
"SCHEDULE OF ASSETS HELD FOR INVESTMENT PURPOSES AT END OF YEAR": one line per
holding as "<security id> <description> <cost> <current value>" followed by a
bare shares/par line, ALL-CAPS Schedule H headings underlined by a dash line,
each leaf closed by "TOTAL <heading> <cost> <value>". There is no grand total;
"TOTAL GENERAL INVESTMENTS" closes the investments block and is the check.
Rows are emitted only inside GENERAL INVESTMENTS -- non-interest cash and the
OTHER LIABILITIES block (overdrafts, futures, shorts, FX losses, payables) are
parsed only to reconcile their own subtotals. First seen on SBC Master Pension
Trust (ack 20251008170354NAL0006170017001, pp.275-889 of 889; pp.1-274 are the
acquired-and-disposed schedule), never extracted, JV section $6.76B.
2026-10-16: Southern Company (pp.68-355) splits the title around its trust
name, wraps long TOTAL amounts onto a second line, omits report pp.1-7 (rows
before the first heading open a partial section), adds a DOL compliance
footer and page number, and needs the registry's grand_value_gap.

A sixth layout, "bny_h30t" (2026-10-15), is the BNY "H30T" composite report
"SCHEDULE H, LINE 4I - SCHEDULE OF ASSETS (HELD AT END OF YEAR)" for a master
trust split across many manager accounts. Each holding prints as its
description line(s) -- bonds ending "<rate> <mm/dd/yyyy>" -- then a
numbers-only "<shares/par> <cost> <current value>" line for the whole trust,
then one "<account> [<security id>] <shares> <cost> <value>" line per manager
account. Headings are underlined by a dash rule; each section closes with a
column-rule line and a numbers-only total; the last page lists the category
totals and "GRAND TOTALS". One row is emitted per holding from the whole-trust
line; the per-account lines must add up to it. The filer's own footer (plan
name, EIN, "Schedule H Line 4i - Assets Held") is printed on every page, often
in the middle of the body, so any line found on at least half the pages is
dropped. First seen on MGB ERISA Master Trust (ack
20260702162718NAL0018068224001, pp.35-95), never extracted, JV section $9.16B.
Some holdings print with no description at all, just the whole-trust line
right after the previous holding's account lines; these are kept as
"UNDESCRIBED HOLDING <security id>" so the section still ties out (J&J,
2026-10-15: 13 JV holdings, $247M).
The same report with id "GEAC" is GE Pension Trust (pp.53-482, JV $8.51B).

A seventh layout, "flat_cost_value" (2026-10-15), is one alphabetical list of
"[*] <security name, rate, maturity> [$] <cost> [$] <fair value>" lines with
no category sections, closed by "Total investments - <pool> $ <cost> $
<value>". Only the first page carries the column header, so the run is the
header page through the page with the total; asset_type comes from each row
name (_FCV_ASSET_TYPES). First seen on Boeing Employee Retirement Plans
Master Trust (ack 20251013144009NAL0002216336001, pp.26-256).

An eighth layout, "cpa_total_lines" (2026-10-15), is the auditor-typeset
"SCHEDULE OF ASSETS (HELD AT END OF YEAR)" with "Form 5500, Schedule H, Line
4i" in every page's title block: an all-caps category heading, then
"<issuer/description> <par or shares> [$] <cost> [$] <current value>" rows
(cost may print "-"), closed by "TOTAL <category> <cost> <value>". "TOTAL
ASSETS (HELD AT END OF YEAR)" ends the held schedule; a securities-loaned
block may follow with its own categories and "TOTAL SECURITIES LOANED ...";
the grand total is the sum of those two lines. Right-aligned padding spaces
are drawn over the first digit of numbers and are dropped before extraction.
First seen on Central Pension Fund of the IUOE (ack
20251124120035NAL0001810145001, pp.31-96), JV section $5.08B. IAM National
Pension Fund (ack 20251104145018NAL0004803075002, pp.34-171) prints the same
schedule without "(b)" in the column header, with "SECURITIES ON LOAN -
<category>" sections and "TOTAL ASSETS ON HAND" / "TOTAL SECURITIES ON LOAN"
rollups, and a held total that already includes the loaned block; rollups
after a section's own total are skipped. National Elevator Industry Pension
Plan (ack 20260415130031NAL0011768563001, pp.86-119) titles its pages
"SCHEDULE H TO FORM 5500, LINE 4i - SCHEDULE OF ASSETS (HELD AT END OF
YEAR)", ends its headings with ":", prints partnership rows with cost and
value only, opens with a one-line "INTEREST-BEARING CASH $ <cost> $ <value>"
section, footers pages "- <n> - (Continued...)", and closes with "TOTAL
INVESTMENTS".

A ninth layout, "gain_loss_totals" (2026-10-15), titles every page "SCHEDULE
OF ASSETS AS OF <date> ... SCH H PART IV LINE 4(i)" and prints "<description>
<shares> <cost> <value> <gain/loss>" rows with no category headings: a
section is named only by its closing "TOTAL <category> <cost> <value>
<gain>" line, and "-TOTAL ASSETS HELD AT THE END OF THE YEAR" is the grand
total. Rows are typed from that closing name once it is reached. First seen
on Bell Atlantic Master Trust (ack 20251003134811NAL0000840547001,
pp.22-46), JV section $2.85B. Highmark Retirement Plan (ack
20251010111745NAL0012420624001, pp.169-288, 2026-10-17) files four BNY
M1102 account reports back to back, each ending in its own bare grand total
(summed), in fonts with no ToUnicode map whose glyph ids are ASCII less 29
("(cid:54)" is "S").

A tenth layout, "nt_par_cost_value" (2026-10-15), is a Northern Trust flat
list titled "Schedule H, Line 4I - Schedule Of Assets Held (Held at End of
Year)" on every page: "[*] <description> <share/par> <cost> <value>" rows
with no category headings or subtotals, closed by "TOTAL <par> <cost>
<value>". Rows are typed from their names, except that a registered plan may
give an "alt_span" (first-row and last-row patterns) for its private
partnership/real estate run, which carries names no pattern can type ("ROHR
ROAD", "GAINLINE GALAXY HOLDINGS LLC"); rows in it are typed "Partnership
Interest". First seen on Honeywell International Inc. Master Retirement
Trust (ack 20251219001905NAL0005704449001, pp.4-21): the span is $3.58B,
certified JV $3.08B plus real estate $0.50B. A variant (2026-10-17) titles
every page "Form 5500, Schedule H Line 4i - Schedule of Assets (Held At End
of Year) EIN <ein>" over a column header ending "maturity value", prints
cents, and closes with a bare "<cost> <value>" grand total. Its alt span
interleaves collective trusts and derivatives with the partnerships, so the
registry's "alt_span_exclude" pattern leaves those rows to name typing.
First seen on Hackensack Meridian Health Master Retirement Trust (ack
20251015063217NAL0001938915001, pp.2-18): span less exclusions
$545,852,464.74, its certified JV to the cent (the collective trusts it
excludes total $414,190,759.50, the certified CCT figure).

An eleventh layout, "bare_subtotals" (2026-10-15), is titled "Schedule H -
Line 4i - Schedule of Assets (Held at End of Year)" on its first page only;
later pages carry just the auditor's footer. A Schedule H category heading,
"<description> [units] <cost> <value>" rows (units may be blank or "n/a"),
then a bare "<cost> <value>" subtotal; a one-holding category prints no
subtotal. A "<fund>, LLC: ASSETS DETAILED BELOW:" block after a subtotal
belongs to the category above it. "Grand Total <cost> <value>" ends it.
pdfplumber's default x_tolerance runs each line's words together here, so
this layout extracts at 0.5. First seen on National Electrical Benefit Fund
(ack 20260730200559NAL0029346880001, pp.26-69): the two partnership/JV
categories total $3,349,872,727, its certified JV to the dollar. National
Electrical Annuity Plan (ack 20260730210143NAL0029408688001, pp.20-61) prints
"$ 279" for "$279" and a few other category names; its real estate and
non-real estate JV categories total $256,540,267, its certified JV (2026-10-16).

A twelfth layout, "inline_category" (2026-10-15), is a Fidelity flat list
titled "5500 Schedule of Investments at End of Plan Year" with the column
header on the first page only: every row prints its own Schedule H category
("<description> <5500 code> <shares/par> <cost> <current value>"), with no
headings or subtotals. Cash balances near the end print the value only. A
bare "<value>" line totals the holdings, then "Net Receivables/Payables" and
"Net Foreign Currency Fluctuations" adjustments, then "Grand Total"; the
holdings line is the one rows reconcile to. Sections are the categories in
order of first appearance. First seen on John Deere Pension Trust (ack
20260804121133NAL0001451057001, pp.1-78): 49 partnership/JV rows, $3.30B.

A thirteenth layout, "nt_held_regions" (2026-10-15), is Northern Trust's
"Schedule of Assets Held for Investment Purposes" ("Security Description /
Asset ID Shares/Par Value Historical Cost Current Value"): a category heading
("Partnership/Joint Venture Interests"), region-currency sub-groups ("United
States - USD") with their own "Total ..." lines, rows "<description> <shares>
<cost> <value>" followed by "CUSIP:"/"SEDOL:" lines, "Total <category>"
subtotals, and an unnamed grand "Total" line. Each page repeats the open
category heading. First seen on Dow (ack 20251008093014NAL0009466912001,
pp.51-183, printed again from p.296): 191 partnership/JV rows, $2.94B; all 14
categories and the grand total $10,675,199,179.42 tie to the dollar.

A fourteenth layout, "titled_appendix" (2026-10-15), is one appendix of a
per-manager 4i schedule, titled "Limited Partnerships and Other Private
Equity" on every page, with "(a) (b) (c) (d) (e)" column letters, rows
"<name> <Equity|Limited Partnership|Other Private Equity> <cost> <value>" and
a closing "Total Limited Partnership and Other Private Equity" line. Every row
is the plan's Schedule H JV line, so all are typed Partnership Interest.
pdfplumber splits some amounts after the first digit ("1 18,018,543"); amounts
are rejoined and must then group by thousands. Whole-dollar rows reconcile
within $0.50 each. First seen on Western Conference of Teamsters (ack
20260923162723NAL0013822515001, Appendix VII pp.415-416, 119 rows,
$14,459,497,754 vs certified JV $14,448,663,252; the 2024 filing's Appendix X,
pp.648-649, 126 rows, $14,576,499,306, also ties).

A fifteenth layout, "typed_description" (2026-10-15), is a tax-software
"Federal Statements" list, "(a) Identity Description Current Cost Current
Value" atop every page: one alphabetical run of "<name> <type> <cost>
<value>" rows, where <type> is the filer's own column ("Limited Partnership",
"Commingled Investment", "Money Market Mutual Funds", ...), ended by "(a) -
There are no parties in interest". No subtotal or total is printed, so a
line that isn't a typed row fails the parse. Commingled rows are index CITs
by name, otherwise offshore hedge funds. First seen on McKinsey Retirement
Trust (ack 20251010153934NAL0004381603001, pp.6-10, 377 rows, $18.76B against
$12.23B total assets: SSALT Fund Ltd, $6.67B, is MIO Partners' pool whose
holdings are also listed). A registry "exclude_rows" pattern drops such
rows; McKinsey's drops SSALT, leaving 274 rows, $12.05B.

A sixteenth layout, "typed_maturity_rate" (2026-10-16), is an auditor
"SCHEDULE OF ASSETS (HELD AT END OF YEAR)" whose column header ends
"Description Maturity Date Rate of Interest Value or Shares Cost Current
Value": a plain category heading, then "<name> <category again> <maturity>
<rate> <par> [$] <cost> [$] <value>" rows, closed by "Total - <category>
<cost> <value>", and "Total assets (held at end of year)" ends the schedule.
Padding spaces split digits ("2 4,894") and are dropped first. First seen on
UFCW Consolidated Pension Fund (ack 20251010142443NAL0012790944005,
pp.58-67), JV section $3.57B. Citigroup Pension Plan (2026-10-16) fits the
same parser: "Heading:" lines closed by "Subtotal <heading> <cost> <value>",
rows "<name> <par> [<rate>] [<maturity>] <cost> <value>", ended by "Total
Investments, at Fair Value". So does 3M Employee Retirement Income Plan
(2026-10-16): the header only on the first page, ALL-CAPS categories over
mixed-case sub-headings with a rollup "Total" after the sub-totals,
"<description> <cost> <value>" rows rounded to the dollar, "Grand Total".
So does FirstEnergy Corp. Master Pension Plan (2026-10-16), once its
Calibri glyph ids ("(cid:1004)" for "0") are mapped back to text.

A seventeenth layout, "closing_totals" (2026-10-16), is a Withum-style
"Schedule H, Line 4i" whose two-line column header ends "(d) Cost (e)
Current Value": rows "<identity> <description> [***] [<CUSIP>] <cost>
<value>" with identity and description in separate columns, sections that
often print no opening heading and are named only by their closing "Total
<category> <cost> <value>", sub-totals (labeled or bare) inside a section,
wrapped identity lines, and "Total investments" ending the schedule. First
seen on Laborers Pension Trust Fund for Northern California (ack
20260316154545NAL0003370770001, pp.22-42), limited partnerships $2.49B.
Its sister Laborers Annuity Plan for Northern California (ack
20260316160114NAL0001199491001, pp.18-30) splits the column header
differently and prints "Total corporate obligations"; limited partnerships
$312,809,825, its certified JV (2026-10-16).

An eighteenth layout, "investments_held_summary" (2026-10-16), is a
"Schedule H, Line 4i ... Investments Held Summary" that rolls an attached
custodial statement up to one line per fund or asset class: category
headings, rows "<custodian> Page <n> - <name> <statement balance>
<adjustments> <market value> <cost | See Attached>", a bare-amount subtotal
closing each category, and a bare grand total. Padding spaces split digits
("1 2,169") and are dropped first. First seen on Chicago & Vicinity
Laborers' District Council Pension Plan (ack 20260311155722NAL0005772992003,
pp.30-31), partnership/joint venture interests $1.25B. A two-amount variant
(2026-10-17) prints only "<fair value> <cost>" under "Fair Cost / Value
Value"; a category of one row prints no subtotal, the next heading closing
it, and a row with no "- <name>" is named by its heading. First seen on
Midwest Operating Engineers Pension Trust Fund (ack
20260114152527NAL0017415696001, p.34): partnerships $728,833,522.

A nineteenth layout, "nt_asset_detail" (2026-10-16), is Northern Trust's
"Asset Detail - Base Currency" statement filed as the schedule: holdings
"<description> CUSIP : <id>" over a line of amounts ending "<market value>
<cost> <gain market> <gain translation> <gain total>", so value and cost are
read from the right. Sub-categories close with "Total <sub-category>";
category and region totals are rollups and are skipped, and an unnamed
"Total" ends the statement. First seen on Motion Picture Industry Individual
Account Plan (ack 20260916145503NAL0004105473001, pp.19-300): 1,899 rows,
$6.76B, all 51 sub-categories tie; partnership interest $443.1M.

A twentieth layout, "colon_headings" (2026-10-16), prints columns "(a) (b)
Identity of Issuer, ... (g) Current Value" on every page: "<id> <name>
<rate> <maturity> <par> <cost> <value>" rows under "<CATEGORY>:" headings,
each category closed by a bare "<par> <cost> <value>" line and the schedule
by "(Concluded)". The first category's heading prints just above its
subtotal instead. Each row is rounded to the dollar, so "row_rounding"
allows half a dollar per row. First seen on Deseret Mutual Master Retirement
Plan (ack 20251012193307NAL0000661057001, pp.25-97, printed again
pp.118-190): 2,831 rows, $6.80B; limited partnerships $2,245,325,102, its
certified JV.

A twenty-first layout, "described_categories" (2026-10-16), is titled
"Schedule H Line 4i Schedule of Assets (Held at End of Year)" on every page
over columns ending "(a) (b) (c) (d) (e)". A line with no amounts is a
category heading; rows end "<description> <rate> <maturity> <par> <cost>
<value>" with N/A for blanks, but partnership rows print "<cost> <value>"
only. A bare "<cost> <value>" line closes each category and "$ <cost> $
<value>" ends the schedule. The text is drawn in Private Use Area glyphs that
_decode_pua_glyphs turns back into ASCII. First seen on Producer-Writers Guild
of America Pension Plan (ack 20251014193728NAL0007280802001, pp.20-43): 2,168
rows, $4.62B; partnership/joint venture interests $1,779,671,664, its
certified JV.

A twenty-second layout, "nt_cusip_columns" (2026-10-16), is a Northern Trust
list titled "Schedule H, line 4i-Schedule of Assets (Held At End of Year)"
with the column header, ending "(CUSIP)", on the first page only. Rows are
"[*] <id> [&&&] <description> <shares> <cost> <value>" with no headings or
subtotals, closed by "$ <shares> $ <cost> $ <value>". Amounts carry stray
spaces and blank columns, so words are placed by x position under the first
page's headers. The JV/other split isn't printed: the registry's alt_ids
names the private-fund id range and type_rows picks out the hedge funds.
First seen on IBM Personal Pension Plan (ack 20260924080256NAL0012578161001,
pp.63-133): 6,418 rows, $18.45B; C994J partnership rows $1,752.5M vs
certified JV $1,756.5M.

A twenty-third layout, "leading_shares" (2026-10-16), heads every page with
columns ending "shares Identity of issue or borrower Cost value". Rows are
"<shares|**> <description> <cost> <value>" under "<Category>:" headings,
each category closed by "Total <category> <cost> <value>" and the schedule
by "Total investments". "**" marks fund holdings, which the registry's
fund_types types by category; other rows are typed by heading, then by
description. First seen on Employees' Retirement Plan of Duke University
(ack 20260412094729NAL0020428274001, pp.42-74).

A twenty-fourth layout, "issuer_shares_category" (2026-10-16), carries its
column header on the first page only. Rows are "<issuer> <shares>
<description> <cost> <value>", where the description is often the 5500
category. Lines without amounts are headings (nesting, innermost wins),
"Total <heading>" closes a section and a bare "Total" line ends the
schedule. First seen on U.S. Bank Pension Plan (ack
20251014103148NAL0002530177001, pp.5-8).

A twenty-fifth layout, "party_type_tree" (2026-10-16), repeats its column
header on every page. Rows are "<party> <issuer> <description> <maturity>
<rate> <shares> <cost> <value>" with "N/A" in the party column. Every
amount-less line is a heading, "TOTAL <heading>" closes it, a parent's total
("TOTAL PARTNERSHIPS") covers its "PARTNERSHIPS: ..." children, and "TOTALS -
ASSETS" ends the schedule. First seen on National Automatic Sprinkler
Industry Pension Fund (ack 20251011102930NAL0008465633001, pp.28-33).

A twenty-sixth layout, "category_column" (2026-10-16), carries the column
header "Description 5500 Sup Category Name Share/Par Value Cost Value Total
Value" on the first page only. Every row prints its own category ("<description>
<category> <shares> <cost> <value>"), which types it; "Total <name> <shares>
<cost> <value>" closes the rows above it as a section. Accrued income and
pending trades follow the last section as amount-only lines, and "Total Master
Trust Assets $ <value>" ends the schedule. Padding spaces split digits and are
dropped first. First seen on L3Harris Pension Master Trust (ack
20251013074942NAL0001486160004, pp.1-47): 4,858 rows, $8.12B; partnerships/
joint ventures $1,496,908,618.

A twenty-seventh layout, "summary_attached" (2026-10-16), is a one-page fund
summary ("<party> <description> <cost> <value>", header "Similar Party
Description of Investment Value Value", closed by "Total <cost> <value>") whose
"Various - See Attached" line stands for the securities on the following pages
("<shares> <description> <cost> <market value>" under "PAR VALUE Description
Cost Market Value"), which end on a bare "<cost> <value>" line repeating the
See Attached amounts. The summary has no category column, so the registry's
row_asset_types types its funds. First seen on Boilermaker-Blacksmith National
Pension Trust (ack 20251014060650NAL0001055651001, pp.31-47): 28 summary
lines and 632 securities, $7.31B; limited partnerships and LLCs $1,826,941,787.

A twenty-eighth layout, "category_rate_totals" (2026-10-17), prints its column
header "(a) lessor or similar party or maturity value (d) Cost (e) Current
Value" on the first page only, with the plan's title block on every page. Each
row is "<description> <category> [<rate> <maturity>] <cost> <value>" ("—" for
zero, "(...)" negative); "TOTAL <category> <cost> <value>" closes the rows
above it, a bare "<cost> <value>" closes a one-category block that prints no
label (the insurance contract), and "TOTAL INVESTMENTS" ends the schedule. The
registry's categories pattern finds each row's category, which types it.
First seen on MassMutual Pension Plan (ack 20251008153331NAL0005767217001,
pp.18-44): limited partnerships $853.2M.

A twenty-ninth layout, "year_end_package" (2026-10-17), is a custodian's
"YEAR END PACKAGE" report: column header "DESCRIPTION OF INVESTMENT PRICE
SHARES/PAR VALUE ADJUSTED COST HISTORICAL COST CURRENT VALUE ADJ. COST HIST.
COST" on every page, rows ending in those seven amounts with any wrapped
description on the line(s) below, a category heading after each "=====" rule
(repeated as "<category> (CONT.)" atop a page), "TOTAL FOR <category, cut to 30
characters>" with six amounts (no price) closing it, and "TOTAL ASSETS" ending
the schedule. Cost is the adjusted cost. The registry's heading_asset_types
types each category.
First seen on SEIU Pension Plan Master Trust (ack 20260918083636NAL0012305074003,
pp.1-80): limited liability partnerships $799,950,765.

A thirtieth layout, "titled_category_pages" (2026-10-17), prints one category
per run of pages, its name in a "<CATEGORY> AS OF <date>" line under the
"(Held at End of Year)" title on every page, then a column header ending "COST
FAIR VALUE". Equity-style pages' rows are "<shares> <description> <cost>
<value>"; pages whose header has a "PAR VALUE" column are "<CUSIP>
<description> <rate> <maturity> <par> <cost> <value>". "TOTAL <CATEGORY>"
closes each category; there is no grand total, and the "(Acquired and
Disposed of Within Year)" pages after it end the schedule. The registry's
heading_asset_types types each category.
First seen on Allstate Retirement Plan (ack 20251007160657NAL0002598803001,
pp.23-64): limited partnerships $1,090,754,734.

A thirty-first layout, "leading_category" (2026-10-17), is a one-page
manager-by-category summary ("TOTAL $ ... $ <grand total>") followed by one
block per investment manager, each page titled "SCHEDULE H ITEM 4I - SCHEDULE
OF ASSETS HELD ..." and numbered "Page <n> of <N>". Rows lead with their 5500
category ("<CATEGORY> <cusip+description> <units> <historic cost> <revalued
cost> <market value>"), "TOTAL <CATEGORY>" closes a category and "TOTAL
INVESTMENTS" closes the manager's block. A block whose header lacks the
revalued column prints "<units> <cost> <value>" under category lines of their
own; a "DESCRIPTION MARKET VALUE" block lists values only under a category
line, closed by its "TOTAL <category> $ <value>". Padding spaces split digits
and words ("CORP ORATE") and are dropped first; categories match with any
spacing. First seen on United Food and Commercial Workers International Union
- Industry Pension Fund (ack 20260406150909NAL0007755856001, pp.30-114):
partnership/joint venture interests $735,443,714.

A thirty-second layout, "fmv_cost_summary" (2026-10-17), is a short summary
under the column header "@ FMV @ COST" (value before cost) on every page:
category headings, "<description> <fmv> <cost>" rows, and a "TOTAL <fmv>
<cost>" or bare "<fmv> <cost>" line closing each category. A "<CATEGORY>
(ATTACHED) <fmv> ATTACHED" line stands for a custodial statement attached
after the summary and is kept as one row, its category's own section (as
investments_held_summary keeps its See Attached funds). No grand total is
printed. First seen on Ohio Carpenters' Pension Plan (ack
20260213183216NAL0002049394001, pp.25-26): limited partnerships $650,271,125.

A thirty-third layout, "cusip_type_blocks" (2026-10-17), prints amounts in
thousands under the column header "of Party Cusip# Description Cost Value"
on its first page. Rows are "<cusip> <description> <cusip> <TYPE> <cost>
<value>", the TYPE column ("SEC PARTNERSHIPS", "REAL ESTATE EQU", "FIXED
INCOME - PUBLIC", ...) typing each row. Blocks open with a "*<party> <fund>"
line and close with a "Total <fund> <cost> <value>" line (wrapping onto a
second line, its name not always the opener's) or a bare "<cost> <value>";
a "*<party> <name> <cost> <value>" line is a one-row block. Rollup totals
between blocks are skipped and "TOTAL ASSETS HELD FOR INVESTMENT PURPOSES"
is the grand total. "—" is zero. First seen on Cigna Pension Plans Trust
(ack 20251014151901NAL0003145969001, pp.2-9): secondary partnerships
$410,009,000 and real estate equity funds $221,164,000.

A thirty-fourth layout, "bold_heading_totals" (2026-10-17), prints one
"<description> <cost> <value>" list under the column header "Cost Current
Value" (first page only), every line at the same indent. Category headings
are set in bold, wrapping onto a second bold line; each category closes
with a "Total <category> <cost> <value>" line (wrapping too, its name not
always the heading's). Before the total a category may print a bare
"<cost> <value>" sub-total and a "Less: Securities on loan (<value>)" line,
which counts in the category's sum without being a row. Rows wrap onto
following lines. "Total Investments $ <cost> $ <value>" is the grand
total; the plan's registry entry types the categories. First seen on Marsh &
McLennan Companies Retirement Plan (ack 20251006164419NAL0003816289001,
pp.21-77): partnership interests $605,910,690.

A thirty-fifth layout, "colon_heading_totals" (2026-10-17), repeats the
column header "(a) (b) Description of Investment (c) Units (d) Cost (e)
Current Value" on every page: "<category>:" headings, rows "[*] <description>
<units> [$] <cost> [$] <value>", each category closed by "Total <category>
<cost> <value>" except a one-row category, which the next heading closes,
and a bare "$ <cost> $ <value>" grand total. First seen on Kaleida Health
Pension Growth Plan (ack 20251013145833NAL0000734531001, pp.63-65): hedge
funds, private equity, global equity and fixed income funds $570,543,285,
its certified JV. A Northern Trust print (2026-10-17) ends a five-line
column header "(a) (b) Identity of Issue, ... (d) Cost (e) Current Value",
closes categories with a bare "Total $ <cost> $ <value>" and the schedule
with "Grand Total", marks rows "^" (not certified) and wraps them, the
description running onto the "CUSIP: <id> <units> <cost> <value>" line (a
line ending "CUSIP:" or "SEDOL:" is a wrapped row, not a heading). First
seen on United States Steel Corporation Plan for Employee Pension Benefits
(ack 20250930165123NAL0029836418001, pp.38-104): private equity
$562,122,640, its certified JV.

A thirty-sixth layout, "cusip_column_totals" (2026-10-17), titled "FORM
5500, SCHEDULE H, LINE 4i:", reprints the column header "Shares Cost
Current Value Cusip #" under every category heading: rows "[*] <description>
[<shares>] <cost> <value> <CUSIP>" ("-" for a nil amount), each category
closed by "TOTAL <name> $ <cost> $ <value>" (its name not always the
heading's) and the schedule by "TOTAL INVESTMENTS". First seen on The
Graham Holdings Company Master Retirement Trust (ack
20251007143913NAL0002550051001, p.1): partnership/joint venture interests
$557,122,138, its certified JV.

A thirty-seventh layout, "typed_column_totals" (2026-10-17), heads every
page "(a) (b) Identity of Issue, Borrow, or Similar Party (c) Type of
Investment (d) Cost (e) Current Value" and repeats the category in each
row's type column: "[#@*] <issuer> <category> <cost> <value>" in thousands
("-" nil, "(x)" negative). A category heading line opens each block and
"Total <category> <cost> <value>" closes it; a "Total <part> $ <cost> $
<value>" with no block open closes one part of the schedule (directly held
investments, then securities lending collateral), and the parts sum to the
grand total. First seen on General Motors Hourly-Rate Employees Pension
Plan (ack 20260713113402NAL0000112945001, pp.35-76): private equity and
debt $423,498,000 plus real estate and other alternatives $116,650,000 =
$540,148,000, its certified JV.

A thirty-eighth layout, "nt_market_erisa" (2026-10-18), is Northern Trust's
"Current Value of Assets and Liabilities" statement ("Security Description /
Asset ID Shares/Par Value Market Price Market Value ERISA Cost Security
Movements Gain/Loss"), printed rotated: category headings, region-currency
sub-groups with their own "Total ..." lines, rows ending "<shares> <price>
<market value> <cost> <movements> <gain>" over a bare asset-id line, "Total
<category>" lines of four amounts, liabilities as categories of their own,
and an unnamed grand "Total". A row's description can print on the line
above its amounts. First seen on PepsiCo, Inc. Defined Benefit Master Trust
(ack 20251006103730NAL0003275857001, pp.73-262 after the 5% reports): all
categories and the grand total $10,749,742,228.97 tie; partnership/joint
venture interests $175,939,811.26.

A thirty-ninth layout, "asset_liability_positions" (2026-10-18), is a
custodian's "Asset and Liability Positions" statement attached in place of a
schedule: nested headings ("Equity" / "Common Stocks" / "Consumer
Discretionary"), each closed by a "Total <heading>" line of "<% of category>
<cost> <market> <appr/depr> <% of account> ..."; rows "<account id> -
<description> <shares/par> <% of category> <cost> <market> <appr/depr> <% of
account> ...", the description continuing over the next lines until the
"<CUSIP> [/ <ticker>]" line; and the statement's "Total Assets and Accruals"
as its grand total. The "Limited Partnerships" and "Other" categories mix
index CITs with private funds, so fund rows are typed by name. First seen on
Local 705, International Brotherhood of Teamsters Pension Plan (ack
20251002153605NAL0000429059002, pp.85-160): every heading and the grand total
$1,788,321,430.58 tie.

Registered per plan, not per filing: PLAN_PARSERS is keyed by plan_master
plan_id (EIN-PN), which stays fixed across years while ack_id and page
numbers change. run_pipeline.py resolves each PDF's ack_id to a plan_id via
default.plan_master and, for registered plans, lets find_schedule_4i_pages
locate the schedule itself -- no PAGES_CSV needed. Returns None when the
pages don't carry this column header, so the caller falls through to the
existing path.
"""

import re
from collections import Counter
from typing import Dict, List, Optional, Set

import pdfplumber
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdftypes import resolve1

from .asset_type_patterns import detect_asset_type

# plan_master plan_id -> filer whose 4i schedule uses this layout (2026-10-12).
# start_heading: parse from the page carrying this section heading to the end
# of the schedule. Mayo: only the Mutual funds / Collective trust / Absolute
# return / Private Investments / Other block (pp.45-52 in the 2025 filing) is
# wanted, not the cash/government/bond/equity pages before it.
# notes: dated, plain-language caveats about the plan's data, kept here until
# plan 360 has a place to show them (2026-10-15).
PLAN_PARSERS = {
    "416011702-001": {"name": "Mayo Pension Plan", "start_heading": r"^mutual\s+funds?$"},
    # Shell: the whole schedule, every section (2026-10-14).
    "131299890-001": {"name": "Shell Pension Plan", "layout": "issue_par_rate",
                      "start_heading": r"^SHORT-TERM\s+INVESTMENTS$"},
    # RTX: the whole balance sheet from "A. NON-INTEREST BEARING CASH" (2026-10-14).
    "133064404-001": {"name": "RTX Master Retirement Trust", "layout": "detail_report",
                      "start_heading": r"^A\.\s+NON-INTEREST\s+BEARING\s+CASH$"},
    # UPS: the whole schedule from its first section (2026-10-14).
    "352371556-001": {"name": "UPS Group Trust", "layout": "soi_revalued_cost",
                      "start_heading": r"^INTEREST-BEARING\s+CASH$"},
    # Disney: the whole held-at-end-of-year composite, pp.11-517 (2026-10-15).
    "954545390-007": {"name": "Disney Master Pension Trust", "layout": "bny_h30t",
                      "start_heading": r"^INTEREST\s+BEARING\s+CASH$"},
    # J&J: report JJ2K, pp.3-226; pp.1-2 are a separate cash-only report (2026-10-15).
    "221024240-015": {"name": "Johnson & Johnson Pension Master Trust", "layout": "bny_h30t",
                      "start_heading": r"^JJ2K\s+PAGE:\s+1$"},
    # Cox: NA101E balance sheet from "A.NON-INTEREST BEARING CASH" (2026-10-15).
    "586043548-001": {"name": "Cox Enterprises Master Trust", "layout": "detail_report",
                      "start_heading": r"^A\.\s*NON-INTEREST\s+BEARING\s+CASH$"},
    # Hearst: NA101E detail from p.3; p.2 is the NA100E summary, whose
    # "A. NON-INTEREST BEARING CASH" line carries amounts (2026-10-16).
    "133691321-001": {"name": "Hearst Corporation Master Trust", "layout": "detail_report",
                      "start_heading": r"^A\.\s*NON-INTEREST\s+BEARING\s+CASH$"},
    # The same trust's 2024 filing under plan 130433120-001, pp.3-47;
    # partnership/joint venture $443,590,673 = cert JV (2026-10-17).
    "130433120-001": {"name": "Hearst Corporation Master Trust for Pension and Profit Sharing Plans",
                      "layout": "detail_report",
                      "start_heading": r"^A\.\s*NON-INTEREST\s+BEARING\s+CASH$"},
    # MGB (Mass General Brigham): the whole held-at-end-of-year composite (2026-10-15).
    "043294527-004": {"name": "MGB ERISA Master Trust", "layout": "bny_h30t",
                      "start_heading": r"^INTEREST\s+BEARING\s+CASH$"},
    # GE Pension Trust: same BNY composite, report id GEAC (2026-10-15).
    "140689340-001": {"name": "GE Pension Trust", "layout": "bny_h30t",
                      "start_heading": r"^INTEREST\s+BEARING\s+CASH$"},
    # Northrop Grumman DB Master Trust: the Alternative Investments Fund composite (2026-10-15).
    "136692740-007": {"name": "Northrop Grumman DB Master Trust", "layout": "bny_h30t",
                      "start_heading": r"^INTEREST\s+BEARING\s+CASH$"},
    # Northrop Grumman DB Master Trust Fixed Income Fund: composite NDJC,
    # pp.2-237; its five PARTN./JV funds = cert JV $488.1M (2026-10-17).
    "136692740-008": {"name": "Northrop Grumman DB Master Trust Fixed Income Fund", "layout": "bny_h30t",
                      "start_heading": r"^INTEREST\s+BEARING\s+CASH$"},
    # Northrop Grumman DB Master Trust Private Credit Pool: one PARTN./JV
    # category, 11 funds = cert JV $1,754.3M (2026-10-16).
    "136692740-021": {"name": "Northrop Grumman DB Master Trust Private Credit Pool", "layout": "bny_h30t",
                      "start_heading": r"^PARTN\./JOINT\s+VENTURE\s+INTERESTS$"},
    # FCA US Master Retirement Trust Balanced Pool: the whole composite (2026-10-15).
    "133112458-059": {"name": "FCA US Master Retirement Trust Balanced Pool", "layout": "bny_h30t",
                      "start_heading": r"^INTEREST\s+BEARING\s+CASH$"},
    # American Airlines (AMR) Master Fixed Benefit Pension Trust: BNY composite 5Y2C (2026-10-15).
    "131502798-020": {"name": "American Airlines Master Fixed Benefit Pension Trust", "layout": "bny_h30t",
                      "start_heading": r"^INTEREST\s+BEARING\s+CASH$"},
    # Boeing: the whole Investment Control Pool list (2026-10-15).
    "910425694-062": {"name": "Boeing Employee Retirement Plans Master Trust", "layout": "flat_cost_value",
                      "start_heading": r"^INVESTMENT\s+CONTROL\s+POOL$"},
    # Twin City Hospitals - Minnesota Nurses Association Pension Plan: pp.25-26,
    # one untyped list; rows typed by the vehicle groups of the plan's NAV note
    # (pp.14-16), each group's total = its line on the statement of net assets
    # (p.6). Limited partnerships $635,957,355 = cert JV (2026-10-17).
    "416184922-001": {"name": "Twin City Hospitals - Minnesota Nurses Association Pension Plan",
                      "layout": "flat_cost_value",
                      "start_heading": r"^SCHEDULE\s+H,\s+LINE\s+4i\W+SCHEDULE\s+OF\s+ASSETS\s+\(HELD\s+AT\s+END\s+OF\s+YEAR\)$",
                      "word_lines": True, "skip": r"^\(\d+\)$|^\*\s+Indicates\s+party-in-interest$",
                      "row_asset_types": [
                          (r"\b144A\b|\bDAY\s+DELAY\b", "Bond"),
                          (r"#\d+$", "Mutual Fund"),
                          (r"COMMINGLED|COLLECTIVE|\bST\s+INV\s+FUND\b|^WELLINGTON\s+GLOBAL|^WTC-CIF|^ACADIAN"
                           r"|^BOSTON\s+PARTNERS", "Common/Collective Trust Fund"),
                          (r"\bREIT\b", "Real Estate"),
                          # Cayman Islands exempted companies and the Ireland PLC hedge fund.
                          (r"^GROSVENOR\s+OPPORTUNISTIC|^CHATHAM|^NORTH\s+STAR|^AURELIUS|^CANYON|^HG\s+VORA|^CAPSTONE"
                           r"|^TUDOR|^WOLVERINE|^MILLSTREET|^MARSHALL\s+WACE", "Hedge Fund"),
                          # Limited partnerships, Cayman Islands exempted LPs and the
                          # private-fund LLCs.
                          (r"^CRESSEY|^PIMCO\s+(?:BRAVO|STOCKSPLUS)|^APOLLO|^AACP\b|^ASIA\s+ALTERNATIVES|^TA\s+X"
                           r"|^WCM\s+FOCUSED|^SIRIS|^PLATINUM\s+EQUITY|^TVM\s+LIFE|^CARLYLE|^CASTLELAKE|^ONE\s+ROCK"
                           r"|^FOUNDRY|^MCGINTY|^OPENGATE|^CRESTLINE|^GSO|^DAVIDSON\s+KEMPNER|^VIKING|^ARES\s+CAPITAL"
                           r"|^AUDAX", "Partnership Interest")]},
    # Koch DB Master Trust: pp.2-4, no total; every row no pattern types is a
    # private fund (Dyal, Kayne, CD&R, Starwood, co-invests) (2026-10-16).
    "256263968-001": {"name": "Koch Companies Defined Benefit Master Trust", "layout": "flat_cost_value",
                      "start_heading": r"^Koch\s+Companies\s+Defined\s+Benefit\s+Master\s+Trust\b",
                      "untyped_as": "Partnership Interest"},
    # UFCW Consolidated Pension Fund: pp.58-67, every section ties (2026-10-16).
    "586101602-001": {"name": "UFCW Consolidated Pension Fund", "layout": "typed_maturity_rate",
                      "start_heading": r"^UFCW\s+CONSOLIDATED\s+PENSION\s+FUND$"},
    # Teamsters Local 639 - Employers Pension Trust Fund: pp.29-37, every
    # section and the grand total tie; limited partnerships $527,434,694 =
    # cert JV (2026-10-17).
    "530237142-001": {"name": "Teamsters Local 639 - Employers Pension Trust Fund", "layout": "typed_maturity_rate",
                      "start_heading": r"^TEAMSTERS\s+LOCAL\s+639\s+-\s+EMPLOYERS\s+PENSION\s+TRUST\s+FUND$",
                      "heading_asset_types": [(r"^Corporate\s+obligations$", "Bond")]},
    # Construction Laborers Pension Trust for Southern California: p.32, every
    # section and the grand total tie; limited partnerships $1,009,067,041 =
    # cert JV (2026-10-17).
    "436159056-001": {"name": "Construction Laborers Pension Trust Fund", "layout": "typed_maturity_rate",
                      "start_heading": r"^CONSTRUCTION\s+LABORERS\s+PENSION\s+TRUST$"},
    # Western States Carpenters Pension Trust: pp.36-38 (a copy repeats at pp.63-65);
    # grand total ties, partnership = cert JV (2026-10-16).
    "956042875-001": {"name": "Western States Carpenters Pension Trust", "layout": "typed_maturity_rate",
                      "start_heading": r"^WESTERN\s+STATES\s+CARPENTERS\s+PENSION\s+TRUST$"},
    # Citigroup Pension Plan: pp.26-111, every section and the grand total tie; the
    # liabilities listed after the grand total are left out (2026-10-16).
    "521568099-020": {"name": "The Citigroup Pension Plan", "layout": "typed_maturity_rate",
                      "start_heading": r"^THE\s+CITIGROUP\s+PENSION\s+PLAN$"},
    # 3M Employee Retirement Income Plan: pp.22-115, rows rounded to the dollar
    # under unrounded totals; partnership = cert JV (2026-10-16).
    "410417775-002": {"name": "3M Employee Retirement Income Plan", "layout": "typed_maturity_rate",
                      "start_heading": r"^3M\s+Employee\s+Retirement\s+Income\s+Plan$", "row_rounding": True},
    # FirstEnergy Corp. Master Pension Plan: pp.19-67 (printed again later), text in
    # Calibri glyph ids; rows rounded under unrounded totals; partnership = cert JV
    # (2026-10-16).
    "341843785-001": {"name": "FirstEnergy Corp. Master Pension Plan", "layout": "typed_maturity_rate",
                      "start_heading": r"^FirstEnergy\s+Corp\.\s+Master\s+Pension\s+Plan$", "row_rounding": True},
    # Elliott's two plans: one page each, cash plus shares of Elliott International
    # Limited, the offshore feeder into Elliott's hedge fund = cert JV (2026-10-16).
    "853352943-001": {"name": "Elliott Money Purchase Pension Plan", "layout": "typed_maturity_rate",
                      "start_heading": r"^Elliott\s+Money\s+Purchase\s+Pension\s+Plan$",
                      "row_asset_types": [(r"\bElliott\s+International\s+Limited\b", "Hedge Fund")]},
    "853352943-002": {"name": "Elliott Section 401(k) Plan", "layout": "typed_maturity_rate",
                      "start_heading": r"^Elliott\s+Section\s+401\(k\)\s+Plan$",
                      "row_asset_types": [(r"\bElliott\s+International\s+Limited\b", "Hedge Fund")]},
    # Lone Pine Capital 401(k): p.19, values only; limited partnerships = cert JV.
    # A typed copy follows on p.20 (2026-10-16).
    "311679699-001": {"name": "Lone Pine Capital LLC 401(k) Profit Sharing Plan", "layout": "typed_maturity_rate",
                      "start_heading": r"^Lone\s+Pine\s+Capital\s+LLC\s+401\(k\)\s+Profit\s+Sharing\s+Plan$",
                      "value_only": True},
    # H-E-B Savings & Retirement Plan: pp.25-28, values only, long collective trust
    # names wrap with the amount on the next line; limited partnerships = cert JV,
    # and the filer types its other alternative funds HEDGE FUNDS (2026-10-16).
    "740537175-001": {"name": "H-E-B Savings & Retirement Plan", "layout": "typed_maturity_rate",
                      "start_heading": r"^H-E-B\s+Savings\s+&\s+Retirement\s+Plan$", "value_only": True,
                      "wrapped_row": r"\s*COMMON/COLLECTIVE\s+TRUST\s+FUND$",
                      "row_asset_types": [(r"\bHEDGE\s+FUNDS\b", "Hedge Fund")]},
    # Les Schwab Profit Sharing Retirement Plan: pp.17-18 (a copy follows); limited
    # partnerships plus "Privately Managed Investment Funds", which are hedge funds
    # (Elliott, King Street, Silver Point), = cert JV. Every section ties; the
    # printed grand total's cost is $15 below the section costs' sum (2026-10-16).
    "930470437-333": {"name": "Les Schwab Profit Sharing Retirement Plan", "layout": "typed_maturity_rate",
                      "start_heading": r"^Les\s+Schwab\s+Profit\s+Sharing\s+Retirement\s+Plan\b", "blank_cost": True,
                      "grand_cost_check": False,
                      "heading_asset_types": [(r"privately\s+managed", "Hedge Fund")]},
    # Carpenters Annuity Trust Fund for Northern California: pp.21-22, bare section
    # totals and an unlabeled grand total; partnership/joint ventures = cert JV
    # (2026-10-16).
    "946534591-001": {"name": "Carpenters Annuity Trust Fund for Northern California",
                      "layout": "typed_maturity_rate",
                      "start_heading": r"^Carpenters\s+Annuity\s+Trust\s+Fund\s+for\s+Northern\s+California$"},
    # The Employees' Retirement Plan of Hajoca Corporation (2024): pp.15-17
    # (printed again at pp.62-64), colon headings over bare cost/value totals
    # and a "$ <cost> <value>" grand total; limited partnerships = cert JV $405M
    # (2026-10-17).
    "232203401-002": {"name": "The Employees' Retirement Plan of Hajoca Corporation",
                      "layout": "typed_maturity_rate", "colon_headings": True,
                      "start_heading": r"^HAJOCA\s+CORPORATION$",
                      "grand_re": r"^\$\s*\d[\d,]*\s+\d[\d,]*$",
                      "heading_asset_types": [(r"^Fixed\s+income", "Bond"),
                                              (r"^Limited\s+partnerships", "Partnership Interest")]},
    # The New York Hotel Trades Council and Hotel Association of New York City,
    # Inc. Pension Fund (2024; the 4i title misprints 2023 but its values are
    # the 2024 balance sheet's): pp.10-15, printed again at pp.56-61, colon
    # headings and "Total <heading>" lines; Partnerships = cert JV $473M
    # (2026-10-17).
    "131764242-001": {"name": "The New York Hotel Trades Council and Hotel Association of New York City, "
                              "Inc. Pension Fund",
                      "layout": "typed_maturity_rate", "colon_headings": True,
                      "start_heading": r"^ASSOCIATION\s+OF\s+NEW\s+YORK\s+CITY,\s+INC\.\s+PENSION\s+FUND$",
                      "grand_re": r"^Total\s+Assets\s+Held\s+\$",
                      "heading_asset_types": [(r"^Partnerships", "Partnership Interest"),
                                              (r"^Corporate\s+Bonds", "Bond")]},
    # Operating Engineers' Local 324 Pension Fund (FYE 2025-04-30): pp.49-52,
    # colon headings and "Total <heading>" lines; a money market row precedes
    # the first heading. Limited partnerships = cert JV $466M (2026-10-17).
    "381900637-001": {"name": "Operating Engineers' Local 324 Pension Fund",
                      "layout": "typed_maturity_rate", "colon_headings": True, "row_rounding": True,
                      "start_heading": r"^Operating\s+Engineers.\s+Local\s+324\s+Pension\s+Fund$",
                      "join_next": r"^(?:Limited\s+partnerships,\s+limited\s+liability\s+companies,\s+and\s+limited"
                                   r"|Total\s+limited\s+partnerships,\s+limited\s+liability\s+companies,\s+and)$",
                      "skip_lines": r"^E$",
                      # identity column is the custodian on every row (2026-10-20)
                      "leading_party": r"^(?:BNY\s+MELLON|JPMORGAN)\s+",
                      "own_section_rows": [(r"^JPMORGAN\s+Money\s+market\s+fund", "Money market fund",
                                            "Short-Term Investment Fund")],
                      "heading_asset_types": [(r"^Limited\s+partnerships", "Partnership Interest")]},
    # Directors Guild of America - Producer Pension Plan: pp.19-65, an asset-type
    # column after each name; some names wrap, some print clipped to a word
    # ("PARTNERSHIP"). Partnership/joint venture = cert JV (2026-10-16).
    "956027308-002": {"name": "Directors Guild of America - Producer Pension Plan",
                      "layout": "typed_maturity_rate",
                      "start_heading": r"^DIRECTORS\s+GUILD\s+OF\s+AMERICA\s+-\s+PRODUCER\s+PENSION\s+PLAN\b",
                      # No word of four letters but a rate/type word: "0.85", "02/35 4.78",
                      # "144A", "3", "PARTNERSHIP" (2026-10-17).
                      "clipped_name": r"(?!.*\b(?!(?:VAR|FIXED|FLT|PARTNERSHIP)\b)[A-Za-z]{4,}).*",
                      "headings": r"^(?:Interest-Bearing\s+Cash|U\.S\.\s+Government\s+Securities|Corporate\s+Debt\s+-\s+"
                                  r"(?:Preferred|All\s+Other)|Common\s+Stock|Partnership/Joint\s+Venture\s+Interests"
                                  r"|Common/Collective\s+Trusts|103-12\s+Investment\s+Entities"
                                  r"|Registered\s+Investment\s+Companies|Other\s+Investments)$",
                      "bare_heading_total": True,
                      # Other Investments: typed by the asset-type column.
                      "row_asset_types": [(r"\sSWAP\s|\sFUT-|\sOP-", "Derivative"),
                                          (r"\s(?:GOVERNMENT\s+ISSUES|MUNICIPALS)\s", "Bond")]},
    # Directors Guild of America - Producer Pension Plan, Basic Benefit Plan: the
    # same layout, pp.21-68 ("1 of 48"); partnership/joint venture = cert JV (2026-10-17).
    "952892780-001": {"name": "Directors Guild of America - Producer Pension Plan (Basic Benefit Plan)",
                      "layout": "typed_maturity_rate",
                      "start_heading": r"^DIRECTORS\s+GUILD\s+OF\s+AMERICA\s+-\s+PRODUCER\s+PENSION\s+PLAN\b",
                      # No word of four letters but a rate/type word: "0.85", "02/35 4.78",
                      # "144A", "3", "PARTNERSHIP" (2026-10-17).
                      "clipped_name": r"(?!.*\b(?!(?:VAR|FIXED|FLT|PARTNERSHIP)\b)[A-Za-z]{4,}).*",
                      "headings": r"^(?:Interest-Bearing\s+Cash|U\.S\.\s+Government\s+Securities|Corporate\s+Debt\s+-\s*"
                                  r"(?:Preferred|All\s+Other)|Common\s+Stock|Partnership/Joint\s+Venture\s+Interests?"
                                  r"|Common/Collective\s+Trusts?|103-12\s+Investment\s+Entities"
                                  r"|Registered\s+Investment\s+Companies|Other\s+Investments)$",
                      "bare_heading_total": True,
                      "row_asset_types": [(r"\sSWAP\s|\sFUT-|\sOP-", "Derivative"),
                                          (r"\s(?:GOVERNMENT\s+ISSUES|MUNICIPALS)\s", "Bond")]},
    # Laborers Pension Trust Fund for Northern California: pp.22-42, every total
    # ties; limited partnerships = cert JV (2026-10-16).
    "946277608-001": {"name": "Laborers Pension Trust Fund for Northern California", "layout": "closing_totals",
                      "start_heading": r"^Laborers\s+Pension\s+Trust\s+Fund\s+for\s+Northern\s+California$",
                      "notes": ["2026-10-16: the $6.8M of Laborers Funds Admin notes receivable print under "
                                "Pooled separate accounts and are typed Separate Account."]},
    # Laborers Annuity Plan for Northern California: the sister plan's schedule,
    # pp.18-30; limited partnerships = cert JV (2026-10-16).
    "946277608-002": {"name": "Laborers Annuity Plan for Northern California", "layout": "closing_totals",
                      "start_heading": r"^Laborers\s+Annuity\s+Plan\s+for\s+Northern\s+California$"},
    # Midwest Operating Engineers Pension Trust Fund: the p.34 summary of the
    # attached Northern Trust statement; partnerships $728,833,522 = cert JV (2026-10-17).
    "366140097-001": {"name": "Midwest Operating Engineers Pension Trust Fund",
                      "layout": "investments_held_summary", "value_cost_pair": True,
                      "start_heading": r"^Midwest\s+Operating\s+Engineers\s+Pension\s+Trust\s+Fund$"},
    # Chicago & Vicinity Laborers' District Council Pension Plan: the pp.30-31 summary;
    # every subtotal and the grand total tie, partnership = cert JV (2026-10-16).
    "362514514-002": {"name": "Chicago & Vicinity Laborers' District Council Pension Plan",
                      "layout": "investments_held_summary",
                      "start_heading": r"^Chicago\s+&\s+Vicinity\s+Laborers.\s+District\s+Council\s+Pension\s+Plan$",
                      "notes": ["2026-10-16: stocks and bonds are the summary's roll-up lines (\"Domestic Common "
                                "Stock\", \"Corporate Issues\"); the attached U.S. Bank statement itemizes them."]},
    # IUOE Central Pension Fund: held and securities-loaned schedules (2026-10-15).
    "366052390-001": {"name": "Central Pension Fund of the IUOE", "layout": "cpa_total_lines",
                      "start_heading": r"^INTEREST\s+BEARING\s+CASH$"},
    # IAM National Pension Fund: same typesetting, loaned block inside the held total (2026-10-15).
    "516031295-002": {"name": "IAM National Pension Fund", "layout": "cpa_total_lines",
                      "start_heading": r"^SHORT-TERM\s+INVESTMENTS$"},
    # National Elevator Industry Pension Plan: same typesetting, colon headings (2026-10-15).
    "232694291-001": {"name": "National Elevator Industry Pension Plan", "layout": "cpa_total_lines",
                      "start_heading": r"^U\.S\.\s+GOVERNMENT\s+AND\s+AGENCY\s+OBLIGATIONS:$"},
    # SBC (AT&T): the whole held-at-end-of-year schedule (2026-10-14).
    "431301883-020": {"name": "SBC Master Pension Trust", "layout": "jpm_held",
                      "start_heading": r"^NONINTEREST-BEARING\s+CASH$"},
    # Bell Atlantic Master Trust: the whole schedule, pp.22-46 (2026-10-15).
    "251448208-020": {"name": "Bell Atlantic Master Trust", "layout": "gain_loss_totals",
                      "start_heading": r"^SCHEDULE\s+OF\s+ASSETS\s+AS\s+OF\b"},
    # CenturyLink, Inc. Defined Benefit Master Trust: the Northern Trust held
    # schedule, pp.245-317, after its 5% report (2026-10-17).
    "840928963-014": {"name": "CenturyLink, Inc. Defined Benefit Master Trust", "layout": "nt_held_regions",
                      "start_heading": r"^uAssets\s+held\s+for\s+Investment\s+Purposes$"},
    # Steelworkers Pension Trust: the Northern Trust held schedule attached to
    # the p.30 Schedule I reconciliation (2026-10-17).
    "236648508-499": {"name": "Steelworkers Pension Trust", "layout": "nt_held_regions",
                      "start_heading": r"^Account\s+Name\s+MASTER\s+CONSOLIDATION$"},
    # Emerson Electric Co. Retirement Master Trust (FYE 2025-09-30): the Northern
    # Trust held schedule, pp.2-243 (2026-10-17).
    "430259330-121": {"name": "Emerson Electric Co. Retirement Master Trust", "layout": "nt_held_regions",
                      "start_heading": r"^Account\s+Name\s+MASTER\s+TRUST$"},
    # Pension Plan for Employees at ORNL: the Northern Trust held schedule,
    # pp.20-33 (a copy repeats at pp.91-104) (2026-10-17).
    "621788235-001": {"name": "Pension Plan for Employees at ORNL", "layout": "nt_held_regions",
                      "start_heading": r"^Account\s+Name\s+OAK\s+RIDGE\s+NATIONAL\s+LABS$"},
    # HCSC Master Pension Trust: the Northern Trust held schedule, pp.3-40;
    # its words interleave unless read in stream order (2026-10-17).
    "831060209-008": {"name": "HCSC Master Pension Trust", "layout": "nt_held_regions",
                      "start_heading": r"^EIN\s+83-1060209$", "no_grand_total": True},
    # General Motors Hourly-Rate Employees Pension Plan: pp.35-76 in thousands,
    # directly held investments then securities lending collateral; private
    # equity and debt plus real estate/other alternatives = cert JV (2026-10-17).
    "270383222-003": {"name": "General Motors Hourly-Rate Employees Pension Plan",
                      "layout": "typed_column_totals",
                      "start_heading": r"^DIRECTLY\s+HELD\s+INVESTMENTS$", "amount_scale": 1000,
                      "section_types": [
                          (r"^Cash\s+equivalents", "Short-Term Investment Fund"),
                          (r"^Equity\s+securities$", "Common Stock"),
                          (r"^Debt\s+securities$", "Bond"),
                          (r"^Private\s+equity\s+and\s+debt|^Real\s+estate\s+and\s+other\s+alternative",
                           "Partnership Interest"),
                          (r"^Derivatives$", "Derivative")]},
    # The Graham Holdings Company Master Retirement Trust: the one-page p.1
    # schedule; partnership/joint venture interests $557,122,138 = cert JV (2026-10-17).
    "136666518-044": {"name": "The Graham Holdings Company Master Retirement Trust",
                      "layout": "cusip_column_totals",
                      "start_heading": r"^GRAHAM\s+HOLDINGS\s+COMPANY$",
                      "section_types": [
                          (r"^U\.S\.\s+GOVERNMENT", "Short-Term Investment Fund"),
                          (r"PARTNERSHIP/JOINT\s+VENTURE", "Partnership Interest"),
                          (r"^COMMON\s+STOCK$", "Common Stock")]},
    # United States Steel Corporation Plan for Employee Pension Benefits: the
    # Northern Trust print, pp.38-104; private equity $562,122,640 = cert JV (2026-10-17).
    "251897152-001": {"name": "United States Steel Corporation Plan for Employee Pension Benefits",
                      "layout": "colon_heading_totals",
                      "start_heading": r"^EIN\s+25-1897152\s+/\s+PN\s+001$",
                      "section_types": [
                          (r"^cash", "Short-Term Investment Fund"),
                          (r"corporate\s+stocks\s+-\s+common", "Common Stock"),
                          (r"commingled\s+funds", "Common/Collective Trust Fund"),
                          (r"^private\s+equity$", "Partnership Interest"),
                          (r"^real\s+estate$", "Real Estate")]},
    # Kaleida Health Pension Growth Plan: pp.63-65 (KeyBank-certified). The
    # certified JV ($571M) is the hedge fund, private equity, global equity and
    # fixed income funds together (2026-10-17).
    "161533232-001": {"name": "Kaleida Health Pension Growth Plan", "layout": "colon_heading_totals",
                      "start_heading": r"^SCHEDULE\s+H,\s+LINE\s+4i\s+-\s+SCHEDULE\s+OF\s+ASSETS\s+\(HELD\s+AT\s+END\s+OF\s+YEAR\)$",
                      "section_types": [
                          (r"cash", "Short-Term Investment Fund"),
                          (r"^equity\s+securities$", "Common Stock"),
                          (r"mutual\s+funds", "Mutual Fund"),
                          (r"hedge\s+funds", "Hedge Fund"),
                          (r"private\s+equity", "Partnership Interest")]},
    # Marsh & McLennan Companies Retirement Plan: pp.21-77. Partnership
    # interests $605,910,690 = cert JV $606M (2026-10-17).
    "362668272-001": {"name": "Marsh & McLennan Companies Retirement Plan", "layout": "bold_heading_totals",
                      "start_heading": r"^FORM\s+5500,\s+SCHEDULE\s+H,\s+PART\s+IV,\s+LINE\s+4i\s+SCHEDULE\s+I$",
                      "section_types": [
                          (r"group\s+annuity", "Insurance General Account"),
                          (r"McLennan", "Employer Stock"),
                          (r"corporate\s+stocks?", "Common Stock"),
                          (r"government|corporate\s+obligations", "Bond"),
                          (r"partnership", "Partnership Interest"),
                          (r"^swaps$", "Derivative"),
                          (r"common\s+collective", "Common/Collective Trust Fund")]},
    # Cigna Pension Plans Trust: amounts in thousands. Secondary partnerships
    # $410,009,000 + real estate equity funds $221,164,000 = cert JV $631M
    # (2026-10-17).
    "061059331-006": {"name": "Cigna Pension Plans Trust", "layout": "cusip_type_blocks",
                      "start_heading": r"^NON-POOLED\s+SEPARATE\s+ACCOUNTS$", "amount_scale": 1000},
    # PepsiCo, Inc. Defined Benefit Master Trust: the rotated Northern Trust
    # statement pp.73-262 (pp.1-72 are 5% reports). Partnership/JV interests
    # $175.9M; Sch H's JV $555M also counts PRISA ($107.1M, a pooled separate
    # account here) and two private credit trusts ($210.2M) (2026-10-18).
    "412205169-120": {"name": "PepsiCo, Inc. Defined Benefit Master Trust", "layout": "nt_market_erisa",
                      "start_heading": r"^Account\s+Name\s+PEPSI\s+NON-UNIT\s+ACCOUNTS$"},
    # Local 705, International Brotherhood of Teamsters Pension Plan: the
    # custodian's statement pp.85-160 stands in for the schedule. Its $522M
    # "Limited Partnerships" bucket is mostly index CITs; the private funds
    # are a realty LP, an infrastructure LP and Grosvenor credit (2026-10-18).
    "366492502-001": {"name": "Local 705, International Brotherhood of Teamsters Pension Plan",
                      "layout": "asset_liability_positions",
                      "start_heading": r"^Asset\s+and\s+Liability\s+Positions$"},
    # Ohio Carpenters' Pension Plan (FYE 2025-04-30): the two-page summary
    # pp.25-26; its attached statements (pp.27-166) stay one row each. Limited
    # partnerships $650,271,125 = cert JV (2026-10-17).
    "346574360-001": {"name": "Ohio Carpenters' Pension Plan", "layout": "fmv_cost_summary",
                      "start_heading": r"^SHORT\s+TERM\s+INVESTMENTS$"},
    # UFCW International Union - Industry Pension Fund (FYE 2025-06-30): the
    # summary p.30 and 23 manager blocks, pp.31-114; partnership/joint venture
    # interests (AB, Crescent, GCM, Hamilton Lane, Mesirow) = cert JV (2026-10-17).
    "516055922-001": {"name": "UFCW International Union - Industry Pension Fund", "layout": "leading_category",
                      "start_heading": r"^SCHEDULE\s+H\s+ITEM\s+4I\s+-\s+SCHEDULE\s+OF\s+ASSETS\s+HELD\s+AT\s+END\s+OF\s+YEAR$"},
    # Allstate Retirement Plan: the held schedule, pp.23-64; pp.65-79 are
    # acquired-and-disposed. Limited partnerships = cert JV (2026-10-17).
    "363871531-002": {"name": "Allstate Retirement Plan", "layout": "titled_category_pages",
                      "start_heading": r"^ALLSTATE\s+RETIREMENT\s+PLAN\s+\(EIN:\s+36-3871531\s+Plan:\s+002\)$",
                      "heading_asset_types": [
                          (r"^LIMITED\s+PARTNERSHIPS$", "Partnership Interest"),
                          (r"^EQUITY\s+SECURITIES$", "Common Stock"),
                          (r"^SHORT-TERM\s+INVESTMENTS$", "Short-Term Investment Fund"),
                          (r"BONDS$|^BANK\s+LOANS$", "Bond")]},
    # SEIU Pension Plan Master Trust: the whole schedule, pp.1-80; p.81's
    # market-value adjustments are not holdings (2026-10-17).
    "566680924-001": {"name": "SEIU Pension Plan Master Trust", "layout": "year_end_package",
                      "start_heading": r"^SEIU\s+Pension\s+Plan\s+Master\s+Trust\s+Form\s+5500,\s+Schedule\s+H\b",
                      "heading_asset_types": [
                          (r"^LIMITED\s+LIABILITY\s+PARTNERSHIPS$|^MISCELLANEOUS$", "Partnership Interest"),
                          (r"^INVESTMENT\s+FUND\s+REAL\s+ESTATE$", "Real Estate"),
                          (r"^COMMON/COLLECTIVE|^COMMON\s+FUND", "Common/Collective Trust Fund"),
                          (r"^MUTUAL\s+FUND$", "Mutual Fund"),
                          (r"COMMON\s+STOCK$|^RIGHTS\s+AND\s+WARRANTS$", "Common Stock"),
                          (r"^SHORT\s+TERM|^MMR\s+INVESTMENT|^OTHER\s+CASH", "Short-Term Investment Fund"),
                          (r"MORT|BONDS?\b|ABS$|TREASURY|STRIPS|AGENCIES|GOVERNMENT|REMIC|FLOATING|VARIABLE|"
                           r"MUNICIPAL|UMBS|SECURITIES$", "Bond")]},
    # Teamsters Western Region and New Jersey Health Care Fund Retiree Plan:
    # pp.4-9 (printed again pp.38-43); no total line, so the run ends at the
    # first page without the header (2026-10-17).
    "866052021-502": {"name": "Teamsters Western Region and New Jersey Health Care Fund Retiree Plan",
                      "layout": "flat_cost_value",
                      "start_heading": r"^Schedule\s+H\s+Part\s+IV,\s+4i\s+-\s+Schedule\s+of\s+Assets\s+\(Held\s+at\s+End\s+of\s+Year\)$",
                      "running_totals": True, "shs_split": True,
                      "skip": r"^party$|^Balance\s+Forwarded\b",
                      "untyped_as": "Common Stock",
                      "row_asset_types": [
                          (r"(?i)\bMM$|Money\s+Market", "Short-Term Investment Fund"),
                          (r"(?i)^CTF\b|\bCF\s+SL\b", "Common/Collective Trust Fund"),
                          (r"(?i)\bL\.?P\.?$|\bI\.?P\.?$|\bLLC$|\bFDR?\b|Portfolio\s+Finance|Fund\s+[IVX]+$|Master\s+F",
                           "Partnership Interest"),
                          (r"(?i)\bFund\b|\bFd\b|\bR-?6$|\bIns(?:tl)?$|\bDisciplined$|\bACWI$|Portfolio$|Value$",
                           "Mutual Fund")]},
    # ExxonMobil Pension Plan: pp.9-19, one alphabetical list in thousands;
    # the private funds are the Horsley Bridge, HarbourVest, Dover Street and
    # Secondary Overflow partnerships, $460.4M vs cert JV $461M (2026-10-17).
    "135409005-001": {"name": "ExxonMobil Pension Plan", "layout": "flat_cost_value",
                      "start_heading": r"^ExxonMobil\s+Pension\s+Plan$",
                      "rate_column": True, "amount_scale": 1000, "word_lines": True,
                      "skip": r"^RATE\s+DATE\s+SHARES\s+VALUE$|^\(000.s\)",
                      "row_asset_types": [
                          (r"(?i)^(?:HORSLEY\s+BRIDGE|HARBOURVEST|DOVER\s+STREET|SECONDARY\s+OVERFLOW)\b",
                           "Partnership Interest"),
                          (r"^CF\s", "Common/Collective Trust Fund"),
                          (r"\bDUE\b|\d%|^FHLMC\b|^FNMA\b|^GNMA\b|\s\d+(?:\.\d+)?\s+\d{1,2}-\d{1,2}-\d{4}$", "Bond"),
                          (r"\bCOM\b|\bCOM\s+STK\b|\bCL\s+[A-C]\b|\bORD\b|\bPLC\b|\bINC\b|\bCORP(?:ORATION)?\b|\bCO\b",
                           "Common Stock")]},
    # Main Line Health, Inc. Retirement Income Plan: held-at-end list pp.22-29
    # (the acquired-and-disposed list follows); the filer's own type text sits
    # mid-row, "VENTURE CAPITAL" for every private fund (2026-10-17).
    "232331531-001": {"name": "Main Line Health, Inc. Retirement Income Plan", "layout": "flat_cost_value",
                      "start_heading": r"^SCHEDULE\s+H,\s+LINE\s+4i\s+-\s+SCHEDULE\s+OF\s+ASSETS\s+\(HELD\b",
                      "bare_grand": True,
                      "skip": r"^\*\s+A\s+party-in-interest\s+to\s+the\s+Plan\.$|^See\s+independent\s+auditor's\s+report\.$"
                              r"|^\d+$",
                      "row_asset_types": [
                          (r"\bCONTRACTS\s+AND\s+SWAPS\b|\bWRITE\s+OPTION\b|^CDS\s", "Derivative"),
                          (r"\bCASH\s+EQUIVALENTS\b|DEPOSIT\s+SWEEP", "Short-Term Investment Fund"),
                          (r"\bVENTURE\s+CAPITAL\b", "Partnership Interest"),
                          (r"\bEQUITY\s+FUNDS\b", "Common/Collective Trust Fund"),
                          (r"\bMUTUAL\s+FUNDS\b", "Mutual Fund"),
                          (r"\bCOMMON\s+STOCK\b", "Common Stock"),
                          (r"\bCONVERTIBLE\b|\bBOND\b|\bNOTE\b|PRIV\s+PLCMT|\bMORTGAGE\b|\bTLB?\b|\bLIEN\b", "Bond")]},
    # MassMutual Pension Plan: the whole schedule, pp.18-44 (printed again
    # pp.64-90); the three limited partnership totals = cert JV (2026-10-17).
    "041590850-001": {"name": "MassMutual Pension Plan", "layout": "category_rate_totals",
                      "start_heading": r"^Schedule\s+H,\s+Line\s+4i\s+-\s+Schedule\s+of\s+Assets\b",
                      "categories": r"Cash\s+and\s+short-term\s+investments|Government\s+Securities|Bonds"
                                    r"|Mutual\s+Funds|Real\s+Estate|Hedge\s+Funds"
                                    r"|Limited\s+Partnerships?\s+-\s+(?:Private\s+Equity/Venture\s+Capital"
                                    r"|Real\s+Estate|Hedge)|Other|Fully\s+Benefit\s+Responsive\s+Contract",
                      "category_asset_types": [
                          (r"^Cash", "Short-Term Investment Fund"), (r"^(?:Government\s+Securities|Bonds)$", "Bond"),
                          (r"^Mutual\s+Funds$", "Mutual Fund"), (r"^Real\s+Estate$", "Real Estate"),
                          (r"^Hedge\s+Funds$", "Hedge Fund"), (r"^Limited\s+Partnership", "Partnership Interest"),
                          (r"Benefit\s+Responsive", "Insurance General Account")],
                      "subtotal_cost_errata": {"Limited Partnership - Private Equity/Venture Capital": 25882.0}},
    # Highmark Retirement Plan: four BNY M1102 account reports, pp.169-288 (COMB
    # MGR, JRMC, SVH, WPAHS), after the Schedule SB attachments (2026-10-17).
    "453674900-333": {"name": "Highmark Retirement Plan", "layout": "gain_loss_totals",
                      "start_heading": r"^Schedule\s+of\s+Investments\s+at\s+End\s+of\s+Plan\s+Year\s+at\s+"
                                       r"Historical\s+Cost\s+Report\s+ID:\s+M1102$",
                      "headings": r"^(?:INTEREST-BEARING\s+CASH|U\.\s+S\.\s+GOVERNMENT\s+SECURITIES"
                                  r"|CORPORATE\s+(?:DEBT\s+INSTRUMENTS|STOCK)(?:\s+-\s+(?:PREFERRED|COMMON))?"
                                  r"|PARTNERSHIP/JOINT\s+VENTURE\s+INTEREST|OTHER\s+INVESTMENTS"
                                  r"|UNALLOCATED\s+INSURANCE\s+CONTRACTS"
                                  r"|COMMON/COLLECTIVE\s+TRUST|103-12\s+INVESTMENT\s+ENTITIES"
                                  r"|REGISTERED\s+INVESTMENT\s+COMPANIES)$",
                      "skip": r"^HMPG\w+\s+(?:\d+/\d+/\d{4}\s+-\s+\d+/\d+/\d{4}\s+)?\d+$"
                              r"|^\d+/\d+/\d{4}\s+\d+:\d\d:\d\d\s+[AP]M\s+EDT\s+Page\s+\d+\s+of\s*\d+$"
                              r"|^\d+$|^GRAND\s+TOTAL$|^-?[\d,]+\.\d\d\s*[CI]$",
                      "row_id": r"^HMPF[A-Z0-9]{8}\s*(\S{9})\s+", "bare_grand": True, "accounts": True},
    # KeySpan Corporation Pension Master Trust: the whole schedule, pp.1-72 (2026-10-17).
    "041663150-002": {"name": "KeySpan Corporation Pension Master Trust", "layout": "gain_loss_totals",
                      "start_heading": r"^SCHEDULE\s+H,\s+LINE\s+4i\s+-\s+SCHEDULE\s+OF\s+ASSETS\b",
                      "headings": r"^(?:INTEREST-BEARING\s+CASH|U\.\s+S\.\s+GOVERNMENT\s+SECURITIES"
                                  r"|CORPORATE\s+(?:DEBT\s+INSTRUMENTS|STOCK)(?:\s+-\s+(?:PREFERRED|COMMON))?"
                                  r"|PARTNERSHIP/JOINT\s+VENTURE\s+INTEREST|OTHER\s+INVESTMENTS"
                                  r"|COMMON/COLLECTIVE\s+TRUST|103-12\s+INVESTMENT\s+ENTITIES"
                                  r"|REGISTERED\s+INVESTMENT\s+COMPANIES)$",
                      "skip": r"^KEDG\d+\s+\d+/\d+/\d{4}\s+-\s+\d+/\d+/\d{4}\s+\d+$"
                              r"|^\d+/\d+/\d{4}\s+\d+:\d\d:\d\d\s+[AP]M\s+EST\s+Page\s+\d+\s+of\s*\d+$",
                      "row_id": r"^KEDF\d+\s+(\S{9})\s+", "bare_grand": True},
    # National Grid USA Companies' Final Average Pay Pension Plan (FYE 2025-03-31):
    # the trust statement, pp.37-133, words run together; partnership/joint
    # venture = cert JV; the 401(h) account after it is left out (2026-10-17).
    "041663150-003": {"name": "National Grid USA Companies' Final Average Pay Pension Plan",
                      "layout": "gain_loss_totals",
                      "start_heading": r"^NATIONAL\s+GRID\s+USA\s+COMPANIES'\s+FINAL\s+AVERAGE\s+PAY\b",
                      "x_tolerance": 2,
                      "headings": r"^(?:INTEREST-BEARING\s*CASH|U\.\s*S\.\s*GOVERNMENT\s*SECURITIES"
                                  r"|CORPORATE\s*DEBT\s*INSTRUMENTS|CORPORATE\s*STOCK\s*-\s*(?:PREFERRED|COMMON)"
                                  r"|PARTNERSHIP/JOINT\s*VENTURE\s*INTER|OTHER\s*INVESTMENTS|COMMON/COLLECTIVE\s*TRUST"
                                  r"|103-12\s*INVESTMENT\s*ENTITIES|REGISTERED\s*INVESTMENT\s*COMPANIES"
                                  r"|SHORT\s*SALES|WRITTEN\s*OPTIONS)$",
                      "skip": r"^-?[\d,]+\.\d\d-?\s+[CI]$|^\(\d+\)$|^TOTAL\s+SHORT\s*INVESTMENT\s"
                              r"|^SHARES/\s+MARKET\s+UNREALIZED$|^PAR\s?VALUE\s+SECURITY\s?DESCRIPTION\s",
                      "price_column": True, "trailing_minus": True, "joined_total": True,
                      "grand_label": r"^TOTAL\s+NET\s*INVESTMENT\s",
                      "heading_asset_types": [(r"^CORPORATE\s*DEBT", "Bond"),
                                              (r"^CORPORATE\s*STOCK\s*-\s*PREFERRED", "Preferred Stock"),
                                              (r"^CORPORATE\s*STOCK\s*-\s*COMMON", "Common Stock"),
                                              (r"^REGISTERED\s*INVESTMENT", "Mutual Fund")]},
    # Honeywell: the whole Northern Trust list, pp.4-21; alt_span is its private
    # partnership/real estate run, 2024 filing (2026-10-15).
    "366968926-001": {"name": "Honeywell International Inc. Master Retirement Trust",
                      "layout": "nt_par_cost_value", "start_heading": r"^Schedule\s+H,\s+Line\s+4I\b",
                      "alt_span": (r"^4702\s+PARK\s+370$", r"^JOHN\s+HANCOCK\s+GAC\s+25$")},
    # Hackensack Meridian Health Master Retirement Trust: the whole Northern
    # Trust list, pp.2-18. The alt span runs from the Blackstone real estate
    # partnerships to BIL Ltd; the collective trusts and derivatives inside it
    # are excluded, leaving $545,852,464.74 = cert JV (2026-10-17).
    "823599691-001": {"name": "Hackensack Meridian Health Master Retirement Trust",
                      "layout": "nt_par_cost_value",
                      "start_heading": r"^Form\s+5500,\s+Schedule\s+H\s+Line\s+4i\s+-\s+Schedule\s+of\s+Assets\s+"
                                       r"\(Held\s+At\s+End\s+of\s+Year\)\s+EIN\s+82-3599691$",
                      "alt_span": (r"^BLACKSTONE\s+REAL\s+ESTATE\s+PARTNERS\s+VI$", r"^BIL\s+LTD\b"),
                      "alt_span_exclude": r"\bCIT\b|^ACADIAN\s|^THE\s+SILCHESTER\s|^ARROWSTREET\s+GLOBAL\s"
                                          r"|^(?:CALL|PUT)\s+SWO\s|^CDS\s|^ICE_CD[SX]\s|^FUT\s|^CCP\s|^OTC\s",
                      # The flat-list name rules would make these partnerships.
                      "type_rows": [(r"\bCIT\b|^ACADIAN\s|^THE\s+SILCHESTER\s|^ARROWSTREET\s+GLOBAL\s",
                                     "Common/Collective Trust Fund"),
                                    (r"\bCOM\b|\bCORPORATION$", "Common Stock")]},
    "362382580-010": {"name": "John Deere Pension Trust", "layout": "inline_category",
                      "start_heading": r"^5500\s+Schedule\s+of\s+Investments\s+at\s+End\s+of\s+Plan\s+Year$"},
    # NEBF (National Electrical Benefit Fund): the whole schedule, pp.26-69 (2026-10-15).
    "530181657-001": {"name": "National Electrical Benefit Fund", "layout": "bare_subtotals",
                      "start_heading": r"^Schedule\s+H\s*-\s*Line\s+4i\b"},
    # NECA (National Electrical Annuity Plan): NEBF's layout, pp.20-67 (2026-10-16).
    "526132372-001": {"name": "National Electrical Annuity Plan", "layout": "bare_subtotals",
                      "start_heading": r"^Schedule\s*H\s*-\s*Line\s*4i\b"},
    # Dow: the first of two printed copies, pp.51-183 of the 2024 filing (2026-10-15).
    "381285128-001": {"name": "Dow Employees' Pension Plan", "layout": "nt_held_regions",
                      "start_heading": r"^Schedule\s+of\s+Assets\s+Held\s+for\s+Investment\s+Purposes$"},
    # Dow Retirement Group Trust - Opportunistic FI: the same Northern Trust
    # schedule, pp.5-6 of a 6-page filing (2026-10-16).
    "381285128-028": {"name": "Dow Retirement Group Trust - Opportunistic FI", "layout": "nt_held_regions",
                      "start_heading": r"^Account\s+Name\s+OPPORTUNISTIC\s+FI$"},
    # Dow Retirement Group Trust - Real Estate: one page, p.5; Lion Industrial +
    # Prime Property $461,047,056 = cert JV (2026-10-17).
    "381285128-030": {"name": "Dow Retirement Group Trust - Real Estate", "layout": "nt_held_regions",
                      "start_heading": r"^Account\s+Name\s+REAL\s+ESTATE\s+CORE\s+OE\s+FUNDS$"},
    # Integrys Energy Group Master Retirement Plan: the Northern Trust held
    # schedule, pp.2-110, after its 5% report page; partnership/joint venture
    # $434,877,352 = cert JV (2026-10-17).
    "456948361-001": {"name": "Integrys Energy Group Master Retirement Plan", "layout": "nt_held_regions",
                      "start_heading": r"^Account\s+Name\s+INTEGRYS\s+ENERGY\s+GROUP\s+MRT$"},
    # Western Teamsters: only the LP appendix (Appendix X in the 2024 filing,
    # VII in 2025); the rest of the 4i schedule is a per-manager rollup (2026-10-15).
    "916145047-001": {"name": "Western Conference of Teamsters Pension Plan", "layout": "titled_appendix",
                      "start_heading": r"^Limited\s+Partnerships?\s+and\s+Other\s+Private\s+Equity$",
                      "notes": [
                          "2026-10-15: Only the Limited Partnerships and Other Private Equity appendix is "
                          "parsed; the rest of the 4i schedule is a per-manager rollup without holdings.",
                      ]},
    # McKinsey Retirement Trust: the held schedule, Statement #1, pp.6-10 (2026-10-15).
    # exclude_rows: SSALT Fund Ltd/SPV LP ($6.69B) is MIO Partners' pool whose
    # hedge fund and LP holdings are also listed line by line; with SSALT the
    # list is $18.73B against $12.20B of Sch H investments, without it $12.05B.
    # Keep the underlying funds, drop the wrapper (2026-10-15).
    "131826332-017": {"name": "McKinsey Retirement Trust", "layout": "typed_description",
                      "start_heading": r"^Statement\s+#\s*\d+\s+\W\s+Schedule\s+H,\s+Line\s+4i\b",
                      "exclude_rows": r"^SSALT\s+Fund\b",
                      "notes": [
                          "2026-10-15: SSALT Fund Ltd + SSALT Fund SPV LP ($6.69B, FY2024) excluded. SSALT is "
                          "an MIO Partners (McKinsey investment office) pool; the 4i schedule also lists its "
                          "hedge fund and LP holdings line by line. With SSALT the schedule totals $18.73B vs "
                          "$12.20B Sch H investments; without it $12.05B. Look-through is inferred from totals, "
                          "not from a published SSALT holdings list.",
                          "2026-10-15: Certified joint-venture amount is $0 because the filer reports all "
                          "alternatives under Sch H 1c(15) other investments ($11.93B); alternatives vs "
                          "certified JV will show a large positive gap by design.",
                      ]},
    # Ford Motor Company Trust Fund hedge fund and real estate sub-trusts: Northern
    # Trust held schedules, pp.5-23 and the whole run respectively (2026-10-16).
    "367324188-110": {"name": "Ford Motor Company Trust Fund Hedge Funds", "layout": "nt_held_regions",
                      "start_heading": r"^Schedule\s+H,\s+line\s+4i\s+-\s+Schedule\s+of\s+Assets\s+\(Held\s+At\s+End\s+of\s+Year\)$"},
    "367324188-109": {"name": "Ford Motor Company Trust Fund Real Estate", "layout": "nt_held_regions",
                      "start_heading": r"^Schedule\s+H,\s+line\s+4i\s+-\s+Schedule\s+of\s+Assets\s+\(Held\s+At\s+End\s+of\s+Year\)$"},
    "367324188-108": {"name": "Ford Motor Company Trust Fund Private Equity", "layout": "nt_held_regions",
                      "start_heading": r"^Schedule\s+H,\s+line\s+4i\s+-\s+Schedule\s+of\s+Assets\s+\(Held\s+At\s+End\s+of\s+Year\)$"},
    # NFL Player Retirement Plan: the whole schedule from its first section, pp.26-63;
    # plan year ends 3/31 (2026-10-16).
    "136043636-001": {"name": "Bert Bell/Pete Rozelle NFL Player Retirement Plan", "layout": "soi_revalued_cost",
                      "start_heading": r"^INTEREST-BEARING\s+CASH$"},
    # Dominion Energy DB Master Trust: the whole schedule, pp.21-110 (2026-10-16).
    "256263994-047": {"name": "Dominion Energy, Inc. Defined Benefit Master Trust", "layout": "soi_revalued_cost",
                      "start_heading": r"^INTEREST-BEARING\s+CASH$"},
    # International Paper pension account: the whole composite, pp.26-244 (2026-10-16).
    "130872805-213": {"name": "International Paper Co Retirement Plan Pension Account", "layout": "bny_h30t",
                      "start_heading": r"^International\s+Paper\s+Co\s+Retirement\s+Plan\s+Pension\s+Account\b"},
    # NRECA Retirement Security Plan: the whole composite, pp.27-342 (2026-10-16).
    "530116145-333": {"name": "NRECA Retirement Security Plan", "layout": "bny_h30t",
                      "start_heading": r"^INTEREST\s+BEARING\s+CASH$"},
    # DuPont Pension Trust: the whole composite, pp.2-174, printed rotated 90° on
    # upright pages (read through _upright_text) (2026-10-16).
    "516012443-001": {"name": "DuPont Pension Trust", "layout": "bny_h30t",
                      "start_heading": r"^INTEREST\s+BEARING\s+CASH$"},
    # Pfizer/Northern Trust master trust: the held schedule, pp.763-1213 of 1,213 (2026-10-16).
    "363146075-001": {"name": "Master Trust between Pfizer Inc and The Northern Trust Company", "layout": "nt_held_regions",
                      "start_heading": r"^Account\s+Name\s+PFIZER-WYETH\s+DB\s+PLANS$"},
    # Abbott Laboratories Annuity Retirement Trust: the Northern Trust held schedule,
    # pp.57-140 (2026-10-16).
    # hedge_fund_rows: the untyped "Other" category holds the hedge funds ($851M)
    # next to long-only funds (WGI and RWC emerging markets), so managers are
    # named rather than matched on "LTD"/"OFFSHORE" (2026-10-16).
    "360698440-337": {"name": "Abbott Laboratories Annuity Retirement Trust", "layout": "nt_held_regions",
                      "start_heading": r"^Account\s+Name\s+ABBOTT\s+CONSOLIDATION$",
                      "hedge_fund_rows": r"^(?!.*CASH\s+HELD).*\b(?:BAXTER\s+STREET|BRIDGEWATER|GLENVIEW|"
                                         r"SILVER\s+POINT|KING\s+STREET|ALEUTIAN|ELLIOTT|CAPULA|VIKING\s+GLOBAL|"
                                         r"TPG\s+AXON|GMO\s+MULTI\s+STRATEGY)\b",
                      "notes": [
                          "2026-10-16: Rows in the untyped Other category from named hedge fund managers "
                          "(Baxter Street, Bridgewater, Glenview, Silver Point, King Street, Aleutian, Elliott, "
                          "Capula, Viking, TPG Axon, GMO Multi-Strategy) are typed Hedge Fund; WGI and RWC "
                          "emerging markets funds stay untyped (long-only).",
                      ]},
    # Motion Picture Industry Individual Account Plan: Northern Trust's Asset
    # Detail statement, pp.19-300 (2026-10-16).
    "950030749-002": {"name": "Motion Picture Industry Individual Account Plan", "layout": "nt_asset_detail",
                      "start_heading": r"^MOTION\s+PICS\s+-\s+IAP$"},
    # Motion Picture Industry Pension Plan: the same statement, pp.22-200; the
    # filing prints it again at pp.239-417 (2026-10-16).
    "951810805-001": {"name": "Motion Picture Industry Pension Plan", "layout": "nt_asset_detail",
                      "start_heading": r"^MOTION\s+PICS\s+-\s+PENSION$"},
    # Novartis Pension Plans Master Trust: BNY composite report 05UC, pp.15-91 (2026-10-17).
    "131834433-300": {"name": "Novartis Pension Plans Master Trust", "layout": "bny_h30t",
                      "start_heading": r"^05UC\s+NOVARTIS\s+PENSION\s+PLANS\s+MT\s+PAGE:\s+1$"},
    # Hartford HealthCare pension: BNY composite report HHPK from p.21 (2026-10-16).
    "454530568-100": {"name": "Hartford HealthCare Corporation Defined Benefit Pension Plan", "layout": "bny_h30t",
                      "start_heading": r"^HHPK\s+HARTFORD\s+PENSION\s+INCL\s+HHPA\s+PAGE:\s+1$"},
    # Deseret Mutual Master Retirement Plan: pp.25-97, printed again pp.118-190;
    # limited partnerships = cert JV (2026-10-16).
    "870440163-001": {"name": "Deseret Mutual Master Retirement Plan", "layout": "colon_headings",
                      "start_heading": r"^DESERET\s+MUTUAL\s+MASTER\s+RETIREMENT\s+PLAN$", "row_rounding": True},
    # Producer-Writers Guild of America Pension Plan: pp.20-43, text in PUA
    # glyphs; partnership/JV interests = cert JV. Its partnership rows sum $2
    # under their printed cost subtotal (2026-10-16).
    "952216351-001": {"name": "Producer-Writers Guild of America Pension Plan", "layout": "described_categories",
                      "start_heading": r"^Producer-Writers\s+Guild\s+of\s+America\s+Pension\s+Plan$",
                      "row_rounding": True},
    # IBM Personal Pension Plan: Northern Trust list pp.63-133 (pp.23-24 are
    # stubs); C994J ids are its private funds. The JV/other split isn't
    # printed: offshore "LTD" funds and the hedge managers' funds are typed
    # Hedge Fund, the rest of C994J is JV. PRIVEST = cert's pooled separate
    # account ($637.8M) (2026-10-16).
    "130871985-001": {"name": "IBM Personal Pension Plan", "layout": "nt_cusip_columns",
                      "start_heading": r"^IBM\s+PERSONAL\s+PENSION\s+PLAN\b",
                      "alt_ids": r"^C994[JL]", "row_rounding": True,
                      "type_rows": [(r"^PRIVEST$", "Separate Account"),
                                    (r"\bLTD\b|\bLIMITED\b(?!\s+PARTNERSHIP)|ABSOLUTE\s+RETURN|LONG\s+SHORT|"
                                     r"\bMACRO\b|\bEUREKA\b|COMMODITY\s+ALPHA|BRIDGEWATER|\bTOPS\b", "Hedge Fund")]},
    # Duke University: "**" marks fund holdings; those under hedged strategies
    # are hedge funds (Blackwell = DUMAC's own vehicles), those under private
    # capital and real assets are JV. Bridgewater prints a share count
    # instead of "**" (2026-10-16).
    "560532129-002": {"name": "Employees' Retirement Plan of Duke University", "layout": "leading_shares",
                      "start_heading": r"^Short-term\s+investments$",
                      "fund_types": [(r"^Hedged\s+strategies$", "Hedge Fund"),
                                     (r"^Private\s+capital$|^Real\s+assets$", "Partnership Interest")],
                      "type_rows": [(r"^Bridgewater\s+Pure\s+Alpha\b", "Hedge Fund")]},
    # U.S. Bank Pension Plan master trust: hedge funds + private equity funds
    # under Partnerships/Joint Ventures = cert JV (2026-10-16).
    "410255900-008": {"name": "U.S. Bank Pension Plan", "layout": "issuer_shares_category",
                      "start_heading": r"^U\.S\.\s+Bank\s+Pension\s+Plan$"},
    # National Automatic Sprinkler Industry Pension Fund: PARTNERSHIPS + HEDGE
    # FUNDS parents hold the JV; schedule printed twice, first copy taken (2026-10-16).
    "526054620-001": {"name": "National Automatic Sprinkler Industry Pension Fund", "layout": "party_type_tree",
                      "start_heading": r"^SCHEDULE\s+OF\s+ASSETS\s+\(HELD\s+AT\s+END\s+OF\s+YEAR\)$"},
    # Southern Company System Master Retirement Trust (JPM FD491, 2026-10-16).
    # The filing omits report pp.1-7 and whites out the TOTAL lines (text
    # layer intact). Every printed subtotal ties and cost foots to the grand
    # total exactly, but the printed value grand total ($15,050,108,266) is
    # $22,735,787 above the sum of its printed subtotals, with zero cost.
    "630274273-001": {"name": "Southern Company Pension Plan", "layout": "jpm_held",
                      "start_heading": r"^GENERAL\s+INVESTMENTS$", "grand_value_gap": 22735787},
    # Ascension master trust (MPT SUPER ACCOUNTS): the Northern Trust held
    # schedule, pp.1323-1519 of 1,544, every page drawn rotated 90° (read through
    # _upright_text); pp.1-1322 are the 5% transaction report (2026-10-16).
    "366891022-001": {"name": "Ascension Pension Plan", "layout": "nt_held_regions",
                      "start_heading": r"^Account\s+Name\s+MPT\s+SUPER\s+ACCOUNTS$"},
    # L3Harris Pension Master Trust: pp.1-47, the category printed on every row
    # and "Total <category>" closing each block (2026-10-16).
    "366668234-300": {"name": "L3Harris Pension Master Trust", "layout": "category_column",
                      "start_heading": r"^L3HARRIS\s+PENSION\s+MASTER\s+TRUST$"},
    # Sutter Health Retirement Plan: the Northern Trust held schedule, pp.26-104.
    # The untyped Other category's "CF ..." offshore funds and the named managers
    # below are hedge funds, next to sovereign bonds and REITs (2026-10-16).
    "942788907-333": {"name": "Sutter Health Retirement Plan", "layout": "nt_held_regions",
                      "start_heading": r"^Account\s+Name\s+SUTTER\s+RETIREMENT\s+TOTAL\s+CONSOL$",
                      "hedge_fund_rows": r"^(?:CF\s|AQR\s+OFFSHORE\s+MULTI-STRATEGY|BRIDGEWATER\s+LYXOR|"
                                         r"ELLIOTT\s+ASSOCIATES|AHL\s+\(CAYMAN\)|D\.E\s+SHAW)"},
    # BJC Pension Plan: the Northern Trust held schedule, pp.21-53, its words set
    # tight enough to need _nhr_text's retry; partnerships = cert JV. Other's
    # hedge funds are typed by manager (2026-10-16).
    "431617558-333": {"name": "BJC Pension Plan", "layout": "nt_held_regions",
                      "start_heading": r"^Account\s+Name\s+BJC\s+PENSION\s+TOTAL\s+ACCOUNTS$",
                      "hedge_fund_rows": r"^(?:CF\s+MW\s+TOPS|VIKING\s+GLOBAL|CF\s+AHL\s+\(CAYMAN\)|THEOREM\s+MAIN)"},
    # Boilermaker-Blacksmith National Pension Trust: p.31 fund summary, pp.32-47
    # its "See Attached" securities (a copy follows at pp.95-111). The summary
    # has no category column; funds are typed by the FS (statement of net assets
    # p.6 and the NAV note p.21, each class tying to the dollar): limited
    # partnerships $660,671,775 and LLCs $1,166,270,012 -> Partnership Interest
    # (cert JV is $1,561,195,321), Cayman exempted companies -> Hedge Fund, pooled
    # separate accounts, the 103-12 entity and the CTFs. The detail's text layer
    # drops each description's first letter ("HLMGOLD POOL") (2026-10-16).
    "486168020-001": {"name": "Boilermaker-Blacksmith National Pension Trust", "layout": "summary_attached",
                      "start_heading": r"^BOILERMAKER-BLACKSMITH\s+NATIONAL\s+PENSION\s+TRUST$",
                      "row_asset_types": [
                          (r"Securities\s+Lending", "Short-Term Investment Fund"),
                          (r"^(?:EnTrust|American\s+Strategic|The\s+Carlyle\s+Group|Prudential\s+PRISA\s+II|"
                           r"ULLICO\s+ULLICO\s+Infrastructure|HIG\s+HIG|First\s+Reserve)\b", "Partnership Interest"),
                          (r"^(?:Hancock\s+(?:Timber|Agriculture)|Timberland\s+Inv\s+Resources|Parametric|Acadian|"
                           r"Partners\s+Group)\b", "Partnership Interest"),
                          (r"^(?:Grosvenor|AQR)\b", "Hedge Fund"),
                          (r"^(?:Prudential\s+(?:PRIVEST|PRISA)|ULLICO\s+J\s+for\s+Jobs)\b", "Separate Account"),
                          (r"^Lazard\s+Lazard\s+International", "103-12 Investment Entity"),
                          (r"^Lazard\s+Global\s+Listed", "Mutual Fund"),
                          (r"^(?:PNC|BlackRock|JP\s+Morgan|BNY\s+Mellon|Loomis\s+Sayles|Kayne\s+Anderson)\b",
                           "Common/Collective Trust Fund")]},
    # Bank of America Pension Plan: cpa_total_lines typesetting with a "Lessor, or
    # Similar Party" column header; its LIMITED PARTNERSHIPS total ($1,258,355,264)
    # is the certified JV figure (2026-10-16).
    "560906609-001": {"name": "Bank of America Pension Plan", "layout": "cpa_total_lines",
                      "start_heading": r"^MONEY\s+MARKET\s+FUNDS\s+AND\s+INTEREST\s+BEARING\s+CASH$"},
    # United Association National Pension Fund: typed_maturity_rate rows; p.57
    # prints 72 stock rows to every other page's 73, and the stocks total and the
    # grand total both include the lost row's cost and value. Its "Total limited
    # partnerships" ($1,256,262,147) is the certified JV figure (2026-10-16).
    "526152779-001": {"name": "United Association National Pension Fund", "layout": "typed_maturity_rate",
                      "start_heading": r"^UNITED\s+ASSOCIATION\s+NATIONAL\s+PENSION\s+FUND$",
                      "heading_asset_types": [(r"^common\s+and\s+preferred\s+stocks$", "Common Stock"),
                                              (r"^pooled\s+separate\s+accounts$", "Separate Account")],
                      "missing_rows": {"Common and preferred stocks": [628044, 758508]}},
    # International Painters and Allied Trades Industry Pension Plan, pp.53-58 (2026-10-16).
    "526073909-001": {"name": "International Painters and Allied Trades Industry Pension Plan",
                      "layout": "typed_maturity_rate", "start_heading": r"^Allied\s+Trades\s+Industry$",
                      "colon_headings": True,
                      "heading_asset_types": [(r"limited\s+partnership", "Partnership Interest"),
                                              (r"^common\s+collective", "Common/Collective Trust Fund"),
                                              (r"^real\s+estate\s+equity", "Real Estate"),
                                              (r"^pooled\s+separate", "Separate Account"),
                                              (r"^guaranteed\s+investment", "Insurance General Account"),
                                              (r"obligations$", "Bond")]},
    # Screen Actors Guild - Producers Pension Plan, pp.25-36 (2026-10-16).
    "952110997-001": {"name": "Screen Actors Guild - Producers Pension Plan", "layout": "typed_maturity_rate",
                      "start_heading": r"^Screen\s+Actors\s+Guild\s+-\s+Producers\s+Pension\s+Plan$",
                      # "Institutional Credit Fund, LP 90,184,728": a partnership with a value, no cost.
                      "blank_cost": True,
                      # Partnership costs sum $3 under their printed total (112 rounded rows).
                      "row_rounding": True,
                      # "Other Investments" types each row in its own column.
                      "row_asset_types": [(r"\sHedge\s+Equity\s+N/A\s", "Hedge Fund")],
                      "heading_asset_types": [(r"^pooled\s+separate", "Separate Account"),
                                              (r"^registered\s+investment", "Mutual Fund")]},
    # Nationwide Defined Benefit Master Trust: Attachment A, pp.4-35 (2026-10-16).
    "146316950-003": {"name": "Nationwide Defined Benefit Master Trust", "layout": "flat_cost_value",
                      "start_heading": r"^Attachment\s+A$",
                      # Custodian abbreviations the layout's name rules miss.
                      "row_asset_types": [
                          (r"\bVAR\s+RT\b|\bSTEP\s+\d\d/", "Bond"),
                          (r"^GLDMN\s+SCHS\s+FIN\s+SQ\b|^EB\s+TEMP\s+INV\s+FD\b", "Short-Term Investment Fund"),
                          (r"^ELLIOTT\s+ARDEN\b", "Hedge Fund"),
                          (r"\bPTNRS?\b|\bPNR\b|\bSCSP\d*$|\bFD\b|CO-?INV|\bCO-IN\b|ALPINVEST|SUPP\s+ACCT|SIDECAR"
                           r"|^ENCAP\b|^PCCP\b|^ACTIS\b|^ARES\s+CAPITAL\s+EUR\b|^INDIUM\b|^PUBLIC\s+PENSION\s+CAPITAL"
                           r"|^CHG\s+PPC\s+INVESTOR|^IRONWOOD\s+POWER|^RANGER\s+LEXIPOL|^HARRISON\s+ST\b",
                           "Partnership Interest")]},
    # Carpenters' Pension Trust Fund of St. Louis, pp.23-59; the filing prints
    # the schedule again at pp.72-108 (2026-10-16).
    "431622970-001": {"name": "Carpenters' Pension Trust Fund of St. Louis", "layout": "typed_maturity_rate",
                      "start_heading": r"^Schedule\s+H,\s+Line\s+4i\s+-\s+Schedule\s+of\s+Assets\b",
                      "colon_headings": True,
                      "heading_asset_types": [(r"^pooled\s+separate", "Separate Account")]},
    # Co-Op Retirement Plan, pp.69-83: "Alternative Investments" lists the
    # private equity, credit and hedge fund partnerships (2026-10-16).
    "010689331-001": {"name": "Co-Op Retirement Plan", "layout": "typed_maturity_rate",
                      "start_heading": r"^CO-OP\s+RETIREMENT\s+PLAN$",
                      "colon_headings": True, "row_rounding": True,
                      "heading_asset_types": [(r"^alternative\s+investments$", "Partnership Interest")]},
    # Pentegra Defined Benefit Plan for Financial Institutions, pp.22-28; the
    # filing prints the schedule again at pp.283-289 (2026-10-16).
    "135645888-333": {"name": "Pentegra Defined Benefit Plan for Financial Institutions",
                      "layout": "typed_maturity_rate",
                      "start_heading": r"^FORM\s+5500,\s+SCHEDULE\s+H,\s+PART\s+IV,\s+LINE\s+4i$",
                      "closing_names": True, "row_rounding": True,
                      "heading_asset_types": [(r"^private\s+equity$", "Partnership Interest"),
                                              (r"^state\s+and\s+municipal", "Bond")]},
    # Liberty Mutual: one Schedule H category per row, no section totals; the
    # MetLife separate account prints without a category (2026-10-16).
    "043583679-001": {"name": "Liberty Mutual Retirement Benefit Plan", "layout": "sch_h_category",
                      "start_heading": r"^Liberty\s+Mutual\s+Retirement\s+Benefit\s+Plan$",
                      "row_asset_types": [(r"^\*?\s*METROPOLITAN\s+LIFE\b", "Separate Account")]},
    # UPMC Master Trust: pp.1-23, a category on every row; pp.25-44 are an
    # unreadable (CID-only) attachment. Rows sum $10 over the printed total
    # (whole-dollar rounding) (2026-10-17).
    "251423657-999": {"name": "UPMC Master Trust", "layout": "sch_h_category",
                      "start_heading": r"^UPMC\s+Master\s+Trust$",
                      "categories": {"Private Equity Investment": "Partnership Interest",
                                     "Registered Investment Company": "Mutual Fund",
                                     "Fixed Income Security": "Bond",
                                     "Common/Collective Trust Fund": "Common/Collective Trust Fund",
                                     "Equity Security": "Common Stock", "Derivative": "Derivative",
                                     "Cash": "Cash"},
                      "row_rounding": True},
    # Farmers Group, Inc. Employees' Pension Plan (FYE 2025-11-30): pp.29-52,
    # printed again at pp.93-116; the type follows the issuer on every row and
    # the unlabeled par/cost/value grand total ends it. "Limited Partnership"
    # (Pathway, JPM Infrastructure, UBS Trumbull) is cert JV $385M (2026-10-17).
    "950725935-001": {"name": "Farmers Group, Inc. Employees' Pension Plan", "layout": "sch_h_category",
                      "start_heading": r"^Employees'\s+Pension\s+Plan$",
                      "wrap_strip": r"^Commercial\s+Mortgage\s+Backed$",
                      "categories": {"Limited Partnership": "Partnership Interest",
                                     "Commingled Funds": "Common/Collective Trust Fund",
                                     "Corporate Bonds": "Bond", "U.S. Government and Agencies": "Bond",
                                     "Other Government": "Bond", "Asset Backed Securities": "Bond",
                                     "Securities": "Bond", "Collateralized Mortgage Obligation": "Bond",
                                     "Common Stocks": "Common Stock", "Preferred Stocks": "Preferred Stock",
                                     "Investment Contracts": "Investment Contract",
                                     "Derivatives (SWAPS)": "Derivative", "Derivatives (Futures)": "Derivative",
                                     "Cash and Cash Equivalents": "Cash", "Short Term": "Cash"}},
    # Paul Hastings LLP Defined Benefit Retirement Plan for Partners (FYE
    # 2025-05-31): pp.16-24, a type on every row, shares before cost; pp.25 on
    # are acquired-and-disposed (2026-10-17).
    "952209675-024": {"name": "Paul Hastings LLP Defined Benefit Retirement Plan for Partners",
                      "layout": "sch_h_category",
                      "start_heading": r"^PAUL\s+HASTINGS\s+LLP\s+DEFINED\s+BENEFIT\s+RETIREMENT\s+PLAN\s+FOR\s+PARTNERS$",
                      "categories": {"Partnership Interest": "Partnership Interest",
                                     "Common stock": "Common Stock", "Mutual Fund": "Mutual Fund",
                                     "Trust Fund": "Common/Collective Trust Fund", "Cash": "Cash"},
                      "wrap_strip": r"\s*Collective/Commingled$"},
    # AFTRA Retirement Fund (FYE 2025-11-30): pp.21-31, AFM's columns less
    # collateral; partnership/joint venture = cert JV (2026-10-17).
    "136414972-001": {"name": "AFTRA Retirement Fund", "layout": "described_categories",
                      "start_heading": r"^AFTRA\s+Retirement\s+Fund$",
                      "title_suffix": r"\s+(?:STIF|Common|P/JVI|CCT|CCTUSD|RIC)(?:\s+N/A\s+N/A)?$",
                      "own_section_rows": [(r"^Interest\s+Bearing\s+Cash\s", "Interest Bearing Cash",
                                            "Short-Term Investment Fund")],
                      "subtotal_cost_errata": {"Common / Collective Trusts": 447858987.0},
                      "row_rounding": True},
    # American Federation of Musicians: the description and collateral columns
    # print between name and rate; Other Investments has no subtotal (2026-10-16).
    "516120204-001": {"name": "American Federation of Musicians and Employers' Pension Fund",
                      "layout": "described_categories",
                      "start_heading": r"^American\s+Federation\s+of\s+Musicians", "overlapping_spaces": True,
                      "title_suffix": r"\s+(?:STIF|Gov't|Bonds|Common|P/JVI|CCT|Mutual\s+Fund|Commingled\s+Fund)"
                                      r"\s+N/A$",
                      "orphan_heading": "Partnership/Joint Venture Interests", "grand_closes_last": True},
    # Teamsters Pension Trust Fund of Philadelphia and Vicinity, pp.27-47:
    # "<Category>:" headings closed by "Total <category>"; limited partnerships
    # = cert JV (2026-10-17).
    "231511735-001": {"name": "Teamsters Pension Trust Fund of Philadelphia and Vicinity",
                      "layout": "typed_maturity_rate", "colon_headings": True,
                      "start_heading": r"^Teamsters\s+Pension\s+Trust\s+Fund$",
                      "skip_lines": r"^\*\s*A\s+party-in-interest|^-\s*\d+\s*-$",
                      "heading_asset_types": [(r"^Corporate\s+obligations", "Bond")]},
    # Legacy Plan of the National Retirement Fund, pp.26-32: "<CATEGORY>:" headings,
    # each closed by a bare "<cost> <value>"; Alico Services, the fund's own
    # subsidiary, prints alone after short-term; partnerships = cert JV (2026-10-17).
    "136130178-001": {"name": "Legacy Plan of the National Retirement Fund", "layout": "typed_maturity_rate",
                      "colon_headings": True, "start_heading": r"^EIN\s+#13-6130178$",
                      "grand_re": r"^Total\s+investments\s+\$",
                      "skip_lines": r"^\*\s*A\s+party-in-interest|^\*\*\s*Includes|"
                                    r"^See\s+accompanying\s+independent|^-\s*\d+\s*-$",
                      "own_section_rows": [(r"^ALICO\s+SERVICES\s", "Alico Services Corporation", "")],
                      "heading_asset_types": [(r"^HEDGE\s+FUND\s+OF\s+FUNDS", "Hedge Fund"),
                                              (r"^COMMON\s+TRUST\s+FUNDS", "Common/Collective Trust Fund")]},
    # Laborers' District Council and Contractors' Pension Fund of Ohio, pp.32-77 (2026-10-16).
    "316129964-001": {"name": "Laborers' District Council and Contractors' Pension Fund of Ohio",
                      "layout": "typed_maturity_rate", "colon_headings": True,
                      "start_heading": r"^Schedule\s+H,\s+Line\s+4i\s+.\s+Schedule\s+of\s+Assets\b",
                      "heading_asset_types": [(r"^Private\s+equity\s+limited\s+partnerships", "Partnership Interest"),
                                              (r"^Asset-backed\s+securities|^Corporate\s+and\s+foreign", "Bond")]},
    # Consolidated Edison Retirement Plan: pp.34-252. The real estate section is
    # typed per row: the two the filer files as partnership/joint venture
    # interests, the funds and insurance contract as real estate (2026-10-17).
    "135009340-001": {"name": "Consolidated Edison Retirement Plan", "layout": "typed_maturity_rate",
                      "start_heading": r"^Schedule\s+H,\s+Line\s+4i\s+-\s+Schedule\s+of\s+Assets\s+\(Held\b",
                      "row_asset_types": [(r"\bPARTNERSHIP/JOINT\s+VENTURE\s+INTEREST\b", "Partnership Interest"),
                                          # Not "Other"'s insurance contract, which has no maturity date.
                                          (r"\bCOMMON/COLLECTIVE\s+TRUST\b|\bUNALLOCATED\s+INSURANCE\s+CONTRACTS\s+\d+/",
                                           "Real Estate")],
                      "heading_asset_types": [(r"^Commingled\s+real\s+estate\s+funds$", ""),
                                              (r"^Private\s+Equity$", "Partnership Interest"),
                                              (r"^Hedge\s+Funds$", "Hedge Fund")]},
    # Building Service 32BJ Pension Fund: pp.29-41, FY ending June 2025 (2026-10-17).
    "131879376-001": {"name": "Building Service 32BJ Pension Fund", "layout": "typed_maturity_rate",
                      "start_heading": r"^FORM\s+5500,\s+SCHEDULE\s+H,\s+LINE\s+4I\s+.\s+SCHEDULE\s+OF\s+ASSETS\b",
                      "value_only_grand": True,
                      "subtotal_cost_errata": {"COMMON INVESTMENT FUNDS": -177250081.0,
                                               "LIMITED PARTNERSHIPS": 64191090.0},
                      "heading_asset_types": [(r"^CORPORATE\s+BONDS\b", "Bond"),
                                              (r"^COMMON\s+INVESTMENT\s+FUNDS$", "Common/Collective Trust Fund")]},
    # Abbott - AbbVie Multiple Employer Pension Plan: pp.22-43 (printed again at
    # pp.87-108); partnerships = cert JV (2026-10-17).
    # The Hartford Retirement Plan for U.S. Employees: pp.52-99, "<HEADING>:" and
    # "SUBTOTAL <HEADING>: <par> <cost> <value>"; the pension account only, its
    # $8.3M 401(h) account (pp.100-102) left out; limited partnerships = cert JV (2026-10-17).
    "133317783-001": {"name": "The Hartford Retirement Plan for U.S. Employees", "layout": "typed_maturity_rate",
                      "start_heading": r"^THE\s+HARTFORD\s+RETIREMENT\s+PLAN\s+FOR\s+U\.S\.\s+EMPLOYEES$",
                      "colon_headings": True, "heading_asset_types": [(r"^MORTGAGE\s+LOANS$", "Bond")]},
    # Carpenters Pension Fund of Illinois: pp.9-10, "<name> <cost> <value>" rows
    # under plain headings, no par; limited partnerships = cert JV (2026-10-17).
    "366147396-001": {"name": "Carpenters Pension Fund of Illinois", "layout": "typed_maturity_rate",
                      "start_heading": r"^CARPENTERS\s+PENSION\s+FUND\s+OF\s+ILLINOIS$",
                      # ULLICO's J for Jobs is a pooled separate account.
                      "heading_asset_types": [(r"^Cash\s+Equivalents$", "Short-Term Investment Fund"),
                                              (r"^Corporate\s+Obligations$", "Bond"),
                                              (r"^Common\s+and\s+Preferred\s+Stock$", "Common Stock"),
                                              (r"^Pooled\s+Investment\s+Funds$", "Separate Account")]},
    # USAA Pension Plan (2025): pp.26-45, "<Heading>:" and "Total <Heading>"; its
    # 401(h) account repeats the headings; real estate and infrastructure plus
    # private equity/debt partnerships = cert JV (2026-10-17).
    "740959140-001": {"name": "USAA Pension Plan", "layout": "typed_maturity_rate",
                      "start_heading": r"^USAA\s+PENSION\s+PLAN$",
                      "heading_asset_types": [(r"^U\.S\.\s+Treasury\s+securities$|^Mortgage\s+and\s+asset-backed", "Bond"),
                                              (r"^Cash\s+and\s+cash\s+equivalents", "Short-Term Investment Fund"),
                                              (r"^Mutual\s+funds\s+and\s+exchange\s+traded", "Mutual Fund"),
                                              (r"partnerships$", "Partnership Interest")]},
    # Intermountain Healthcare Pension Plan: pp.45-90 in thousands, "<Heading>:"
    # and a bare "<cost> <value>" closing each; alternative investments hold
    # cert JV and the hedge funds (2026-10-17).
    "870269232-001": {"name": "Intermountain Healthcare Pension Plan", "layout": "typed_maturity_rate",
                      "start_heading": r"^Intermountain\s+Healthcare\s+Pension\s+Plan$", "amount_scale": 1000,
                      "heading_asset_types": [(r"^Cash\s+and\s+equivalents$", "Short-Term Investment Fund"),
                                              (r"^Fixed-income\s+securities$", "Bond")],
                      # Alternative investments: the hedge funds by name, the rest partnerships.
                      "row_asset_types": [(r"^\*?\s*(?:ABS\s+Emerging\s+Markets|AHL\s+Alpha|Alphadyne|Brevan\s+Howard"
                                           r"|Caspian\s+Select|Elliott\s+International|Magnitude\s+International"
                                           r"|Nokota\s+Capital|Palmerston\s+Credit|Polar\s+Multi-Strategy|Rokos"
                                           r"|RV\s+Aristeia|Two\s+Sigma|Verition|Voloridge)\b", "Hedge Fund"),
                                          (r".", "Partnership Interest")]},
    # LIUNA National (Industrial) Pension Fund: pp.26-46; per-manager totals inside
    # sections; limited partnerships and other investments = cert JV (2026-10-17).
    "526074345-001": {"name": "Laborers' International Union of North America National (Industrial) Pension Fund",
                      "layout": "typed_maturity_rate",
                      "start_heading": r"^AMERICA\s+NATIONAL\s+\(INDUSTRIAL\)\s+PENSION\s+FUND$",
                      "skip_lines": r"^Investment\s+Manager\s+-\s|^Working\s+Account$",
                      "join_next": r"^(?:Total\s+)?U\.S\.\s+Government,\s+government\s+guaranteed\s+and$",
                      "group_totals": True},
    # The Children's Hospital Corporation Pension Plan: pp.17-22, colon headings,
    # wrapped partnership names; limited partnerships = cert JV (2026-10-17).
    "042774441-001": {"name": "The Children's Hospital Corporation Pension Plan", "layout": "typed_maturity_rate",
                      "start_heading": r"^THE\s+CHILDREN'S\s+HOSPITAL\s+CORPORATION\s+PENSION\s+PLAN$",
                      # 3M's "Description of Investment" starts its header; the rest of it.
                      "skip_lines": r"^Identity\s+of\s+Including\s+Maturity\s+Date,$"
                                    r"|^Issuer,\s+Borrower\s+Rate\s+of\s+Interest,\s+Collateral,\s+Current$"
                                    r"|^Lessor\s+or\s+Similar\s+Party\s+Par\s+or\s+Maturity\s+Value\*\*\s+Cost\s+Value$",
                      "colon_headings": True,
                      "heading_asset_types": [(r"^Mutual\s+and\s+exchange-traded\s+funds$", "Mutual Fund")]},
    # Xcel Energy Inc. Master Pension Trust: the JPMorgan held schedule (FD491),
    # pp.97-; the acquired-and-disposed schedule (FD492) follows (2026-10-17).
    "901071182-010": {"name": "Xcel Energy Inc. Master Pension Trust", "layout": "jpm_held",
                      "start_heading": r"^NONINTEREST-BEARING\s+CASH$"},
    # Master Trust for Employee Benefit Plans of Advance Publications: FD491
    # pp.1-33 (FD492 4i-2 and FD495 4j follow); PARTNERSHIP/JV $235,538,505
    # as printed, the cert JV is $446M (2026-10-17).
    "135576716-001": {"name": "Master Trust for Employee Benefit Plans of Advance Publications",
                      "layout": "jpm_held", "start_heading": r"^NONINTEREST-BEARING\s+CASH$"},
    "360698440-300": {"name": "Abbott - AbbVie Multiple Employer Pension Plan", "layout": "typed_maturity_rate",
                      "start_heading": r"^Abbott\s+-\s+AbbVie$",
                      # Also keeps "TOTAL CAP INTL 3.127% ..." (a bond) from closing a section.
                      "headings": r"^(?:U\.S\.\s+government\s+securities|Corporate\s+debt\s+instruments\s+-\s+"
                                  r"(?:preferred|other)|Corporate\s+(?:preferred|common)\s+stocks"
                                  r"|103-12\s+investment\s+entities|Collective\s+trusts|Registered\s+investment\s+"
                                  r"companies|Partnerships|Other\s+assets)$",
                      "own_section_rows": [(r"^Interest\s+bearing\s+cash\s", "Interest bearing cash",
                                            "Short-Term Investment Fund")],
                      "heading_asset_types": [(r"^Collective\s+trusts$", "Common/Collective Trust Fund"),
                                              (r"^Other\s+assets$", "Derivative")]},
    # TCCC Master Retirement Trust: Northern Trust pp.539-629; the filing
    # skips NT page 58 (Corporate Stock - Common, countries between Netherlands
    # and Russia), whose amounts are the category total less its printed
    # regions. Partnership/JV $468,523,497 = cert JV (2026-10-17).
    "580628465-003": {"name": "TCCC Master Retirement Trust", "layout": "nt_held_regions",
                      "start_heading": r"^5500\s+Supplemental\s+Schedules\s+Account\s+number\s+COCINT$",
                      "missing_page_totals": {"Corporate Stock - Common": [5003096.34, 5329534.05]}},
    # M&T Bank Corporation Pension Plan: pp.19-36; limited partner and member
    # interests $475,157,432 = cert JV (2026-10-17).
    "160538020-002": {"name": "M&T Bank Corporation Pension Plan", "layout": "typed_maturity_rate",
                      "start_heading": r"^Ein\s+16-0538020\s+Plan\s+002$",
                      "heading_asset_types": [(r"^Limited\s+partner\s+and\s+member\s+interests$", "Partnership Interest"),
                                              (r"^Common\s+and\s+Collective\s+Funds$", "Common/Collective Trust Fund"),
                                              (r"^Exchange-traded\s+funds$", "ETF"),
                                              (r"^Commercial\s+Paper$", "Bond")]},
    # Factory Mutual Insurance Company Pension Plan: pp.27-53. Limited
    # partnerships $528,837,776 = cert JV; the filer's cost subtotal for them
    # is $450 under its rows (value ties), and its grand total prints one
    # digit short at the page edge (2026-10-17).
    "050316605-001": {"name": "Factory Mutual Insurance Company Pension Plan", "layout": "typed_maturity_rate",
                      "start_heading": r"^Factory\s+Mutual\s+Insurance\s+Company\s+Pension\s+Plan$",
                      "row_rounding": True, "leading_id": True, "grand_clipped_digits": 1,
                      "headings": r"^(?:Cash|Currency|Common\s+stocks?|U\.S\.\s+government\s+securities|Corporate\s+bonds"
                                  r"|Municipal\s+bonds|Sovereign\s+debt|Mutual\s+funds\s+and\s+exchange-traded\s+funds"
                                  r"|Common\s+collective\s+trusts|Limited\s+partnerships)$",
                      # Cash and currency are each one "Total interest bearing ..." line;
                      # two rows print no amounts (912810TW8's second lot, 31418ASC3's).
                      "skip_lines": r"^(?:Cash|Currency)$|^912810TW8\s.*4\.75%$|^31418ASC3\s.*MA1414$",
                      "own_section_rows": [(r"^Total\s+interest\s+bearing\s+cash\s", "Interest bearing cash",
                                            "Short-Term Investment Fund"),
                                           (r"^Total\s+interest\s+bearing\s+currency\s", "Interest bearing currency",
                                            "Short-Term Investment Fund")],
                      "subtotal_cost_errata": {"Limited partnerships": -450},
                      "heading_asset_types": [(r"^Common\s+collective\s+trusts$", "Common/Collective Trust Fund"),
                                              (r"^Mutual\s+funds\s+and\s+exchange-traded\s+funds$", "Mutual Fund"),
                                              (r"^Sovereign\s+debt$", "Bond")]},
    # Exelon Corp Pension Master Trust - Private Equity Pool 2016: one page, p.10 (2026-10-17).
    "232990190-419": {"name": "Exelon Corp Pension Master Trust - Private Equity Pool 2016",
                      "layout": "typed_maturity_rate",
                      "start_heading": r"^EXELON\s+CORP\s+PENSION\s+MASTER\s+TRUST-\s*PRIVATE\s+EQUITY\s+POOL\s+2016$",
                      "row_rounding": True},
    # Constellation Pension Master Trust - Private Equity New Pool (2024): Exelon's
    # page, p.9; its 29 LPs print under CORPORATE STOCK - PREFERRED, $390,876,151
    # (cert JV $391M, 2026-10-17).
    "233064219-311": {"name": "Constellation Pension Master Trust - Private Equity New Pool",
                      "layout": "typed_maturity_rate",
                      "start_heading": r"^CONSTELLATION\s+PENSION\s+MASTER\s+TRUST-\s*PRIVATE\s+EQUITY\s+NEW\s+POOL$",
                      "row_rounding": True,
                      "heading_asset_types": [(r"^CORPORATE\s+STOCK\s+-\s+PREFERRED$", "Partnership Interest")]},
    # Textron Inc. Master Trust: the whole schedule from its first section (2026-10-16).
    "133380685-001": {"name": "Textron Inc. Master Trust", "layout": "soi_revalued_cost",
                      "start_heading": r"^INTEREST-BEARING\s+CASH$"},
    # PG&E Corporation Retirement Master Trust: State Street's M1102E report,
    # pp.2-183; its spaces print as "\x03" (2026-10-17).
    "946222282-002": {"name": "PG&E Corporation Retirement Master Trust", "layout": "soi_revalued_cost",
                      "start_heading": r"^INTEREST-BEARING\s+CASH$"},
    # California Ironworkers Field Pension Trust (plan year ended 5/31/2025): the
    # p.51 4i says "See attached", State Street's M1102E report at pp.53-104;
    # PARTNERSHIP/JOINT VENTURE INTEREST $521,531,825 (cert JV $404M, 2026-10-17).
    "956042866-001": {"name": "California Ironworkers Field Pension Trust", "layout": "soi_revalued_cost",
                      "start_heading": r"^INTEREST-BEARING\s+CASH$"},
    # Merck & Co., Inc. Master Retirement Trust (2024): the M1102E report at
    # pp.2-95. PARTNERSHIP/JOINT VENTURE INTEREST prints $186,351,755 (IDR Core,
    # GS PE IX, Pantheon) against cert JV $375M (2026-10-17).
    "136366355-001": {"name": "Merck & Co., Inc. Master Retirement Trust", "layout": "soi_revalued_cost",
                      "start_heading": r"^INTEREST-BEARING\s+CASH$"},
    # BP Master Trust: the JPMorgan held schedule, pp.132-278 (2026-10-16).
    "361812780-300": {"name": "BP Master Trust for Employee Pension Plans", "layout": "jpm_held",
                      "start_heading": r"^NONINTEREST-BEARING\s+CASH$"},
    # Central Pennsylvania Teamsters Defined Benefit Plan (2024): pp.37-45,
    # Teamsters Philadelphia's typed columns with "Category:" headings; one
    # printed without its colon. Its three limited partnership sections are
    # cert JV $422M; "private equity" would type them Common Stock (2026-10-17).
    "236262789-001": {"name": "Central Pennsylvania Teamsters Defined Benefit Plan",
                      "layout": "typed_maturity_rate", "colon_headings": True,
                      "start_heading": r"^Central\s+Pennsylvania\s+Teamsters$",
                      "skip_lines": r"^\*\s*A\s+party-in-interest|^-\s*\d+\s*-$",
                      "bare_headings": r"^Common\s+collective\s+trust\s+-\s+equity$",
                      "heading_asset_types": [(r"^Limited\s+partnership", "Partnership Interest"),
                                              (r"hedged?\s+fund\s+of\s+funds", "Hedge Fund"),
                                              (r"^Common\s+collective\s+trust", "Common/Collective Trust Fund")]},
    # International Brotherhood of Teamsters Union Local No. 710 Pension Fund
    # (2025): pp.40-54, printed again at pp.70-84. Each row repeats its section
    # heading as its description column; "Partnership Interests" is cert JV
    # $845M; "Corporate Obligations - Preferred" are bonds (2026-10-17).
    "362377656-001": {"name": "International Brotherhood of Teamsters Union Local No. 710 Pension Fund",
                      "layout": "typed_maturity_rate",
                      "start_heading": r"^LOCAL\s+NO\.\s+710\s+PENSION\s+FUND$",
                      "headings": r"^(?:U\.S\.\s+Government\s+Securities|Municipal\s+Obligations|Corporate\s+Obligations"
                                  r"|Common\s+Stock|Common\s+Commingled\s+Trusts|103-12\s+Investment\s+Entities"
                                  r"|Partnerships?\s+(?:and\s+Other|Interests)|Other\s+Pooled\s+and\s+Offshore"
                                  r"|Registered\s+Investment\s+Companies|Cash\s+Equivalents|Investment\s+in\s+Office"
                                  r"|Collateral\s+Held\s+for)",
                      "skip_lines": r"^Total\s+assets\s+\(held\s+at\s+end\s+of\s+period\)\s+at\s+fair\s+value\s"
                                    r"|^real\s+estate\s+investment\s+trusts$",
                      "join_next": r"^Total\s+common\s+commingled\s+trusts\s+-$",
                      "grand_re": r"^Total\s+assets\s+\(held\s+at\s+end\s+of\s+period\)\s+\$",
                      "heading_asset_types": [(r"^Corporate\s+Obligations", "Bond"),
                                              (r"^Partnership\s+Interests", "Partnership Interest"),
                                              (r"^Other\s+Pooled\s+and\s+Offshore", "Hedge Fund"),
                                              (r"^Common\s+Commingled", "Common/Collective Trust Fund")]},
    # BASF Master Trust (2024): the JPMorgan held schedule FD491, its pages 6-12
    # at pp.85-91 after the 4i-2/4j reports. PARTNERSHIP/JV prints $339,594,938
    # (cert JV $423M); the filer lists private funds under COMMON ($131.7M:
    # Blackstone MAC, H/2, Oaktree, GSO, EIG, PEG, TPG, Fortress) and hedge
    # funds under its fund categories (2026-10-17).
    "256263985-101": {"name": "BASF Corporation Pension Master Trust", "layout": "jpm_held",
                      "start_heading": r"^NONINTEREST-BEARING\s+CASH$",
                      "row_asset_types": [
                          (r"^(?:BLACKSTONE\s+MULTI-ASSET\s+CREDIT\s+FUND\s+\(CAYM|EIG\s+ENERGY\s+FUND"
                           r"|FORTRESS\s+CREDIT\s+OPPORTUNITY|GSO\s+CAPITAL\s+SOLUTIONS|H/2\s+SPECIAL\s+OPPORTUNITIES"
                           r"|OAKTREE\s+REAL\s+ESTATE\s+OPPORTUNITIES|PEG\s+SECONDARY\s+PRIVATE\s+EQUITY"
                           r"|TPG\s+PARTNERS\b)", "Partnership Interest"),
                          (r"^(?:MAGNITUDE\s+INSTITUTIONAL|HILDENE\s+OPPORTUNITIES)", "Hedge Fund")]},
    # Reynolds American Defined Benefit Master Trust: the JPMorgan held
    # schedule (FD491), pp.1-78 (2026-10-17).
    "200546644-011": {"name": "Reynolds American Defined Benefit Master Trust", "layout": "jpm_held",
                      "start_heading": r"^NONINTEREST-BEARING\s+CASH$"},
    # DTE Energy Company Affiliates Employee Benefit Plans Master Trust, pp.33-86 (2026-10-16).
    "256264027-021": {"name": "DTE Energy Company Affiliates Employee Benefit Plans Master Trust",
                      "layout": "typed_maturity_rate", "colon_headings": True, "row_rounding": True,
                      "start_heading": r"^DTE\s+Energy\s+Company\s+Affiliates\s+Employee\s+Benefit\s+Plans\s+Master\s+Trust$",
                      "heading_asset_types": [(r"^Common\s+collective\s+trusts", "Common/Collective Trust Fund"),
                                              (r"^Common\s+and\s+preferred\s+stocks", "Common Stock")]},
    # Pension, Hospitalization and Benefit Plan of the Electrical Industry: one
    # alphabetical list, pp.27-58; bond rates print without "%" (2026-10-16).
    "136123601-001": {"name": "Pension, Hospitalization and Benefit Plan of the Electrical Industry",
                      "layout": "flat_cost_value", "word_lines": True,
                      "start_heading": r"^Pension,\s+Hospitalization\s+and\s+Benefit\s+Plan\b",
                      "row_asset_types": [
                          (r"SHORT-TERM\s+INVESTMENT\s+FUND", "Short-Term Investment Fund"),
                          (r"\bINDEX\s+FUND$|\bALPHA\s+FUND$|^FIRST\s+EAGLE\s+INTERNATIONAL\s+VALUE\s+FUND$|"
                           r"INVESTMENT\s+TRUST$|PROPERTY\s+TRUST$|^PRISA\b", "Common/Collective Trust Fund"),
                          (r"\b144A\b|\bREGS\b|\bTERM\s+LOAN\b|\bPOOL\b|\bSR\s+(?:UN)?SECURED\b|\bCOMPANY\s+GUAR\b|"
                           r"\bSUBORDINA|\bN/B\b|\b\d\d/\d\d\s+(?:VAR|\d+(?:\.\d+)?)$|"
                           # Structured bonds print as issuer, ticker, year, series and class.
                           r"\b(?:FNR|FHR|FNS|SCRT|FWLS|TBA)\b|\b(?:19|20)\d\d\s+\w+\s+\w+$", "Bond")]},
}

# Every requested page must carry this column-header line, else we don't own the layout.
_HEADER_RE = re.compile(r"Identifier\s+Security\s+Title\b.*\bValue\b", re.I)
_IPR_HEADER_RE = re.compile(r"Identity\s+of\s+Issue\b.*\bPar\s+Value\b.*\bCurrent\s+Value\b", re.I)
_IPR_HEADING_RE = re.compile(r"^[A-Z][A-Z ,&/()'\-]*[A-Z)]$")
_DR_HEADER_RE = re.compile(r"Assets\s+and\s+Liabilities\s+Detail\s+Report", re.I)
# Column-date line ("1/1/2024 12/31/2024") that ends each page's title block.
_DR_DATES_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}/\d{1,2}/\d{4}$")
# Footer: "4/29/2025 7:43:38 PM EDT Page 1 of 203" (pdfplumber garbles some to "o2f03").
# Also the 24-hour "07/29/2025 10:28:19 EDT (GMT -4) Page 7 of38" footer (NFL) and
# the two-digit-year "07/30/25 4:40:00 PM EDT" one (Dominion, 2026-10-16).
_DR_FOOTER_RE = re.compile(r"^\d{1,2}/\d{1,2}/(?:\d{2}|\d{4})\s+\d{1,2}:\d{2}:\d{2}\s+(?:[AP]M\b|[A-Z]{2,4}\s+\(GMT\b)")
# Some filers' text drops the space after the letter ("A.NON-INTEREST BEARING
# CASH", Cox); then the heading must be all capitals, unlike "T.ROWE ... 1,000".
_DR_LETTER_RE = re.compile(r"^([A-Z])\.(?:\s+|(?=[A-Z]{2}[A-Z\- ]*$))(.+)$")
_DR_ITEM_RE = re.compile(r"^\((\d{1,2})\)\s*(.+)$")
_DR_SUBITEM_RE = re.compile(r"^\(([A-Z])\)\s*(.+)$")
# Schedule H line-item path ("<item> / <sub-item>") -> asset_type, first match wins.
_DR_ASSET_TYPES = [
    (r"(?<!non-)interest\s+bearing\s+cash", "Short-Term Investment Fund"),
    (r"government\s+securities|corporate\s+debt", "Bond"),
    (r"corporate\s+stocks\s*/\s*preferred", "Preferred Stock"),
    (r"corporate\s+stocks", "Common Stock"),
    (r"partnership|joint\s+venture", "Partnership Interest"),
    (r"^real\s+estate", "Real Estate"),
    (r"common/collective", "Common/Collective Trust Fund"),
    (r"pooled\s+separate", "Separate Account"),
    (r"103-12", "103-12 Investment Entity"),
    (r"registered\s+investment", "Mutual Fund"),
    (r"insurance", "Insurance General Account"),
    (r"employer\s+securities", "Employer Stock"),
]
# Letter sections parsed only to reconcile against TOTAL ASSETS.
_DR_NON_INVESTMENT_LETTERS = {"A", "B"}
_SOI_HEADER_RE = re.compile(r"Schedule\s+of\s+Investments\s+at\s+End\s+of\s+Plan\s+Year", re.I)
# Column-header line that ends each page's title block.
_SOI_COLUMNS_RE = re.compile(r"^Security\s+ID\s+Security\s+Description\b.*\bMarket\s+Value\b", re.I)
_SOI_ROW_RE = re.compile(r"^([A-Z0-9]{12})\s+([A-Z0-9]{9})\s+(.+)$")
_SOI_CAPS_RE = re.compile(r"^[A-Z][A-Z0-9 .,&/()'\-]*$")
# Section heading -> asset_type, first match wins.
_SOI_ASSET_TYPES = [
    (r"interest-?\s*bearing\s+cash", "Short-Term Investment Fund"),
    (r"government\s+securities|corporate\s+debt", "Bond"),
    (r"corporate\s+stock\s*-\s*preferred", "Preferred Stock"),
    (r"corporate\s+stock", "Common Stock"),
    (r"partnership|joint\s+venture", "Partnership Interest"),
    (r"^real\s+estate", "Real Estate"),
    (r"common/collective", "Common/Collective Trust Fund"),
    (r"pooled\s+separate", "Separate Account"),
    (r"103-12", "103-12 Investment Entity"),
    (r"registered\s+investment", "Mutual Fund"),
    (r"insurance", "Insurance General Account"),
    (r"employer\s+securities", "Employer Stock"),
]
# Southern Company's trust name splits the title: "HELD FOR TRUST .\nINVESTMENT PURPOSES".
_JPM_HEADER_RE = re.compile(r"SCHEDULE\s+OF\s+ASSETS\s+HELD\s+FOR\s+(?:\S.*?\s+)?INVESTMENT\s+PURPOSES\s+AT\s+END\s+OF\s+YEAR")
# Second half of an amount wrapped onto its own line ("... 15," then "050,108,266").
_JPM_WRAPPED_RE = re.compile(r"^\d{3}(?:,\d{3})*$")
# Last line of each page's title block.
_JPM_COLUMNS_RE = re.compile(r"^SHARES\s*/\s*PAR$")
_JPM_FOOTER_RE = re.compile(r"^JPMorgan\s+Investor\s+Services$")
_JPM_RULE_RE = re.compile(r"^[-=]{3,}[-= ]*$")
# Footer a filer adds above its page number (Southern Company).
_JPM_FILER_FOOTER_RE = re.compile(r"^This\s+schedule\s+is\s+presented\s+in\s+compliance\s+with\s+DOL\b")
# Shares/par line under each row: "1,000.000", ".270", "(3.000)".
_JPM_SHARES_RE = re.compile(r"^\(?[\d,]*\.\d+\)?$")
# The Schedule H line-item block whose rows are holdings.
_JPM_INVESTMENTS = "GENERAL INVESTMENTS"
# Leaf heading -> asset_type, first match wins.
_JPM_ASSET_TYPES = [
    (r"interest\s+bearing\s+cash|^interest-bearing\s+cash", "Short-Term Investment Fund"),
    (r"government|corporate\s+debt|municipal", "Bond"),
    (r"^preferred$", "Preferred Stock"),
    (r"^common$", "Common Stock"),
    (r"partnership|joint\s+venture", "Partnership Interest"),
    (r"^real\s+estate", "Real Estate"),
    (r"common/collective", "Common/Collective Trust Fund"),
    (r"pooled\s+separate", "Separate Account"),
    (r"103-12", "103-12 Investment Entity"),
    (r"registered\s+investment", "Mutual Fund"),
    (r"insurance", "Insurance General Account"),
    (r"employer\s+securities", "Employer Stock"),
]
# Report id varies by client ("H30T" MGB, "GEAC" GE); the title block doesn't.
_H30T_HEADER_RE = re.compile(r"SCHEDULE\s+H,\s+LINE\s+4I\s+-\s+SCHEDULE\s+OF\s+ASSETS\s+\(HELD\s+AT\s+END\s+OF\s+YEAR\)")
# Last line of the title block on holdings pages and on the category-summary page.
_H30T_COLUMNS_RE = re.compile(r"^FUND\s+SHARES/PAR\b")
_H30T_SUMMARY_RE = re.compile(r"^ASSET\s+CATEGORY\s+UNITS\b")
_H30T_RULE_RE = re.compile(r"^-{5,}$")
_H30T_TOTAL_RULE_RE = re.compile(r"^-{5,}(\s+-{5,})+$")
# Per-manager-account line: "H32L CAD ...", "H37M ACI0BDLV3 ...".
_H30T_ACCOUNT_RE = re.compile(r"^[A-Z0-9]{4}\s")
_H30T_RATE_DATE_RE = re.compile(r"^(.*?)\s+(\d{1,3}\.\d+)\s+(\d{2}/\d{2}/\d{4})$")
# Section heading -> asset_type, first match wins.
_H30T_ASSET_TYPES = [
    (r"interest\s+bearing\s+cash|certificates\s+of\s+deposit", "Short-Term Investment Fund"),
    (r"government|corp\.?\s+debt|loans\s+secured\s+by\s+mtges", "Bond"),
    (r"corporate\s+stocks\s*-\s*preferred", "Preferred Stock"),
    (r"corporate\s+stocks", "Common Stock"),
    (r"partn|joint\s+venture", "Partnership Interest"),
    (r"^real\s+estate", "Real Estate"),
    (r"common/collective", "Common/Collective Trust Fund"),
    (r"pooled\s+separate", "Separate Account"),
    (r"103-12", "103-12 Investment Entity"),
    (r"registered\s+investment", "Mutual Fund"),
    (r"insurance", "Insurance General Account"),
    (r"employer\s+securities", "Employer Stock"),
]
# Also Koch's "a) b) Name c) Description d) Cost e) Current Value" (2026-10-16).
_FCV_HEADER_RE = re.compile(r"^Security\s+Name,\s+Maturity\s+Date,\s+and\s+Rate\s+Cost\s+Fair\s+Value$"
                            r"|^a\)\s+b\)\s+Name\s+c\)\s+Description\s+d\)\s+Cost\s+e\)\s+Current\s+Value$"
                            # Nationwide Defined Benefit Master Trust's Attachment A (2026-10-16).
                            r"|^Security\s+Description\s+1\s+Security\s+Description\s+2\s+Shares/Par\s+Base\s+Cost\s+"
                            r"Base\s+Market\s+Value$"
                            # Pension, Hospitalization and Benefit Plan of the Electrical
                            # Industry's; its last page drops "Cost" from the line (2026-10-16).
                            r"|^\(a\)\s+party\s+\"\s+or\s+maturity\s+value\s+\(d\)\s+(?:Cost\s+)?\(e\)\s+Current\s+value$"
                            # Main Line Health, Inc. Retirement Income Plan's (2026-10-17).
                            r"|^Identity\s+of\s+issue,\s+borrower,\s+lessor\s+or\s+similar\s+party\s+collateral,\s+par,\s+"
                            r"or\s+maturity\s+value\s+Cost\s+Current\s+value$"
                            # Teamsters Western Region and New Jersey Health Care Fund
                            # Retiree Plan's; a lone "party" line follows it (2026-10-17).
                            r"|^rate\s+of\s+interest,\s+collateral,\s+par\s+or\s+maturity\s+value\s+Value$"
                            # Twin City Hospitals - Minnesota Nurses Association Pension Plan's (2026-10-17).
                            r"|^Similar\s+Party\s+Collateral,\s+Par,\s+or\s+Maturity\s+Value\s+Cost\s+Market$"
                            # ExxonMobil Pension Plan's (2026-10-17).
                            r"|^SECURITY\s+DESCRIPTION\s+INTEREST\s+MATURITY\s+PAR\s+VALUE/\s+COST\s+CURRENT$",
                            re.M)
# Headers whose rows print "<shares/par> <cost> <value>".
_FCV_PAR_HEADER_RE = re.compile(r"^Security\s+Description\s+1\s+.*\bShares/Par\s+Base\s+Cost\b"
                                r"|^SECURITY\s+DESCRIPTION\s+INTEREST\s+MATURITY\s+PAR\s+VALUE/", re.M)
_FCV_TOTAL_RE = re.compile(r"^Total\s+investments\b|^Total\s+assets\s+held\s+at\s+end\s+of\s+year\b"
                           # Twin City's bare "Total $ <cost> $ <value>" (2026-10-17).
                           r"|^Total\s+\$\s*\d[\d, ]*\s+\$\s*\d[\d, ]*$"
                           # ExxonMobil's unlabeled "<par> <cost> <value>" (2026-10-17).
                           r"|^\d{1,3}(?:,\d{3})+\s+\d{1,3}(?:,\d{3})+\s+\d{1,3}(?:,\d{3})+$", re.I | re.M)
# Koch prints no total; its list ends at the page before the next schedule.
_FCV_NEXT_SCHEDULE_RE = re.compile(r"Schedule\s+of\s+Assets\s+\(Acquired\s+and\s+Disposed", re.I)
# Page number "- 27 -" and the party-in-interest legend.
_FCV_SKIP_RE = re.compile(r"^-\s*\d+\s*-$|^\*\s*Party-in-interest$", re.I)
_FCV_RATE_DATE_RE = re.compile(r"(\d{1,3}\.\d+)%\s+(\d{2}/\d{2}/\d{4})$")
# Row name -> asset_type, first match wins. There are no category headings, and
# detect_asset_type is DC-tuned (any "COMMON STOCK" -> Employer Stock), so this
# list is the layout's own. Securities are matched before funds so an issuer
# named "... PARTNERS LP CALLABLE NOTES" stays a bond.
_FCV_ASSET_TYPES = [
    (r"\bSWAPS?\b|\bTRS\b|\bCDS\b|\bCDI\b|\bFUT(?:URES?)?\b|\bFORWARD\b|\bOPTIONS?\b", "Derivative"),
    (r"LIQUIDITY\s+FUND|MONEY\s+MARKET|\bSTIF\b|COMMERCIAL\s+PAPER|CERTIFICATE\s+OF\s+DEPOSIT|^CASH\b",
     "Short-Term Investment Fund"),
    (r"\bPREFERENCE\b|\bPREFERRED\b", "Preferred Stock"),
    (r"COMMON\s+STOCK|\bADR\b|\bGDR\b|\bNY\s+REG\b|\bREIT\b|\bUNITS?\s+[A-Z]{3}\b|STAPLED|DUTCH\s+CERT", "Common Stock"),
    (r"\bETF\b|\bETP\b", "ETF"),
    (r"\bFDS\b", "Mutual Fund"),
    (r"\d\.\d+%|\bNOTES?\b|\bBONDS?\b|\bBDS\b|\bBILL\b|MORTPASS|MORTGAGE|PASS[- ]THROUGH|RECEIVABLES|\bCLO\b|"
     r"\bFLOATING\b|\bFIXED\b|\bTREASURY\b|\bDEBENTURES?\b|\bMTN\b", "Bond"),
    (r"INS(?:URANCE)?\s+CO\b.*\bPOLICY\b|GROUP\s+ANNUITY", "Separate Account"),
    (r"\bTR(?:UST)?\s+FD\b|\bNL\b|\bCOL\b|UNIT\s+CLASS|COLLECTIVE", "Common/Collective Trust Fund"),
    (r"FARMLAND|TIMBER|FORESTRY|PROPERTY|PROPERTIES|REAL\s+ESTATE|REALTY|LOGISTICS", "Partnership Interest"),
    (r"\bL\.*\s?P\.?(?=\W|$)|LTD\s+PART\b|\bPTRS\b|\bGPE\b|LIMITED\s+PARTNERSHIP|\bLLP\b|S\.?C\.?\s?SP\b|\bSCA\b|SICAV|FEEDER|\bPARTNERS\b|"
     r"\bVENTURES?\b|\bINVESTORS\b|\bCAPITAL\b.*\bFUND\b|\bFUND\s+[IVX]+\b|\b[IVX]+,?\s+(?:L\.?P|LLC)\b", "Partnership Interest"),
    (r"OFFSHORE|\bFUND,?\s+LTD\.?$|\b(?:INTERNATIONAL|EQUITIES(?:\s+[IVX]+)?|NEUTRAL)\b,?\s+(?:LTD|LIMITED)\.?$|"
     r"\bLIMITED$|INTERNATIONAL\s+FUND|\bSP$|MASTER\s+FUND|\(CAYMAN\)|\(BVI\)|130-30", "Hedge Fund"),
    # Any other pooled fund left in a DB master trust's flat list is a private fund.
    (r"\bFUND\b|\bFINANCE$|STRATEGIES", "Partnership Interest"),
    (r"^[A-Z0-9 .,&/'\-]+\b(?:INC|CORP|PLC|AG|SA|NV|LTD|COMPANY)\.?$", "Common Stock"),
]
_CTL_HEADER_RE = re.compile(r"Form\s+5500,\s+Schedule\s+H,\s+Line\s+4i\b"
                            r"|SCHEDULE\s+H\s+TO\s+FORM\s+5500,\s+LINE\s+4i\s+-\s+SCHEDULE\s+OF\s+ASSETS\s+\(HELD"
                            # Bank of America (2026-10-16).
                            r"|^Schedule\s+H,\s+Line\s+4i\s+-\s+Schedule\s+of\s+Assets\s+\(Held\s+at\s+End\s+of\s+Year\)$",
                            re.M)
# Last line of the title block; holdings start below it.
_CTL_COLUMNS_RE = re.compile(r"^\(a\)\s+(?:\(b\)\s+)?Identity\s+of\s+issuer|^SIMILAR\s+PARTY\b.*\bVALUE$"
                             r"|^Lessor,\s+or\s+Similar\s+Party\b.*\bCurrent\s+Value$", re.I)
# Page footer "- 89 - (Continued...)"; Bank of America's legend ends in its page number.
_CTL_FOOTER_RE = re.compile(r"^-\s*\d+\s*-|^\*Investments\s+with\s+parties-in-interest\b")
# Grand-total lines: IUOE prints held and loaned separately; IAM's held line
# already includes its loaned block ("TOTAL ASSETS ON HAND" / "TOTAL
# SECURITIES ON LOAN" are intermediate rollups and are skipped).
_CTL_GRAND_RE = re.compile(r"^TOTAL\s+(?:ASSETS\s+\(HELD\b|SECURITIES\s+LOANED\b|"
                           r"INVESTMENTS(?:\s+PER\s+FINANCIAL\s+STATEMENTS)?$)")
_CTL_RATE_DATE_RE = re.compile(r"(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,3}\.\d+)%|(\d{1,3}\.\d+)%\s+(\d{1,2}/\d{1,2}/\d{4})\b")
# Category heading -> asset_type, first match wins ("OTHER" stays blank).
_CTL_ASSET_TYPES = [
    (r"interest\W+bearing\s+cash|short-term|certificates\s+of\s+deposit", "Short-Term Investment Fund"),
    (r"government|municipal|corporate\s+(?:debt|obligations)", "Bond"),
    (r"corporate\s+stocks?\W+preferred|^preferred\s+stock", "Preferred Stock"),
    # Listed MLP units are equities, not private partnership interests.
    (r"\bstocks?\b|master\s+limited", "Common Stock"),
    (r"partnership|joint\s+venture", "Partnership Interest"),
    (r"real\s+estate", "Real Estate"),
    (r"common\W+collective", "Common/Collective Trust Fund"),
    (r"pooled\s+separate", "Separate Account"),
    (r"103-12", "103-12 Investment Entity"),
    (r"registered\s+investment", "Mutual Fund"),
    (r"insurance", "Insurance General Account"),
]
_GLT_HEADER_RE = re.compile(
    r"SCHEDULE\s+OF\s+ASSETS\s+AS\s+OF\b.{0,40}\bSCH\s+H\s+PART\s+IV\s+LINE\s+4\(i\)"
    # KeySpan Corporation Pension Master Trust's column header (2026-10-17).
    r"|^Security\s+ID\s+Security\s+Description\s+Shares\s+Cost\s+Market\s+Value\s+Unrealized\s+Gain/Loss$"
    # National Grid's Final Average Pay Pension Plan (2026-10-17).
    r"|^PAR\s?VALUE\s+SECURITY\s?DESCRIPTION\s+COST\s+PRICE\s+VALUE\s+GAIN/LOSS$",
    re.M)
# Last line of the title block; holdings start below it.
_GLT_COLUMNS_RE = re.compile(
    r"^GAIN/LOSS$"
    r"|^Security\s+ID\s+Security\s+Description\s+Shares\s+Cost\s+Market\s+Value\s+Unrealized\s+Gain/Loss$"
    r"|^PAR\s?VALUE\s+SECURITY\s?DESCRIPTION\s+COST\s+PRICE\s+VALUE\s+GAIN/LOSS$")
_GLT_GRAND_RE = re.compile(r"-TOTAL\s+ASSETS\s+HELD\s+AT\s+THE\s+END\s+OF\s+THE\s+YEAR$")
# Column header, the last line of every page's title block.
_NTP_HEADER_RE = re.compile(r"^\(a\)\s+\(c\)\s+Description\s+of\s+Investment\b.*\bShare/Par\s+Value\s+Cost\s+Value$"
                            # Hackensack's header ends a five-line block (2026-10-17).
                            r"|^maturity\s+value$",
                            re.M)
# Row name -> asset_type ahead of _FCV_ASSET_TYPES, for Northern Trust's own wording.
_NTP_ASSET_TYPES = [
    (r"^FUT\s", "Derivative"),
    (r"\bCOML\s+PAPER\b|\bCP\s+\d{2}-\d{2}-\d{4}$", "Short-Term Investment Fund"),
]
_BS_TITLE_RE = re.compile(
    r"Schedule\s+H\s*-\s*Line\s+4i\s*-\s*Schedule\s+of\s+Assets\s+\(Held\s+at\s+End\s+of\s+Year\)", re.I)
# Last line of the first page's column header; holdings start below it.
_BS_COLUMNS_RE = re.compile(r"^\(a\)\s+\(b\)\s+Identity\s+of\s+issue\b.*\bCurrent\s+Value$", re.I | re.M)
_BS_GRAND_RE = re.compile(r"^Grand\s+Total$", re.I)
# Every page's footer: the auditor's reference and the page number.
_BS_FOOTER_RE = re.compile(r"^See\s+Independent\s+Auditor|^Reporting\s+and\s+Disclosure\s+Under\b|^\d+$")
# Schedule H line 1c category headings. Any other unnumbered line opens a new
# section only right after a subtotal; inside a section it is a wrapped name.
_BS_CATEGORY_RE = re.compile(
    r"^(INTEREST-BEARING\s+CASH|(U\.S\.\s+)?GOVERNMENT\s+SECURITIES|CORPORATE\s+(DEBT|STOCK|BONDS)|LOANS\b|"
    r"PARTNERSHIP/JOINT\s+VENTURE|(NON-)?REAL\s+ESTATE|103-12|COMMON/COLLECTIVE|POOLED\s+SEPARATE|"
    r"REGISTERED\s+INVESTMENT|OTHER\s+INVESTMENTS|EMPLOYER|INSURANCE|MASTER\s+TRUST|"
    # NECA's (2026-10-16).
    r"VALUE\s+OF\s+FUNDS\s+HELD\s+IN\s+INSURANCE)", re.I)
# Column header, first page only; holdings start below it.
_IC_COLUMNS_RE = re.compile(r"^Identity\s+of\s+Issue,\s+Borrower\b.*\b5500\s+Code\s+Description\s+"
                            r"Shares/Par\s+Cost\s+Current\s+Value$", re.M)
_IC_GRAND_RE = re.compile(r"^Grand\s+Total$", re.I)
# The 5500 code that ends every row's text, ahead of its amounts.
_IC_CATEGORY_RE = re.compile(
    r"^(.*?)\s*\b(INTEREST-BEARING\s+CASH|(?:U\.S\.\s+)?GOVERNMENT\s+SECURITIES|"
    r"CORPORATE\s+DEBT\s+INSTRUMENTS(?:\s+-\s+(?:PREFERRED|ALL\s+OTHER))?|CORPORATE\s+STOCK\s+-\s+(?:PREFERRED|COMMON)|"
    r"PARTNERSHIP/JOINT\s+VENTURE\s+INTEREST|REAL\s+ESTATE(?:\s+-\s+[A-Z ]+)?|LOANS(?:\s+-\s+[A-Z ]+)?|"
    r"COMMON/COLLECTIVE\s+TRUSTS?|POOLED\s+SEPARATE\s+ACCOUNTS?|MASTER\s+TRUST\s+INVESTMENT\s+ACCOUNTS?|"
    r"103-12\s+INVESTMENT\s+ENTITIES|REGISTERED\s+INVESTMENT\s+COMPANIES|(?:UN)?ALLOCATED\s+INSURANCE\s+CONTRACTS|"
    r"EMPLOYER\s+(?:SECURITIES|REAL\s+PROPERTY)|OTHER\s+INVESTMENTS)$")
# A footnote printed after a few rows' value.
_IC_NOTE_RE = re.compile(r"\s+Because\s+they\s+do\s+not\s+file\s+a\s+5500$")
# Classification footer and the "#" page marker.
_IC_SKIP_RE = re.compile(r"^Fidelity\s+Internal\s+Information$|^#$")
# CenturyLink's "uAssets held for Investment Purposes" (a bullet glyph read as
# "u", 2026-10-17).
_NHR_HEADER_RE = re.compile(r"^(?:Schedule\s+of\s+Assets\s+Held|u?Assets\s+held)\s+for\s+Investment\s+Purposes$", re.M)
# Column header, the last line of every page's title block.
# CenturyLink's prints "Cost" alone (2026-10-17).
_NHR_COLUMNS_RE = re.compile(r"^Security\s+Description\s+/\s+Asset\s+ID\s+Shares/Par\s+Value\s+"
                             r"(?:Historical\s+)?Cost\s+Current\s+Value$", re.M)
# The unnamed grand total; category and sub-group totals carry a name.
_NHR_GRAND_RE = re.compile(r"^Total\s+\(?-?[\d,]+\.\d{2}\)?\s+\(?-?[\d,]+\.\d{2}\)?$", re.M)
# Securities-lending legend and Northern Trust's page footer.
_NHR_FOOTER_RE = re.compile(r"^(?:\*\*|••)\s*All\s+or\s+a\s+portion\b|^Northern\s+Trust\s+-|^Generated\s+by\s+Northern\s+Trust\b"
                            r"|^Northern\s+Trust$")
# Identifier lines under a holding ("CUSIP: ...", "SWU020HB3 SEDOL: ...", a bare id,
# or a swap's id range "99F576690-99F57668", Ascension 2026-10-16).
# A bare SEDOL is 7 characters ("9MY6W8L", or ": 9MX4LDU" under a "SEDOL"
# label, CenturyLink 2026-10-17).
_NHR_ID_RE = re.compile(r"\b(?:CUSIP|SEDOL|ISIN|TICKER)\s*:\s*(\S+)"
                        r"|^(?::\s*)?((?=[A-Z0-9]*\d)[A-Z0-9]{7}(?:[A-Z0-9]{2})?)(?:-[A-Z0-9]{8,9})?$",
                        re.I)
# asset_liability_positions: page title, last title-block line, footer, grand
# total, rows "<account> - <description> <shares> <% cat> <cost> <market>
# [-] <appr> <% acct> ..." and "Total <heading> [<% cat>] <cost> <market> ..."
# (the accrual totals print no % of category).
_ALP_TITLE_RE = re.compile(r"^Asset\s+and\s+Liability\s+Positions$", re.M)
_ALP_COLUMNS_RE = re.compile(r"^Asset\s+ID\s+\(CUSIP\)\s+/\s+Ticker\s+Unit\s+Price\s+Unit\s+Price\s+Rate$", re.M)
_ALP_FOOTER_RE = re.compile(r"^\d{3}\s+Page\s+\d+\s+of\s+\d+$")
_ALP_GRAND_RE = re.compile(r"^Total\s+Assets\s+and\s+Accruals\s+([\d,]+\.\d\d)\s+([\d,]+\.\d\d)\s", re.M)
_ALP_AMT = r"-?[\d,]*\d\.\d\d"
_ALP_ROW_RE = re.compile(rf"^([A-Z]{{2}})\s+-\s+(.*?)\s+(-?[\d,]*\.\d{{3}})\s+(\d*\.\d\d)\s+({_ALP_AMT})\s+"
                         rf"({_ALP_AMT})\s+(?:-\s)?[\d,]*\.\d\d\s+\d*\.\d\d\b")
_ALP_TOTAL_RE = re.compile(rf"^Total\s+(.+?)\s+(?:\d*\.\d\d\s+)?({_ALP_AMT})\s+({_ALP_AMT})\s+"
                           rf"(?:-\s)?[\d,]*\.\d\d\s+\d*\.\d\d\b")
# "<CUSIP/SEDOL/internal id> [/ <ticker>]", unit prices after it.
_ALP_ID_RE = re.compile(r"^((?=[A-Z0-9]*\d)[A-Z0-9]{7,10})(?:\s+/\s+([A-Z][A-Z0-9.]*))?(?:\s+-?[\d,]*\.\d+)*$")
# Heading path -> asset_type, first match wins; fund rows under "Limited
# Partnerships", "Closely Held", "Other" and the fund headings are typed by
# _ALP_FUND_TYPES instead.
_ALP_HEADING_TYPES = [
    (r"\bCommon\s+Stocks\b", "Common Stock"),
    (r"\bObligations\b|Treasury|Federal\s+Agenc", "Bond"),
    (r"^Cash\s+Equivalent", "Short-Term Investment Fund"),
    (r"^Real\s+Estate$", "Real Estate"),
]
_ALP_FUND_TYPES = [
    (r"\bRealty\b|\bPropert(?:y|ies)\b|\bReal\s+Estate\b|\bREIT\b", "Real Estate"),
    (r"\bHedged?\s+(?:LP|Fund)\b|\bInfrastructure\b|\bOpportunistic\s+Credit\b|\bPrivate\b", "Partnership Interest"),
]
# nt_market_erisa: page title, column header (last title-block line), the
# unnamed grand total (four amounts) and the page footer.
_NME_TITLE_RE = re.compile(r"^Current\s+Value\s+of\s+Assets\s+and\s+Liabilities$", re.M)
_NME_COLUMNS_RE = re.compile(r"^Security\s+Description\s+/\s+Asset\s+ID\s+Shares/Par\s+Value\s+Market\s+Price\s+"
                             r"Market\s+Value\s+ERISA\s+Cost\s+Security\s+Movements\s+Gain/Loss$", re.M)
_NME_GRAND_RE = re.compile(r"^Total(?:\s+-?[\d,]+\.\d{2}){4}$", re.M)
_NME_FOOTER_RE = re.compile(r"^Northern\s+Trust\s+-|^Generated\s+by\s+Northern\s+Trust\b")
_NAD_HEADER_RE = re.compile(r"^Asset\s+Detail\s+-\s+Base\s+Currency$", re.M)
# Last line of the column header; holdings start below it.
_NAD_COLUMNS_RE = re.compile(r"^Shares/PAR\s+value\s+local\s+market\s+price\s+income/expense\s+Market\s+Value\s+"
                             r"Cost\s+Market\s+Translation\s+Total$", re.M)
# The unnamed grand "Total", its amounts on the next line.
_NAD_GRAND_RE = re.compile(r"^Total\n-?[\d,]+\.\d+(?:\s+-?[\d,]+\.\d+){6}$", re.M)
# A holding's first line: "<description> CUSIP : <id>".
_NAD_ID_RE = re.compile(r"\s+(?:CUSIP|SEDOL|ISIN)\s*:\s*(\S+)$")
_NAD_NUM_RE = re.compile(r"^-?\d[\d,]*(?:\.\d+)?$")
# A sub-group heading "<region> - <currency>".
_NAD_REGION_RE = re.compile(r"\s-\s[A-Z]{3}$")
# Footnote marks, valuation notes, bond terms and the page footer.
_NAD_SKIP_RE = re.compile(r"^\*|^(?:Issue\s+Date|Rate|Yield\s+to\s+Maturity|Maturity\s+Date|Call\s+Date)\s*:|"
                          r"^Page\s+\d+\s+of\s+\d+$")
# Category (and sub-category) heading -> asset_type, first match wins; the
# private-fund categories come before the row's fund-vehicle prefix.
_NAD_ALT_TYPES = [
    (r"\bhedge\b", "Hedge Fund"),
    (r"partnership|venture\s+capital|private\s+equity", "Partnership Interest"),
    (r"real\s+estate", "Real Estate"),
]
# Northern Trust's fund-vehicle prefixes: "MFO" mutual fund, "CF" commingled fund.
_NAD_ROW_TYPES = [(r"^MFO\s", "Mutual Fund"), (r"^CF\s", "Common/Collective Trust Fund")]
_NAD_ASSET_TYPES = [
    (r"^cash\s+and\s+cash\s+equivalents\b.*(?:short\s+term|bills)", "Short-Term Investment Fund"),
    (r"futures|options|swap|derivative", "Derivative"),
    (r"\betf\b", "ETF"),
    (r"^funds\s+-", ""),
    (r"^equities\b", "Common Stock"),
    (r"^fixed\s+income\b", "Bond"),
]
_TA_TITLE_RE = re.compile(r"^Limited\s+Partnerships?\s+and\s+Other\s+Private\s+Equity$", re.M | re.I)
_TA_COLUMNS_RE = re.compile(r"^\(a\)\s+\(b\)\s+\(c\)\s+\(d\)\s+\(e\)$", re.M)
_TA_TOTAL_RE = re.compile(r"^Total\s+Limited\s+Partnerships?\s+and\s+Other\s+Private\s+Equity\b", re.M | re.I)
# An amount whose first digit pdfplumber may split off ("1 18,018,543", "4 ,755,905").
_TA_AMT = r"-|\(?\d\s?[\d,]*\)?"
_TA_ROW_RE = re.compile(r"^(.+?)\s+(Equity|Limited\s+Partnership|Other\s+Private\s+Equity)\s+"
                        rf"(?:\$\s*)?({_TA_AMT})\s+(?:\$\s*)?({_TA_AMT})$")
_TA_TOTAL_LINE_RE = re.compile(rf"^Total\b.*?\s*(?:\$\s*)?({_TA_AMT})\s+(?:\$\s*)?({_TA_AMT})$")
_TA_GROUPED_RE = re.compile(r"^\(?\d{1,3}(,\d{3})*\)?$|^-$")
# Column header, the first line of every page's body.
_TD_COLUMNS_RE = re.compile(r"^\(a\)\s+Identity\s+Description\s+Current\s+Cost\s+Current\s+Value$", re.M)
# The parties-in-interest note that follows the last row.
_TD_END_RE = re.compile(r"^\(a\)\s+\W\s+There\s+are\s+no\s+parties\s+in\s+interest\b", re.M)
# The filer's own type column -> asset_type; "Commingled Investment" is typed by name.
_TD_TYPES = {
    "Limited Partnership": "Partnership Interest",
    "Private Equity": "Partnership Interest",
    "Commingled Investment": None,
    "Money Market Mutual Funds": "Short-Term Investment Fund",
    "Treasury Bills": "Bond",
    "Currency Forward": "Derivative",
    "Equity": "Common Stock",
}
_TD_ROW_RE = re.compile(r"^(.+?)\s+(" + "|".join(sorted(map(re.escape, _TD_TYPES), key=len, reverse=True))
                        + r")\s+(-|\(?\d[\d,]*\)?)\s+(-|\(?\d[\d,]*\)?)$")
# Commingled index funds; every other commingled vehicle in this list is an offshore hedge fund.
_TD_INDEX_CIT_RE = re.compile(r"\bSSGA\b|\bState\s+Street\b|\bBlackRock\b.*\bIndex\b|\bNL\s+SF\b", re.I)
# Column header, the last line of every page's title block.
# Also Western States Carpenters' two-line header, whose second line is this.
_TMR_COLUMNS_RE = re.compile(r"^\(a\)\s+Identity\s+of\s+issuer,\s+borrower,\s+lessor\s+or\s+similar\s+party\s+"
                             r"Description\s+Maturity\s+Date\s+Rate\s+of\s+Interest\b.*\bCurrent\s+Value$"
                             r"|^\(a\)\s+lessor\s+or\s+similar\s+party\s+Description\s+Collateral\s+Date\s+Interest\s+"
                             r"or\s+Shares\s+\(d\)\s+Cost\s+Value$"
                             # Citigroup's two-line header (2026-10-16).
                             r"|^Security\s+description\s+amount/units\s+percentage\s+date\s+Cost\s+value$"
                             # 3M's header, split over several lines (2026-10-16).
                             r"|^Description\s+of\s+Investment$"
                             # FirstEnergy's (2026-10-16).
                             r"|^Interest\s+Identity\s+of\s+issue,\s+borrower,\s+lessor\s+or\s+similar\s+party\s+"
                             r"par\s+or\s+maturity\s+value\s+Cost\s+Value$"
                             # Teamsters Philadelphia's, the last of a five-line header (2026-10-17).
                             r"|^Type\s+Principal\s+Rate\s+Date$"
                             # National Retirement Fund's Legacy Plan (2026-10-17).
                             r"|^Lessor\s+or\s+Similar\s+Party\s+Amount\s+Shares\s+Rate\s+Date\s+Cost\s+Value$"
                             # Elliott's two plans (2026-10-16).
                             r"|^Lessor\s+or\s+Similar\s+Party\s+Collateral,\s+Par\s+or\s+Maturity\s+Value\s+Cost\s+Value$"
                             # Lone Pine's and H-E-B's value-only schedules (2026-10-16).
                             r"|^Identity\s+of\s+Issuer,\s+Borrower,\s+Lessor\s+or\s+Similar\s+Party\s+Fair\s+Value$"
                             r"|^Lessor,\s+or\s+Similar\s+Party\s+Investment/Number\s+of\s+Shares\s+Value$"
                             # Les Schwab's (2026-10-16).
                             r"|^Party\s+of\s+Interest,\s+Collateral,\s+Par\s+or\s+Maturity\s+Value$"
                             # Carpenters Annuity Trust Fund for Northern California's (2026-10-16).
                             r"|^Lessor,\s+or\s+Similar\s+Party\s+par\s+or\s+maturity\s+value\s+Cost\s+Value$"
                             # Directors Guild of America - Producer Pension Plan's (2026-10-16);
                             # its Basic Benefit Plan's prints "(a (b)", "paty" and a par column (2026-10-17).
                             r"|^\(a\)?\s+\(b\)\s+Identity\s+of\s+issuer,\s+borrower,\s+or\s+similar\s+par?ty\s+"
                             r"Description\s+Maturity\s+Date\s+(?:Par/Maturity\s+Value\s+)?\(d\)\s+Cost\s+\(\$\)$"
                             # United Association National Pension Fund's (2026-10-16).
                             r"|^\(a\)\s+lessor\s+or\s+similar\s+party\s+Description\s+date\s+interest\s+value\s+or\s+"
                             r"shares\s+Cost\s+Current\s+value$"
                             # International Painters and Allied Trades Industry Pension Plan's,
                             # the last of five header lines (2026-10-16).
                             r"|^Type\s+Principal\s+Rate\s+Date$"
                             # Screen Actors Guild - Producers Pension Plan's (2026-10-16).
                             r"|^\(a\)\s+or\s+Similar\s+Party\s+Description\s+Interest\s+Date\s+Value\s+\(d\)\s+Cost\s+Value$"
                             # Carpenters' Pension Trust Fund of St. Louis's (2026-10-16).
                             r"|^\(a\)\s+\(b\)\s+and\s+\(c\)\s+\(d\)\s+\(e\)$"
                             # Co-Op Retirement Plan's, the last of six header lines (2026-10-16).
                             r"|^lessor,\s+or\s+similar\s+party\s+maturity\s+value\s+Cost\s+value$"
                             # Pentegra Defined Benefit Plan's, the second of two header lines (2026-10-16).
                             r"|^ISIN/IDENTIFIER\s+Rate\s+Maturity\s+Par\s+Value$"
                             # Laborers' District Council and Contractors' Pension Fund of Ohio's (2026-10-16).
                             r"|^Identity\s+of\s+issue,\s+borrower,\s+lessor,\s+or\s+similar\s+party\s+rate\s+of\s+"
                             r"interest,\s+collateral,\s+par\s+or\s+maturity\s+date\s+Cost\s+Current\s+value$"
                             # DTE Energy Company Affiliates Employee Benefit Plans Master Trust's (2026-10-16).
                             r"|^Identity\s+of\s+Issuer\s+Description\s+of\s+Investment\s+Cost\s+Current\s+Value$"
                             # Exelon Corp Pension Master Trust - Private Equity Pool 2016's (2026-10-17).
                             r"|^\(a\)\s+or\s+Similar\s+Party\s+Par\s+or\s+Maturity\s+Value\s+Cost\s+Value$"
                             # Consolidated Edison Retirement Plan's (2026-10-17).
                             r"|^Investment\s+Name\s+Description\s+of\s+Investment\s+Rate\s+Date\s+Cost\s+of\s+Asset\s+"
                             r"Current\s+Value$"
                             # Building Service 32BJ Pension Fund's (2026-10-17).
                             r"|^\(a\)\s+or\s+Similar\s+Party\s+Description\s+Collateral\s+Interest\s+Date\s+Value\s+"
                             r"\(d\)\s+Cost\s+Value$"
                             # Abbott - AbbVie Multiple Employer Pension Plan's (2026-10-17).
                             r"|^\(a\)\s+Identity\s+of\s+issuer\s+and\s+description\s+of\s+asset\s+par\s+value\s+"
                             r"Cost\s+value$"
                             # The Hartford Retirement Plan for U.S. Employees's (2026-10-17).
                             r"|^(?:LESSOR,\s+)?OR\s+SIMILAR\s+PARTY\s+MATURITY\s+VALUE\s+\(C\)\s+COST\s+VALUE$"
                             # Carpenters Pension Fund of Illinois's (2026-10-17).
                             r"|^lessor\s+or\s+similar\s+party\s+Cost\s+Fair\s+Value$"
                             # USAA Pension Plan's (2026-10-17).
                             r"|^of\s+investment,\s+including\s+maturity\s+date\s+and\s+rate\s+of\s+interest\s+Cost\s+"
                             r"Current\s+value$"
                             # Intermountain Healthcare Pension Plan's (2026-10-17).
                             r"|^Description/issuer/terms\s+Cost\s+value$"
                             # LIUNA National (Industrial) Pension Fund's (2026-10-17).
                             r"|^\(a\)\s+lessor,\s+or\s+similar\s+party\s+Collateral\s+Date\s+Rate\s+Par\s+Value\s+"
                             r"\(d\)\s+Cost\s+Value$"
                             # M&T Bank Corporation Pension Plan's, the last of three header lines (2026-10-17).
                             r"|^Description\s+Value\s+Cost\s+Current\s+Value$"
                             # Factory Mutual Insurance Company Pension Plan's (2026-10-17).
                             r"|^\(a\)\s+Similar\s+Party\s+Rate\s+of\s+Interest,\s+Collateral,\s+Par,\s+or\s+Maturity\s+"
                             r"Value\s+Cost\s+Value$"
                             # International Brotherhood of Teamsters Union Local No. 710 Pension Fund's (2026-10-17).
                             r"|^\(a\)\s+\(b\)\s+Identity\s+of\s+issuer,\s+borrower,\s+lessor\s+or\s+similar\s+party\s+"
                             r"Description\s+Date\s+Rate\s+Maturity\s+Value\s+Cost\s+Value$"
                             # The Employees' Retirement Plan of Hajoca Corporation's (2026-10-17).
                             r"|^lessor,\s+or\s+similar\s+party\s+Cost\s+value$"
                             # The New York Hotel Trades Council and Hotel Association of New York City,
                             # Inc. Pension Fund's (2026-10-17).
                             r"|^Similar\s+Party\s+Par\s+or\s+Maturity\s+Value\s+Cost\s+Value$",
                             re.M)
_TMR_SUBTOTAL_RE = re.compile(r"^(?:Sub)?total\s+(?:-\s+)?(.+)$", re.I)
_TMR_GRAND_RE = re.compile(r"^Total\s+assets\s+\(held\s+at\s+end\s+of\s+year\)"
                           r"|^Total\s+Investments,\s+at\s+Fair\s+Value\b|^Grand\s+Total\b"
                           # Elliott; Les Schwab; H-E-B (2026-10-16).
                           r"|^Total\s+assets\s+held\s+\(?at\s+end\s+of\s+year\b|^Total\s+Investments\s+\$"
                           r"|^Total\s+\$\s*[\d,]+$"
                           # Carpenters NorCal: an unlabeled "$ <cost> $ <value>" (2026-10-16).
                           r"|^\$\s*\d[\d ,]*\s+\$\s*\d[\d ,]*$"
                           # Screen Actors Guild: "TOTAL $ <cost> $ <value>" (2026-10-16).
                           r"|^Total\s+\$\s*[\d,]+\s+\$\s*[\d,]+$"
                           # DTE: "Total: <cost> <value>" (2026-10-16).
                           r"|^Total:\s+[\d,]+\s+[\d,]+$"
                           # Exelon's private equity pool: "TOTAL INVESTMENTS <cost> <value>",
                           # upper case only; the page finder reads its digits split (2026-10-17).
                           r"|(?-i:^TOTAL\s+INVESTMENTS\s+\d[\d ,]*$)"
                           # The Hartford's pension account, its 401(h) account after (2026-10-17).
                           r"|^TOTAL\s+PENSION\s+INVESTMENTS\s"
                           # Factory Mutual (2026-10-17).
                           r"|^Total\s+cash\s+and\s+investments\s", re.I | re.M)
# Citigroup's umbrella heading over every section, and its "(Continued)" page
# footers; 3M's "Schedule 1" and FirstEnergy's "Page 17" page footers; the
# last line of Directors Guild's header, below its column header; IUPAT's
# "- 51 -" page footers; Screen Actors Guild's auditor's-report footer, and
# Carpenters St. Louis's with a page number.
_TMR_SKIP_RE = re.compile(r"^Investments,\s+at\s+fair\s+value:$|^\d+\s+\(Continued\)$|^Schedule\s+\d+$|^Page\s+\d+$"
                          r"|^Rate\s+\(%\)\s+(?:Value\s+)?\(\$\)$|^-\s*\d+\s*-$"
                          r"|^See\s+Independent\s+Auditors?'?\s+Report\.?(?:\s+Page\s+\d+)?$"
                          # DTE's page footer (2026-10-16).
                          r"|^Information\s+Classification:\s+Limited\s+Access(?:\s+Page\s+\d+\s+of\s+\d+)?$"
                          # Building Service 32BJ's two-line footer and every page's title block (2026-10-17).
                          r"|^See\s+Independent\s+Auditor.s\s+Report\s+on\s+Supplementary\s+Information\b"
                          r"|^and\s+Disclosure\s+under\s+the\s+Employee\s+Retirement\s+Income\s+Security\s+Act\b"
                          r"|^BUILDING\s+SERVICE\s+32BJ\s+PENSION\s+FUND$|^EIN:\s+\d\d-\d{7}\s+PLAN\s+NUMBER:\s+\d{3}$"
                          r"|^FORM\s+5500,\s+SCHEDULE\s+H,\s+LINE\s+4I\s+.\s+SCHEDULE\s+OF\s+ASSETS\b"
                          r"|^AS\s+OF\s+JUNE\s+30,\s+\d{4}$"
                          # The Hartford's column-header tail (2026-10-17).
                          r"|(?-i:^PENSION\s+INVESTMENTS$|^INTEREST$|^(?:MATURITY\s+RATE\s+)?SHARES$)"
                          # USAA's umbrella over its 401(h) account's sections, and its two
                          # accounts' totals (2026-10-17).
                          r"|^Investments\s+held\s+in\s+401\(h\)\s+account(?::|\s+\$\s*[\d,]+\s+\$\s*[\d,]+)$"
                          r"|^Investments,\s+excluding\s+401\(h\)\s+[\d,]+\s+[\d,]+$", re.I)
# Calibri glyph ids of printable ASCII. FirstEnergy embeds Calibri subsets with
# no ToUnicode map, so pdfplumber reads "0" as "(cid:1004)" (2026-10-16).
_CALIBRI_GLYPHS = dict(zip(
    [3, 4, 17, 18, 24, 28, 38, 39, 44, 47, 58, 60, 62, 68, 69, 75, 87, 89, 90, 94, 100, 104, 115, 116, 121, 122, 127,
     258, 271, 272, 282, 286, 296, 336, 346, 349, 361, 364, 367, 373, 374, 381, 393, 395, 396, 400, 410, 437, 448, 449,
     454, 455, 460, 493, 842, 845, 853, 854, 855, 856, 876, 878, 880, 882, 890, 894, 895, 896, 897, 898, 899, 910, 916,
     917, 918, 919, 920, 923, 936, 951, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1081, 1085, 1089,
     1092, 1093],
    " ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz`!?,;:./|\\-_()[]{}*^~'\"&@$#0123456789%+=<>"))
_CID_RE = re.compile(r"^\(cid:(\d+)\)$")

# closing_totals (Laborers Pension Trust Fund for Northern California, 2026-10-16).
_CT_COLUMNS_RE = re.compile(r"^\(a\)\s+Lessor,\s+or\s+Similar\s+Party\s+Rate\s+of\s+Interest,.*"
                            r"\(d\)\s*Cost\s+\(e\)\s*Current\s+Value$"
                            # Laborers Annuity Plan for Northern California's (2026-10-16).
                            r"|^\(a\)\s+or\s+Similar\s+Party\s+Interest,\s+Collateral,.*\(d\)\s*Cost\s+Value$", re.M)
_CT_TOTAL_RE = re.compile(r"^Total\s+(.+)$", re.I)
_CT_GRAND_RE = re.compile(r"^Total\s+investments\s+\$?\s*[\d,]+", re.I | re.M)
_CT_FOOTER_RE = re.compile(r"^See\s+Independent\s+Auditor's\s+Report|^\d+$")
# Lines at or right of the identity column are rows or wrapped identities;
# headings sit in column (a).
_CT_ROW_X = 80.0
_CT_ASSET_TYPES = [
    (r"money\s+market", "Short-Term Investment Fund"),
    (r"government|bonds?\b|corporate\s+obligations", "Bond"),
    (r"real\s+estate", "Real Estate"),
    (r"mutual\s+funds?", "Mutual Fund"),
    (r"limited\s+partnerships?", "Partnership Interest"),
    (r"hedge\s+funds?", "Hedge Fund"),
    (r"103-12", "103-12 Investment Entity"),
    (r"common\s+collective", "Common/Collective Trust Fund"),
    (r"pooled\s+separate", "Separate Account"),
]
# A description naming the fund outright beats a bare sponsor identity
# ("GCM Grosvenor" / "Gcm Gros Infra Advantage Fund II LP").
_CT_FUND_RE = re.compile(r"\b(?:Fund|Fd|LP|L\.P\.|Partners|Ptnr|Prtnr|Trust|Portfolio)\b", re.I)

# investments_held_summary (Chicago & Vicinity Laborers' District Council Pension Plan, 2026-10-16).
_IHS_TITLE_RE = re.compile(r"^Investments\s+Held\s+Summary$", re.M)
_IHS_COLUMNS_RE = re.compile(r"^Balance\s+Adjustments\s+Value\s+Value$"
                             # Midwest Operating Engineers' "Fair Cost / Value Value" (2026-10-17).
                             r"|^Value\s+Value$")
# "U.S. Bank Page 196 - " points into the attached custodial statement.
_IHS_REF_RE = re.compile(r"^.*?\bPage\s+\d+\s*-\s*")
_IHS_ATTACHED_RE = re.compile(r"\s+See\s+Attached$", re.I)
# Section heading -> asset_type, first match wins.
_TMR_ASSET_TYPES = [
    (r"short\W+term|money\s+market|^interest\W+bearing\s+cash$", "Short-Term Investment Fund"),
    (r"hedge\s+funds?", "Hedge Fund"),
    (r"^mutual\s+funds?$", "Mutual Fund"),
    (r"forward|swaps?\b|options|futures|derivatives?\b", "Derivative"),
    # 3M's "Corporate debt instruments - preferred" is debt, not stock.
    (r"corporate\s+debt", "Bond"),
    (r"preferred", "Preferred Stock"),
    (r"equity|stocks?\b", "Common Stock"),
    (r"government|agency|corporate\s+debt|debt\s+securities|bonds?\b", "Bond"),
    (r"partnership|joint\s+venture", "Partnership Interest"),
    (r"real\s+estate", "Real Estate"),
    (r"common\W+collective", "Common/Collective Trust Fund"),
    (r"103-12", "103-12 Investment Entity"),
    (r"registered\s+investment", "Mutual Fund"),
    (r"insurance\s+contracts?", "Insurance General Account"),
    (r"commingled", "Commingled Fund"),
]
_IHS_ASSET_TYPES = _TMR_ASSET_TYPES + [(r"pooled\s+separate", "Separate Account")]

# colon_headings (Deseret Mutual Master Retirement Plan, 2026-10-16).
# Column header, atop every page; holdings start below it.
_CH_COLUMNS_RE = re.compile(r"^\(a\)\s+\(b\)\s+Identity\s+of\s+Issuer,\s+Borrower,\s+Lessor,\s+or\s+Similar\s+Party\s+"
                            r"\(c\)\s+Coupon\s+Rate\s+\(d\)\s+Maturity\s+Date\s+\(e\)\s+Par\s+or\s+Shares\s+"
                            r"\(f\)\s+Cost\s+\(g\)\s+Current\s+Value$", re.M)
# No grand total is printed; the last page says so.
_CH_END_RE = re.compile(r"^\(Concluded\)$", re.M)
_CH_FOOTER_RE = re.compile(r"^\((?:Continued|Concluded)\)$|^-\s*\d+\s*-$")
_CH_HEADING_RE = re.compile(r"^[A-Z0-9][A-Z0-9 .,&/'-]*:$")
_CH_RATE_RE = re.compile(r"^(?:\d*\.\d+|-)$")
_CH_MATURITY_RE = re.compile(r"^(?:\d{1,2}/\d{1,2}/\d{4}|-)$")
_CH_ASSET_TYPES = _IHS_ASSET_TYPES + [(r"^collective\s+trusts?\b", "Common/Collective Trust Fund")]
# described_categories (Producer-Writers Guild of America Pension Plan, 2026-10-16).
_DC_TITLE_RE = re.compile(r"^Schedule\s+H,?\s+Line\s+4i\s+(?:[-–]\s+)?Schedule\s+of\s+Assets\s+\(Held\s+at\s+End\s+of\s+Year\)$",
                          re.M)
# Last line of every page's column header; holdings start below it.
_DC_COLUMNS_RE = re.compile(r"^\(a\)\s+\(b\)\s+\(c\)\s+\(d\)\s+\(e\)$"
                            # American Federation of Musicians' (2026-10-16).
                            r"|^\(a\)\s+or\s+Similar\s+Party\s+Description\s+Collateral\s+Interest\s+Date\s+Value\s+"
                            r"\(d\)\s*Cost\s+Value$"
                            # AFTRA Retirement Fund's (2026-10-17).
                            r"|^\(a\)\s+or\s+Similar\s+Party\s+Description\s+Interest\s+Date\s+Value\s+\(d\)\s*Cost\s+Value$",
                            re.M)
_DC_GRAND_RE = re.compile(r"^\$\s*[\d,]+\s+\$\s*[\d,]+$", re.M)
_DC_FOOTER_RE = re.compile(r"^See\s+Independent\s+Auditor|^\*\s*Denote\s*s\s+Party-in-interest|^\d+$"
                           r"|^See\s+Report\s+on\s+Supplementary\s+Information|^Employee\s+Retirement\s+Income\s+Security"
                           r"|^\*\s*-\s*Indicates\s+a\s+party-in-interest$")
_DC_RATE_RE = re.compile(r"^(?:N/A|VAR|\d*\.\d+%)$")
_DC_MATURITY_RE = re.compile(r"^(?:N/A|\d{1,2}/\d{1,2}/\d{4})$")
# The Description column, run into the name.
_DC_TYPE_RE = re.compile(r"\s+(?:Common\s+Stock|Preferred\s+Stock|Bonds|STIF|Mutual\s+Fund|Cas\s?h\s+Collateral)$")
# nt_cusip_columns (IBM Personal Pension Plan, 2026-10-16).
_NCC_TITLE_RE = re.compile(r"^Schedule\s+H,\s+line\s+4i-Schedule\s+of\s+Assets\s+\(Held\s+At\s+End\s+of\s+Year\)$",
                           re.M | re.I)
# Last line of the first page's column header; holdings start below it.
_NCC_COLUMNS_RE = re.compile(r"^\(CUSIP\)$", re.M)
_NCC_GRAND_RE = re.compile(r"^\$\s*[\d, ]+\$\s*[\d, ]+\$\s*[\d, ]+$", re.M)
_NCC_FOOTER_RE = re.compile(r"^NTAC:|^#$")
_NCC_ID_RE = re.compile(r"^(?=[A-Z0-9]*\d)[A-Z0-9]{8,10}$")
# Row name -> asset_type ahead of _FCV_ASSET_TYPES (whose partnership and
# hedge fund rules are left out: alt_ids and type_rows decide those). Northern
# Trust prefixes "MFC"/"MFO" (registered funds) and "CF" (commingled funds).
_NCC_ASSET_TYPES = [
    (r"\bCDS\b|\bCDX\b|\bSWPC|\bSWAPS?\b|^FUT\s", "Derivative"),
    (r"INTEREST\s+BEARING\s+DEPOSIT|\bCOML\s+PAPER\b", "Short-Term Investment Fund"),
    (r"\bETF\b|\bBDETF\b", "ETF"),
    (r"\bSTRIPPED\b|\bZERO\s+CPN\b|T-?BILL|\bFLTG\b|\bPOOL\b|\bCMO\b|\bPVTPL\b|\bPROM(?:ISSORY)?\s*N(?:O)?TE?\b|"
     r"\d%|\b\d+(?:\.\d+)?\s+DUE\b|\bDUE\s+\d{2}-\d{2}-\d{4}|\d{2}-\d{2}-\d{4}\s+(?:REG|BEO)$", "Bond"),
    (r"^MF[CO]\s", "Mutual Fund"),
    (r"\bCOM\b|\bNPV\b|\bORD\b|\bADR\b|\bGDR\b|\bGENUSSCHEINE\b|\bCLASS\s?'|COMMON\s+STOCK|\bVOTING\s+SHS\b",
     "Common Stock"),
]
# Checked after the plan's type_rows.
_NCC_POOLED_TYPES = [(r"^CF\s|\bCIT\b|\bCTF\b", "Common/Collective Trust Fund")]
_NCC_FCV_TYPES = [(p, label) for p, label in _FCV_ASSET_TYPES if label not in ("Partnership Interest", "Hedge Fund")]
# leading_shares (Employees' Retirement Plan of Duke University, 2026-10-16).
# Column header (its last line), atop every page; holdings start below it.
_LS_COLUMNS_RE = re.compile(r"^shares\s+Identity\s+of\s+issue\s+or\s+borrower\s+Cost\s+value$", re.M | re.I)
_LS_GRAND_RE = re.compile(r"^Total\s+investments\s+\$?\s*\(?[\d,]+", re.M | re.I)
_LS_FOOTER_RE = re.compile(r"^\d+\s+\(Continued\)$|^\d+$")
_LS_TOTAL_RE = re.compile(r"^Total\s+(.+)$", re.I)
# issuer_shares_category (U.S. Bank Pension Plan master trust, 2026-10-16).
# Column header's last line, on the first page only.
_ISC_COLUMNS_RE = re.compile(r"^Par\s+Value\s+Rate\s+of\s+Interest,\s+Par,\s+or\s+Maturity\s+Value\s+Cost\s+"
                             r"Current\s+Value$", re.M)
# Raw text splits the value's first digit off ("7 ,835,469,577").
_ISC_GRAND_RE = re.compile(r"^Total\s+[\d,]+\s+[\d ,]+$", re.M)
# party_type_tree (National Automatic Sprinkler Industry Pension Fund, 2026-10-16).
_PTT_COLUMNS_RE = re.compile(r"^Interest\s+\(b\)\s+Identity\s+of\s+issuer\b.*\(e\)\s+Current\s+Value$", re.M)
_PTT_GRAND_RE = re.compile(r"^TOTALS\s+-\s+ASSETS\b.*?([\d,]+)$", re.M)
# Description-column values that end a row's title.
_PTT_DESCRIPTION_RE = re.compile(r"\s+(LP|Hedge\s+Fund|Fund|Equity|RE|STIF|Bonds|Fixed\s+Income)$")
_CC_COLUMNS_RE = re.compile(r"^Description\s+5500\s+Sup\s+Category\s+Name\s+Share/Par\s+Value\s+Cost\s+Value\s+"
                            r"Total\s+Value$", re.M)
_CC_GRAND_RE = re.compile(r"^Total\s+Master\s+Trust\s+Assets\b", re.M)
# The category that ends every row's text, ahead of its amounts, and its asset_type
# ("Other" rows are typed by description).
_CC_CATEGORY_RE = re.compile(
    r"^(.*?)\s+(Cash|U\.S\.\s+Government\s+Securities|Corporate\s+Debt\s+Instruments\s+-\s+(?:Preferred|Other)|"
    r"Corporate\s+Stock\s+-\s+(?:Preferred|Common)|"
    r"Value\s+of\s+Interest\s+in\s+(?:Common/Collective\s+Trusts|Registered\s+Investment\s+Companies)|"
    r"Partnership/Joint\s+Venture\s+Interests|Hedge\s+Funds|Other)$")
_CC_ASSET_TYPES = [(r"^Cash$", "Cash"), (r"Government|Debt", "Bond"), (r"Stock\s+-\s+Preferred", "Preferred Stock"),
                   (r"Stock\s+-\s+Common", "Common Stock"), (r"Collective", "Common/Collective Trust Fund"),
                   (r"Registered", "Mutual Fund"), (r"Partnership", "Partnership Interest"),
                   (r"Hedge", "Hedge Fund")]
_SA_COLUMNS_RE = re.compile(r"^Similar\s+Party\s+Description\s+of\s+Investment\s+Value\s+Value$", re.M)
_SA_DETAIL_COLUMNS_RE = re.compile(r"^PAR\s+VALUE\s+Description\s+Cost\s+Market\s+Value$", re.M)
_SA_ATTACHED_RE = re.compile(r"\bSee\s+Attached$", re.I)
# The attached detail ends on its total, printed as a bare "<cost> <value>" line.
_SA_CLOSING_RE = re.compile(r"^(?:\$\s+)?\d[\d,]*\s+(?:\$\s+)?\d[\d,]*$", re.M)
_SA_PAGE_NUMBER_RE = re.compile(r"^\(\d+\)$")
# sch_h_category (Liberty Mutual Retirement Benefit Plan, 2026-10-16).
# Last line of every page's column header; holdings start below it.
# year_end_package (SEIU Pension Plan Master Trust, 2026-10-17).
_YEP_COLUMNS_RE = re.compile(r"^DESCRIPTION\s+OF\s+INVESTMENT\s+PRICE\s+SHARES/PAR\s+VALUE\s+ADJUSTED\s+COST\s+"
                             r"HISTORICAL\s+COST\s+CURRENT\s+VALUE\s+ADJ\.\s+COST\s+HIST\.\s+COST$", re.M)
_YEP_GRAND_RE = re.compile(r"^TOTAL\s+ASSETS(?:\s+\S+){6}$", re.M)
_YEP_RULE_RE = re.compile(r"^[-=]+(?:\s+[-=]+)*$")
_YEP_FOOTER_RE = re.compile(r"^Page\s+\d+\s+of\s+\d+$")
# leading_category (UFCW International Union - Industry Pension Fund, 2026-10-17).
# bold_heading_totals (Marsh & McLennan Companies Retirement Plan, 2026-10-17).
_BHT_COLUMNS_RE = re.compile(r"^Cost\s+Current\s+Value$", re.M)
_BHT_TOTAL_RE = re.compile(r"^Total\s+(.+)$")   # not "TOTAL CAP INTL" (a bond)
_BHT_GRAND_RE = re.compile(r"(?m)^Total\s+Investments\s+\$?\s*\d")
_BHT_LESS_RE = re.compile(r"^Less:\s", re.I)
_BHT_PAGE_RE = re.compile(r"^\d+$")
# colon_heading_totals (Kaleida Health Pension Growth Plan, 2026-10-17).
_CHT_COLUMNS_RE = re.compile(r"^\(a\)\s+\(b\)\s+Description\s+of\s+Investment\s+\(c\)\s+Units\s+"
                             r"\(d\)\s+Cost\s+\(e\)\s+Current\s+Value$"
                             # US Steel's Northern Trust print (2026-10-17).
                             r"|^\(a\)\s+\(b\)\s+Identity\s+of\s+Issue,.*\(d\)\s+Cost\s+\(e\)\s+Current\s+Value$", re.M)
_CHT_TOTAL_RE = re.compile(r"^Total(?:\s+(.+))?$", re.I)
_CHT_GRAND_LABEL_RE = re.compile(r"^Grand\s+Total$", re.I)
_CHT_GRAND_RE = re.compile(r"(?m)^(?:Grand\s+Total\s+)?\$\s*[\d,]+\s+\$\s*[\d,]+$")
_CHT_WRAPPED_RE = re.compile(r"\b(?:CUSIP|SEDOL):$")
# A row's trailing "CUSIP: <id>" / "SEDOL: ^ <id>" (2026-10-20): moved to asset_identifier,
# since the staging writer drops every name containing CUSIP/SEDOL as a continuation-line
# artifact -- US Steel's first run lost all 3,221 such rows and kept its stale staging.
_CHT_ID_RE = re.compile(r"\s+(?:CUSIP|SEDOL|ISIN)\s*:\s*[*^]*\s*(\S+)$")
_CHT_FOOTER_RE = re.compile(r"^\d+$|^\*\s*The\s+above\s+named\s+is\s+a\s+party-in-interest$"
                            r"|^The\s+above\s+data\s+is\s+based\s+solely\b|^complete\s+and\s+accurate\s+by\b"
                            r"|^Information\s+certified\s+as\s+complete\b|^Northern\s+Trust\s+Company,\s+excluding\b", re.I)
# typed_column_totals (General Motors Hourly-Rate Employees Pension Plan, 2026-10-17).
_TCT_COLUMNS_RE = re.compile(r"^\(a\)\s+\(b\)\s+Identity\s+of\s+Issue,\s+Borrow,\s+or\s+Similar\s+Party\s+"
                             r"\(c\)\s+Type\s+of\s+Investment\s+\(d\)\s+Cost\s+\(e\)\s+Current\s+Value$", re.M)
_TCT_TOTAL_RE = re.compile(r"^Total\s+(.+)$", re.I)
_TCT_FOOTER_RE = re.compile(r"^Page\s+\d+\s+of\s+\d+$|^[@#*]\s+(?:Denotes\b|Securities\s+lending\s+collateral$)"
                            r"|^\[\d+\]\s+includes\s+rounding$|^\(in\s+thousands\)$|^AS\s+OF\s+[A-Z]+\s+\d", re.I)
_TCT_NOTE_RE = re.compile(r"\s+\[\d+\]$")
# cusip_column_totals (Graham Holdings Company Master Retirement Trust, 2026-10-17).
_CCT_TITLE_RE = re.compile(r"^FORM\s+5500,\s+SCHEDULE\s+H,\s+LINE\s+4i:?$", re.I | re.M)
_CCT_COLUMNS_RE = re.compile(r"^Shares\s+Cost\s+Current\s+Value\s+Cusip\s*#$", re.I | re.M)
_CCT_GRAND_RE = re.compile(r"(?m)^TOTAL\s+INVESTMENTS\s+\$?\s*[\d,]+\s+\$?\s*[\d,]+$", re.I)
_CCT_TOTAL_RE = re.compile(r"^TOTAL\s+(.+)$", re.I)
_CCT_CUSIP_RE = re.compile(r"^(?=[A-Z0-9]*\d)[A-Z0-9]{8,9}$")
# cusip_type_blocks (Cigna Pension Plans Trust, 2026-10-17).
_CTB_COLUMNS_RE = re.compile(r"^of\s+Party\s+Cusip#\s+Description\s+Cost\s+Value$", re.M)
_CTB_TOTAL_RE = re.compile(r"^(?:\S{1,2}\s+)?total\b", re.I)
_CTB_GRAND_RE = re.compile(r"(?m)^TOTAL\s+ASSETS\s+HELD\s+FOR\s+INVESTMENT\s+PURPOSES\b")
_CTB_ROW_TYPES = [
    (r"SEC\s+PARTNERSHIPS", "Partnership Interest"),
    (r"REAL\s+ESTATE\s+EQU", "Real Estate"),
    (r"Hedge\s+Funds?", "Hedge Fund"),
    (r"MORTGAGE", "Bond"),
    (r"FIXED\s+INCOME\s+-\s+(?:PUBLIC|PRIVATE)", "Bond"),
    (r"STRUCTURED\s+-\s+(?:PUBLIC|PRIVATE)", "Bond"),
    (r"MONEY\s+MARKET", "Money Market Fund"),
    (r"COMMON\s+STOCK", "Common Stock"),
    (r"PREFERRED\s+STOCK", "Preferred Stock"),
]
_CTB_TYPE_RE = re.compile(r"(?:\s+|(?<=[0-9A-Z#]))(" + "|".join(p for p, _ in _CTB_ROW_TYPES) + r")$")
_CTB_BLOCK_TYPES = [
    (r"money\s+market", "Money Market Fund"),
    (r"mortgage|fixed\s+mat", "Bond"),
    (r"hedge", "Hedge Fund"),
    (r"partnership|private\s+equity|mezzanine", "Partnership Interest"),
    (r"real\s+estate", "Real Estate"),
    (r"equity", "Common Stock"),
    (r"guaranteed\s+deposit", "Insurance General Account"),
]
# fmv_cost_summary (Ohio Carpenters' Pension Plan, 2026-10-17).
_FCS_COLUMNS_RE = re.compile(r"^@\s*FMV\s+@\s*COST$", re.M)
_FCS_ATTACHED_RE = re.compile(r"\s+\(ATTACHED\)(\s+\S+)\s+ATTACHED$")
_FCS_ASSET_TYPES = [
    (r"short\s+term", "Short-Term Investment Fund"),
    (r"government|corporate\s+obligations", "Bond"),
    (r"corporate\s+stocks?", "Common Stock"),
    (r"collective\s+investment", "Common/Collective Trust Fund"),
    (r"partnership", "Partnership Interest"),
    (r"registered\s+investment", "Mutual Fund"),
]
_LC_TITLE_RE = re.compile(r"^SCHEDULE\s+H\s+ITEM\s+4I\s+-\s+SCHEDULE\s+OF\s+ASSETS\s+HELD\b", re.M)
_LC_PAGE_RE = re.compile(r"^Page\s+(\d+)\s+of\s+(\d+)$", re.M)
# The last title-block line: a detail block's column header, a value-only
# block's, or the summary's.
_LC_HEADER_END_RE = re.compile(r"^(?:CATEGORY\s+CUSIP\s|SECURITY\s+DESCRIPTION\s|DES\s?CRIPTION\s+MARKET\s+VALUE$"
                               r"|Cash\s+Securities\s+Preferred\s)")
# (category, asset_type); longest first so "... - ALL OTHER" beats "OTHER".
_LC_CATEGORIES = [
    ("CORP. DEBT INSTR. - ALL OTHER", "Bond"), ("CORP. DEBT INSTR. - PREFERRED", "Bond"),
    ("PARTNERSHIPS / JOINT VENTURE INTERESTS", "Partnership Interest"),
    ("PARTNERSHIPS / JOINT VENTUIRE INTERESTS", "Partnership Interest"),
    ("PARTN./JOINT VENTURE INTERESTS", "Partnership Interest"),
    ("REGISTERED INVESTMENT COMPANIES", "Mutual Fund"), ("REGISTERED INVESTMENT COMPANY", "Mutual Fund"),
    ("CORPORATE STOCKS - PREFERRED", "Preferred Stock"), ("CORPORATE STOCKS - COMMON", "Common Stock"),
    ("COMMON/COLLECTIVE TRUSTS", "Common/Collective Trust Fund"),
    ("COMMON/COLLECTIVE", "Common/Collective Trust Fund"),
    ("U.S. GOVERNMENT SECURITIES", "Bond"), ("POOLED SEPARATE ACCOUNTS", "Separate Account"),
    ("INTEREST BEARING CASH", "Short-Term Investment Fund"), ("103-12 INVESTMENTS", "103-12 Investment Entity"),
    ("EMPLOYER SECURITIES", ""), ("REAL ESTATE", "Real Estate"), ("OTHER", ""),
]


def _lc_loose(text: str) -> str:
    """text as a pattern matching it with spaces anywhere (or none)."""
    return r"\s*".join(re.escape(c) for c in text.replace(" ", ""))


_LC_CATEGORY_RE = re.compile(r"^(" + "|".join(f"(?P<c{i}>{_lc_loose(c)})" for i, (c, _) in enumerate(_LC_CATEGORIES))
                             + r")(?![A-Z])\s*(.*)$")
_LC_TOTAL_RE = re.compile(r"^" + _lc_loose("TOTAL") + r"(?![A-Z])\s*(.*)$", re.I)
_LC_BLOCK_TOTAL_RE = re.compile(r"^" + _lc_loose("TOTAL INVESTMENTS") + r"\s", re.I)

# titled_category_pages (Allstate Retirement Plan, 2026-10-17).
_TCP_HELD_RE = re.compile(r"^Form\s+5500,\s+Schedule\s+H,\s+Line\s+4i\s+-\s+Schedule\s+of\s+Assets\s+"
                          r"\(Held\s+at\s+End\s+of\s+Year\)", re.M)
_TCP_CATEGORY_RE = re.compile(r"^([A-Z][A-Z&/,\- ]+?)\s+AS\s+OF\s+[A-Z]+\s+\d{1,2},\s+\d{4}$")
_TCP_COLUMNS_END_RE = re.compile(r"\sCOST\s+FAIR\s+VALUE$")
# The page number, and the "* Permitted party in interest" legend.
_TCP_FOOTER_RE = re.compile(r"^-\s+\d+\s+-$|^\*\s+Permitted\s+party\s+in\s+interest$", re.I)
_SHC_COLUMNS_RE = re.compile(r"^\(a\)\s*maturity\s+value\s+interest,\s+collateral,\s+par,\s+or\s+maturity\s+date\s+"
                             r"-\s+continued\s+\(d\)\s+Cost\s+\(e\)\s+Current\s+Value$"
                             # UPMC Master Trust's (2026-10-17).
                             r"|^Identity\s+of\s+Investment\s+Description\s+Rate\s+information\s+Maturity\s+Date\s+"
                             r"Cost\s+Current\s+Value$"
                             # Paul Hastings LLP DB Retirement Plan for Partners' last header line (2026-10-17).
                             r"|^par,\s+or\s+maturity\s+value$"
                             # Farmers Group, Inc. Employees' Pension Plan's (2026-10-17).
                             r"|^\(a\)\s+Party\s+Description\s+of\s+Investment\s+Interest\s+Maturity\s+"
                             r"Value/Shares\s+Cost\s+Current\s+Value$", re.M)
# UPMC's grand total is its value alone, on a line of its own; Paul Hastings'
# prints cents and no "$".
_SHC_GRAND_RE = re.compile(r"^Total\s+\$\s*\d[\d,]*\s+\$?\s*\d[\d,]*$|^\d{1,3}(?:,\d{3}){2,}$"
                           r"|^Total\s+\d[\d,]*\.\d\d\s+\d[\d,]*\.\d\d$", re.M)
# category_rate_totals (MassMutual Pension Plan, 2026-10-17). Last line of the
# first page's column header; holdings start below it.
_CRT_COLUMNS_RE = re.compile(r"^\(a\)\s+lessor\s+or\s+similar\s+party\s+or\s+maturity\s+value\s+"
                             r"\(d\)\s+Cost\s+\(e\)\s+Current\s+Value$", re.M)
_CRT_GRAND_RE = re.compile(r"^TOTAL\s+INVESTMENTS\s+\S+\s+\S+$", re.M)
_CRT_FOOTER_RE = re.compile(r"^\*\s*DENOTES\s+PARTY-IN-INTEREST$|^See\s+accompanying\s+independent\s+auditors'\s+report$",
                            re.I)
_SHC_FOOTER_RE = re.compile(r"^\*\s*Denoted\s+party\s+in\s+interest\b", re.I)
# The Schedule H category that ends every row's text, ahead of its cost and
# value, and its asset_type ("OTHER INVESTMENTS" mixes swaps, sovereigns and munis).
_SHC_CATEGORIES = {
    "PARTNERSHIP/JOINT VENTURE INTEREST": "Partnership Interest",
    "CORPORATE DEBT INSTRUMENTS": "Bond",
    "U. S. GOVERNMENT SECURITIES": "Bond",
    "CORPORATE STOCK - COMMON": "Common Stock",
    "CORPORATE STOCK - PREFERRED": "Preferred Stock",
    "COMMON/COLLECTIVE TRUST": "Common/Collective Trust Fund",
    "REGISTERED INVESTMENT COMPANIES": "Mutual Fund",
    "INTEREST-BEARING CASH": "Short-Term Investment Fund",
    "OTHER INVESTMENTS": "",
}
_SHC_AMOUNT = r"(-|\(?-?\d[\d,]*\)?)"
_SHC_ROW_RE = re.compile(r"^(.+?)\s+(" + "|".join(sorted(map(re.escape, _SHC_CATEGORIES), key=len, reverse=True))
                         + r")\s+" + _SHC_AMOUNT + r"\s+" + _SHC_AMOUNT + "$")
_SHC_CENTS = r"(-|\(?-?\d[\d,]*(?:\.\d+)?\)?)"
_SHC_BARE_ROW_RE = re.compile(r"^(.+?)\s+" + _SHC_AMOUNT + r"\s+" + _SHC_AMOUNT + "$")
_ISC_GLUED_RE = re.compile(r"^(.*[A-Za-z)])(\d{1,3}(?:,\d{3})+)$")
# Checked ahead of _HEADING_ASSET_TYPES.
_ISC_ASSET_TYPES = [(r"collective\s+investment", "Common/Collective Trust Fund")]
_PUA_RE = re.compile("[-]")
_BOILERPLATE_RE = re.compile(
    r"^(EIN\s|Schedule\s+H,|December\s+\d|Face\s+Amount|Asset\s+Interest|Identifier\s+Security)", re.I)
_NUM_RE = re.compile(r"^\(?-?\$?\d[\d,]*(\.\d+)?\)?$|^[-—–]$")
_ID_RE = re.compile(r"^(?=[A-Z0-9]*\d)[A-Z0-9]{7,10}$")
_DATE_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}$")
_RATE_RE = re.compile(r"^\d{1,2}\.\d+%?$|^\d{1,2}%$")
_HEADING_RE = re.compile(r"^[A-Za-z][A-Za-z0-9 ,&/()'.\-]*:$")

# Section heading -> asset_type, first match wins. Labels must be in
# llm_row_extract._KNOWN_ASSET_TYPES so downstream vocab checks agree.
_HEADING_ASSET_TYPES = [
    (r"mutual\s+fund|registered\s+investment", "Mutual Fund"),
    (r"collective\s+trust|common/collective", "Common/Collective Trust Fund"),
    (r"absolute\s+return|hedge\s+fund", "Hedge Fund"),
    (r"commingled", "Commingled Fund"),
    (r"private\s+invest|private\s+equity|partnership", "Partnership Interest"),
    (r"insurance\s+contract", "Insurance General Account"),
    (r"real\s+estate", "Real Estate"),
    (r"preferred\s+stock", "Preferred Stock"),
    (r"common\s+stock|corporate\s+stock|equit", "Common Stock"),
    (r"bond|government|fixed\s+income|debt", "Bond"),
    (r"money\s+market", "Money Market Fund"),
    (r"short[\s-]term", "Short-Term Investment Fund"),
    (r"^cash", "Cash"),
]

# Applied to the row title when its heading maps to nothing (e.g. "Other:").
_TITLE_ASSET_TYPES = [
    (r"\bcash\b", "Cash"),
]

# Rows sitting above the first heading on the first requested page belong to
# a section opened on an earlier page; look back this far for its heading.
_LOOKBACK_PAGES = 15


def _num(tok: str) -> Optional[float]:
    if tok in ("-", "—", "–"):
        return 0.0
    neg = tok.startswith("(") or tok.startswith("-")
    digits = tok.strip("()-$").replace(",", "")
    try:
        val = float(digits)
    except ValueError:
        return None
    return -val if neg else val


def _fmt(val: Optional[float]) -> str:
    if val is None:
        return ""
    return str(int(val)) if val == int(val) else str(val)


def heading_asset_type(heading: str) -> str:
    for pattern, label in _HEADING_ASSET_TYPES:
        if re.search(pattern, heading, re.I):
            return label
    return ""


def _is_page_footer(tokens: List[str]) -> bool:
    return len(tokens) == 1 and tokens[0].isdigit()


def _heading_of(line: str) -> Optional[str]:
    return line[:-1].strip() if _HEADING_RE.match(line) else None


def _classify_line(line: str) -> tuple:
    """Return (kind, payload) for one text line.

    kind is one of: skip, heading, subtotal, row, unparsed.
    """
    line = line.strip()
    if not line or _BOILERPLATE_RE.match(line):
        return "skip", None
    heading = _heading_of(line)
    if heading:
        return "heading", heading
    tokens = line.split()
    if _is_page_footer(tokens):
        return "skip", None
    if all(_NUM_RE.match(t) for t in tokens):
        return "subtotal", [_num(t) for t in tokens]

    trailing = []
    while tokens and _NUM_RE.match(tokens[-1]) and len(trailing) < 3:
        trailing.insert(0, tokens.pop())
    if not trailing or not tokens:
        return "unparsed", line

    identifier = tokens.pop(0) if _ID_RE.match(tokens[0]) else ""
    maturity = tokens.pop() if tokens and _DATE_RE.match(tokens[-1]) else ""
    rate = tokens.pop() if maturity and tokens and _RATE_RE.match(tokens[-1]) else ""
    title = " ".join(tokens)
    if not title:
        return "unparsed", line

    nums = [_num(t) for t in trailing]
    shares, cost, value = ([None] * (3 - len(nums)) + nums)
    return "row", {
        "identifier": identifier, "title": title, "rate": rate, "maturity": maturity,
        "shares": shares, "cost": cost, "value": value,
    }


def _ipr_classify_line(line: str, heading: str) -> tuple:
    """_classify_line for the issue_par_rate layout (title, par, rate, maturity, cost, value)."""
    line = line.strip()
    if not line:
        return "skip", None
    tokens = line.split()
    if heading and line.upper().startswith(f"TOTAL {heading.upper()} "):
        rest = line[len(heading) + 7:].split()
        if rest and all(_NUM_RE.match(t) for t in rest):
            return "subtotal", [_num(t) for t in rest]
    if _IPR_HEADING_RE.match(line):
        return "heading", line
    if _is_page_footer(tokens):
        return "skip", None

    trailing = []
    while tokens and _NUM_RE.match(tokens[-1]) and len(trailing) < 2:
        trailing.insert(0, tokens.pop())
    maturity = tokens.pop() if len(trailing) == 2 and tokens and _DATE_RE.match(tokens[-1]) else ""
    rate = tokens.pop() if maturity and tokens and _RATE_RE.match(tokens[-1]) else ""
    # Par value (bonds) or shares/units (no maturity) sits left of cost.
    shares = _num(tokens.pop()) if len(trailing) == 2 and tokens and _NUM_RE.match(tokens[-1]) else None
    title = " ".join(tokens)
    if not trailing or not re.search(r"[A-Za-z]", title):
        return "unparsed", line

    cost, value = ([None] * (2 - len(trailing)) + [_num(t) for t in trailing])
    return "row", {
        "identifier": "", "title": title, "rate": rate, "maturity": maturity,
        "shares": shares, "cost": cost, "value": value,
    }


def _drop_overlapping_spaces(page):
    """Remove space glyphs drawn on top of a real character (right-aligned
    padding), which otherwise split one number into two tokens."""
    rows: Dict[int, list] = {}
    for c in page.chars:
        if c["text"] != " ":
            rows.setdefault(round(c["top"]), []).append(c)

    def overlaps(sp) -> bool:
        top = round(sp["top"])
        return any(min(sp["x1"], c["x1"]) - max(sp["x0"], c["x0"]) > 0.5
                   for k in (top - 1, top, top + 1) for c in rows.get(k, ()))

    bad = {id(c) for c in page.chars if c["text"] == " " and overlaps(c)}
    return page.filter(lambda obj: id(obj) not in bad) if bad else page


def _decode_calibri_cids(page):
    """Rewrite pdfplumber's "(cid:N)" chars as the Calibri glyph they draw, in
    place; a no-op on a page without them. Glyph 1 is .notdef."""
    for c in page.chars:
        m = _CID_RE.match(c["text"])
        if m:
            gid = int(m.group(1))
            c["text"] = "”" if gid == 863 else _CALIBRI_GLYPHS.get(gid, "")
    return page


def _decode_pua_glyphs(page):
    """Rewrite Private Use Area chars as the ASCII glyph they draw, in place; a
    no-op on a page without them. Producer-Writers Guild's Arial subsets map
    every glyph to U+E0xx in ToUnicode but keep Arial's glyph ids under
    Identity-H, so CIDs 3-97 are ASCII 32-126 (2026-10-16)."""
    if not any(_PUA_RE.match(c["text"]) for c in page.chars):
        return page
    manager, maps = PDFResourceManager(), {}
    for ref in (resolve1(resolve1(page.page_obj.resources).get("Font", {})) or {}).values():
        try:
            font = manager.get_font(None, resolve1(ref))
        except Exception:
            continue
        cid2unichr = getattr(getattr(font, "unicode_map", None), "cid2unichr", {})
        maps[font.fontname] = {u: chr(cid + 29) for cid, u in cid2unichr.items()
                               if _PUA_RE.fullmatch(u) and 3 <= cid <= 97}
    for c in page.chars:
        m = maps.get(c["fontname"])
        if m and c["text"] in m:
            c["text"] = m[c["text"]]
    return page


def _dc_text(page) -> str:
    # The default tolerance runs some columns together.
    return _decode_pua_glyphs(page).extract_text(x_tolerance=1) or ""


def _upright_text(page, x_tolerance: float = 3) -> str:
    """page.extract_text(), but a page whose text is mostly drawn rotated 90°
    counter-clockwise on an upright page (DuPont's schedule) is read with its
    characters turned back first; otherwise pdfplumber returns one reversed
    word per line (2026-10-16). A subset font whose space glyph comes through
    as "\x03" (PG&E's State Street report) gets its spaces back (2026-10-17)."""
    chars = page.chars
    rotated = [c for c in chars if not c["upright"] and c["matrix"][1] > 0 > c["matrix"][2]]
    if len(rotated) * 2 <= len(chars):
        return (page.extract_text(x_tolerance=x_tolerance) or "").replace("\x03", " ")
    turned = []
    for c in rotated:
        t = dict(c)
        t["x0"], t["x1"] = page.height - c["bottom"], page.height - c["top"]
        t["top"], t["bottom"] = c["x0"], c["x1"]
        t["doctop"], t["upright"] = t["top"], True
        turned.append(t)
    return (pdfplumber.utils.extract_text(turned) or "").replace("\x03", " ")


def _stream_runs_text(page, tol: float = 2.0) -> str:
    """Page text rebuilt from runs of characters in content-stream order. Some
    filings' glyph widths overrun the next word, so sorting characters by x
    interleaves neighbouring words ("HistoCroisctal" for "Historical Cost",
    HCSC 2026-10-17); a run is the characters drawn one after another along a
    baseline, and a line is the runs sharing a baseline, left to right."""
    runs = []
    for c in page.chars:
        if not c["upright"]:
            continue
        run = runs[-1] if runs else None
        if run and abs(c["matrix"][5] - run["base"]) <= tol and -1 <= c["x0"] - run["x1"] <= max(4.0, c["size"]):
            run["text"] += (" " if c["x0"] - run["x1"] > c["size"] * 0.25 else "") + c["text"]
            run["x1"] = c["x1"]
        else:
            runs.append({"base": c["matrix"][5], "x0": c["x0"], "x1": c["x1"], "text": c["text"]})
    lines = []
    for run in sorted(runs, key=lambda r: -r["base"]):
        if lines and abs(run["base"] - lines[-1][0]) <= tol:
            lines[-1][1].append(run)
        else:
            lines.append([run["base"], [run]])
    return "\n".join(" ".join(" ".join(r["text"].split()) for r in sorted(rs, key=lambda r: r["x0"]))
                     for _, rs in lines)


def _nhr_text(page) -> str:
    """_upright_text for nt_held_regions. Some filings set the words so close
    that the default tolerance glues them ("SecurityDescription/AssetID", BJC);
    when the column header is lost that way, read again with a tighter one,
    then from content-stream runs (HCSC's interleaved words)."""
    text = _upright_text(page)
    if not _NHR_COLUMNS_RE.search(text):
        tight = page.extract_text(x_tolerance=1.5) or ""
        if _NHR_COLUMNS_RE.search(tight):
            return tight
        runs = _stream_runs_text(page)
        if _NHR_COLUMNS_RE.search(runs):
            runs = re.sub(r"^♦\s*(.*?)\s+(PLAN\s+\d+\s+Page\s+\d+\s+of\s+\d+)$", r"\1\n\2", runs, flags=re.M)
            runs = re.sub(r"^US\. ", "U.S. ", runs, flags=re.M)
            runs = re.sub(r"(\d,) +(?=\d)", r"\1", runs)
            runs = re.sub(r"(,\d{1,2}) +(?=\d)", r"\1", runs)
            runs = re.sub(r"(\d) +(?=\.\d{2}(?:\s|$))", r"\1", runs)
            # A period for the thousands comma ("434.455.66", HCSC's WI TREASURY row).
            runs = re.sub(r"(?<![\d.,])(\d{1,3})\.(\d{3}\.\d{2})(?=\s)", r"\1,\2", runs)
            return re.sub(r"(?<!\S)-\s+(?=\d{1,3}(?:,\d{3})*\.\d{2}(?:\s|$))", "-", runs)
    return text


def _decode_offset_cids(page):
    """Rewrite pdfplumber's "(cid:N)" chars as chr(N + 29), in place; a no-op
    on a page without them. Highmark's BNY report fonts carry no ToUnicode
    map and number their glyphs ASCII less 29 (2026-10-17)."""
    for c in page.chars:
        m = _CID_RE.match(c["text"])
        if m and 3 <= int(m.group(1)) <= 97:
            c["text"] = chr(int(m.group(1)) + 29)
    return page


def _page_text(page, layout: str, x_tolerance: float = 3) -> str:
    if layout == "gain_loss_totals":
        _decode_offset_cids(page)
    if layout in ("issue_par_rate", "cpa_total_lines", "gain_loss_totals", "issuer_shares_category",
                  "party_type_tree", "category_column", "sch_h_category", "nt_par_cost_value",
                  "typed_column_totals"):
        page = _drop_overlapping_spaces(page)
    return _upright_text(page, x_tolerance)


def _header_re(layout: str):
    if layout == "detail_report":
        return _DR_HEADER_RE
    if layout == "soi_revalued_cost":
        return _SOI_HEADER_RE
    if layout == "jpm_held":
        return _JPM_HEADER_RE
    if layout == "bny_h30t":
        return _H30T_HEADER_RE
    if layout == "cpa_total_lines":
        return _CTL_HEADER_RE
    if layout == "gain_loss_totals":
        return _GLT_HEADER_RE
    if layout == "nt_par_cost_value":
        return _NTP_HEADER_RE
    return _IPR_HEADER_RE if layout == "issue_par_rate" else _HEADER_RE


def _dr_body(text: str) -> List[str]:
    """detail_report lines below the page's title block, footer dropped."""
    lines = [l.strip() for l in text.splitlines()]
    start = next((i + 1 for i, l in enumerate(lines) if _DR_DATES_RE.match(l)), len(lines))
    return [l for l in lines[start:] if l and not _DR_FOOTER_RE.match(l)]


def _soi_body(text: str) -> List[str]:
    """soi_revalued_cost lines below the column header, footer dropped."""
    lines = [l.strip() for l in text.splitlines()]
    start = next((i + 1 for i, l in enumerate(lines) if _SOI_COLUMNS_RE.match(l)), len(lines))
    return [l for l in lines[start:] if l and not _DR_FOOTER_RE.match(l)]


def _jpm_body(text: str) -> List[str]:
    """jpm_held lines below the "SHARES / PAR" title-block line, footer dropped."""
    lines = [l.strip() for l in text.splitlines()]
    start = next((i + 1 for i, l in enumerate(lines) if _JPM_COLUMNS_RE.match(l)), len(lines))
    return [l for l in lines[start:] if l and not _JPM_FOOTER_RE.match(l) and not _JPM_FILER_FOOTER_RE.match(l)
            and not l.isdigit()]


def _h30t_body(text: str) -> List[str]:
    """bny_h30t lines below the column header (holdings or summary page)."""
    lines = [l.strip() for l in text.splitlines()]
    start = next((i + 1 for i, l in enumerate(lines)
                  if _H30T_COLUMNS_RE.match(l) or _H30T_SUMMARY_RE.match(l)), len(lines))
    if start < len(lines) and _H30T_RULE_RE.match(lines[start]):
        start += 1
    return [l for l in lines[start:] if l]


def _ctl_body(text: str) -> List[str]:
    """cpa_total_lines lines below the column header, page number dropped."""
    lines = [l.strip() for l in text.splitlines()]
    start = next((i + 1 for i, l in enumerate(lines) if _CTL_COLUMNS_RE.match(l)), len(lines))
    return [l for l in lines[start:] if l and not l.isdigit() and not _CTL_FOOTER_RE.match(l)]


def _glt_body(text: str) -> List[str]:
    """gain_loss_totals lines below the column header."""
    lines = [l.strip() for l in text.splitlines()]
    start = next((i + 1 for i, l in enumerate(lines) if _GLT_COLUMNS_RE.match(l)), len(lines))
    return [l for l in lines[start:] if l]


def _ntp_body(text: str) -> List[str]:
    """nt_par_cost_value lines below the column header."""
    lines = [l.strip() for l in text.splitlines()]
    start = next((i + 1 for i, l in enumerate(lines) if _NTP_HEADER_RE.match(l)), len(lines))
    return [l for l in lines[start:] if l]


def _bs_text(page) -> str:
    # The default tolerance runs the words of every line together; NECA's
    # tighter word gaps still run together at 1 (2026-10-16).
    return page.extract_text(x_tolerance=0.5) or ""


def _bs_body(text: str) -> List[str]:
    """bare_subtotals lines below the first page's column header, footer dropped."""
    lines = [l.strip() for l in text.splitlines()]
    start = next((i + 1 for i, l in enumerate(lines) if _BS_COLUMNS_RE.match(l)), 0)
    # NECA prints "$ 279" where NEBF prints "$279" (2026-10-16).
    return [re.sub(r"\$\s+(?=[\d(])", "$", l) for l in lines[start:] if l and not _BS_FOOTER_RE.match(l)]


def _headings(text: str, layout: str) -> List[str]:
    if layout == "bare_subtotals":
        # Title-block lines above the column header (first page only).
        lines = [l.strip() for l in text.splitlines()]
        end = next((i for i, l in enumerate(lines) if _BS_COLUMNS_RE.match(l)), 0)
        return [l for l in lines[:end] if l]
    if layout in ("gain_loss_totals", "nt_par_cost_value"):
        # No category headings: the title block's lines, so start_heading can
        # match the page title.
        columns_re = _GLT_COLUMNS_RE if layout == "gain_loss_totals" else _NTP_HEADER_RE
        lines = [l.strip() for l in text.splitlines()]
        end = next((i for i, l in enumerate(lines) if columns_re.match(l)), len(lines))
        return [l for l in lines[:end] if l]
    if layout == "cpa_total_lines":
        return [l for l in _ctl_body(text) if not l.startswith("TOTAL ") and not _trailing_nums(l, 2)]
    if layout in ("flat_cost_value", "inline_category", "nt_held_regions", "titled_appendix", "typed_description",
                  "typed_maturity_rate", "closing_totals", "investments_held_summary", "nt_asset_detail",
                  "colon_headings", "described_categories", "nt_cusip_columns", "issuer_shares_category",
                  "party_type_tree", "category_column", "summary_attached", "sch_h_category",
                  "category_rate_totals", "year_end_package"):
        # Title-block lines above the column header (pool name among them).
        columns_re = {"flat_cost_value": _FCV_HEADER_RE, "inline_category": _IC_COLUMNS_RE,
                      "nt_cusip_columns": _NCC_COLUMNS_RE, "issuer_shares_category": _ISC_COLUMNS_RE,
                      "party_type_tree": _PTT_COLUMNS_RE, "category_column": _CC_COLUMNS_RE,
                      "sch_h_category": _SHC_COLUMNS_RE, "category_rate_totals": _CRT_COLUMNS_RE,
                      "year_end_package": _YEP_COLUMNS_RE,
                      "summary_attached": _SA_COLUMNS_RE, "nt_held_regions": _NHR_COLUMNS_RE, "titled_appendix": _TA_COLUMNS_RE,
                      "typed_description": _TD_COLUMNS_RE, "typed_maturity_rate": _TMR_COLUMNS_RE,
                      "closing_totals": _CT_COLUMNS_RE, "investments_held_summary": _IHS_COLUMNS_RE,
                      "nt_asset_detail": _NAD_COLUMNS_RE, "colon_headings": _CH_COLUMNS_RE,
                      "described_categories": _DC_COLUMNS_RE}[layout]
        lines = [l.strip() for l in text.splitlines()]
        end = next((i for i, l in enumerate(lines) if columns_re.match(l)), 0)
        return [l for l in lines[:end] if l]
    if layout == "bny_h30t":
        # The report's title line ("JJ2K PAGE: 1") comes first, so start_heading
        # can pick one report when a PDF carries several.
        title = next((l.strip() for l in text.splitlines() if l.strip()), "")
        body = _h30t_body(text)
        return [title] + [l for l, nxt in zip(body, body[1:])
                          if _H30T_RULE_RE.match(nxt) and not l.startswith("-")]
    if layout == "jpm_held":
        body = _jpm_body(text)
        return [l for l, nxt in zip(body, body[1:])
                if _JPM_RULE_RE.match(nxt) and not _JPM_RULE_RE.match(l)]
    if layout == "detail_report":
        return [l for l in _dr_body(text) if _DR_LETTER_RE.match(l)]
    if layout == "soi_revalued_cost":
        # Candidates only (continuation lines look the same); enough to find start_heading.
        return [l for l in _soi_body(text) if _SOI_CAPS_RE.match(l) and not l.startswith("TOTAL ")]
    if layout == "issue_par_rate":
        lines = text.splitlines()
        header_idx = next((i for i, l in enumerate(lines) if _IPR_HEADER_RE.search(l)), len(lines))
        return [l.strip() for l in lines[header_idx + 1:] if _IPR_HEADING_RE.match(l.strip())]
    return [h for h in (_heading_of(l.strip()) for l in text.splitlines()) if h]


def registered_ack_ids(workgroup: str, s3_output: str) -> Dict[str, str]:
    """ack_id -> plan_id for every plan_master filing of a registered plan."""
    import awswrangler as wr

    plan_ids = ", ".join(f"'{p}'" for p in PLAN_PARSERS)
    df = wr.athena.read_sql_query(
        sql=f"SELECT ack_id, plan_id FROM plan_master WHERE plan_id IN ({plan_ids})",
        database="default", workgroup=workgroup, s3_output=s3_output, ctas_approach=False,
    )
    return dict(zip(df["ack_id"], df["plan_id"]))


def find_schedule_4i_pages(pdf_path: str, start_heading: str, layout: str = "default") -> List[int]:
    """Pages of the first 4i schedule copy, from start_heading's page to its end.

    The schedule is the first contiguous run of pages carrying the 4i column
    header and start_heading; some filers print it twice (Mayo 2025: pp.28-52
    and again pp.103-127), and only the first copy is taken. Earlier runs
    without the heading (National Elevator's p.7 "see attached" stub) are
    skipped. Returns [] if no such run is found.
    """
    run: List[int] = []
    start = None
    if layout in ("flat_cost_value", "inline_category", "nt_held_regions", "titled_appendix", "typed_description",
                  "typed_maturity_rate", "closing_totals", "nt_asset_detail", "colon_headings", "leading_shares",
                  "issuer_shares_category", "party_type_tree", "category_column", "summary_attached",
                  "sch_h_category", "category_rate_totals", "year_end_package"):
        # Run from the first page with the header to the page with the total
        # (nt_held_regions repeats the header, and its schedule is printed twice).
        header_re, total_re = {
            "flat_cost_value": (_FCV_HEADER_RE, _FCV_TOTAL_RE),
            "inline_category": (_IC_COLUMNS_RE, re.compile(r"^Grand\s+Total\s", re.I | re.M)),
            "nt_held_regions": (_NHR_COLUMNS_RE, _NHR_GRAND_RE),
            "titled_appendix": (_TA_TITLE_RE, _TA_TOTAL_RE),
            "typed_description": (_TD_COLUMNS_RE, _TD_END_RE),
            "typed_maturity_rate": (_TMR_COLUMNS_RE, _TMR_GRAND_RE),
            "closing_totals": (_CT_COLUMNS_RE, _CT_GRAND_RE),
            "nt_asset_detail": (_NAD_COLUMNS_RE, _NAD_GRAND_RE),
            "colon_headings": (_CH_COLUMNS_RE, _CH_END_RE),
            "leading_shares": (_LS_COLUMNS_RE, _LS_GRAND_RE),
            "issuer_shares_category": (_ISC_COLUMNS_RE, _ISC_GRAND_RE),
            "party_type_tree": (_PTT_COLUMNS_RE, _PTT_GRAND_RE),
            "category_column": (_CC_COLUMNS_RE, _CC_GRAND_RE),
            "sch_h_category": (_SHC_COLUMNS_RE, _SHC_GRAND_RE),
            "category_rate_totals": (_CRT_COLUMNS_RE, _CRT_GRAND_RE),
            "year_end_package": (_YEP_COLUMNS_RE, _YEP_GRAND_RE),
            "summary_attached": (_SA_COLUMNS_RE, _SA_CLOSING_RE)}[layout]
        if layout == "typed_maturity_rate":
            total_re = next((re.compile(o["grand_re"], re.I | re.M) for o in PLAN_PARSERS.values()
                             if o.get("layout") == layout and o.get("start_heading") == start_heading
                             and o.get("grand_re")), total_re)
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                if layout == "typed_maturity_rate":
                    _decode_calibri_cids(page)
                text = _nhr_text(page) if layout == "nt_held_regions" else _upright_text(page)
                if start is None and header_re.search(text) and any(
                        re.match(start_heading, h, re.I) for h in _headings(text, layout)):
                    start = page_num
                if start is not None:
                    if layout == "flat_cost_value" and page_num > start and _FCV_NEXT_SCHEDULE_RE.search(text):
                        return run
                    # running_totals plans print no total line; the run ends at
                    # the first page without the column header.
                    if (layout == "flat_cost_value" and page_num > start and not header_re.search(text)
                            and any(o.get("running_totals") for o in PLAN_PARSERS.values()
                                    if o.get("layout") == layout and o.get("start_heading") == start_heading)):
                        return run
                    run.append(page_num)
                    if total_re.search(text):
                        return run
                    if layout == "nt_held_regions" and any(
                            o.get("no_grand_total") for o in PLAN_PARSERS.values()
                            if o.get("layout") == layout and o.get("start_heading") == start_heading):
                        m = re.search(r"\bPage\s+(\d+)\s+of\s+(\d+)$", text, re.M)
                        if m and m.group(1) == m.group(2):
                            return run
        return []
    if layout == "investments_held_summary":
        # Every summary page carries the title; the custodial statement follows.
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                if not _IHS_TITLE_RE.search(text):
                    if start is not None:
                        return run
                    continue
                if start is None and any(re.match(start_heading, h, re.I) for h in _headings(text, layout)):
                    start = page_num
                if start is not None:
                    run.append(page_num)
        return run
    if layout == "described_categories":
        # Title on every page: run from start_heading's page to the grand total.
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = _dc_text(page)
                if start is None and _DC_TITLE_RE.search(text) and _DC_COLUMNS_RE.search(text) and any(
                        re.match(start_heading, h, re.I) for h in _headings(text, layout)):
                    start = page_num
                if start is not None:
                    run.append(page_num)
                    if _DC_GRAND_RE.search(text):
                        return run
        return []
    if layout == "nt_cusip_columns":
        # Title and header on the first page only: run from it to the grand total.
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                if start is None and _NCC_TITLE_RE.search(text) and _NCC_COLUMNS_RE.search(text) and any(
                        re.match(start_heading, h, re.I) for h in _headings(text, layout)):
                    start = page_num
                if start is not None:
                    run.append(page_num)
                    if _NCC_GRAND_RE.search(text):
                        return run
        return []
    if layout == "typed_column_totals":
        # Header on every page: run from the plan's first page while it lasts.
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                if not _TCT_COLUMNS_RE.search(text):
                    if start is not None:
                        return run
                    continue
                if start is None and re.search(start_heading, text, re.M):
                    start = page_num
                if start is not None:
                    run.append(page_num)
        return run
    if layout == "cusip_column_totals":
        # Title on every page: run from the plan's first page to "TOTAL INVESTMENTS".
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                if not (_CCT_TITLE_RE.search(text) and _CCT_COLUMNS_RE.search(text)):
                    if start is not None:
                        return []
                    continue
                if start is None and re.search(start_heading, text, re.M):
                    start = page_num
                if start is not None:
                    run.append(page_num)
                    if _CCT_GRAND_RE.search(text):
                        return run
        return []
    if layout == "colon_heading_totals":
        # Header on every page: run from the plan's title page to the grand total.
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                if not _CHT_COLUMNS_RE.search(text):
                    if start is not None:
                        return []
                    continue
                if start is None and re.search(start_heading, text, re.M):
                    start = page_num
                if start is not None:
                    run.append(page_num)
                    if _CHT_GRAND_RE.search(text):
                        return run
        return []
    if layout == "bold_heading_totals":
        # Header on the first page only: run from it to the grand total.
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                if start is None and _BHT_COLUMNS_RE.search(text) and re.search(start_heading, text, re.M):
                    start = page_num
                if start is not None:
                    run.append(page_num)
                    if _BHT_GRAND_RE.search(text):
                        return run
        return []
    if layout == "cusip_type_blocks":
        # Header on the first page only: run from it to the grand total.
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                if start is None and _CTB_COLUMNS_RE.search(text) and re.search(start_heading, text, re.M):
                    start = page_num
                if start is not None:
                    run.append(page_num)
                    if _CTB_GRAND_RE.search(text):
                        return run
        return []
    if layout == "asset_liability_positions":
        # Title on every statement page: run from the first page matching
        # start_heading to the page with "Total Assets and Accruals".
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                if start is None and _ALP_TITLE_RE.search(text) and re.search(start_heading, text, re.M):
                    start = page_num
                if start is not None:
                    if not _ALP_TITLE_RE.search(text):
                        return []
                    run.append(page_num)
                    if _ALP_GRAND_RE.search(text):
                        return run
        return []
    if layout == "nt_market_erisa":
        # Title on every page: run from the first page whose title block matches
        # start_heading to the page with the unnamed grand total.
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = _upright_text(page)
                if start is None and _NME_COLUMNS_RE.search(text) and re.search(start_heading, text, re.M):
                    start = page_num
                if start is not None:
                    run.append(page_num)
                    if any(_NME_GRAND_RE.match(_nme_join_split_amounts(l.strip())) for l in text.splitlines()):
                        return run
        return []
    if layout == "fmv_cost_summary":
        # Header on every summary page: run from start_heading's page until it stops.
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                if not _FCS_COLUMNS_RE.search(text):
                    if start is not None:
                        return run
                    continue
                if start is None and re.search(start_heading, text, re.M):
                    start = page_num
                if start is not None:
                    run.append(page_num)
        return run
    if layout == "leading_category":
        # Title on every page: run from start_heading's page to "Page <N> of <N>".
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                if not _LC_TITLE_RE.search(text):
                    if start is not None:
                        return run
                    continue
                if start is None and re.search(start_heading, text, re.M):
                    start = page_num
                if start is not None:
                    run.append(page_num)
                    m = _LC_PAGE_RE.search(text)
                    if m and m.group(1) == m.group(2):
                        return run
        return run
    if layout == "titled_category_pages":
        # Held title on every page: run from start_heading's page until it stops.
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                held = bool(_TCP_HELD_RE.search(text))
                if start is None and held and re.search(start_heading, text, re.M):
                    start = page_num
                if start is not None:
                    if not held:
                        return run
                    run.append(page_num)
        return run
    if layout == "bare_subtotals":
        # Title on the first page only: run from it to the page with the grand total.
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = _bs_text(page)
                if start is None and _BS_TITLE_RE.search(text) and _BS_COLUMNS_RE.search(text) and any(
                        re.match(start_heading, h, re.I) for h in _headings(text, layout)):
                    start = page_num
                if start is not None:
                    run.append(page_num)
                    if any(_BS_GRAND_RE.match((_trailing_nums(l, 2) or ("",))[0]) for l in _bs_body(text)):
                        return run
        return []
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            text = _page_text(page, layout)
            if not _header_re(layout).search(text):
                if start:
                    break
                # A run without the heading is a stub ("see attached"), not the schedule.
                run = []
                continue
            run.append(page_num)
            if start is None and any(re.match(start_heading, h, re.I) for h in _headings(text, layout)):
                start = page_num
    return [p for p in run if p >= start] if start else []


def _inherited_heading(pdf, first_page: int, layout: str = "default") -> str:
    """Last section heading printed before first_page, scanning backwards."""
    for page_num in range(first_page - 1, max(0, first_page - 1 - _LOOKBACK_PAGES), -1):
        text = _page_text(pdf.pages[page_num - 1], layout)
        if layout == "issue_par_rate" and not _IPR_HEADER_RE.search(text):
            continue
        headings = _headings(text, layout)
        if headings:
            return headings[-1]
    return ""


def parse_schedule_4i_pages(pdf_path: str, page_nums: List[int], layout: str = "default",
                            plan_id: Optional[str] = None) -> Optional[Dict]:
    """Parse the requested pages. Returns None if any page lacks the 4i header.
    plan_id picks up the registry's per-plan options (nt_par_cost_value's alt_span).

    Result: {"pages": [<run_pipeline page_data dicts>], "sections": [...],
             "unparsed": [...], "grand_total": [cost, value] or None}
    """
    if layout == "gain_loss_totals":
        return _parse_gain_loss_totals(pdf_path, page_nums, PLAN_PARSERS.get(plan_id, {}))
    if layout == "nt_par_cost_value":
        return _parse_nt_par_cost_value(pdf_path, page_nums, PLAN_PARSERS.get(plan_id, {}).get("alt_span"),
                                        PLAN_PARSERS.get(plan_id, {}).get("alt_span_exclude"),
                                        PLAN_PARSERS.get(plan_id, {}).get("type_rows", []))
    if layout == "bare_subtotals":
        return _parse_bare_subtotals(pdf_path, page_nums)
    if layout == "inline_category":
        return _parse_inline_category(pdf_path, page_nums)
    if layout == "nt_asset_detail":
        return _parse_nt_asset_detail(pdf_path, page_nums)
    if layout == "described_categories":
        return _parse_described_categories(pdf_path, page_nums, PLAN_PARSERS.get(plan_id, {}))
    if layout == "nt_cusip_columns":
        return _parse_nt_cusip_columns(pdf_path, page_nums, PLAN_PARSERS.get(plan_id, {}))
    if layout == "leading_shares":
        return _parse_leading_shares(pdf_path, page_nums, PLAN_PARSERS.get(plan_id, {}))
    if layout == "issuer_shares_category":
        return _parse_issuer_shares_category(pdf_path, page_nums)
    if layout == "party_type_tree":
        return _parse_party_type_tree(pdf_path, page_nums)
    if layout == "category_column":
        return _parse_category_column(pdf_path, page_nums)
    if layout == "sch_h_category":
        return _parse_sch_h_category(pdf_path, page_nums, PLAN_PARSERS.get(plan_id, {}))
    if layout == "category_rate_totals":
        return _parse_category_rate_totals(pdf_path, page_nums, PLAN_PARSERS.get(plan_id, {}))
    if layout == "year_end_package":
        return _parse_year_end_package(pdf_path, page_nums, PLAN_PARSERS.get(plan_id, {}))
    if layout == "titled_category_pages":
        return _parse_titled_category_pages(pdf_path, page_nums, PLAN_PARSERS.get(plan_id, {}))
    if layout == "leading_category":
        return _parse_leading_category(pdf_path, page_nums)
    if layout == "fmv_cost_summary":
        return _parse_fmv_cost_summary(pdf_path, page_nums)
    if layout == "nt_market_erisa":
        return _parse_nt_market_erisa(pdf_path, page_nums)
    if layout == "asset_liability_positions":
        return _parse_asset_liability_positions(pdf_path, page_nums)
    if layout == "typed_column_totals":
        return _parse_typed_column_totals(pdf_path, page_nums,
                                          PLAN_PARSERS.get(plan_id, {}).get("section_types", []),
                                          PLAN_PARSERS.get(plan_id, {}).get("amount_scale", 1))
    if layout == "cusip_column_totals":
        return _parse_cusip_column_totals(pdf_path, page_nums,
                                          PLAN_PARSERS.get(plan_id, {}).get("section_types", []))
    if layout == "colon_heading_totals":
        return _parse_colon_heading_totals(pdf_path, page_nums,
                                           PLAN_PARSERS.get(plan_id, {}).get("section_types", []))
    if layout == "bold_heading_totals":
        return _parse_bold_heading_totals(pdf_path, page_nums,
                                          PLAN_PARSERS.get(plan_id, {}).get("section_types", []))
    if layout == "cusip_type_blocks":
        return _parse_cusip_type_blocks(pdf_path, page_nums,
                                        PLAN_PARSERS.get(plan_id, {}).get("amount_scale", 1))
    if layout == "summary_attached":
        return _parse_summary_attached(pdf_path, page_nums, PLAN_PARSERS.get(plan_id, {}))
    if layout == "colon_headings":
        return _parse_colon_headings(pdf_path, page_nums,
                                     PLAN_PARSERS.get(plan_id, {}).get("row_rounding", False))
    if layout == "nt_held_regions":
        return _parse_nt_held_regions(pdf_path, page_nums, PLAN_PARSERS.get(plan_id, {}).get("hedge_fund_rows"),
                                      PLAN_PARSERS.get(plan_id, {}).get("no_grand_total", False),
                                      PLAN_PARSERS.get(plan_id, {}).get("missing_page_totals"))
    if layout == "titled_appendix":
        return _parse_titled_appendix(pdf_path, page_nums)
    if layout == "typed_description":
        return _parse_typed_description(pdf_path, page_nums, PLAN_PARSERS.get(plan_id, {}).get("exclude_rows"))
    if layout == "typed_maturity_rate":
        return _parse_typed_maturity_rate(pdf_path, page_nums, PLAN_PARSERS.get(plan_id, {}))
    if layout == "closing_totals":
        return _parse_closing_totals(pdf_path, page_nums)
    if layout == "investments_held_summary":
        return _parse_investments_held_summary(pdf_path, page_nums,
                                               PLAN_PARSERS.get(plan_id, {}).get("value_cost_pair", False))
    if layout == "detail_report":
        return _parse_detail_report(pdf_path, page_nums)
    if layout == "soi_revalued_cost":
        return _parse_soi_revalued_cost(pdf_path, page_nums)
    if layout == "jpm_held":
        return _parse_jpm_held(pdf_path, page_nums, PLAN_PARSERS.get(plan_id, {}))
    if layout == "bny_h30t":
        return _parse_bny_h30t(pdf_path, page_nums)
    if layout == "flat_cost_value":
        return _parse_flat_cost_value(pdf_path, page_nums, PLAN_PARSERS.get(plan_id, {}).get("untyped_as", ""),
                                      PLAN_PARSERS.get(plan_id, {}).get("row_asset_types"),
                                      PLAN_PARSERS.get(plan_id, {}).get("word_lines", False),
                                      PLAN_PARSERS.get(plan_id, {}).get("bare_grand", False),
                                      PLAN_PARSERS.get(plan_id, {}).get("skip", ""),
                                      PLAN_PARSERS.get(plan_id, {}).get("running_totals", False),
                                      PLAN_PARSERS.get(plan_id, {}).get("shs_split", False),
                                      PLAN_PARSERS.get(plan_id, {}).get("rate_column", False),
                                      PLAN_PARSERS.get(plan_id, {}).get("amount_scale", 1))
    if layout == "cpa_total_lines":
        return _parse_cpa_total_lines(pdf_path, page_nums)
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        texts = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            texts[page_num] = _page_text(pdf.pages[page_num - 1], layout)
            if not _header_re(layout).search(texts[page_num]):
                return None
        inherited = _inherited_heading(pdf, page_nums[0], layout)

    sections = []
    current = {"heading": inherited, "asset_type": heading_asset_type(inherited),
               "partial": True, "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None}
    sections.append(current)
    unparsed = []
    grand_total = None
    last_kind = None
    pages = []

    for page_num in page_nums:
        mapped_rows = []
        lines = texts[page_num].splitlines()
        # Everything above the column header is the page's title block (plan name, EIN, ...).
        header_idx = next(i for i, l in enumerate(lines) if _header_re(layout).search(l))
        for line in lines[header_idx + 1:]:
            if layout == "issue_par_rate":
                kind, payload = _ipr_classify_line(line, current["heading"])
            else:
                kind, payload = _classify_line(line)
            if kind == "skip":
                continue
            if kind == "heading":
                current = {"heading": payload, "asset_type": heading_asset_type(payload),
                           "partial": False, "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None}
                sections.append(current)
            elif kind == "subtotal":
                # A second totals line straight after a subtotal is the schedule grand total.
                if last_kind == "subtotal":
                    grand_total = payload
                else:
                    current["subtotal"] = payload
            elif kind == "unparsed":
                unparsed.append({"page_number": page_num, "line": payload})
            else:
                asset_type = current["asset_type"]
                source = "heading" if asset_type else "none"
                if not asset_type:
                    asset_type = detect_asset_type(f"{current['heading']} {payload['title']}")
                    if not asset_type:
                        asset_type = next((label for pattern, label in _TITLE_ASSET_TYPES
                                           if re.search(pattern, payload["title"], re.I)), "")
                    source = "row" if asset_type else "none"
                current["rows"] += 1
                current["cost"] += payload["cost"] or 0.0
                current["value"] += payload["value"] or 0.0
                mapped_rows.append({
                    "issuer_name": "",
                    "investment_description": payload["title"],
                    "asset_type": asset_type,
                    "asset_type_source": source,
                    "par_value": "",
                    "cost": _fmt(payload["cost"]),
                    "current_value": _fmt(payload["value"]),
                    "units_or_shares": _fmt(payload["shares"]),
                    "asset_identifier": payload["identifier"],
                    "interest_rate": payload["rate"],
                    "maturity_date": payload["maturity"],
                    "section_heading": current["heading"],
                    "extraction_method": "sch4i_text",
                    "page_number": page_num,
                    "row_id": len(mapped_rows) + 1,
                })
            last_kind = kind
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    if not sections[0]["rows"]:
        sections.pop(0)
    for section in sections:
        sub = section["subtotal"]
        sub_cost, sub_value = (sub[-2], sub[-1]) if sub and len(sub) >= 2 else (None, sub[-1] if sub else None)
        section["cost_diff"] = None if sub_cost is None else section["cost"] - sub_cost
        section["value_diff"] = None if sub_value is None else section["value"] - sub_value
        if section["partial"] or sub_value is None:
            section["reconciled"] = None
        else:
            # Filers print whole dollars but total the unrounded figures, so allow
            # up to $1 of rounding drift per row before calling it a mismatch.
            tol = max(1.0, float(section["rows"]))
            # issue_par_rate: cost is advisory. Shell's own printed cost subtotals
            # don't foot (stocks +$831, insurance +$5,343, partnerships +$21,864
            # over the printed rows, checked by hand) while every value subtotal
            # does to the dollar, and value is the only figure loaded.
            cost_ok = (layout == "issue_par_rate" or section["cost_diff"] is None
                       or abs(section["cost_diff"]) <= tol)
            section["reconciled"] = abs(section["value_diff"]) <= tol and cost_ok

    # When the printed grand total is in range and no section was cut off at
    # the start, every row of the schedule was parsed: the rows must sum to it.
    grand_reconciled = None
    if grand_total and not any(s["partial"] for s in sections):
        total_rows = sum(s["rows"] for s in sections)
        grand_diff = sum(s["value"] for s in sections) - grand_total[-1]
        grand_reconciled = abs(grand_diff) <= max(1.0, float(total_rows))
    return {"pages": pages, "sections": sections, "unparsed": unparsed,
            "grand_total": grand_total, "grand_reconciled": grand_reconciled}


def _dr_trailing_pair(line: str) -> Optional[tuple]:
    """(text, boy, eoy) when the line ends in two amounts, else None."""
    tokens = line.split()
    if len(tokens) < 2 or not (_NUM_RE.match(tokens[-1]) and _NUM_RE.match(tokens[-2])):
        return None
    return " ".join(tokens[:-2]), _num(tokens[-2]), _num(tokens[-1])


def _dr_asset_type(path: str) -> str:
    return next((label for pattern, label in _DR_ASSET_TYPES if re.search(pattern, path, re.I)), "")


def _parse_detail_report(pdf_path: str, page_nums: List[int]) -> Optional[Dict]:
    """parse_schedule_4i_pages for the detail_report layout. The pages must
    start at "A." so every section opens in range; the end-of-year column is
    the value, and the report carries no cost."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            text = pdf.pages[page_num - 1].extract_text() or ""
            if not _DR_HEADER_RE.search(text):
                return None
            bodies[page_num] = _dr_body(text)

    letter, item, sub = "", "", ""
    current = None
    sections, unparsed, pages = [], [], []
    grand_total = None
    done = False

    def open_section(heading: str, path: str) -> Dict:
        section = {"heading": heading, "asset_type": _dr_asset_type(path), "letter": letter,
                   "partial": False, "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None}
        sections.append(section)
        return section

    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            if done:
                break
            m = _DR_LETTER_RE.match(line)
            if m:
                letter, item, sub = m.group(1), "", ""
                pair = _dr_trailing_pair(m.group(2))
                if pair and re.match(r"total\s+assets$", pair[0], re.I):
                    grand_total = [pair[2]]
                    done = True
                    break
                # Letter sections holding rows directly ("A. NON-INTEREST BEARING CASH").
                current = open_section(pair[0] if pair else m.group(2), m.group(2))
                if pair:
                    current["subtotal"] = [pair[2]]
                continue
            m = _DR_ITEM_RE.match(line) or _DR_SUBITEM_RE.match(line)
            if m:
                pair = _dr_trailing_pair(m.group(2))
                heading = pair[0] if pair else m.group(2)
                if _DR_ITEM_RE.match(line):
                    item, sub = heading, ""
                else:
                    sub = heading
                current = open_section(heading, f"{item} / {sub}" if sub else item)
                if pair:  # "(7) LOANS OTHER THAN PARTICIPANT 0.00 0.00": empty line item
                    current["subtotal"] = [pair[2]]
                continue
            pair = _dr_trailing_pair(line)
            if pair is None:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            title, boy, eoy = pair
            if current and re.fullmatch(rf"TOTAL\s+{re.escape(current['heading'])}", title, re.I):
                current["subtotal"] = [eoy]
                continue
            if current is None or not re.search(r"[A-Za-z]", title):
                unparsed.append({"page_number": page_num, "line": line})
                continue
            current["rows"] += 1
            current["value"] += eoy or 0.0
            if not eoy or current["letter"] in _DR_NON_INVESTMENT_LETTERS:
                continue
            asset_type = current["asset_type"]
            source = "heading" if asset_type else "none"
            if not asset_type:
                asset_type = detect_asset_type(f"{current['heading']} {title}")
                source = "row" if asset_type else "none"
            mapped_rows.append({
                "issuer_name": "",
                "investment_description": title,
                "asset_type": asset_type,
                "asset_type_source": source,
                "par_value": "",
                "cost": "",
                "current_value": _fmt(eoy),
                "units_or_shares": "",
                "asset_identifier": "",
                "interest_rate": "",
                "maturity_date": "",
                "section_heading": current["heading"],
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            })
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    for section in sections:
        section["cost_diff"] = None
        sub_value = section["subtotal"][-1] if section["subtotal"] else None
        section["value_diff"] = None if sub_value is None else section["value"] - sub_value
        if sub_value is None:
            # A heading with rows but no TOTAL line can't be checked; one with
            # neither ("E. BUILDINGS/OTHER PROPERTY USED BY PLAN") is empty.
            section["reconciled"] = None if section["rows"] else True
        else:
            section["reconciled"] = abs(section["value_diff"]) <= 1.0
    grand_reconciled = None
    if grand_total:
        grand_diff = sum(s["value"] for s in sections) - grand_total[-1]
        grand_reconciled = abs(grand_diff) <= 1.0
    return {"pages": pages, "sections": sections, "unparsed": unparsed,
            "grand_total": grand_total, "grand_reconciled": grand_reconciled}


def _trailing_nums(line: str, n: int) -> Optional[tuple]:
    """(text, [n amounts]) when the line ends in n amounts, else None."""
    tokens = line.split()
    if len(tokens) < n or not all(_NUM_RE.match(t) for t in tokens[-n:]):
        return None
    return " ".join(tokens[:-n]), [_num(t) for t in tokens[-n:]]


def _parse_soi_revalued_cost(pdf_path: str, page_nums: List[int]) -> Optional[Dict]:
    """parse_schedule_4i_pages for the soi_revalued_cost layout. The pages must
    start at the first section heading so every section opens in range."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            # PG&E's space glyph comes through as "\x03".
            text = (pdf.pages[page_num - 1].extract_text() or "").replace("\x03", " ")
            if not _SOI_HEADER_RE.search(text):
                return None
            bodies[page_num] = _soi_body(text)

    # Headings are exactly the names the schedule prints a TOTAL line for.
    total_names = set()
    for body in bodies.values():
        for line in body:
            t = _trailing_nums(line, 3)
            if t and t[0].startswith("TOTAL "):
                total_names.add(t[0][len("TOTAL "):].strip())

    current = None
    sections, unparsed, pages = [], [], []
    grand_total = None
    numbers_only = None
    last_row = None
    done = False

    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            if done:
                break
            if line in total_names:
                current = {"heading": line, "asset_type": next(
                    (label for pattern, label in _SOI_ASSET_TYPES if re.search(pattern, line, re.I)), ""),
                    "partial": False, "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None}
                sections.append(current)
                last_row = None
                continue
            if line == "GRAND TOTAL" and numbers_only:
                grand_total = numbers_only[:2]
                done = True
                break
            m = _SOI_ROW_RE.match(line)
            t = _trailing_nums(m.group(3), 4) if m else None
            if t and current is not None:
                title, (shares, cost, value, _gain) = t
                current["rows"] += 1
                current["cost"] += cost or 0.0
                current["value"] += value or 0.0
                last_row = {
                    "issuer_name": "",
                    "investment_description": title,
                    "asset_type": current["asset_type"],
                    "asset_type_source": "heading" if current["asset_type"] else "none",
                    "par_value": "",
                    "cost": _fmt(cost),
                    "current_value": _fmt(value),
                    "units_or_shares": _fmt(shares),
                    "asset_identifier": m.group(2),
                    "interest_rate": "",
                    "maturity_date": "",
                    "section_heading": current["heading"],
                    "extraction_method": "sch4i_text",
                    "page_number": page_num,
                    "row_id": len(mapped_rows) + 1,
                }
                if not current["asset_type"]:
                    asset_type = detect_asset_type(f"{current['heading']} {title}")
                    last_row["asset_type"] = asset_type
                    last_row["asset_type_source"] = "row" if asset_type else "none"
                # Holdings sold during the year (market value 0) count but aren't loaded.
                if value:
                    mapped_rows.append(last_row)
                continue
            total = _trailing_nums(line, 3)
            if total and current is not None and total[0] == f"TOTAL {current['heading']}":
                current["subtotal"] = total[1][:2]
                last_row = None
                continue
            if total and not total[0]:
                numbers_only = total[1]
                continue
            if m is None and last_row is not None:
                # Wrapped description ("FUND", "DD 10/20/09") continues the row above.
                last_row["investment_description"] += " " + line
                continue
            unparsed.append({"page_number": page_num, "line": line})
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    for section in sections:
        sub = section["subtotal"]
        section["cost_diff"] = None if sub is None else section["cost"] - sub[0]
        section["value_diff"] = None if sub is None else section["value"] - sub[1]
        if sub is None:
            section["reconciled"] = None if section["rows"] else True
        else:
            # Printed to the cent, so only float drift is tolerated.
            section["reconciled"] = abs(section["value_diff"]) <= 1.0 and abs(section["cost_diff"]) <= 1.0
    grand_reconciled = None
    if grand_total:
        grand_diff = sum(s["value"] for s in sections) - grand_total[-1]
        grand_reconciled = abs(grand_diff) <= 1.0
    return {"pages": pages, "sections": sections, "unparsed": unparsed,
            "grand_total": grand_total, "grand_reconciled": grand_reconciled}


def _parse_jpm_held(pdf_path: str, page_nums: List[int], options: Optional[Dict] = None) -> Optional[Dict]:
    """parse_schedule_4i_pages for the jpm_held layout. The pages must start
    at the first section heading so every section opens in range. The
    registry's grand_value_gap is a filer's own footing gap: the printed
    TOTAL GENERAL INVESTMENTS value less the sum of its printed subtotals.
    Its row_asset_types, [(pattern, asset_type)], type a row whose
    description matches over its heading's type (BASF files private funds
    under COMMON and hedge funds under its fund categories, 2026-10-17)."""
    options = options or {}
    row_types = options.get("row_asset_types", [])
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            text = pdf.pages[page_num - 1].extract_text() or ""
            if not _JPM_HEADER_RE.search(text):
                return None
            bodies[page_num] = _jpm_body(text)

    current = None
    in_investments = False
    sections, unparsed, pages = [], [], []
    grand_total = None
    invest_value = 0.0

    for page_num in page_nums:
        mapped_rows = []
        body = bodies[page_num]
        for i, line in enumerate(body):
            if i and _JPM_WRAPPED_RE.match(line) and body[i - 1].startswith("TOTAL ") and body[i - 1].endswith(","):
                continue  # joined onto the TOTAL line above
            if _JPM_RULE_RE.match(line) or _JPM_SHARES_RE.match(line) or line.startswith("RATINGS:"):
                continue
            nxt = body[i + 1] if i + 1 < len(body) else ""
            if line.startswith("TOTAL ") and line.endswith(",") and _JPM_WRAPPED_RE.match(nxt):
                line += nxt
                nxt = body[i + 2] if i + 2 < len(body) else ""
            # A filing that leaves out the report's first pages opens mid-section:
            # its rows count toward nothing and the section's total is not checked.
            if current is None and not sections and page_num == page_nums[0] and _trailing_nums(line, 2):
                current = {"heading": "", "asset_type": "", "partial": True, "rows": 0, "cost": 0.0,
                           "value": 0.0, "subtotal": None, "investments": False}
                sections.append(current)
            if _JPM_RULE_RE.match(nxt) and not line.startswith("TOTAL "):
                if line == _JPM_INVESTMENTS:
                    in_investments = True
                current = {"heading": line, "asset_type": next(
                    (label for pattern, label in _JPM_ASSET_TYPES if re.search(pattern, line, re.I)), ""),
                    "partial": False, "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None,
                    "investments": in_investments}
                sections.append(current)
                continue
            if line.startswith("TOTAL "):
                total = _trailing_nums(line, 2) or _trailing_nums(line, 1)
                if total is None:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                name = total[0][len("TOTAL "):].strip()
                if name == _JPM_INVESTMENTS:
                    grand_total = total[1]
                    in_investments = False
                # "TOTAL FUTURES CONTRACTS BEFORE CASH FLOWS" closes "FUTURES CONTRACTS".
                elif (current is not None and current["subtotal"] is None and len(total[1]) == 2
                        and not current["partial"] and name.startswith(current["heading"])):
                    current["subtotal"] = total[1]
                # Any other TOTAL is a rollup or a row-less line item (payables, cash flows).
                current = None
                continue
            t = _trailing_nums(line, 2)
            if t and current is not None:
                ident, _, title = t[0].partition(" ")
                cost, value = t[1]
                current["rows"] += 1
                current["cost"] += cost or 0.0
                current["value"] += value or 0.0
                if not current["investments"]:
                    continue
                invest_value += value or 0.0
                row = {
                    "issuer_name": "",
                    # One pooled separate account prints only its id.
                    "investment_description": title or ident,
                    "asset_type": current["asset_type"],
                    "asset_type_source": "heading" if current["asset_type"] else "none",
                    "par_value": "",
                    "cost": _fmt(cost),
                    "current_value": _fmt(value),
                    "units_or_shares": "",
                    "asset_identifier": ident,
                    "interest_rate": "",
                    "maturity_date": "",
                    "section_heading": current["heading"],
                    "extraction_method": "sch4i_text",
                    "page_number": page_num,
                    "row_id": len(mapped_rows) + 1,
                }
                if not current["asset_type"]:
                    asset_type = detect_asset_type(f"{current['heading']} {title}")
                    row["asset_type"] = asset_type
                    row["asset_type_source"] = "row" if asset_type else "none"
                row_type = next((label for pattern, label in row_types
                                 if re.search(pattern, title or ident, re.I)), None)
                if row_type:
                    row["asset_type"], row["asset_type_source"] = row_type, "row"
                # Holdings sold during the year (current value 0) count but aren't loaded.
                if value:
                    mapped_rows.append(row)
                continue
            unparsed.append({"page_number": page_num, "line": line})
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    for section in sections:
        section.pop("investments")
        sub = section["subtotal"]
        section["cost_diff"] = None if sub is None else section["cost"] - sub[0]
        section["value_diff"] = None if sub is None else section["value"] - sub[1]
        if sub is None:
            section["reconciled"] = None if section["rows"] else True
        else:
            # Rows and totals are each rounded to whole dollars, so allow $0.50 per row.
            tol = 1.0 + 0.5 * section["rows"]
            section["reconciled"] = abs(section["value_diff"]) <= tol and abs(section["cost_diff"]) <= tol
    grand_reconciled = None
    if grand_total:
        invest_rows = sum(len(p["mapped_rows"]) for p in pages)
        grand_reconciled = abs(invest_value + options.get("grand_value_gap", 0) - grand_total[-1]) <= (
            1.0 + 0.5 * invest_rows)
    return {"pages": pages, "sections": sections, "unparsed": unparsed,
            "grand_total": grand_total, "grand_reconciled": grand_reconciled}


def _parse_bny_h30t(pdf_path: str, page_nums: List[int]) -> Optional[Dict]:
    """parse_schedule_4i_pages for the bny_h30t layout. The pages must start
    at the first section heading so every section opens in range."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies, summary_pages = {}, set()
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            text = _upright_text(pdf.pages[page_num - 1])
            if not _H30T_HEADER_RE.search(text):
                return None
            bodies[page_num] = _h30t_body(text)
            if any(_H30T_SUMMARY_RE.match(l.strip()) for l in text.splitlines()):
                summary_pages.add(page_num)

    # The filer's footer lands anywhere in the body; it is on every page, holdings aren't.
    seen = Counter(l for body in bodies.values() for l in set(body))
    footer = {l for l, n in seen.items()
              if len(page_nums) >= 4 and n * 2 >= len(page_nums) and not _trailing_nums(l, 3)}

    current = None
    sections, unparsed, pages = [], [], []
    grand_total = None
    name_lines: List[str] = []
    holding = None  # whole-trust row whose per-account lines are being summed
    after_rule = False

    def close_holding():
        if holding and abs(holding["accounts"] - holding["value"]) > 0.01 * max(1, holding["n_accounts"]):
            unparsed.append({"page_number": holding["row"]["page_number"],
                             "line": f"per-account lines {holding['accounts']:,.2f} != "
                                     f"{holding['value']:,.2f}: {holding['row']['investment_description']}"})

    for page_num in page_nums:
        mapped_rows = []
        body = [l for l in bodies[page_num] if l not in footer and not l.startswith("RUN DATE:")]
        for i, line in enumerate(body):
            if page_num in summary_pages:
                t = _trailing_nums(line, 3)
                if t and t[0] == "GRAND TOTALS":
                    grand_total = t[1][1:]
                continue
            if _H30T_TOTAL_RULE_RE.match(line):
                after_rule = True
                continue
            if _H30T_RULE_RE.match(line):
                continue
            nxt = body[i + 1] if i + 1 < len(body) else ""
            if _H30T_RULE_RE.match(nxt):
                close_holding()
                holding, name_lines = None, []
                current = {"heading": line, "asset_type": next(
                    (label for pattern, label in _H30T_ASSET_TYPES if re.search(pattern, line, re.I)), ""),
                    "partial": False, "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None}
                sections.append(current)
                continue
            t = _trailing_nums(line, 3)
            if t and not t[0]:
                if after_rule:
                    after_rule = False
                    close_holding()
                    holding = None
                    if current is not None:
                        current["subtotal"] = t[1][1:]
                    continue
                # A holding printed with no description follows straight on from
                # the previous holding's account lines; it is named after its
                # security id below.
                if current is None or not (name_lines or holding):
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                close_holding()
                shares, cost, value = t[1]
                title = " ".join(name_lines)
                name_lines = []
                rate = maturity = ""
                m = _H30T_RATE_DATE_RE.match(title)
                if m:
                    title, rate, maturity = m.groups()
                current["rows"] += 1
                current["cost"] += cost or 0.0
                current["value"] += value or 0.0
                row = {
                    "issuer_name": "",
                    "investment_description": title,
                    "asset_type": current["asset_type"],
                    "asset_type_source": "heading" if current["asset_type"] else "none",
                    "par_value": "",
                    "cost": _fmt(cost),
                    "current_value": _fmt(value),
                    "units_or_shares": _fmt(shares),
                    "asset_identifier": "",
                    "interest_rate": rate,
                    "maturity_date": maturity,
                    "section_heading": current["heading"],
                    "extraction_method": "sch4i_text",
                    "page_number": page_num,
                    "row_id": len(mapped_rows) + 1,
                }
                if not current["asset_type"]:
                    asset_type = detect_asset_type(f"{current['heading']} {title}")
                    row["asset_type"] = asset_type
                    row["asset_type_source"] = "row" if asset_type else "none"
                holding = {"row": row, "value": value or 0.0, "accounts": 0.0, "n_accounts": 0}
                # Holdings sold during the year (current value 0) count but aren't loaded.
                if value:
                    mapped_rows.append(row)
                continue
            if t and holding is not None and _H30T_ACCOUNT_RE.match(line):
                ident = t[0].split()[1] if len(t[0].split()) > 1 else ""
                if not holding["row"]["asset_identifier"] and len(ident) >= 8 and re.search(r"\d", ident):
                    holding["row"]["asset_identifier"] = ident
                if not holding["row"]["investment_description"] and ident:
                    holding["row"]["investment_description"] = f"UNDESCRIBED HOLDING {ident}"
                holding["accounts"] += t[1][2] or 0.0
                holding["n_accounts"] += 1
                continue
            if current is None and not t and _H30T_ACCOUNT_RE.match(line):
                # Roster of manager accounts on the report's first page.
                continue
            if current is not None and not t:
                # Description of the next holding.
                close_holding()
                holding = None
                name_lines.append(line)
                continue
            unparsed.append({"page_number": page_num, "line": line})
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })
    close_holding()

    for section in sections:
        sub = section["subtotal"]
        section["cost_diff"] = None if sub is None else section["cost"] - sub[0]
        section["value_diff"] = None if sub is None else section["value"] - sub[1]
        if sub is None:
            section["reconciled"] = None if section["rows"] else True
        else:
            # Printed to the cent, so only float drift is tolerated.
            section["reconciled"] = abs(section["value_diff"]) <= 1.0 and abs(section["cost_diff"]) <= 1.0
    grand_reconciled = None
    if grand_total:
        grand_reconciled = abs(sum(s["value"] for s in sections) - grand_total[-1]) <= 1.0
    return {"pages": pages, "sections": sections, "unparsed": unparsed,
            "grand_total": grand_total, "grand_reconciled": grand_reconciled}


def _word_line_text(page) -> str:
    """The page's words regrouped into lines by position, for a filing whose
    text order prints a column after the rows it belongs to (PHB's last page
    lists its costs below "Total investments"). Overlapping spaces that split
    digits ("6 79,191") are dropped first."""
    lines: List[List] = []
    for word in sorted(_drop_overlapping_spaces(page).extract_words(), key=lambda w: (w["top"], w["x0"])):
        if lines and abs(word["top"] - lines[-1][0]) <= 2:
            lines[-1][1].append(word)
        else:
            lines.append([word["top"], [word]])
    return "\n".join(" ".join(w["text"] for w in sorted(ws, key=lambda w: w["x0"])) for _, ws in lines)


def _parse_flat_cost_value(pdf_path: str, page_nums: List[int], untyped_as: str = "",
                           row_types: Optional[List] = None, word_lines: bool = False,
                           bare_grand: bool = False, skip: str = "", running_totals: bool = False,
                           shs_split: bool = False, rate_column: bool = False,
                           amount_scale: int = 1) -> Optional[Dict]:
    """parse_schedule_4i_pages for the flat_cost_value layout. The first page
    must carry the column header and the last the "Total investments" line.
    untyped_as (registry) types rows no name pattern matched, for a list whose
    only remaining holdings are private funds (Koch). row_types (registry
    row_asset_types) are [(pattern, asset_type)] tried before _FCV_ASSET_TYPES
    (Nationwide's abbreviated fund names, "VAR RT" bonds). word_lines (registry)
    reads each page with _word_line_text (PHB). bare_grand (registry): the
    grand total prints "$ <cost> $ <value>" with no label (Main Line Health).
    skip (registry): pattern of page-footer lines to drop. running_totals
    (registry): each page ends with a bare "$ <cost> $ <value>" running total
    of every row so far, checked against the rows, and the last is the grand
    total (Teamsters Western). shs_split (registry): rows print "<short name>
    <n> shs <full name>"; the full name is the description and n the units.
    rate_column (registry): every row prints "<rate> [<maturity>]" before its
    amounts, a dividend yield for stocks ("0.000" for funds); both are cut
    from the name; word_lines is needed too, as extract_text splits digits. amount_scale (registry): amounts are in this many dollars
    (ExxonMobil, 2026-10-17)."""
    skip_re = re.compile(skip) if skip else None
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        texts = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            page = pdf.pages[page_num - 1]
            texts[page_num] = _word_line_text(page) if word_lines else page.extract_text() or ""
    if not page_nums or not _FCV_HEADER_RE.search(texts[page_nums[0]]):
        return None
    # Nationwide prints a shares/par column before cost and value.
    n_amounts = 3 if _FCV_PAR_HEADER_RE.search(texts[page_nums[0]]) else 2

    # One unheaded section; every row is typed from its own name.
    section = {"heading": "", "asset_type": "", "partial": False, "rows": 0,
               "cost": 0.0, "value": 0.0, "subtotal": None}
    unparsed, pages = [], []
    grand_total = None
    for page_num in page_nums:
        lines = [l.strip() for l in texts[page_num].splitlines()]
        # Koch repeats the title block and header on every page.
        header_idx = next((i for i, l in enumerate(lines) if _FCV_HEADER_RE.match(l)), None)
        if header_idx is not None:
            lines = lines[header_idx + 1:]
        mapped_rows = []
        for line in lines:
            if not line or _FCV_SKIP_RE.match(line) or (skip_re and skip_re.match(line)):
                continue
            # "$" prints as its own token on the first and total lines.
            clean = " ".join(tok for tok in line.split() if tok != "$")
            t = _trailing_nums(clean, n_amounts)
            if running_totals and t and not t[0]:
                running = t[1][-2:]
                if abs(section["cost"] - running[0]) > 1.0 or abs(section["value"] - running[1]) > 1.0:
                    unparsed.append({"page_number": page_num, "line": line})
                grand_total = None if page_num != page_nums[-1] else running
                continue
            if bare_grand and t and not t[0] and grand_total is None:
                grand_total = t[1][-2:]
                continue
            if _FCV_TOTAL_RE.match(line):
                if t:
                    grand_total = t[1][-2:]
                continue
            if grand_total is not None:
                continue
            if not t or not t[0]:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            title = t[0].lstrip("* ").strip()
            units = ""
            if shs_split:
                m = re.match(r"^.*?\s+([\d,]+)\s+s\w?s\s+(.+)$", title)
                if not m:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                units, title = m.group(1).replace(",", ""), m.group(2)
            par = t[1][0] if n_amounts == 3 else None
            cost, value = t[1][-2:]
            rate_m = None
            if rate_column:
                # The STIF row leaves the rate blank.
                rate_m = re.match(r"^(.*?)\s+(\d+(?:\.\d+)?)(?:\s+(\d{1,2}/\d{1,2}/\d{4}))?$", title)
                if rate_m:
                    title = rate_m.group(1)
            section["rows"] += 1
            section["cost"] += cost or 0.0
            section["value"] += value or 0.0
            if not value:
                continue
            m = _FCV_RATE_DATE_RE.search(title)
            if rate_m:
                m = rate_m.group(2, 3) if rate_m.group(3) else None
            asset_type = next((label for pattern, label in list(row_types or []) + _FCV_ASSET_TYPES
                               if re.search(pattern, title)), untyped_as)
            mapped_rows.append({
                "issuer_name": "",
                "investment_description": title,
                "asset_type": asset_type,
                "asset_type_source": "row" if asset_type else "none",
                "par_value": _fmt(None if par is None else par * amount_scale),
                "cost": _fmt(None if cost is None else cost * amount_scale),
                "current_value": _fmt(value * amount_scale),
                "units_or_shares": units,
                "asset_identifier": "",
                "interest_rate": (m[0] if rate_m else m.group(1)) if m else "",
                "maturity_date": (m[1] if rate_m else m.group(2)) if m else "",
                "section_heading": "",
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            })
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    section["subtotal"] = grand_total
    section["cost_diff"] = None if grand_total is None else section["cost"] - grand_total[0]
    section["value_diff"] = None if grand_total is None else section["value"] - grand_total[1]
    section["reconciled"] = None if grand_total is None else (
        abs(section["value_diff"]) <= 1.0 and abs(section["cost_diff"]) <= 1.0)
    return {"pages": pages, "sections": [section], "unparsed": unparsed, "grand_total": grand_total,
            "grand_reconciled": section["reconciled"]}


def _parse_gain_loss_totals(pdf_path: str, page_nums: List[int],
                            options: Optional[Dict] = None) -> Optional[Dict]:
    """parse_schedule_4i_pages for the gain_loss_totals layout. The pages must
    start at the schedule's first page so every section opens in range.
    options is the plan's registry entry (KeySpan, 2026-10-17):
      headings: pattern of the category headings printed above each
        section's rows; any other line without amounts continues the row
        above it ("DD 07/01/08"), not the one below.
      skip: pattern of lines to drop (page footers).
      row_id: pattern at the start of a row whose group 1 is its asset
        identifier; the whole match is cut from the name.
      bare_grand: the grand total prints "<cost> <value> <gain>" with no
        label (its "GRAND TOTAL" follows on its own line); nothing after it
        is read.
      accounts: with bare_grand, several account reports follow one
        another; their grand totals are summed and reading goes on
        (Highmark, 2026-10-17).
      price_column: rows are "<shares> <description> <cost> <price> <value>
        <gain>", shares first and a unit price between cost and value
        (National Grid's Final Average Pay plan, 2026-10-17).
      trailing_minus: a negative amount prints its sign last ("250.12-").
      joined_total: "TOTAL" runs into its heading ("TOTALOTHERINVESTMENTS").
      grand_label: pattern of the labeled grand total line; nothing after it
        is read (National Grid's 401(h) account follows its "TOTALNETINVESTMENT").
      heading_asset_types: [(pattern, asset_type)] tried on a TOTAL line's
        heading before _CTL_ASSET_TYPES (National Grid's "CORPORATE DEBT").
      x_tolerance: pdfplumber's, for reading the pages; National Grid's word
        gaps are 2.96pt, so the default 3 runs "SILVER LAKEPARTNERSIV LP"
        together (2026-10-17)."""
    options = options or {}
    headings_re = re.compile(options["headings"]) if options.get("headings") else None
    skip_re = re.compile(options["skip"]) if options.get("skip") else None
    row_id_re = re.compile(options["row_id"]) if options.get("row_id") else None
    grand_re = re.compile(options["grand_label"]) if options.get("grand_label") else None
    heading_types = list(options.get("heading_asset_types", [])) + _CTL_ASSET_TYPES
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            text = _page_text(pdf.pages[page_num - 1], "gain_loss_totals", options.get("x_tolerance", 3))
            if not _GLT_HEADER_RE.search(text):
                return None
            bodies[page_num] = _glt_body(text)

    sections, unparsed, pages = [], [], []
    grand_total = None
    # Open section; its name is only printed on its closing TOTAL line.
    current = None
    name_lines: List[str] = []

    def open_section():
        return {"heading": "", "asset_type": "", "partial": False, "rows": 0,
                "cost": 0.0, "value": 0.0, "subtotal": None, "row_dicts": []}

    last_row = None   # headings: the row a wrapped line continues
    after_row = False   # headings: a row (kept or not) was the last line read
    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            if options.get("trailing_minus"):
                line = re.sub(r"(?<!\S)(\d[\d,]*(?:\.\d+)?)-(?!\S)", r"-\1", line)
            if options.get("joined_total"):
                line = re.sub(r"^TOTAL(?=\S)", "TOTAL ", line)
            if ((options.get("bare_grand") or grand_re) and not options.get("accounts") and grand_total is not None) \
                    or (skip_re and skip_re.match(line)):
                continue
            if grand_re and grand_re.match(line):
                t = _trailing_nums(line, 3)
                if t:
                    grand_total = t[1][:2]
                    current, last_row, after_row, name_lines = None, None, False, []
                    continue
            row = _trailing_nums(line, 4)
            if not row:
                t = _trailing_nums(line, 3)
                if t and (_GLT_GRAND_RE.search(t[0]) or (options.get("bare_grand") and not t[0])):
                    if options.get("accounts") and grand_total is not None:
                        grand_total = [grand_total[0] + t[1][0], grand_total[1] + t[1][1]]
                    else:
                        grand_total = t[1][:2]
                    current, last_row, after_row, name_lines = None, None, False, []
                    continue
                if headings_re and not t:
                    if headings_re.match(line):
                        current, last_row, after_row = open_section(), None, False
                        current["heading"] = line
                    elif last_row is not None:
                        last_row["investment_description"] += f" {line}"
                    elif not after_row:
                        # After a zero-value row the line is dropped with it.
                        unparsed.append({"page_number": page_num, "line": line})
                    continue
                if t and t[0].startswith("TOTAL "):
                    heading = t[0][len("TOTAL "):].strip()
                    section = current or open_section()
                    asset_type = next((label for pattern, label in heading_types
                                       if re.search(pattern, heading, re.I)), "")
                    for mapped in section.pop("row_dicts"):
                        mapped["section_heading"] = heading
                        if asset_type:
                            mapped["asset_type"], mapped["asset_type_source"] = asset_type, "heading"
                        else:
                            mapped["asset_type"] = detect_asset_type(
                                f"{heading} {mapped['investment_description']}")
                            mapped["asset_type_source"] = "row" if mapped["asset_type"] else "none"
                    section.update({"heading": heading, "asset_type": asset_type, "subtotal": t[1][:2]})
                    sections.append(section)
                    current, name_lines = None, []
                    continue
                if not _trailing_nums(line, 1):
                    # Wrapped description, carried onto the next row.
                    name_lines.append(line)
                    continue
                unparsed.append({"page_number": page_num, "line": line})
                continue
            if current is None:
                current = open_section()
            text, amounts = row
            if options.get("price_column"):
                lead = text.split(None, 1)
                if not lead or not _NUM_RE.match(lead[0]):
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                shares, text = _num(lead[0]), (lead[1] if len(lead) > 1 else "")
                cost, _price, value, gain = amounts
            else:
                shares, cost, value, gain = amounts
            title = " ".join(name_lines + ([text] if text else []))
            name_lines, last_row, after_row, asset_id = [], None, True, ""
            idm = row_id_re.match(title) if row_id_re else None
            if idm:
                title, asset_id = title[idm.end():].strip(), idm.group(1)
            current["rows"] += 1
            current["cost"] += cost or 0.0
            current["value"] += value or 0.0
            if abs((value or 0.0) - (cost or 0.0) - (gain or 0.0)) > 2:
                unparsed.append({"page_number": page_num, "line": f"value - cost != gain: {line}"})
            if not value or not title:
                continue
            m = _CTL_RATE_DATE_RE.search(title)
            rate, maturity = ((m.group(2), m.group(1)) if m.group(1) else (m.group(3), m.group(4))) \
                if m else ("", "")
            mapped = {
                "issuer_name": "",
                "investment_description": title,
                "asset_type": "",
                "asset_type_source": "none",
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": _fmt(shares),
                "asset_identifier": asset_id,
                "interest_rate": rate,
                "maturity_date": maturity,
                "section_heading": "",
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            }
            current["row_dicts"].append(mapped)
            mapped_rows.append(mapped)
            last_row = mapped
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })
    if current is not None and current["rows"]:
        # Rows after the last TOTAL line have no total to check against.
        current.pop("row_dicts")
        current["partial"] = True
        sections.append(current)

    for section in sections:
        sub = section["subtotal"]
        section["cost_diff"] = None if sub is None else section["cost"] - sub[0]
        section["value_diff"] = None if sub is None else section["value"] - sub[1]
        if sub is None:
            section["reconciled"] = False if section["rows"] else True
        else:
            # Printed in whole dollars, so allow a dollar of rounding per row.
            tol = max(1.0, section["rows"])
            section["reconciled"] = abs(section["value_diff"]) <= tol and abs(section["cost_diff"]) <= tol
    grand_reconciled = None
    if grand_total:
        grand_reconciled = abs(sum(s["value"] for s in sections) - grand_total[1]) <= max(
            1.0, sum(s["rows"] for s in sections))
    return {"pages": pages, "sections": sections, "unparsed": unparsed,
            "grand_total": grand_total, "grand_reconciled": grand_reconciled}


def _parse_nt_par_cost_value(pdf_path: str, page_nums: List[int],
                             alt_span: Optional[tuple] = None,
                             alt_span_exclude: Optional[str] = None,
                             type_rows=()) -> Optional[Dict]:
    """parse_schedule_4i_pages for the nt_par_cost_value layout. alt_span is
    (first-row pattern, last-row pattern) of the plan's partnership run;
    rows in it matching alt_span_exclude are typed by name instead.
    type_rows: [(pattern, asset_type)] tried on a row's name before the name rules."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            text = _page_text(pdf.pages[page_num - 1], "nt_par_cost_value")
            if not _NTP_HEADER_RE.search(text):
                return None
            bodies[page_num] = _ntp_body(text)

    # One unheaded section; rows are typed from their names or the alt span.
    section = {"heading": "", "asset_type": "", "partial": False, "rows": 0,
               "cost": 0.0, "value": 0.0, "subtotal": None}
    unparsed, pages = [], []
    grand_total = None
    in_span = False
    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            t = _trailing_nums(line, 3)
            if not t:
                t2 = _trailing_nums(line, 2)
                if t2 and not t2[0]:
                    grand_total = t2[1]   # Hackensack's bare "<cost> <value>" (2026-10-17)
                    continue
            if not t or not t[0]:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            if t[0] == "TOTAL":
                grand_total = t[1][1:]
                continue
            # "*" marks a party in interest; "***" / "&&&" are custodian flags.
            title = t[0].lstrip("*& ").strip()
            par, cost, value = t[1]
            section["rows"] += 1
            section["cost"] += cost or 0.0
            section["value"] += value or 0.0
            span_row = False
            if alt_span and not in_span and re.match(alt_span[0], title, re.I):
                in_span = True
            if in_span:
                span_row = not (alt_span_exclude and re.search(alt_span_exclude, title, re.I))
                if re.match(alt_span[1], title, re.I):
                    in_span = False
            if not value:
                continue
            if span_row:
                asset_type, source = "Partnership Interest", "span"
            elif line.startswith("* ") and re.search(r"\bCOM\b", title):
                asset_type, source = "Employer Stock", "row"
            else:
                asset_type = next((label for pattern, label in list(type_rows) + _NTP_ASSET_TYPES + _FCV_ASSET_TYPES
                                   if re.search(pattern, title)), "")
                source = "row" if asset_type else "none"
            m = _CTL_RATE_DATE_RE.search(title)
            rate, maturity = ((m.group(2), m.group(1)) if m.group(1) else (m.group(3), m.group(4))) \
                if m else ("", "")
            mapped_rows.append({
                "issuer_name": "",
                "investment_description": title,
                "asset_type": asset_type,
                "asset_type_source": source,
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": _fmt(par),
                "asset_identifier": "",
                "interest_rate": rate,
                "maturity_date": maturity,
                "section_heading": "",
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            })
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    section["subtotal"] = grand_total
    section["cost_diff"] = None if grand_total is None else section["cost"] - grand_total[0]
    section["value_diff"] = None if grand_total is None else section["value"] - grand_total[1]
    # Rows print whole dollars and the total prints cents: a dollar per row.
    tol = max(1.0, section["rows"])
    section["reconciled"] = None if grand_total is None else (
        abs(section["value_diff"]) <= tol and abs(section["cost_diff"]) <= tol)
    return {"pages": pages, "sections": [section], "unparsed": unparsed, "grand_total": grand_total,
            "grand_reconciled": section["reconciled"]}


def _parse_bare_subtotals(pdf_path: str, page_nums: List[int]) -> Optional[Dict]:
    """parse_schedule_4i_pages for the bare_subtotals layout. The first page
    must carry the title and the last the "Grand Total" line."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            bodies[page_num] = _bs_body(_bs_text(pdf.pages[page_num - 1]))
        if not page_nums or not _BS_TITLE_RE.search(_bs_text(pdf.pages[page_nums[0] - 1])):
            return None

    sections, unparsed, pages = [], [], []
    grand_total = None
    current = None
    # Unnumbered lines waiting for a row (wrapped name) or a row-less run (heading).
    pending: List[str] = []
    closed = True  # no section open, or the open one just printed its subtotal
    parent = None  # category a "... FUND, LLC: ASSETS DETAILED BELOW:" block belongs to

    def new_section(heading):
        nonlocal parent
        if _BS_CATEGORY_RE.match(heading) or parent is None:
            parent = heading
            asset_type = next((label for pattern, label in _CTL_ASSET_TYPES
                               if re.search(pattern, heading, re.I)), "")
        else:
            # A detailed-holdings block is part of the category above it.
            asset_type = sections[-1]["asset_type"] if sections else ""
            heading = f"{parent} / {heading}"
        section = {"heading": heading, "asset_type": asset_type, "partial": False,
                   "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None}
        sections.append(section)
        return section

    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            # "n/a" prints in the units column of directly held real estate.
            clean = " ".join("-" if tok.lower() == "n/a" else tok for tok in line.split())
            t2 = _trailing_nums(clean, 2)
            if t2 and _BS_GRAND_RE.match(t2[0]):
                grand_total = t2[1]
                break
            if t2 and not t2[0]:
                # Bare "<cost> <value>": the open section's subtotal.
                if current is None or current["subtotal"] is not None:
                    unparsed.append({"page_number": page_num, "line": line})
                else:
                    current["subtotal"] = t2[1]
                closed, pending = True, []
                continue
            if not t2:
                if pending and (closed or _BS_CATEGORY_RE.match(pending[0])):
                    pending.append(line)  # wrapped heading
                elif closed or _BS_CATEGORY_RE.match(line):
                    pending = [line]
                    closed = True
                else:
                    pending.append(line)  # wrapped row name
                continue
            if closed:
                if pending:
                    current = new_section(" ".join(pending))
                    pending = []
                elif current is None:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                closed = False
            t3 = _trailing_nums(clean, 3)
            row = t3 if t3 and t3[0] else (t2[0], [None] + t2[1])
            title = " ".join(pending + [row[0]])
            pending = []
            units, cost, value = row[1]
            current["rows"] += 1
            current["cost"] += cost or 0.0
            current["value"] += value or 0.0
            if not value:
                continue
            asset_type = current["asset_type"] or detect_asset_type(f"{current['heading']} {title}")
            m = _CTL_RATE_DATE_RE.search(title)
            rate, maturity = ((m.group(2), m.group(1)) if m.group(1) else (m.group(3), m.group(4))) \
                if m else ("", "")
            mapped_rows.append({
                "issuer_name": "",
                "investment_description": title,
                "asset_type": asset_type,
                "asset_type_source": "heading" if current["asset_type"] else ("row" if asset_type else "none"),
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": _fmt(units),
                "asset_identifier": "",
                "interest_rate": rate,
                "maturity_date": maturity,
                "section_heading": current["heading"],
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            })
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    for section in sections:
        sub = section["subtotal"]
        if sub is None and section["rows"] == 1:
            # A one-holding category prints no subtotal; the row is its total.
            sub = section["subtotal"] = [section["cost"], section["value"]]
        section["cost_diff"] = None if sub is None else section["cost"] - (sub[0] or 0.0)
        section["value_diff"] = None if sub is None else section["value"] - sub[1]
        section["reconciled"] = False if sub is None else (
            abs(section["value_diff"]) <= 1.0 and abs(section["cost_diff"]) <= 1.0)
    grand_reconciled = None
    if grand_total:
        grand_reconciled = abs(sum(s["value"] for s in sections) - grand_total[1]) <= 1.0
    return {"pages": pages, "sections": sections, "unparsed": unparsed,
            "grand_total": grand_total, "grand_reconciled": grand_reconciled}


def _parse_inline_category(pdf_path: str, page_nums: List[int]) -> Optional[Dict]:
    """parse_schedule_4i_pages for the inline_category layout. The first page
    must carry the column header and the last the "Grand Total" line."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        texts = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            texts[page_num] = pdf.pages[page_num - 1].extract_text() or ""
    if not page_nums or not _IC_COLUMNS_RE.search(texts[page_nums[0]]):
        return None

    sections: Dict[str, Dict] = {}  # 5500 code -> section, in order of first appearance
    unparsed, pages = [], []
    holdings_total = None
    done = False
    for page_num in page_nums:
        lines = [l.strip() for l in texts[page_num].splitlines()]
        if page_num == page_nums[0]:
            lines = lines[next(i + 1 for i, l in enumerate(lines) if _IC_COLUMNS_RE.match(l)):]
        mapped_rows = []
        for line in lines:
            if done or not line or _IC_SKIP_RE.match(line):
                continue
            clean = _IC_NOTE_RE.sub("", line)
            if holdings_total is not None:
                # Receivable/currency adjustments, then the grand total.
                t = _trailing_nums(clean, 1)
                done = bool(t and _IC_GRAND_RE.match(t[0]))
                continue
            t = _trailing_nums(clean, 3)
            m = _IC_CATEGORY_RE.match(t[0]) if t else None
            if m:
                units, cost, value = t[1]
            else:
                # Cash balances print the value only; a bare value is the holdings total.
                t = _trailing_nums(clean, 1)
                if t and not t[0]:
                    holdings_total = t[1][0]
                    continue
                m = _IC_CATEGORY_RE.match(t[0]) if t else None
                if not m or not m.group(1):
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                units, cost, value = None, None, t[1][0]
            title, code = m.group(1).lstrip("* ").strip(), m.group(2)
            if code not in sections:
                sections[code] = {"heading": code, "partial": False, "rows": 0, "cost": 0.0, "value": 0.0,
                                  "subtotal": None, "cost_diff": None, "value_diff": None, "reconciled": None,
                                  "asset_type": next((label for pattern, label in _CTL_ASSET_TYPES
                                                      + [(r"employer", "Employer Stock")]
                                                      if re.search(pattern, code, re.I)), "")}
            section = sections[code]
            section["rows"] += 1
            section["cost"] += cost or 0.0
            section["value"] += value or 0.0
            if not value:
                continue
            # "OTHER INVESTMENTS" types each row from its name. Private funds carry
            # their own code here, so a name never makes one (short-sold REITs).
            asset_type = section["asset_type"] or next(
                (label for pattern, label in _FCV_ASSET_TYPES if re.search(pattern, title)
                 and label not in ("Partnership Interest", "Hedge Fund")), "")
            rd = _CTL_RATE_DATE_RE.search(title)
            rate, maturity = ((rd.group(2), rd.group(1)) if rd.group(1) else (rd.group(3), rd.group(4))) \
                if rd else ("", "")
            mapped_rows.append({
                "issuer_name": "",
                "investment_description": title,
                "asset_type": asset_type,
                "asset_type_source": "heading" if section["asset_type"] else ("row" if asset_type else "none"),
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": _fmt(units),
                "asset_identifier": "",
                "interest_rate": rate,
                "maturity_date": maturity,
                "section_heading": code,
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            })
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    sections_list = list(sections.values())
    grand_total = None if holdings_total is None else [None, holdings_total]
    # No subtotals to check, so a missing holdings total fails the parse.
    grand_reconciled = done and holdings_total is not None and \
        abs(sum(s["value"] for s in sections_list) - holdings_total) <= 1.0
    return {"pages": pages, "sections": sections_list, "unparsed": unparsed,
            "grand_total": grand_total, "grand_reconciled": grand_reconciled}


def _nhr_body(text: str) -> List[str]:
    """nt_held_regions lines below the column header, footer dropped. Nothing
    after Northern Trust's "Generated by" line is the report's (BJC's filer
    prints its own page number there, 2026-10-16)."""
    lines = [l.strip() for l in text.splitlines()]
    start = next((i + 1 for i, l in enumerate(lines) if _NHR_COLUMNS_RE.match(l)), len(lines))
    # "*Generated by ..." on CenturyLink's (2026-10-17).
    end = next((i for i, l in enumerate(lines) if i >= start and l.lstrip("*").startswith("Generated by Northern Trust")),
               len(lines))
    return [l for l in lines[start:end] if l and not _NHR_FOOTER_RE.match(l)]


def _parse_nt_held_regions(pdf_path: str, page_nums: List[int],
                           hedge_fund_rows: Optional[str] = None, no_grand_total: bool = False,
                           missing_page_totals=None) -> Optional[Dict]:
    """parse_schedule_4i_pages for the nt_held_regions layout. The last page
    must carry the unnamed grand "Total" line.
    hedge_fund_rows: rows in an untyped category whose name matches are typed
    Hedge Fund (Abbott's "Other").
    no_grand_total: the schedule prints none (HCSC, 2026-10-17); the category
    totals alone check it.
    missing_page_totals: {category: [cost, value]} held on a page the filing
    leaves out ("Page 57 of 92" is followed by "Page 59 of 92"); counted in
    the category's and the grand checks, with no rows (TCCC, 2026-10-17)."""
    missing = {k.lower(): v for k, v in (missing_page_totals or {}).items()}
    hedge_re = re.compile(hedge_fund_rows, re.I) if hedge_fund_rows else None
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            text = _nhr_text(pdf.pages[page_num - 1])
            if not _NHR_HEADER_RE.search(text):
                return None
            bodies[page_num] = _nhr_body(text)

    sections, unparsed, pages = [], [], []
    grand_total = None
    current = None  # open category
    group = None  # open "<region> - <currency>" sub-group heading
    # "category": the next unnumbered line is a category heading; "group": a
    # sub-group heading; "row": a continuation of the last row's description.
    expect = "category"
    last_row = None
    for page_num in page_nums:
        mapped_rows = []
        body = bodies[page_num]
        # Every page repeats the open category (and sometimes sub-group) heading.
        while body and current and body[0] in (current["heading"], group):
            body = body[1:]
        for line in body:
            # An ID label whose ID wraps to the next line ("... CUSIP : <shares>
            # <cost> <value>" / "35563P2U2", CenturyLink 2026-10-17).
            line = re.sub(r"\s+(?:CUSIP|SEDOL|ISIN|TICKER)\s*:?(?=\s+-?[\d,]+\.\d+\s)", "", line)
            idm = _NHR_ID_RE.search(line)
            if idm and _trailing_nums(line, 3):
                # An ID printed inside a row's description ("CF WGI EMERGING MARKETS
                # LLC FD CUSIP : 8796521H8 <shares> <cost> <value>", Abbott 2026-10-16).
                line, idm = (line[:idm.start()] + line[idm.end():]).replace("  ", " "), None
            if idm:
                if last_row is not None and not last_row["asset_identifier"]:
                    last_row["asset_identifier"] = idm.group(1) or idm.group(2)
                continue
            t2 = _trailing_nums(line, 2)
            if t2 and (t2[0] == "Total" or t2[0].startswith("Total ")):
                name = t2[0][len("Total"):].strip()
                last_row = None
                if not name:
                    grand_total = t2[1]
                    break
                if current and current["heading"].startswith(name):
                    current["subtotal"] = t2[1]
                    current, group, expect = None, None, "category"
                else:
                    # A sub-group (or "- all currencies") total; the category total checks the sum.
                    group, expect = None, "group"
                continue
            t3 = _trailing_nums(line, 3)
            if not t3 or not t3[0]:
                if t2 or t3:
                    unparsed.append({"page_number": page_num, "line": line})
                elif expect == "category":
                    current = {"heading": line, "partial": False, "rows": 0, "cost": 0.0, "value": 0.0,
                               "subtotal": None,
                               "asset_type": next((label for pattern, label in _CTL_ASSET_TYPES
                                                   if re.search(pattern, line, re.I)), "")}
                    sections.append(current)
                    expect = "group"
                elif expect == "group":
                    group, expect = line, "row"
                elif last_row is not None:
                    last_row["investment_description"] += " " + line
                else:
                    unparsed.append({"page_number": page_num, "line": line})
                continue
            if current is None:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            expect = "row"
            title = t3[0].lstrip("* ").strip()
            units, cost, value = t3[1]
            current["rows"] += 1
            current["cost"] += cost or 0.0
            current["value"] += value or 0.0
            last_row = None
            if not value:
                continue
            # Private funds carry their own category here, so a name never makes one
            # unless the registry names the plan's hedge fund managers.
            asset_type = current["asset_type"] or (
                "Hedge Fund" if hedge_re and hedge_re.search(title) else next(
                    (label for pattern, label in _FCV_ASSET_TYPES if re.search(pattern, title)
                     and label not in ("Partnership Interest", "Hedge Fund")), ""))
            rd = _CTL_RATE_DATE_RE.search(title)
            rate, maturity = ((rd.group(2), rd.group(1)) if rd.group(1) else (rd.group(3), rd.group(4))) \
                if rd else ("", "")
            last_row = {
                "issuer_name": "",
                "investment_description": title,
                "asset_type": asset_type,
                "asset_type_source": "heading" if current["asset_type"] else ("row" if asset_type else "none"),
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": _fmt(units),
                "asset_identifier": "",
                "interest_rate": rate,
                "maturity_date": maturity,
                "section_heading": current["heading"] + (f" / {group}" if group else ""),
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            }
            mapped_rows.append(last_row)
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    for section in sections:
        sub = section["subtotal"]
        gap = missing.get(section["heading"].lower(), [0.0, 0.0])
        section["cost_diff"] = None if sub is None else section["cost"] + gap[0] - sub[0]
        section["value_diff"] = None if sub is None else section["value"] + gap[1] - sub[1]
        section["reconciled"] = False if sub is None else (
            abs(section["value_diff"]) <= 1.0 and abs(section["cost_diff"]) <= 1.0)
    grand_reconciled = False if grand_total is None else \
        abs(sum(s["value"] for s in sections) + sum(v[1] for v in missing.values()) - grand_total[1]) <= 1.0
    if no_grand_total and grand_total is None:
        grand_reconciled = None if not unparsed else False
    return {"pages": pages, "sections": sections, "unparsed": unparsed,
            "grand_total": grand_total, "grand_reconciled": grand_reconciled}


def _alp_body(text: str) -> List[str]:
    """asset_liability_positions lines below the title block, footer dropped."""
    lines = [l.strip() for l in text.splitlines()]
    start = next((i + 1 for i, l in enumerate(lines) if _ALP_COLUMNS_RE.match(l)), len(lines))
    end = next((i for i, l in enumerate(lines) if i >= start and _ALP_FOOTER_RE.match(l)), len(lines))
    return [l for l in lines[start:end] if l]


def _parse_asset_liability_positions(pdf_path: str, page_nums: List[int]) -> Optional[Dict]:
    """parse_schedule_4i_pages for the asset_liability_positions layout. The
    pages must run from the first statement page to "Total Assets and Accruals"."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            text = pdf.pages[page_num - 1].extract_text() or ""
            if not _ALP_COLUMNS_RE.search(text):
                return None
            bodies[page_num] = _alp_body(text)

    sections, unparsed, pages = [], [], []
    stack = []  # open headings, outermost first: {"heading", "cost", "value", "section"}
    grand_total, accrued, parents_ok = None, None, True
    row = None  # last row; its description runs on until its id line
    done = False
    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            if done:
                break
            g = _ALP_GRAND_RE.match(line)
            if g:
                grand_total, done = [_num(g.group(1)), _num(g.group(2))], True
                break
            t = _ALP_TOTAL_RE.match(line)
            if t:
                row = None
                name, cost, value = t.group(1), _num(t.group(2)), _num(t.group(3))
                if name == "Accrued Income":
                    accrued = value
                    while stack and stack[-1]["heading"] != "Accruals":
                        stack.pop()
                    stack = stack[:-1]
                    continue
                depth = next((i for i in range(len(stack) - 1, -1, -1) if stack[i]["heading"] == name), None)
                if depth is None:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                node = stack[depth]
                if node["section"] is not None:
                    node["section"]["subtotal"] = [cost, value]
                elif abs(node["value"] - value) > 1.0 or abs(node["cost"] - cost) > 1.0:
                    parents_ok = False
                    unparsed.append({"page_number": page_num, "line": f"{line} (rows sum {node['value']:,.2f})"})
                del stack[depth:]
                continue
            r = _ALP_ROW_RE.match(line)
            if r:
                if not stack:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                node = stack[-1]
                if node["section"] is None:
                    node["section"] = {"heading": " / ".join(n["heading"] for n in stack), "partial": False,
                                       "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None,
                                       "asset_type": ""}
                    node["section"]["asset_type"] = next((label for pattern, label in _ALP_HEADING_TYPES
                                                          if re.search(pattern, node["section"]["heading"])), "")
                    sections.append(node["section"])
                section = node["section"]
                units, cost, value = _num(r.group(3)), _num(r.group(5)), _num(r.group(6))
                section["rows"] += 1
                section["cost"] += cost or 0.0
                section["value"] += value or 0.0
                for n in stack:
                    n["cost"] += cost or 0.0
                    n["value"] += value or 0.0
                row = {
                    "issuer_name": "",
                    "investment_description": r.group(2),
                    "asset_type": section["asset_type"],
                    "asset_type_source": "heading" if section["asset_type"] else "none",
                    "par_value": "",
                    "cost": _fmt(cost),
                    "current_value": _fmt(value),
                    "units_or_shares": _fmt(abs(units) if units is not None else None),
                    "asset_identifier": "",
                    "interest_rate": "",
                    "maturity_date": "",
                    "section_heading": section["heading"],
                    "extraction_method": "sch4i_text",
                    "page_number": page_num,
                    "row_id": len(mapped_rows) + 1,
                    "_ticker": "",
                }
                if value:
                    # A zero row still owns its description and id lines.
                    mapped_rows.append(row)
                continue
            m = _ALP_ID_RE.match(line)
            if m and row is not None:
                row["asset_identifier"], row["_ticker"] = m.group(1), m.group(2) or ""
                row = None
                continue
            if row is not None:
                # Description continues ("Group #G18461 2.500% Dtd 04/01/2013 103.361 97.231 0.02500"),
                # the unit prices and rate trailing it.
                text = re.sub(r"(?:\s+-?[\d,]*\.\d+)+$", "", line)
                if not re.match(r"^Last\s+Priced\b", text) and text:
                    row["investment_description"] += " " + text
                continue
            if stack and stack[-1]["heading"] == "Accruals":
                continue  # "Dividends <cost> <market> ...", checked through "Total Accrued Income"
            if re.search(r"\d\.\d\d$", line) or m:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            stack.append({"heading": line, "cost": 0.0, "value": 0.0, "section": None})
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    for page in pages:
        for m in page["mapped_rows"]:
            ticker = m.pop("_ticker")
            if m["asset_type"] == "Bond":
                # "... 2.500% Dtd 04/01/2013 Due 04/01/2028 IPD14"
                rate = re.search(r"\b(\d{1,2}\.\d+)%", m["investment_description"])
                due = re.search(r"\bDue\s+(\d{2}/\d{2}/\d{4})\b", m["investment_description"])
                m["interest_rate"], m["maturity_date"] = rate.group(1) if rate else "", due.group(1) if due else ""
            if m["asset_type"]:
                continue
            m["asset_type"] = "Mutual Fund" if ticker else next(
                (label for pattern, label in _ALP_FUND_TYPES if re.search(pattern, m["investment_description"])),
                "Common/Collective Trust Fund")
            m["asset_type_source"] = "row"
    for section in sections:
        sub = section["subtotal"]
        section["cost_diff"] = None if sub is None else section["cost"] - sub[0]
        section["value_diff"] = None if sub is None else section["value"] - sub[1]
        section["reconciled"] = False if sub is None else (
            abs(section["value_diff"]) <= 1.0 and abs(section["cost_diff"]) <= 1.0)
    grand_reconciled = False if grand_total is None or not parents_ok or stack else \
        abs(sum(s["value"] for s in sections) + (accrued or 0.0) - grand_total[1]) <= 1.0
    return {"pages": pages, "sections": sections, "unparsed": unparsed,
            "grand_total": grand_total, "grand_reconciled": grand_reconciled}


def _nme_body(text: str) -> List[str]:
    """nt_market_erisa lines below the column header, footer dropped."""
    lines = [l.strip() for l in text.splitlines()]
    start = next((i + 1 for i, l in enumerate(lines) if _NME_COLUMNS_RE.match(l)), len(lines))
    end = next((i for i, l in enumerate(lines) if i >= start and _NME_FOOTER_RE.match(l)), len(lines))
    return [_nme_join_split_amounts(l) for l in lines[start:end] if l]


def _nme_join_split_amounts(line: str) -> str:
    """Every amount here prints with decimals, so a bare digit run after a
    decimal amount among the line's trailing amounts is that amount's tail
    ("Total 10,749,742,228.9 7 ...", PepsiCo 2026-10-18). Two amounts run
    together ("-969,442,656.25-1,005,122,079.12") are split."""
    tokens = re.sub(r"(?<=\d,\d{3}\.\d\d)(?=-\d)", " ", line).split()
    i = len(tokens)
    while i > 0 and _NUM_RE.match(tokens[i - 1]):
        i -= 1
    tail = tokens[i:]
    j = 0
    while j < len(tail) - 1:
        if re.match(r"^-?[\d,]*\.\d+$", tail[j]) and re.match(r"^\d+$", tail[j + 1]):
            tail[j:j + 2] = [tail[j] + tail[j + 1]]
        else:
            j += 1
    return " ".join(tokens[:i] + tail)


def _parse_nt_market_erisa(pdf_path: str, page_nums: List[int]) -> Optional[Dict]:
    """parse_schedule_4i_pages for the nt_market_erisa layout. The last page
    must carry the unnamed grand "Total" line."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            text = _upright_text(pdf.pages[page_num - 1])
            if not _NME_COLUMNS_RE.search(text):
                return None
            bodies[page_num] = _nme_body(text)

    sections, unparsed, pages = [], [], []
    grand_total = None
    current = None  # open category
    group = None  # open "<region> - <currency>" sub-group heading
    # "category": the next unnumbered line is a category heading; "group": a
    # sub-group heading; "row": a description line (of the last row, or of
    # the next row when that one prints its amounts alone).
    expect = "category"
    last_row = None
    pending = None  # (row, text) last appended to a row's description
    done = False
    for page_num in page_nums:
        mapped_rows = []
        body = bodies[page_num]
        # Every page repeats the open category (and sometimes sub-group) heading.
        while body and current and body[0] in (current["heading"], group):
            body = body[1:]
        for line in body:
            if done:
                break
            t4 = _trailing_nums(line, 4)
            if t4 and (t4[0] == "Total" or t4[0].startswith("Total ")):
                name = t4[0][len("Total"):].strip()
                last_row, pending = None, None
                market, cost = t4[1][0], t4[1][1]
                if not name:
                    grand_total, done = [cost, market], True
                    break
                if current and current["heading"].startswith(name):
                    current["subtotal"] = [cost, market]
                    current, group, expect = None, None, "category"
                else:
                    # A sub-group (or "- all currencies") total; the category total checks the sum.
                    group, expect = None, "group"
                continue
            t6 = _trailing_nums(line, 6)
            if not t6:
                if _ID_RE.match(line) or re.match(r"^MAT:\s", line):
                    if last_row is not None and not last_row["asset_identifier"]:
                        last_row["asset_identifier"] = line.split()[-1]
                    continue
                if _trailing_nums(line, 2):
                    unparsed.append({"page_number": page_num, "line": line})
                elif expect == "category":
                    current = {"heading": line, "partial": False, "rows": 0, "cost": 0.0, "value": 0.0,
                               "subtotal": None,
                               "asset_type": next((label for pattern, label in _CTL_ASSET_TYPES
                                                   if re.search(pattern, line, re.I)), "")}
                    sections.append(current)
                    expect = "group"
                elif expect == "group" and re.search(r"\s-\s[A-Z]{3}$", line):
                    group, expect = line, "row"
                elif last_row is not None:
                    # "CLASS C998035125": description tail plus the row's asset id.
                    words = line.split()
                    if len(words) > 1 and _ID_RE.match(words[-1]):
                        if not last_row["asset_identifier"]:
                            last_row["asset_identifier"] = words[-1]
                        line = " ".join(words[:-1])
                    last_row["investment_description"] += " " + line
                    pending = (last_row, line)
                elif current is not None:
                    pending = (None, line)
                else:
                    unparsed.append({"page_number": page_num, "line": line})
                continue
            if current is None:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            expect = "row"
            title = re.sub(r"(?:^|\s+)-$", "", t6[0].lstrip("* ").strip())
            if not title and pending is not None:
                # The description printed on the line above the amounts.
                owner, text = pending
                if owner is not None:
                    owner["investment_description"] = owner["investment_description"][:-len(text) - 1]
                title = text
            pending = None
            units, _price, value, cost = t6[1][0], t6[1][1], t6[1][2], t6[1][3]
            current["rows"] += 1
            current["cost"] += cost or 0.0
            current["value"] += value or 0.0
            last_row = None
            if not value:
                continue
            asset_type = current["asset_type"] or next(
                (label for pattern, label in _FCV_ASSET_TYPES if re.search(pattern, title)
                 and label not in ("Partnership Interest", "Hedge Fund")), "")
            rd = _CTL_RATE_DATE_RE.search(title)
            rate, maturity = ((rd.group(2), rd.group(1)) if rd.group(1) else (rd.group(3), rd.group(4))) \
                if rd else ("", "")
            last_row = {
                "issuer_name": "",
                "investment_description": title,
                "asset_type": asset_type,
                "asset_type_source": "heading" if current["asset_type"] else ("row" if asset_type else "none"),
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": _fmt(abs(units) if units is not None else None),
                "asset_identifier": "",
                "interest_rate": rate,
                "maturity_date": maturity,
                "section_heading": current["heading"] + (f" / {group}" if group else ""),
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            }
            mapped_rows.append(last_row)
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    for section in sections:
        sub = section["subtotal"]
        section["cost_diff"] = None if sub is None else section["cost"] - sub[0]
        section["value_diff"] = None if sub is None else section["value"] - sub[1]
        section["reconciled"] = False if sub is None else (
            abs(section["value_diff"]) <= 1.0 and abs(section["cost_diff"]) <= 1.0)
    grand_reconciled = False if grand_total is None else \
        abs(sum(s["value"] for s in sections) - grand_total[1]) <= 1.0
    return {"pages": pages, "sections": sections, "unparsed": unparsed,
            "grand_total": grand_total, "grand_reconciled": grand_reconciled}


def _nad_body(text: str) -> List[str]:
    """nt_asset_detail lines below the column header, notes and footer dropped."""
    lines = [l.strip() for l in text.splitlines()]
    start = next((i + 1 for i, l in enumerate(lines) if _NAD_COLUMNS_RE.match(l)), len(lines))
    return [l for l in lines[start:] if l and not _NAD_SKIP_RE.match(l)]


def _nad_nums(line: str) -> Optional[List[float]]:
    """A line of amounts only (at least six), else None."""
    toks = line.split()
    if len(toks) < 6 or not all(_NAD_NUM_RE.match(t) for t in toks):
        return None
    return [_num(t) for t in toks]


def _parse_nt_asset_detail(pdf_path: str, page_nums: List[int]) -> Optional[Dict]:
    """parse_schedule_4i_pages for the nt_asset_detail layout. Every amount
    line ends "<market value> <cost> <gain market> <gain translation> <gain
    total>", so value and cost are read from the right. Sections are
    sub-categories, checked against their "Total <sub-category>" line; the
    category and region totals are rollups and are skipped."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            text = pdf.pages[page_num - 1].extract_text() or ""
            if not _NAD_HEADER_RE.search(text):
                return None
            bodies[page_num] = _nad_body(text)

    sections, unparsed, pages = [], [], []
    grand_total = None
    category = None  # open category heading
    current = None  # open sub-category section
    region = None
    headings: List[str] = []  # unnumbered heading lines not yet assigned
    pending_total = None  # "Total <name>" waiting for its amounts line
    row_name = None  # description waiting for its amounts line
    row_id = ""
    same = lambda a, b: a is not None and b is not None and a.lower() == b.lower()

    def flush(page_num):
        # One heading opens a sub-category under the open category, two open
        # both; a repeat of the open ones (every page's top) changes nothing.
        nonlocal category, current, headings
        hs = [h for h in headings if not (same(h, category) or (current and same(h, current["sub"])))]
        if len(headings) >= 2 and not same(headings[-2], category):
            category = headings[-2]
        if hs:
            sub = hs[-1]
            key = f"{category} / {sub}" if category else sub
            current = {"heading": key, "sub": sub, "partial": False, "rows": 0, "cost": 0.0, "value": 0.0,
                       "subtotal": None, "asset_type": next(
                           (label for pattern, label in _NAD_ALT_TYPES if re.search(pattern, key, re.I)), None)}
            if current["asset_type"] is None:
                current["fallback"] = next(
                    (label for pattern, label in _NAD_ASSET_TYPES if re.search(pattern, sub if label == "" else key,
                                                                               re.I)), "")
            sections.append(current)
        headings = []

    for page_num in page_nums:
        mapped_rows = []
        body = bodies[page_num]
        for i, line in enumerate(body):
            nums = _nad_nums(line)
            if pending_total is not None:
                name, pending_total = pending_total, None
                if nums is None:
                    unparsed.append({"page_number": page_num, "line": f"Total {name}"})
                elif not name:
                    grand_total = [nums[-4], nums[-5]]
                    break
                elif current and same(name, current["sub"]):
                    current["subtotal"] = [nums[-4], nums[-5]]
                    current, region = None, None
                    continue
                elif same(name, category):
                    category, region = None, None
                    continue
                else:
                    unparsed.append({"page_number": page_num, "line": f"Total {name}"})
                    continue
            if line == "Total" or (line.startswith("Total ") and not re.search(r"\d", line)):
                flush(page_num)
                pending_total = line[len("Total"):].strip()
                continue
            if line.startswith("Total ") and _trailing_nums(line, 6):
                continue  # region / currency rollup
            if row_name is not None and nums is None:
                if " " not in line:
                    continue  # ticker or manager id under the description
                unparsed.append({"page_number": page_num, "line": row_name})
                row_name = None
            if nums is not None:
                if row_name is None:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                if headings:
                    flush(page_num)
                if current is None:
                    unparsed.append({"page_number": page_num, "line": row_name})
                    row_name = None
                    continue
                value, cost = nums[-5], nums[-4]
                current["rows"] += 1
                current["value"] += value
                current["cost"] += cost
                name, row_name = row_name, None
                if not value:
                    continue
                row_type = "" if current["asset_type"] else next(
                    (label for pattern, label in _NAD_ROW_TYPES if re.search(pattern, name)), "")
                asset_type = current["asset_type"] or row_type or current["fallback"]
                mapped_rows.append({
                    "issuer_name": "",
                    "investment_description": name,
                    "asset_type": asset_type,
                    "asset_type_source": "row" if row_type else ("heading" if asset_type else "none"),
                    "par_value": "",
                    "cost": _fmt(cost),
                    "current_value": _fmt(value),
                    "units_or_shares": _fmt(nums[0]) if len(nums) >= 8 else "",
                    "asset_identifier": row_id,
                    "interest_rate": "",
                    "maturity_date": "",
                    "section_heading": current["heading"] + (f" / {region}" if region else ""),
                    "extraction_method": "sch4i_text",
                    "page_number": page_num,
                    "row_id": len(mapped_rows) + 1,
                })
                continue
            idm = _NAD_ID_RE.search(line)
            if idm:
                if headings:
                    flush(page_num)
                row_name, row_id = line[:idm.start()].strip(), idm.group(1)
                continue
            nxt = body[i + 1] if i + 1 < len(body) else ""
            if _nad_nums(nxt) is not None and not _NAD_REGION_RE.search(line):
                # A holding without an id ("USD - United States dollar" cash).
                if headings:
                    flush(page_num)
                row_name, row_id = line, ""
                continue
            if _NAD_REGION_RE.search(line):
                if headings:
                    flush(page_num)
                region = line
                continue
            headings.append(line)
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })
        if grand_total is not None:
            break

    for section in sections:
        fallback = section.pop("fallback", "")
        section["asset_type"] = section["asset_type"] or fallback
        sub = section["subtotal"]
        section["cost_diff"] = None if sub is None else section["cost"] - sub[0]
        section["value_diff"] = None if sub is None else section["value"] - sub[1]
        section["reconciled"] = False if sub is None else (
            abs(section["value_diff"]) <= 1.0 and abs(section["cost_diff"]) <= 1.0)
    grand_reconciled = False if grand_total is None else \
        abs(sum(s["value"] for s in sections) - grand_total[1]) <= 1.0
    return {"pages": pages, "sections": sections, "unparsed": unparsed,
            "grand_total": grand_total, "grand_reconciled": grand_reconciled}


def _ta_num(tok: str) -> Optional[float]:
    """A titled_appendix amount, split first digit rejoined; None unless it groups by thousands."""
    tok = tok.replace(" ", "")
    return _num(tok) if _TA_GROUPED_RE.match(tok) else None


def _parse_titled_appendix(pdf_path: str, page_nums: List[int]) -> Optional[Dict]:
    """parse_schedule_4i_pages for the titled_appendix layout. The last page
    must carry the "Total Limited Partnership(s) and Other Private Equity" line."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        texts = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            texts[page_num] = pdf.pages[page_num - 1].extract_text() or ""
            if not (_TA_TITLE_RE.search(texts[page_num]) and _TA_COLUMNS_RE.search(texts[page_num])):
                return None

    heading = "Limited Partnerships and Other Private Equity"
    section = {"heading": heading, "asset_type": "Partnership Interest", "partial": False,
               "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None}
    unparsed, pages = [], []
    for page_num in page_nums:
        lines = [l.strip() for l in texts[page_num].splitlines()]
        start = next(i + 1 for i, l in enumerate(lines) if _TA_COLUMNS_RE.match(l))
        mapped_rows = []
        for line in lines[start:]:
            if not line:
                continue
            if _TA_TOTAL_RE.match(line):
                m = _TA_TOTAL_LINE_RE.match(line)
                cost, value = (_ta_num(m.group(1)), _ta_num(m.group(2))) if m else (None, None)
                if value is None:
                    unparsed.append({"page_number": page_num, "line": line})
                else:
                    section["subtotal"] = [cost, value]
                break
            m = _TA_ROW_RE.match(line)
            cost, value = (_ta_num(m.group(3)), _ta_num(m.group(4))) if m else (None, None)
            if not m or value is None or (cost is None and m.group(3) != "-"):
                unparsed.append({"page_number": page_num, "line": line})
                continue
            section["rows"] += 1
            section["cost"] += cost or 0.0
            section["value"] += value
            if not value:
                continue
            mapped_rows.append({
                "issuer_name": "",
                "investment_description": m.group(1),
                "asset_type": "Partnership Interest",
                "asset_type_source": "heading",
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": "",
                "asset_identifier": "",
                "interest_rate": "",
                "maturity_date": "",
                "section_heading": heading,
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            })
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    sub = section["subtotal"]
    # Whole-dollar rows: each may round by up to $0.50.
    tol = max(1.0, 0.5 * section["rows"])
    section["cost_diff"] = None if sub is None or sub[0] is None else section["cost"] - sub[0]
    section["value_diff"] = None if sub is None else section["value"] - sub[1]
    section["reconciled"] = sub is not None and abs(section["value_diff"]) <= tol and (
        section["cost_diff"] is None or abs(section["cost_diff"]) <= tol)
    return {"pages": pages, "sections": [section], "unparsed": unparsed,
            "grand_total": sub, "grand_reconciled": section["reconciled"]}


def _parse_typed_description(pdf_path: str, page_nums: List[int],
                             exclude_rows: Optional[str] = None) -> Optional[Dict]:
    """parse_schedule_4i_pages for the typed_description layout. The last page
    must carry the parties-in-interest note. Nothing is printed to reconcile
    to, so any line that isn't a typed row fails the parse instead.
    exclude_rows: rows whose name matches are parsed but not emitted (a pool
    whose holdings are listed too); they are returned under "excluded"."""
    exclude_re = re.compile(exclude_rows, re.I) if exclude_rows else None
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        texts = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            texts[page_num] = pdf.pages[page_num - 1].extract_text() or ""
            if not _TD_COLUMNS_RE.search(texts[page_num]):
                return None

    sections: Dict[str, Dict] = {}
    unparsed, pages, excluded = [], [], []
    ended = False
    for page_num in page_nums:
        lines = [l.strip() for l in texts[page_num].splitlines()]
        start = next(i + 1 for i, l in enumerate(lines) if _TD_COLUMNS_RE.match(l))
        mapped_rows = []
        for line in lines[start:]:
            if not line:
                continue
            if _TD_END_RE.match(line):
                ended = True
                break
            m = _TD_ROW_RE.match(line)
            if not m:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            title, kind = m.group(1), m.group(2)
            cost, value = (None if g == "-" else _num(g) for g in (m.group(3), m.group(4)))
            if exclude_re and exclude_re.search(title):
                excluded.append({"page_number": page_num, "line": line, "value": value})
                continue
            section = sections.setdefault(kind, {"heading": kind, "asset_type": _TD_TYPES[kind] or "",
                                                 "partial": False, "rows": 0, "cost": 0.0, "value": 0.0,
                                                 "subtotal": None, "cost_diff": None, "value_diff": None,
                                                 "reconciled": None})
            section["rows"] += 1
            section["cost"] += cost or 0.0
            section["value"] += value or 0.0
            if not value:
                continue
            asset_type = _TD_TYPES[kind] or (
                "Common/Collective Trust Fund" if _TD_INDEX_CIT_RE.search(title) else "Hedge Fund")
            mapped_rows.append({
                "issuer_name": "",
                "investment_description": title,
                "asset_type": asset_type,
                "asset_type_source": "heading" if _TD_TYPES[kind] else "row",
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": "",
                "asset_identifier": "",
                "interest_rate": "",
                "maturity_date": "",
                "section_heading": kind,
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            })
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })
        if ended:
            break

    return {"pages": pages, "sections": list(sections.values()), "unparsed": unparsed,
            "excluded": excluded,
            "grand_total": None, "grand_reconciled": False if unparsed or not ended else None}


def _parse_typed_maturity_rate(pdf_path: str, page_nums: List[int],
                               options: Optional[Dict] = None) -> Optional[Dict]:
    """parse_schedule_4i_pages for the typed_maturity_rate layout. The pages
    must start at the first section heading and end at the grand total.
    options is the plan's registry entry:
      row_rounding: half a dollar per row when a filer rounds each row but
        totals the unrounded amounts (3M).
      value_only: rows and totals carry a current value only, any share count
        printed just before it (Lone Pine, H-E-B).
      blank_cost: a row may print a value with no cost (Les Schwab).
      heading_asset_types: [(pattern, asset_type)] tried on a heading before
        _TMR_ASSET_TYPES (Les Schwab's privately managed funds).
      row_asset_types: [(pattern, asset_type)] tried on a row's line when its
        section has no type (Elliott's one fund, H-E-B's "HEDGE FUNDS" column).
      wrapped_row: pattern ending a row's first line when its name wraps and
        the amounts print on the next line (H-E-B); the match is dropped.
      grand_cost_check: False when the printed grand total's cost disagrees
        with the filer's own section totals (Les Schwab, $15).
      headings: pattern every section heading matches; any other line without
        amounts is the first line of a wrapped name, and a "Total ..." line is
        a subtotal only when it totals a heading (Directors Guild, whose rows
        include "TOTAL FINA ELF CAP").
      bare_heading_total: a section may close with "<heading> <cost> <value>",
        no "Total" (Directors Guild's Interest-Bearing Cash).
      missing_rows: {heading: [cost, value]} of a row the filer's printout lost
        that its section total and the grand total still include (United
        Association National Pension Fund's stocks, one row short on p.57).
      colon_headings: only a line ending in ":" is a heading; any other line
        without amounts is the first line of a wrapped name, heading or
        "Total ..." subtotal (IUPAT's "U.S. Government and" / "Government
        Agency obligations:").
      bare_headings: under colon_headings, pattern of a heading line the filer
        printed without its colon (Central Pennsylvania Teamsters' "Common
        collective trust - equity", 2026-10-17).
      closing_names: no headings; rows open a section that its
        "TOTAL <name> <par> <cost> <value>" names and types, and the grand
        total may print a value only (Pentegra Defined Benefit Plan).
      value_only_grand: the grand total prints a value only (Building Service
        32BJ's "TOTAL INVESTMENTS $ <value>").
      subtotal_cost_errata: {heading: amount} the filer's printed cost
        subtotal exceeds its rows' costs by, its value still tying (32BJ's
        limited partnerships subtotal adds in the 103-12 row's cost above it;
        its common investment funds subtotal leaves out Altrinsic's cost).
      clipped_name: pattern a row's whole name matches when the filer's
        printout cut it to its last few characters ("0.85", "3", a lone
        "PARTNERSHIP"); it is kept, named "<heading>: <fragment> (name not
        printed)", where staging would drop the fragment as junk (Directors
        Guild, 527 rows / $179M, 2026-10-17).
      own_section_rows: [(pattern, heading, asset_type)]: a row matching
        outside an open section is a section of its own, its amounts its
        subtotal (Abbott - AbbVie's interest bearing cash, 2026-10-17).
      amount_scale: rows' amounts are in this many dollars; totals reconcile
        as printed (Intermountain's thousands, 2026-10-17).
      skip_lines: pattern of lines to drop (LIUNA's "Investment Manager - ..."
        captions inside a section, 2026-10-17).
      join_next: pattern of a line that wraps onto the next, heading or
        "Total ..." alike (LIUNA's "U.S. Government, government guaranteed
        and" / "government agency obligations").
      group_totals: a bare "<cost> <value>" inside a section totals its
        rows since the last one, not the section (LIUNA's per-manager totals).
      leading_id: every row's name starts with its CUSIP or SEDOL, which
        fills asset_identifier; the name is the rest of the line before the
        amounts (Factory Mutual, 2026-10-17).
      leading_party: pattern of the custodian a row's identity column names;
        the name is the description after it (Operating Engineers' Local 324:
        "BNY MELLON <fund> <cost> <value>", 2026-10-20).
      grand_clipped_digits: the grand total's last digits fall off the page
        edge (Factory Mutual's "$ 2,693,464,31", one digit short); its
        printed digits are scaled back up and checked to that many places.
      grand_re: pattern of the grand total line, in place of _TMR_GRAND_RE,
        here and in find_schedule_4i_pages (NRF Legacy's common stocks close
        with "$ <cost> $ <value>", Carpenters NorCal's grand total; its own
        reads "Total investments $ ..."; 2026-10-17).
    A heading ending "(continued)" or "- Continued" (Abbott - AbbVie) reopens
    its section if still open; a closed section's heading printed again opens
    a new section (USAA's 401(h) account repeats the pension's headings)."""
    options = options or {}
    colon_headings = options.get("colon_headings", False)
    bare_re = re.compile(options["bare_headings"], re.I) if options.get("bare_headings") else None
    closing_names = options.get("closing_names", False)
    missing = {k.lower(): v for k, v in options.get("missing_rows", {}).items()}
    cost_errata = {k.lower(): v for k, v in options.get("subtotal_cost_errata", {}).items()}
    headings_re = re.compile(options["headings"], re.I) if options.get("headings") else None
    row_rounding = options.get("row_rounding", False)
    value_only = options.get("value_only", False)
    n_amounts = 1 if value_only else 2
    heading_types = list(options.get("heading_asset_types", [])) + _TMR_ASSET_TYPES
    row_types = options.get("row_asset_types", [])
    scale = options.get("amount_scale", 1)
    skip_re = re.compile(options["skip_lines"]) if options.get("skip_lines") else None
    join_re = re.compile(options["join_next"]) if options.get("join_next") else None
    party_re = re.compile(options["leading_party"]) if options.get("leading_party") else None
    held = None   # join_next: the line wrapping onto this one

    def scaled(x: Optional[float]) -> Optional[float]:
        return None if x is None else x * scale

    wrapped_re = re.compile(options["wrapped_row"]) if options.get("wrapped_row") else None
    grand_re = re.compile(options["grand_re"], re.I) if options.get("grand_re") else _TMR_GRAND_RE
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        text_lines = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            # Padding spaces drawn over digits split "24,894" into "2 4,894".
            page = _decode_calibri_cids(pdf.pages[page_num - 1])
            text_lines[page_num] = _drop_overlapping_spaces(page).extract_text_lines(
                return_chars=True)
        # 3M prints the header only on its first page.
        if not any(_TMR_COLUMNS_RE.match(tl["text"].strip()) for tl in text_lines[page_nums[0]]):
            return None

    def close(section: Dict, subtotal: List[float]) -> None:
        section["subtotal"] = subtotal
        gap = missing.get(section["heading"].lower(), [0.0, 0.0])
        errata = cost_errata.get(section["heading"].lower(), 0.0)
        section["cost_diff"] = section["cost"] + gap[0] + errata - subtotal[0]
        section["value_diff"] = section["value"] + gap[1] - subtotal[1]
        tol = 1.0 + (0.5 * section["rows"] if row_rounding else 0.0)
        section["reconciled"] = abs(section["value_diff"]) <= tol and abs(section["cost_diff"]) <= tol

    def amounts(t: Optional[tuple]) -> Optional[List[float]]:
        # [cost, value]; value-only schedules print no cost.
        return None if not t else [0.0] + t[1] if value_only else t[1]

    def heading_key(heading: str) -> str:
        # H-E-B continues "Limited Partnerships" as "Limited Partnership (continued)".
        return re.sub(r"s\b", "", heading.lower())

    sections, unparsed, pages = [], [], []
    current = parent = None
    grand_total = None
    pending = None   # first line of a wrapped row
    section_rows = []   # closing_names: mapped rows of the open section
    for page_num in page_nums:
        lines = [tl["text"].strip() for tl in text_lines[page_num]]
        start = next((i + 1 for i, l in enumerate(lines) if _TMR_COLUMNS_RE.match(l)), 0)
        mapped_rows = []
        for line, tl in zip(lines[start:], text_lines[page_num][start:]):
            if not line or re.fullmatch(r"\d+", line) or grand_total is not None or _TMR_SKIP_RE.match(line):
                continue
            if skip_re and skip_re.match(line):
                continue
            if join_re and join_re.match(line):
                held = line
                continue
            if held is not None:
                line, held = f"{held} {line}", None
            # Les Schwab prints a negative cost as "( 2,365,804)".
            clean = re.sub(r"\(\s+(?=\d)", "(", " ".join(tok for tok in line.split() if tok != "$"))
            if colon_headings:
                if pending is not None and pending.startswith("Total "):
                    # A wrapped subtotal label.
                    line = clean = f"{pending} {clean}"
                    pending = None
                elif not _trailing_nums(clean, 2) or re.search(r"\s-\s+(?:19|20)\d\d$", clean):
                    # Every row prints a cost and a value; "Adams Street Partnership
                    # Fund - 2009" is a wrapped name, not a "-" cost and a 2,009 value.
                    if not line.endswith(":") and not (bare_re and bare_re.match(clean)):
                        pending = f"{pending} {clean}".strip() if pending else clean
                        continue
                    # A heading, perhaps wrapped; "(continued):" reopens one.
                    line = f"{pending} {line}".rstrip(":") if pending else line.rstrip(":")
                    pending = None
            if grand_re.match(line):
                grand_total = amounts(_trailing_nums(clean, n_amounts))
                if grand_total is None and (closing_names or options.get("value_only_grand")):
                    t = _trailing_nums(clean, 1)
                    grand_total = [None, t[1][0]] if t else None
                continue
            m = _TMR_SUBTOTAL_RE.match(clean)
            if m and closing_names:
                t = _trailing_nums(m.group(1), 4)
                if t and t[1][2] == 0:
                    # Pentegra's fund total prints a stray 0 before its value.
                    t = (t[0], [t[1][0], t[1][1], t[1][3]])
                else:
                    t = _trailing_nums(m.group(1), 3)
                if current is None or current["subtotal"] is not None or not t:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                current["heading"] = t[0]
                current["asset_type"] = next((label for p, label in heading_types
                                              if re.search(p, t[0], re.I)), "")
                for row in section_rows:
                    row["section_heading"] = t[0]
                    if current["asset_type"]:
                        row["asset_type"], row["asset_type_source"] = current["asset_type"], "total"
                close(current, t[1][1:])
                continue
            if m and headings_re:
                t = _trailing_nums(m.group(1), n_amounts)
                if not t or not headings_re.match(t[0]):
                    m = None
            if m and colon_headings and current is not None:
                # A subtotal names its heading; "TOTAL SA 1,837,004 2,534,870"
                # under "Common Stocks:" is TotalEnergies stock (Carpenters St. Louis).
                t = _trailing_nums(m.group(1), n_amounts)
                # DTE's one-word "Total other:" keeps its colon.
                if t and t[0].split()[0].lower().rstrip(":") != current["heading"].split()[0].lower():
                    m = None
            if m:
                t = amounts(_trailing_nums(m.group(1), n_amounts))
                if current is None or not t:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                if current["subtotal"] is not None:
                    # 3M rolls sub-headings up under their category ("Total corporate
                    # stock" after "Total corporate stock - preferred"); the grand
                    # total still checks every row.
                    continue
                close(current, t)
                continue
            if wrapped_re and wrapped_re.search(clean) and not _trailing_nums(clean, 1):
                pending = wrapped_re.sub("", clean).strip()
                continue
            if headings_re and not _trailing_nums(clean, 1):
                base = re.sub(r"\s*(?:\(continued\)|-\s+continued)$", "", line, flags=re.I).rstrip(":")
                if not headings_re.match(base):
                    pending = f"{pending} {clean}".strip() if pending else clean
                    continue
            if options.get("bare_heading_total") and current is not None and current["subtotal"] is None:
                t = _trailing_nums(clean, 2)
                if t and heading_key(t[0]) == heading_key(current["heading"]):
                    close(current, t[1])
                    continue
            t = _trailing_nums(clean, 1 if value_only else 3)
            if value_only:
                if t:
                    title, (value,), cost, par = t[0], t[1], None, None
                    # H-E-B's share count sits just before the value.
                    shares = title.rsplit(None, 1)
                    if len(shares) == 2 and re.fullmatch(r"\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+\.\d+", shares[1]):
                        title, par = shares[0], _num(shares[1])
            elif t:
                title, (par, cost, value) = t
            else:
                # Western States prints "N/A" for a partnership's par.
                t = _trailing_nums(clean, 2)
                if t and t[0].endswith(" N/A"):
                    title, (cost, value), par = t[0][:-4], t[1], None
                elif t and re.search(r"\s(?:\d{1,2}/\d{1,2}/\d{4}|\d+(?:\.\d+)?%)$", t[0]):
                    # Citigroup: "<name> <par> [<rate>] [<maturity>] <cost> <value>", with the
                    # par sitting before the rate and maturity rather than before the cost.
                    title, (cost, value) = t
                    head = re.sub(r"(?:\s+(?:\d{1,2}/\d{1,2}/\d{4}|\d+(?:\.\d+)?%))+$", "", title)
                    p = _trailing_nums(head, 1)
                    par = p[1][0] if p else None
                elif t:
                    # No heading ends in two amounts: a row with no par (Citigroup's MetLife contract).
                    title, (cost, value), par = t[0], t[1], None
                else:
                    t = _trailing_nums(clean, 1)
                    if t and not t[0]:
                        # 3M prints one unlabeled value under "Interest bearing cash";
                        # its section total includes it.
                        title, (value,), cost, par = "", t[1], None, None
                    elif t and (t[0].endswith("**") or options.get("blank_cost")):
                        # FirstEnergy's written-down Russia fund: a value and no cost.
                        title, (value,), cost, par = t[0], t[1], None, None
                    else:
                        t = None
            if (t and not title and not value_only and cost is not None and par is None and pending is None
                    and current is not None and current["rows"] and current["subtotal"] is None):
                # Carpenters NorCal closes each section with a bare "<cost> <value>";
                # after a wrapped name it is that row's amounts (DTE's Oaktree fund).
                if not options.get("group_totals"):
                    close(current, [cost, value])
                continue
            if closing_names:
                if not t:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                if current is None or current["subtotal"] is not None:
                    # Named by its total.
                    current = {"heading": "", "asset_type": "", "partial": False, "rows": 0, "cost": 0.0,
                               "value": 0.0, "subtotal": None, "cost_diff": None, "value_diff": None,
                               "reconciled": None}
                    sections.append(current)
                    section_rows = []
            if not t:
                # A heading line opens the next section; "(continued)" reopens one.
                base = re.sub(r"\s*(?:\(continued\)|-\s+continued)$", "", line, flags=re.I).rstrip(":")
                current = next((s for s in sections if heading_key(s["heading"]) == heading_key(base)
                                and s["subtotal"] is None), None)
                if current is None and base != line.rstrip(":") and any(
                        heading_key(s["heading"]) == heading_key(base) for s in sections):
                    # A closed section's "(continued)" left over at a page top
                    # (Western States' p.38 "Corporate bonds (continued)").
                    continue
                if current is None:
                    current = {"heading": base,
                               "asset_type": next((label for p, label in heading_types
                                                   if re.search(p, base, re.I)), ""),
                               "partial": False, "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None,
                               "cost_diff": None, "value_diff": None, "reconciled": None}
                    if base.isupper():
                        # 3M's ALL-CAPS category; its sub-headings ("Repurchase
                        # agreements") take its type when they name none.
                        parent = current
                    elif not current["asset_type"] and parent is not None:
                        current["asset_type"] = parent["asset_type"]
                    sections.append(current)
                continue
            own = next(((h, a) for p, h, a in options.get("own_section_rows", []) if re.match(p, line)), None)
            if own and (current is None or current["subtotal"] is not None):
                current = {"heading": own[0], "asset_type": own[1], "partial": False, "rows": 0, "cost": 0.0,
                           "value": 0.0, "subtotal": None, "cost_diff": None, "value_diff": None,
                           "reconciled": None}
                sections.append(current)
            if current is None:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            # UFCW repeats its section heading as column (c); the name is what precedes it.
            type_re = r"[\s\-]+".join(map(re.escape, re.split(r"[\s\-]+", current["heading"])))
            tm = re.search(r"\s" + type_re + r"(?=\s|$)", title, re.I) if current["heading"] else None
            ident = ""
            if pending is not None and not headings_re:
                name, rest, pending = f"{pending} {title}".strip(), ["", ""], None
                if party_re:
                    # leading_party: a wrapped row's first line opens with the party too.
                    name = party_re.sub("", name, count=1)
            elif not title:
                name, rest = current["heading"], ["", ""]
            elif options.get("leading_id"):
                ident, name = (title.split(None, 1) + [""])[:2]
                rest = ["", ""]
            elif party_re and party_re.match(title):
                name, rest = title[party_re.match(title).end():].strip(), ["", ""]
            elif tm:
                name = title[:tm.start()].strip()
                # "<maturity> <rate>", where the rate may be "VARIABLE RATE".
                rest = (title[tm.end():].split(None, 1) + ["", ""])[:2]
            else:
                # Western States: "<name> <type> <collateral> <maturity> <rate>", the
                # type a separate column; the name ends at the first column gap.
                chars = [c for c in tl["chars"] if c["text"].strip()]
                if chars and chars[0]["text"] == "*":
                    # Elliott's and H-E-B's party-in-interest mark sits in its own column.
                    chars = chars[1:]
                cut = next((i for i in range(1, len(chars)) if chars[i]["x0"] - chars[i - 1]["x1"] > 6), len(chars))
                name = " ".join(pdfplumber.utils.extract_text(chars[:cut], x_tolerance=1).split()) if chars else title
                fields = title.split()
                rest = [next((f for f in fields if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", f)), ""),
                        next((f for f in fields if re.fullmatch(r"\d+(?:\.\d+)?%", f)), "")]
            if pending is not None:
                # Directors Guild: the wrapped name's first line, then this line's name column.
                name, pending = f"{pending} {name}".strip(), None
            current["rows"] += 1
            current["cost"] += cost or 0.0
            current["value"] += value or 0.0
            if own and current["heading"] == own[0]:
                close(current, [cost or 0.0, value or 0.0])
            if not value:
                continue
            row_type = "" if current["asset_type"] else next(
                (label for p, label in row_types if re.search(p, line, re.I)), "")
            if options.get("clipped_name") and re.fullmatch(options["clipped_name"], name.lstrip("* ").strip()):
                name = f"{current['heading']}: {name.lstrip('* ').strip()} (name not printed)"
            mapped_rows.append({
                "issuer_name": "",
                "investment_description": name.lstrip("* ").strip(),
                "asset_type": current["asset_type"] or row_type,
                "asset_type_source": "heading" if current["asset_type"] else "row" if row_type else "none",
                "par_value": _fmt(scaled(par)),
                "cost": _fmt(scaled(cost)),
                "current_value": _fmt(scaled(value)),
                "units_or_shares": "",
                "asset_identifier": ident,
                "interest_rate": "" if set(rest[1].split()) <= {"N/A"} else rest[1],
                "maturity_date": "" if rest[0] == "N/A" else rest[0],
                "section_heading": current["heading"],
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            })
            section_rows.append(mapped_rows[-1])
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    grand_reconciled = None
    if grand_total is not None:
        tol = 1.0 + (0.5 * sum(s["rows"] for s in sections) if row_rounding else 0.0)
        clipped = options.get("grand_clipped_digits", 0)
        if clipped:
            grand_total = [None if g is None else g * 10 ** clipped for g in grand_total]
            tol += 10 ** clipped
        gap_cost, gap_value = sum(g[0] for g in missing.values()), sum(g[1] for g in missing.values())
        grand_reconciled = (abs(sum(s["value"] for s in sections) + gap_value - grand_total[1]) <= tol
                            and (grand_total[0] is None
                                 or abs(sum(s["cost"] for s in sections) + gap_cost - grand_total[0]) <= tol
                                 or not options.get("grand_cost_check", True)))
    return {"pages": pages, "sections": sections, "unparsed": unparsed, "grand_total": grand_total,
            "grand_reconciled": False if unparsed else grand_reconciled}


def _parse_closing_totals(pdf_path: str, page_nums: List[int]) -> Optional[Dict]:
    """parse_schedule_4i_pages for the closing_totals layout. The pages must
    run from the schedule's first page to "Total investments"."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        text_lines, c_x = {}, {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            page = pdf.pages[page_num - 1]
            text_lines[page_num] = page.extract_text_lines(return_chars=True)
            if not any(_CT_COLUMNS_RE.match(tl["text"].strip()) for tl in text_lines[page_num]):
                return None
            # Column (c) starts where its header label does.
            c_x[page_num] = next((w["x0"] for w in page.extract_words() if w["text"].startswith("(c)")), None)
            if c_x[page_num] is None:
                return None

    sections, unparsed, pages = [], [], []
    heading = None   # opening heading of the rows since the last close, if printed
    open_rows = []   # {"cost", "value", "row", "ident", "desc"} since the last close
    grand_total = None

    def close(label: Optional[str], subtotal: Optional[List[float]]) -> None:
        name = label or heading or ""
        asset_type = next((t for p, t in _CT_ASSET_TYPES if re.search(p, name, re.I)), "")
        cost = sum(r["cost"] for r in open_rows)
        value = sum(r["value"] for r in open_rows)
        section = {"heading": name, "asset_type": asset_type, "partial": False, "rows": len(open_rows),
                   "cost": cost, "value": value, "subtotal": subtotal,
                   "cost_diff": None, "value_diff": None, "reconciled": None}
        if subtotal is not None:
            section["cost_diff"], section["value_diff"] = cost - subtotal[0], value - subtotal[1]
            section["reconciled"] = abs(section["cost_diff"]) <= 1.0 and abs(section["value_diff"]) <= 1.0
        for r in open_rows:
            if r["row"] is None:
                continue
            # A trailing "*" marks a party in interest.
            ident, desc = r["ident"].rstrip(" *"), r["desc"]
            r["row"]["investment_description"] = (
                desc if desc and (not ident or (_CT_FUND_RE.search(desc) and not _CT_FUND_RE.search(ident)))
                else ident)
            r["row"]["asset_type"] = asset_type
            r["row"]["asset_type_source"] = ("total" if label else "heading") if asset_type else "none"
            r["row"]["section_heading"] = name
        sections.append(section)
        open_rows.clear()

    def text_of(chars) -> str:
        return " ".join(pdfplumber.utils.extract_text(chars, x_tolerance=1).split()) if chars else ""

    for page_num in page_nums:
        tls = text_lines[page_num]
        start = next(i + 1 for i, tl in enumerate(tls) if _CT_COLUMNS_RE.match(tl["text"].strip()))
        cx = c_x[page_num] - 1
        mapped_rows = []
        for tl in tls[start:]:
            line = tl["text"].strip()
            if not line or grand_total is not None or _CT_FOOTER_RE.match(line):
                continue
            clean = " ".join(tok for tok in line.split() if tok != "$")
            t = _trailing_nums(clean, 2)
            if _CT_GRAND_RE.match(clean):
                grand_total = t[1] if t else None
                continue
            if t and _CT_TOTAL_RE.match(t[0]):
                close(t[0].split(None, 1)[1], t[1])
                heading = None
                continue
            chars = [c for c in tl["chars"] if c["text"].strip()]
            if not t:
                if tl["x0"] < _CT_ROW_X:
                    # A heading opens a section, closing one that printed no total (Mortgages).
                    if open_rows:
                        close(None, None)
                    heading = line
                elif open_rows and open_rows[-1]["row"] is not None:
                    # Wrapped line ("Silver Rock ... Vintage" / "2022 LP").
                    last = open_rows[-1]
                    last["ident"] = " ".join(filter(None, [last["ident"], text_of([c for c in chars if c["x0"] < cx])]))
                    last["desc"] = " ".join(filter(None, [last["desc"], text_of([c for c in chars if c["x0"] >= cx])]))
                else:
                    unparsed.append({"page_number": page_num, "line": line})
                continue
            cost, value = t[1]
            # A sub-total inside a section ("Foreign obligations", or bare amounts)
            # equals the rows just above it.
            if cost or value:
                cs = vs = 0.0
                is_sub = False
                for k, r in enumerate(reversed(open_rows), 1):
                    cs += r["cost"]
                    vs += r["value"]
                    if k >= 2 and abs(cs - cost) <= 1.0 and abs(vs - value) <= 1.0:
                        is_sub = True
                        break
                if is_sub:
                    continue
            ident = text_of([c for c in chars if _CT_ROW_X <= c["x0"] < cx])
            desc_t = _trailing_nums(" ".join(tok for tok in text_of([c for c in chars if c["x0"] >= cx]).split()
                                             if tok != "$"), 2)
            fields = [f for f in (desc_t[0] if desc_t else "").split() if f not in ("***", "Asset")]
            cusip = fields.pop() if fields and re.fullmatch(r"(?=.*\d)[0-9A-Z]{9}", fields[-1]) else ""
            rate = next((f for f in fields if re.fullmatch(r"\d+(?:\.\d+)?%", f)), "")
            maturity = next((f for f in fields if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{2,4}", f)), "")
            row = None
            if value:
                row = {
                    "issuer_name": "",
                    "investment_description": ident,
                    "asset_type": "",
                    "asset_type_source": "none",
                    "par_value": "",
                    "cost": _fmt(cost),
                    "current_value": _fmt(value),
                    "units_or_shares": "",
                    "asset_identifier": cusip,
                    "interest_rate": rate,
                    "maturity_date": maturity,
                    "section_heading": "",
                    "extraction_method": "sch4i_text",
                    "page_number": page_num,
                    "row_id": len(mapped_rows) + 1,
                }
                mapped_rows.append(row)
            open_rows.append({"cost": cost or 0.0, "value": value or 0.0, "row": row,
                              "ident": ident, "desc": " ".join(fields)})
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })
    if open_rows:
        # Pooled separate accounts print no total of their own.
        close(None, None)

    grand_reconciled = False
    if grand_total is not None:
        grand_reconciled = (abs(sum(s["value"] for s in sections) - grand_total[1]) <= 1.0
                            and abs(sum(s["cost"] for s in sections) - grand_total[0]) <= 1.0)
    return {"pages": pages, "sections": sections, "unparsed": unparsed, "grand_total": grand_total,
            "grand_reconciled": False if unparsed else grand_reconciled}


def _parse_investments_held_summary(pdf_path: str, page_nums: List[int],
                                    value_cost_pair: bool = False) -> Optional[Dict]:
    """parse_schedule_4i_pages for the investments_held_summary layout. The
    pages must run from the summary's first page to its last.
    value_cost_pair: rows print only "<fair value> <cost>"."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            lines = [l.strip() for l in
                     (_drop_overlapping_spaces(pdf.pages[page_num - 1]).extract_text() or "").splitlines()]
            start = next((i + 1 for i, l in enumerate(lines) if _IHS_COLUMNS_RE.match(l)), None)
            if not _IHS_TITLE_RE.search("\n".join(lines)) or start is None:
                return None
            bodies[page_num] = [l for l in lines[start:] if l]

    sections, unparsed, pages = [], [], []
    heading = None
    open_rows = []   # mapped rows since the heading
    grand_total = None

    def amounts(line: str):
        """(text, balance, adjustment, market value, cost or None)."""
        line = _IHS_ATTACHED_RE.sub("", line)
        if value_cost_pair:
            t = _trailing_nums(line, 2)
            return (t[0], None, None, t[1][0], t[1][1]) if t else None
        t = _trailing_nums(line, 4)
        if t and t[0] and _NUM_RE.match(t[0].split()[-1]):
            t = None   # five amounts: not a row
        if t:
            return t[0], t[1][0], t[1][1], t[1][2], t[1][3]
        t = _trailing_nums(line, 3)
        return (t[0], t[1][0], t[1][1], t[1][2], None) if t else None

    def close_unprinted():
        """A one-row category prints no subtotal: its row is the subtotal."""
        r = open_rows[0]
        return _num(r["current_value"]), _num(r["cost"])

    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            a = amounts(line)
            if a is None and value_cost_pair and grand_total is None and len(open_rows) == 1:
                value, cost = close_unprinted()
                a = ("", None, None, value, cost)
                pending = line
            else:
                pending = None
            if a is None:
                if grand_total is None and not open_rows:
                    heading = line
                else:
                    unparsed.append({"page_number": page_num, "line": line})
                continue
            text, _bal, _adj, value, cost = a
            if not text:
                # Bare amounts: the subtotal closing the open heading, or the grand total.
                if heading is None:
                    grand_total = [cost, value]
                    continue
                name = heading
                asset_type = next((t for p, t in _IHS_ASSET_TYPES if re.search(p, name, re.I)), "")
                row_value = sum(_num(r["current_value"]) for r in open_rows)
                row_cost = sum(_num(r["cost"]) or 0.0 for r in open_rows)
                section = {"heading": name, "asset_type": asset_type, "partial": False, "rows": len(open_rows),
                           "cost": row_cost, "value": row_value, "subtotal": [cost, value],
                           "cost_diff": None if cost is None else row_cost - cost,
                           "value_diff": row_value - value, "reconciled": None}
                section["reconciled"] = abs(section["value_diff"]) <= 1.0 and (
                    cost is None or abs(section["cost_diff"]) <= 1.0)
                for r in open_rows:
                    r["asset_type"] = asset_type
                    r["asset_type_source"] = "heading" if asset_type else "none"
                    r["section_heading"] = name
                sections.append(section)
                open_rows.clear()
                heading = pending
                continue
            if heading is None:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            description = _IHS_REF_RE.sub("", text)
            if value_cost_pair and description == text and re.search(r"\bPage\s+\d+$", text):
                description = heading   # "Northern Trust Statement Page 132": the category itself
            row = {
                "issuer_name": "",
                "investment_description": description,
                "asset_type": "",
                "asset_type_source": "none",
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": "",
                "asset_identifier": "",
                "interest_rate": "",
                "maturity_date": "",
                "section_heading": "",
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            }
            mapped_rows.append(row)
            open_rows.append(row)
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })
    if open_rows:
        unparsed.extend({"page_number": r["page_number"], "line": r["investment_description"]} for r in open_rows)

    grand_reconciled = False
    if grand_total is not None:
        grand_reconciled = abs(sum(s["value"] for s in sections) - grand_total[1]) <= 1.0
    return {"pages": pages, "sections": sections, "unparsed": unparsed, "grand_total": grand_total,
            "grand_reconciled": False if unparsed else grand_reconciled}


def _ch_clean(line: str) -> str:
    """Rejoin "1 ,913,559" and "( 189,100)", drop "$" markers."""
    line = re.sub(r"(?<=\d) ,(?=\d{3})", ",", line)
    line = re.sub(r"\(\s+", "(", line)
    return " ".join(t for t in line.split() if t != "$")


def _parse_colon_headings(pdf_path: str, page_nums: List[int], row_rounding: bool = False) -> Optional[Dict]:
    """parse_schedule_4i_pages for the colon_headings layout. Rows end
    "<rate> <maturity> <par> <cost> <value>"; a "<CATEGORY>:" line opens a
    section and a bare "<par> <cost> <value>" line closes it. The first
    section's heading prints just above its subtotal instead, so a heading
    met inside an unnamed open section names that section. row_rounding
    allows half a dollar per row, as in typed_maturity_rate."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            text = pdf.pages[page_num - 1].extract_text() or ""
            if not _CH_COLUMNS_RE.search(text):
                return None
            lines = [l.strip() for l in text.splitlines()]
            start = next(i + 1 for i, l in enumerate(lines) if _CH_COLUMNS_RE.match(l))
            bodies[page_num] = [l for l in lines[start:] if l and not _CH_FOOTER_RE.match(l)]

    sections, unparsed, pages = [], [], []
    current = None

    def set_heading(section, heading):
        section["heading"] = heading
        section["asset_type"] = next((label for pattern, label in _CH_ASSET_TYPES
                                      if re.search(pattern, heading, re.I)), "")

    def new_section(heading=""):
        section = {"heading": "", "asset_type": "", "partial": False,
                   "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None, "mapped": []}
        set_heading(section, heading)
        sections.append(section)
        return section

    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            if _CH_HEADING_RE.match(line):
                heading = line[:-1].strip()
                if current is not None and not current["heading"] and current["rows"]:
                    set_heading(current, heading)
                else:
                    current = new_section(heading)
                continue
            clean = _ch_clean(line)
            toks = clean.split()
            if len(toks) == 3 and all(_NUM_RE.match(t) for t in toks):
                if current is None or current["subtotal"] is not None:
                    unparsed.append({"page_number": page_num, "line": line})
                else:
                    current["subtotal"] = [_num(toks[1]), _num(toks[2])]
                    current = None
                continue
            t3 = _trailing_nums(clean, 3)
            if not t3 or not t3[0]:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            if current is None:
                current = new_section()
            words = t3[0].split()
            rate = maturity = ""
            if len(words) >= 3 and _CH_RATE_RE.match(words[-2]) and _CH_MATURITY_RE.match(words[-1]):
                rate, maturity = (w if w != "-" else "" for w in words[-2:])
                words = words[:-2]
            identifier, title = (words[0], " ".join(words[1:])) if len(words) > 1 else ("", " ".join(words))
            par, cost, value = t3[1]
            current["rows"] += 1
            current["cost"] += cost or 0.0
            current["value"] += value or 0.0
            if not value:
                continue
            row = {
                "issuer_name": "",
                "investment_description": title,
                "asset_type": "",
                "asset_type_source": "none",
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": _fmt(par),
                "asset_identifier": identifier,
                "interest_rate": rate,
                "maturity_date": maturity,
                "section_heading": "",
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            }
            current["mapped"].append(row)
            mapped_rows.append(row)
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    # Typed once every section has its heading (the first one's comes last).
    for section in sections:
        for row in section.pop("mapped"):
            row["section_heading"] = section["heading"]
            row["asset_type"] = section["asset_type"] or detect_asset_type(
                f"{section['heading']} {row['investment_description']}")
            row["asset_type_source"] = "heading" if section["asset_type"] else (
                "row" if row["asset_type"] else "none")
        sub = section["subtotal"]
        section["cost_diff"] = None if sub is None else section["cost"] - sub[0]
        section["value_diff"] = None if sub is None else section["value"] - sub[1]
        tol = 1.0 + (0.5 * section["rows"] if row_rounding else 0.0)
        section["reconciled"] = False if sub is None else (
            abs(section["value_diff"]) <= tol and abs(section["cost_diff"]) <= tol)
    return {"pages": pages, "sections": sections, "unparsed": unparsed, "grand_total": None,
            "grand_reconciled": False if unparsed else None}


def _parse_described_categories(pdf_path: str, page_nums: List[int], options: Optional[Dict] = None) -> Optional[Dict]:
    """parse_schedule_4i_pages for the described_categories layout. A line
    with no amounts is a category heading; rows end "<rate> <maturity> <par>
    <cost> <value>" (rate and maturity may be N/A), except partnership rows,
    which print "<cost> <value>" only. A bare "<cost> <value>" line closes the
    category and "$ <cost> $ <value>" is the grand total. options is the
    plan's registry entry:
      row_rounding: half a dollar per row, as in colon_headings.
      title_suffix: pattern ending a row's name ahead of its rate (the
        description and collateral columns), dropped (AFM).
      overlapping_spaces: the page prints stray spaces over its digits
        (AFM's "5 16,834"); _drop_overlapping_spaces removes them.
      orphan_heading: names a category whose rows follow a subtotal with no
        heading of their own (AFM's partnerships).
      grand_closes_last: the last category prints no subtotal, so the grand
        total less the other subtotals stands for it (AFM's Other Investments).
      own_section_rows: [(pattern, heading, asset_type)]: a row matching
        outside any category is a category of its own, its amounts its
        subtotal (AFTRA's interest-bearing cash, 2026-10-17).
      subtotal_cost_errata: {heading: amount} a printed cost subtotal exceeds
        its rows' costs by, its value still tying (AFTRA's common/collective
        trusts subtotal adds in the partnerships' cost; the grand total
        doesn't)."""
    options = options or {}
    row_rounding = options.get("row_rounding", False)
    title_suffix = options.get("title_suffix")
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            page = pdf.pages[page_num - 1]
            text = _dc_text(_drop_overlapping_spaces(page) if options.get("overlapping_spaces") else page)
            if not _DC_COLUMNS_RE.search(text):
                return None
            lines = [l.strip() for l in text.splitlines()]
            start = next(i + 1 for i, l in enumerate(lines) if _DC_COLUMNS_RE.match(l))
            bodies[page_num] = [l for l in lines[start:] if l and not _DC_FOOTER_RE.match(l)]

    sections, unparsed, pages = [], [], []
    current, grand_total = None, None
    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            toks = _ch_clean(line).split()
            if _DC_GRAND_RE.match(line):
                grand_total = [_num(t) for t in toks]
                if options.get("grand_closes_last") and current is not None and current["subtotal"] is None:
                    others = [x["subtotal"] for x in sections if x is not current]
                    if all(others):
                        current["subtotal"] = [grand_total[i] - sum(o[i] for o in others) for i in (0, 1)]
                current = None
                continue
            if not _NUM_RE.match(toks[-1]):
                asset_type = next((label for pattern, label in _TMR_ASSET_TYPES if re.search(pattern, line, re.I)), "")
                current = {"heading": line, "asset_type": asset_type, "partial": False,
                           "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None}
                sections.append(current)
                continue
            if len(toks) == 2 and all(_NUM_RE.match(t) for t in toks):
                if current is None or current["subtotal"] is not None:
                    unparsed.append({"page_number": page_num, "line": line})
                else:
                    current["subtotal"] = [_num(toks[0]), _num(toks[1])]
                    current = None
                continue
            clean = re.sub(r"\bVAR RT\b", "VAR", " ".join(toks))
            t3 = _trailing_nums(clean, 3)
            words = t3[0].split() if t3 else []
            if len(words) >= 3 and _DC_RATE_RE.match(words[-2]) and _DC_MATURITY_RE.match(words[-1]):
                rate, maturity = (w if w != "N/A" else "" for w in words[-2:])
                title, (par, cost, value) = " ".join(words[:-2]), t3[1]
            else:
                t2 = _trailing_nums(clean, 2)
                if not t2 or not t2[0]:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                rate = maturity = ""
                title, par, (cost, value) = t2[0], None, t2[1]
            own = next(((h, a) for p, h, a in options.get("own_section_rows", []) if re.match(p, line)), None)
            if current is None and own:
                current = {"heading": own[0], "asset_type": own[1], "partial": False, "rows": 0, "cost": 0.0,
                           "value": 0.0, "subtotal": [cost or 0.0, value or 0.0]}
                sections.append(current)
            if current is None and options.get("orphan_heading") and sections and grand_total is None:
                heading = options["orphan_heading"]
                current = {"heading": heading, "partial": False, "rows": 0, "cost": 0.0, "value": 0.0,
                           "subtotal": None, "asset_type": next(
                               (label for pattern, label in _TMR_ASSET_TYPES if re.search(pattern, heading, re.I)), "")}
                sections.append(current)
            if current is None:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            title = _DC_TYPE_RE.sub("", re.sub(r"^\*\s*", "", title))
            if title_suffix:
                title = re.sub(title_suffix, "", title)
            current["rows"] += 1
            current["cost"] += cost or 0.0
            current["value"] += value or 0.0
            if not value:
                continue
            asset_type = current["asset_type"] or detect_asset_type(f"{current['heading']} {title}")
            row = {
                "issuer_name": "",
                "investment_description": title,
                "asset_type": asset_type,
                "asset_type_source": "heading" if current["asset_type"] else ("row" if asset_type else "none"),
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": _fmt(par),
                "asset_identifier": "",
                "interest_rate": rate,
                "maturity_date": maturity,
                "section_heading": current["heading"],
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            }
            mapped_rows.append(row)
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    cost_errata = {k.lower(): v for k, v in options.get("subtotal_cost_errata", {}).items()}
    for section in sections:
        sub = section["subtotal"]
        section["cost_diff"] = None if sub is None else (
            section["cost"] + cost_errata.get(section["heading"].lower(), 0.0) - sub[0])
        section["value_diff"] = None if sub is None else section["value"] - sub[1]
        tol = 1.0 + (0.5 * section["rows"] if row_rounding else 0.0)
        section["reconciled"] = False if sub is None else (
            abs(section["value_diff"]) <= tol and abs(section["cost_diff"]) <= tol)
    grand_tol = 1.0 + (0.5 * sum(s["rows"] for s in sections) if row_rounding else 0.0)
    grand_reconciled = False if unparsed or not grand_total else (
        abs(sum(s["value"] for s in sections) - grand_total[1]) <= grand_tol)
    return {"pages": pages, "sections": sections, "unparsed": unparsed,
            "grand_total": grand_total, "grand_reconciled": grand_reconciled}


def _parse_nt_cusip_columns(pdf_path: str, page_nums: List[int], options: Dict) -> Optional[Dict]:
    """parse_schedule_4i_pages for the nt_cusip_columns layout. Rows are
    "[*] [<id>] [&&&] <description> <shares> <cost> <value>" with no headings or
    subtotals; amounts print with stray spaces ("1 ,485,940", "1 00 1 07") and
    a column may be blank, so each amount word goes to the column whose header
    it sits under on the first page. A row without an id continues the one
    above it when both names start with the same word (the Blackstone TE.1
    sub-entities); currency rows stand alone.

    options (registry): "alt_ids" - id pattern of the private funds, typed
    "Partnership Interest" unless type_rows says otherwise; "type_rows" -
    [(name pattern, asset_type)] for funds, checked after the security
    patterns and never on SEDOL ("S" + 7) ids, which are listed securities;
    "row_rounding" - half a dollar per row. Other rows are never typed
    partnership or hedge fund; an unmatched SEDOL row is a listed stock.
    """
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    alt_re = re.compile(options["alt_ids"]) if options.get("alt_ids") else None
    type_rows = [(re.compile(p, re.I), label) for p, label in options.get("type_rows", [])]
    with pdfplumber.open(pdf_path) as pdf:
        lines_by_page = {}
        bounds = None
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            page = pdf.pages[page_num - 1]
            words = page.extract_words()
            if bounds is None:
                head = {w["text"]: w for w in words if w["text"] in ("Shares/Par", "Cost", "value", "(CUSIP)")}
                if len(head) < 4:
                    return None
                body_top = head["(CUSIP)"]["bottom"]
                bounds = (head["Shares/Par"]["x0"] - 18,
                          (head["Shares/Par"]["x1"] + head["Cost"]["x1"]) / 2 + 10,
                          (head["Cost"]["x1"] + head["value"]["x1"]) / 2 + 10)
            else:
                body_top = 0
            lines = {}
            for w in words:
                if w["top"] > body_top:
                    lines.setdefault(round(w["top"]), []).append(w)
            lines_by_page[page_num] = [sorted(lines[t], key=lambda w: w["x0"]) for t in sorted(lines)]

    section = {"heading": "", "asset_type": "", "partial": False, "rows": 0,
               "cost": 0.0, "value": 0.0, "subtotal": None}
    unparsed, pages = [], []
    grand_total = None
    prev_id, prev_first = "", ""
    for page_num in page_nums:
        mapped_rows = []
        for ws in lines_by_page[page_num]:
            line = " ".join(w["text"] for w in ws)
            if _NCC_FOOTER_RE.match(line):
                continue
            if _NCC_GRAND_RE.match(line):
                grand_total = [v for v in (_num(t) for t in line.replace(" ", "").split("$")[1:])][1:]
                continue
            cols, desc = ["", "", ""], []
            for w in ws:
                if w["x0"] >= bounds[0]:
                    cols[0 if w["x1"] <= bounds[1] else 1 if w["x1"] <= bounds[2] else 2] += w["text"]
                else:
                    desc.append(w["text"])
            amounts = [_num(c) if c else None for c in cols]
            if not cols[2] or any(c and a is None for c, a in zip(cols, amounts)):
                unparsed.append({"page_number": page_num, "line": line})
                continue
            # "*" marks a party in interest, "&&&" a custodian flag.
            desc = [d for d in desc if d not in ("*", "&&&")]
            ident = desc.pop(0) if desc and _NCC_ID_RE.match(desc[0]) else ""
            title = " ".join(desc)
            if not title:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            first = title.split()[0].upper()
            if not ident and first == prev_first:
                ident = prev_id
            prev_id, prev_first = ident, first
            shares, cost, value = amounts
            section["rows"] += 1
            section["cost"] += cost or 0.0
            section["value"] += value or 0.0
            if not value:
                continue
            upper = title.upper()
            sedol = len(ident) == 8 and ident.startswith("S")
            plan_type = "" if sedol else next(
                (label for pattern, label in type_rows if pattern.search(title)), "")
            if alt_re and ident and alt_re.search(ident):
                asset_type, source = plan_type or "Partnership Interest", "alt_id"
            else:
                asset_type = next((label for pattern, label in _NCC_ASSET_TYPES if re.search(pattern, upper)), "") \
                    or plan_type or next((label for pattern, label in _NCC_POOLED_TYPES + _NCC_FCV_TYPES
                                          if re.search(pattern, upper)), "")
                if not asset_type and sedol:
                    asset_type = "Common Stock"
                source = "row" if asset_type else "none"
            m = _CTL_RATE_DATE_RE.search(title)
            rate, maturity = ((m.group(2), m.group(1)) if m.group(1) else (m.group(3), m.group(4))) \
                if m else ("", "")
            mapped_rows.append({
                "issuer_name": "",
                "investment_description": title,
                "asset_type": asset_type,
                "asset_type_source": source,
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": _fmt(shares),
                "asset_identifier": ident,
                "interest_rate": rate,
                "maturity_date": maturity,
                "section_heading": "",
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            })
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    section["subtotal"] = grand_total
    section["cost_diff"] = None if grand_total is None else section["cost"] - grand_total[0]
    section["value_diff"] = None if grand_total is None else section["value"] - grand_total[1]
    tol = 1.0 + (0.5 * section["rows"] if options.get("row_rounding") else 0.0)
    section["reconciled"] = False if grand_total is None else (
        abs(section["value_diff"]) <= tol and abs(section["cost_diff"]) <= tol)
    return {"pages": pages, "sections": [section], "unparsed": unparsed, "grand_total": grand_total,
            "grand_reconciled": False if unparsed else section["reconciled"]}


def _parse_leading_shares(pdf_path: str, page_nums: List[int], options: Dict) -> Optional[Dict]:
    """parse_schedule_4i_pages for the leading_shares layout. Rows are
    "<shares|**> <description> <cost> <value>"; "<Category>:" opens a section,
    "Total <category> <cost> <value>" closes it and "Total investments" ends
    the schedule. "**" rows take options' fund_types label for their
    category; type_rows (description patterns) override both."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            text = pdf.pages[page_num - 1].extract_text() or ""
            if not _LS_COLUMNS_RE.search(text):
                return None
            lines = [l.strip() for l in text.splitlines()]
            start = next(i + 1 for i, l in enumerate(lines) if _LS_COLUMNS_RE.match(l))
            bodies[page_num] = [l for l in lines[start:] if l and not _LS_FOOTER_RE.match(l)]

    fund_types = options.get("fund_types", [])
    type_rows = options.get("type_rows", [])
    sections, unparsed, pages = [], [], []
    current = None
    grand_total = None
    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            if grand_total is not None:
                break
            heading = _heading_of(line)
            if heading:
                current = {"heading": heading, "asset_type": heading_asset_type(heading), "partial": False,
                           "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None}
                sections.append(current)
                continue
            toks = [t for t in line.split() if t != "$"]
            if len(toks) < 3 or not (_NUM_RE.match(toks[-1]) and _NUM_RE.match(toks[-2])):
                unparsed.append({"page_number": page_num, "line": line})
                continue
            cost, value = _num(toks[-2]), _num(toks[-1])
            words = toks[:-2]
            total = _LS_TOTAL_RE.match(" ".join(words))
            if total:
                if re.match(r"^investments$", total.group(1), re.I):
                    grand_total = [cost, value]
                elif current is not None and current["subtotal"] is None and \
                        total.group(1).lower() == current["heading"].lower():
                    current["subtotal"] = [cost, value]
                else:
                    unparsed.append({"page_number": page_num, "line": line})
                continue
            if current is None:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            star = words[0] == "**"
            shares = None
            if star or (_NUM_RE.match(words[0]) and len(words) > 1):
                shares = None if star else _num(words[0])
                words = words[1:]
            title = " ".join(words)
            current["rows"] += 1
            current["cost"] += cost or 0.0
            current["value"] += value or 0.0
            if not value:
                continue
            asset_type = next((label for pattern, label in type_rows if re.search(pattern, title, re.I)), "")
            source = "row" if asset_type else "none"
            if not asset_type and star:
                asset_type = next((label for pattern, label in fund_types
                                   if re.search(pattern, current["heading"], re.I)), "")
                source = "heading" if asset_type else "none"
            if not asset_type and current["asset_type"]:
                asset_type, source = current["asset_type"], "heading"
            if not asset_type:
                asset_type = detect_asset_type(title)
                source = "row" if asset_type else "none"
            mapped_rows.append({
                "issuer_name": "",
                "investment_description": title,
                "asset_type": asset_type,
                "asset_type_source": source,
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": _fmt(shares),
                "asset_identifier": "",
                "interest_rate": "",
                "maturity_date": "",
                "section_heading": current["heading"],
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            })
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    for section in sections:
        sub = section["subtotal"]
        section["cost_diff"] = None if sub is None else section["cost"] - sub[0]
        section["value_diff"] = None if sub is None else section["value"] - sub[1]
        tol = max(1.0, float(section["rows"]))
        section["reconciled"] = False if sub is None else (
            abs(section["value_diff"]) <= tol and abs(section["cost_diff"]) <= tol)
    grand_reconciled = False if unparsed or grand_total is None else (
        abs(sum(s["value"] for s in sections) - grand_total[1]) <= max(1.0, float(sum(s["rows"] for s in sections))))
    return {"pages": pages, "sections": sections, "unparsed": unparsed, "grand_total": grand_total,
            "grand_reconciled": grand_reconciled}


def _parse_issuer_shares_category(pdf_path: str, page_nums: List[int]) -> Optional[Dict]:
    """parse_schedule_4i_pages for the issuer_shares_category layout. Rows are
    "<issuer> <shares> <description> <cost> <value>"; the shares figure is the
    last amount ahead of the description. A line without amounts is a heading
    (the innermost names the section), "Total <heading> <cost> <value>" closes
    it and a bare "Total <cost> <value>" ends the schedule. Rows after a
    closed section open an unnamed one, which its own Total line names."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        if not all(1 <= p <= len(pdf.pages) for p in page_nums):
            return None
        bodies = {}
        for i, page_num in enumerate(page_nums):
            text = _page_text(pdf.pages[page_num - 1], "issuer_shares_category")
            lines = [l.strip() for l in text.splitlines()]
            if i == 0:
                start = next((j + 1 for j, l in enumerate(lines) if _ISC_COLUMNS_RE.match(l)), None)
                if start is None:
                    return None
                lines = lines[start:]
            bodies[page_num] = [l for l in lines if l and not l.isdigit()]

    def asset_type_of(heading):
        return next((label for pattern, label in _ISC_ASSET_TYPES + _HEADING_ASSET_TYPES
                     if re.search(pattern, heading, re.I)), "")

    def new_section(heading=""):
        section = {"heading": heading, "asset_type": asset_type_of(heading), "partial": False,
                   "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None, "mapped": []}
        sections.append(section)
        return section

    sections, unparsed, pages = [], [], []
    current = None
    grand_total = None
    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            if grand_total is not None:
                break
            t2 = _trailing_nums(line, 2)
            if not t2:
                # Consecutive headings nest; the innermost names the section.
                if current is not None and not current["rows"]:
                    sections.remove(current)
                current = new_section(line)
                continue
            words, (cost, value) = t2[0].split(), t2[1]
            if words == ["Total"]:
                grand_total = [cost, value]
                continue
            if words and words[0] == "Total":
                name = " ".join(words[1:])
                if current is not None and current["subtotal"] is None and (
                        name.lower() == current["heading"].lower() or not current["heading"]):
                    if not current["heading"]:
                        current["heading"], current["asset_type"] = name, asset_type_of(name)
                    current["subtotal"] = [cost, value]
                    current = None
                else:
                    unparsed.append({"page_number": page_num, "line": line})
                continue
            idx = max((j for j, w in enumerate(words) if _NUM_RE.match(w)), default=None)
            if idx is None:
                # Issuer run into the shares figure ("...VI-A LP22,623,341").
                glued = max((j for j, w in enumerate(words) if _ISC_GLUED_RE.match(w)), default=None)
                if glued is not None:
                    m = _ISC_GLUED_RE.match(words[glued])
                    words[glued:glued + 1] = [m.group(1), m.group(2)]
                    idx = glued + 1
            if idx is None or idx == 0:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            if current is None:
                current = new_section()
            issuer, description = " ".join(words[:idx]), " ".join(words[idx + 1:])
            current["rows"] += 1
            current["cost"] += cost or 0.0
            current["value"] += value or 0.0
            if not value:
                continue
            row = {
                "issuer_name": "",
                "investment_description": issuer,
                "asset_type": "",
                "asset_type_source": "none",
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": _fmt(_num(words[idx])),
                "asset_identifier": "",
                "interest_rate": "",
                "maturity_date": "",
                "section_heading": "",
                "description": description,
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            }
            current["mapped"].append(row)
            mapped_rows.append(row)
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    # Typed once every section has its heading (an unnamed one's comes last).
    for section in sections:
        for row in section.pop("mapped"):
            description = row.pop("description")
            row["section_heading"] = section["heading"]
            row["asset_type"] = section["asset_type"] or asset_type_of(description) or detect_asset_type(
                f"{row['investment_description']} {description}")
            row["asset_type_source"] = "heading" if section["asset_type"] else (
                "row" if row["asset_type"] else "none")
        sub = section["subtotal"]
        section["cost_diff"] = None if sub is None else section["cost"] - sub[0]
        section["value_diff"] = None if sub is None else section["value"] - sub[1]
        tol = max(1.0, float(section["rows"]))
        section["reconciled"] = False if sub is None else (
            abs(section["value_diff"]) <= tol and abs(section["cost_diff"]) <= tol)
    grand_reconciled = False if unparsed or grand_total is None else (
        abs(sum(s["value"] for s in sections) - grand_total[1]) <= max(1.0, float(sum(s["rows"] for s in sections))))
    return {"pages": pages, "sections": sections, "unparsed": unparsed, "grand_total": grand_total,
            "grand_reconciled": grand_reconciled}


def _parse_party_type_tree(pdf_path: str, page_nums: List[int]) -> Optional[Dict]:
    """parse_schedule_4i_pages for the party_type_tree layout. Rows are
    "<party> <issuer> <description> <maturity> <rate> <shares> <cost> <value>"
    with "N/A" (or "*") in the party column. Every amount-less line is a
    heading; "TOTAL <heading>" closes it, and a parent heading's total
    ("TOTAL PARTNERSHIPS") covers its "PARTNERSHIPS: ..."/"... - ..." children.
    Sections without a total line are left unreconciled; the "TOTALS - ASSETS"
    grand total still covers them."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        if not all(1 <= p <= len(pdf.pages) for p in page_nums):
            return None
        bodies = {}
        for page_num in page_nums:
            lines = [l.strip() for l in _page_text(pdf.pages[page_num - 1], "party_type_tree").splitlines()]
            start = next((j + 1 for j, l in enumerate(lines) if _PTT_COLUMNS_RE.match(l)), None)
            if start is None:
                return None
            bodies[page_num] = [l for l in lines[start:] if l and not l.isdigit()]

    sections, unparsed, pages = [], [], []
    current = None
    grand_total = None
    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            if grand_total is not None:
                break
            t2 = _trailing_nums(line.replace("$", ""), 2)
            if _PTT_GRAND_RE.match(line):
                grand_total = t2[1] if t2 else None
                if grand_total is None:
                    unparsed.append({"page_number": page_num, "line": line})
                continue
            if not t2:
                current = {"heading": line, "asset_type": heading_asset_type(line), "partial": False,
                           "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None}
                sections.append(current)
                continue
            text, (cost, value) = t2
            if text.startswith("TOTAL "):
                name = text[len("TOTAL "):].strip().upper()
                section = next((s for s in sections if s["heading"].upper() == name), None)
                if section is None or section["subtotal"] is not None:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                section["subtotal"] = [cost, value]
                if section is current:
                    current = None
                continue
            words = text.split()
            if len(words) < 5 or words[0] not in ("N/A", "*") or not (
                    _NUM_RE.match(words[-1]) or words[-1] == "N/A"):
                unparsed.append({"page_number": page_num, "line": line})
                continue
            if current is None:
                current = {"heading": "", "asset_type": "", "partial": False,
                           "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None}
                sections.append(current)
            title_desc = " ".join(words[1:-3])
            m = _PTT_DESCRIPTION_RE.search(title_desc)
            title, description = (title_desc[:m.start()], m.group(1)) if m else (title_desc, "")
            current["rows"] += 1
            current["cost"] += cost or 0.0
            current["value"] += value or 0.0
            if not value:
                continue
            maturity, rate = (w if w != "N/A" else "" for w in words[-3:-1])
            row = {
                "issuer_name": "",
                "investment_description": title,
                "asset_type": current["asset_type"] or detect_asset_type(f"{title} {description}"),
                "asset_type_source": "heading" if current["asset_type"] else "row",
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": "" if words[-1] == "N/A" else _fmt(_num(words[-1])),
                "asset_identifier": "",
                "interest_rate": rate,
                "maturity_date": maturity,
                "section_heading": current["heading"],
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            }
            if not row["asset_type"]:
                row["asset_type_source"] = "none"
            mapped_rows.append(row)
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    # Untotalled parent headings with no rows of their own carry nothing.
    sections = [s for s in sections if s["rows"] or s["subtotal"] is not None]
    for section in sections:
        sub = section["subtotal"]
        head = section["heading"].upper()
        covered = [s for s in sections if s is section or (head and (
            s["heading"].upper().startswith(head + ":") or s["heading"].upper().startswith(head + " -")))]
        rows = sum(s["rows"] for s in covered)
        section["cost_diff"] = None if sub is None else sum(s["cost"] for s in covered) - sub[0]
        section["value_diff"] = None if sub is None else sum(s["value"] for s in covered) - sub[1]
        tol = max(1.0, float(rows))
        section["reconciled"] = None if sub is None else (
            abs(section["value_diff"]) <= tol and abs(section["cost_diff"]) <= tol)
    grand_reconciled = False if unparsed or grand_total is None else (
        abs(sum(s["value"] for s in sections) - grand_total[1]) <= max(1.0, float(sum(s["rows"] for s in sections))))
    return {"pages": pages, "sections": sections, "unparsed": unparsed, "grand_total": grand_total,
            "grand_reconciled": grand_reconciled}


def _parse_category_column(pdf_path: str, page_nums: List[int]) -> Optional[Dict]:
    """parse_schedule_4i_pages for the category_column layout. Rows are
    "<description> <category> <shares> <cost> <value>"; "Total <name> <shares>
    <cost> <value>" closes the rows above it as section <name>. Amount-only
    lines after the last section (accrued income, pending trades) adjust the
    holdings to the "Total Master Trust Assets $ <value>" grand total."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        if not all(1 <= p <= len(pdf.pages) for p in page_nums):
            return None
        bodies = {}
        for k, page_num in enumerate(page_nums):
            lines = [l.strip() for l in _page_text(pdf.pages[page_num - 1], "category_column").splitlines()]
            start = next((j + 1 for j, l in enumerate(lines) if _CC_COLUMNS_RE.match(l)), None)
            if start is None and k == 0:
                return None
            bodies[page_num] = [l for l in lines[start or 0:] if l and not l.isdigit()]

    def new_section():
        section = {"heading": "", "asset_type": "", "partial": False,
                   "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None, "typed": []}
        sections.append(section)
        return section

    sections, unparsed, pages, adjustments = [], [], [], []
    current = None
    grand_total = None
    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            if grand_total is not None:
                break
            clean = " ".join(tok for tok in line.split() if tok != "$")
            if _CC_GRAND_RE.match(clean):
                t1 = _trailing_nums(clean, 1)
                grand_total = [t1[1][0]] if t1 else None
                if grand_total is None:
                    unparsed.append({"page_number": page_num, "line": line})
                continue
            t3 = _trailing_nums(clean, 3)
            if t3 is None:
                t1 = _trailing_nums(clean, 1)
                if t1 and t1[0] and sections and current is None:
                    adjustments.append(t1[1][0])
                else:
                    unparsed.append({"page_number": page_num, "line": line})
                continue
            text, (shares, cost, value) = t3
            if text.startswith("Total "):
                if current is None:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                current["heading"] = text[len("Total "):].strip()
                current["asset_type"] = heading_asset_type(current["heading"])
                current["subtotal"] = [cost, value]
                for row in current.pop("typed"):
                    row["section_heading"] = current["heading"]
                    if not row["asset_type"]:
                        row["asset_type"] = current["asset_type"] or detect_asset_type(row["investment_description"])
                        row["asset_type_source"] = "heading" if current["asset_type"] else (
                            "row" if row["asset_type"] else "none")
                current = None
                continue
            m = _CC_CATEGORY_RE.match(text)
            if not m:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            if current is None:
                current = new_section()
            current["rows"] += 1
            current["cost"] += cost or 0.0
            current["value"] += value or 0.0
            if not value:
                continue
            title, category = m.group(1), m.group(2)
            row = {
                "issuer_name": "",
                "investment_description": title,
                "asset_type": next((label for pattern, label in _CC_ASSET_TYPES if re.search(pattern, category)), ""),
                "asset_type_source": "row",
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": _fmt(shares) if shares else "",
                "asset_identifier": "",
                "interest_rate": "",
                "maturity_date": "",
                "section_heading": "",
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            }
            current["typed"].append(row)
            mapped_rows.append(row)
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    # A section still open at the grand total was never closed by a Total line.
    for section in sections:
        for row in section.pop("typed", []):
            unparsed.append({"page_number": row["page_number"], "line": row["investment_description"]})
    for section in sections:
        sub = section["subtotal"]
        section["cost_diff"] = None if sub is None else section["cost"] - sub[0]
        section["value_diff"] = None if sub is None else section["value"] - sub[1]
        tol = max(1.0, 0.5 * section["rows"])
        section["reconciled"] = None if sub is None else (
            abs(section["value_diff"]) <= tol and abs(section["cost_diff"]) <= tol)
    grand_reconciled = False if unparsed or grand_total is None else (
        abs(sum(s["value"] for s in sections) + sum(adjustments) - grand_total[0])
        <= max(1.0, 0.5 * sum(s["rows"] for s in sections)))
    return {"pages": pages, "sections": sections, "unparsed": unparsed, "grand_total": grand_total,
            "grand_reconciled": grand_reconciled}


def _parse_sch_h_category(pdf_path: str, page_nums: List[int], options: Optional[Dict] = None) -> Optional[Dict]:
    """parse_schedule_4i_pages for the sch_h_category layout. Rows are
    "<description> <Schedule H category> <cost> <value>" with no section totals,
    so each category gathers its rows into one section, all checked together
    against the "Total $ <cost> <value>" grand total. Swap legs print
    negatives in parentheses and "-" for zero. A row without a category takes
    options' row_asset_types (regex on the line -> type).
    options' categories ({category: asset_type}, UPMC 2026-10-17) replace the
    Schedule H ones; such rows may run the name into the category, print
    rate/maturity or share text between it and the amounts, and print cents,
    and their grand total may be the value alone on a line of its own, or
    par, cost and value with no label (Farmers Group, 2026-10-17). A line
    without amounts starts the next row's name (Paul Hastings), less any
    options' wrap_strip match (the first half of a wrapped category)."""
    options = options or {}
    row_types = options.get("row_asset_types", [])
    categories = options.get("categories", _SHC_CATEGORIES)
    row_re = _SHC_ROW_RE
    if "categories" in options:
        row_re = re.compile(r"^(.+?)\s*(" + "|".join(sorted(map(re.escape, categories), key=len, reverse=True))
                            + r")(?:\s+.*?)?\s+" + _SHC_CENTS + r"\s+" + _SHC_CENTS + "$")
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        if not all(1 <= p <= len(pdf.pages) for p in page_nums):
            return None
        bodies = {}
        for k, page_num in enumerate(page_nums):
            lines = [l.strip() for l in _page_text(pdf.pages[page_num - 1], "sch_h_category").splitlines()]
            start = next((j + 1 for j, l in enumerate(lines) if _SHC_COLUMNS_RE.match(l)), None)
            if start is None and k == 0:
                return None
            bodies[page_num] = [l for l in lines[start or 0:] if l and not l.isdigit()
                                and not _SHC_FOOTER_RE.match(l)]

    by_category, unparsed, pages = {}, [], []
    grand_total = None
    wrapped = ""   # a categories plan's name printed above its row
    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            if grand_total is not None:
                break
            clean = " ".join(tok for tok in line.split() if tok != "$")
            if "categories" in options and re.fullmatch(r"(?:\$?\d{1,3}(?:,\d{3}){2,}\s*){1,3}", clean):
                grand_total = [None, _num(clean.split()[-1].lstrip("$"))]
                continue
            if _SHC_GRAND_RE.match(line):
                t2 = _trailing_nums(clean, 2)
                grand_total = t2[1] if t2 and t2[0] == "Total" else None
                if grand_total is None:
                    unparsed.append({"page_number": page_num, "line": line})
                continue
            m = row_re.match(clean)
            if m:
                title, category, cost, value = m.groups()
                asset_type, source = categories[category], "row"
                if wrapped:
                    title, wrapped = wrapped + " " + re.sub(r"^\*\s*", "", title), ""
            elif "categories" in options and not _SHC_BARE_ROW_RE.match(clean):
                part = re.sub(options.get("wrap_strip", r"(?!)"), "", re.sub(r"^\*\s*", "", clean)).strip()
                wrapped = f"{wrapped} {part}".strip()
                continue
            else:
                m = _SHC_BARE_ROW_RE.match(clean)
                asset_type = m and next((label for p, label in row_types if re.search(p, m.group(1), re.I)), "")
                if not asset_type:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                (title, cost, value), category, source = m.groups(), "", "row"
            cost, value = _num(cost) or 0.0, _num(value) or 0.0
            if not asset_type:
                asset_type = detect_asset_type(title)
                source = "row" if asset_type else "none"
            section = by_category.setdefault(category, {
                "heading": category, "asset_type": categories.get(category, ""), "partial": False,
                "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None})
            section["rows"] += 1
            section["cost"] += cost
            section["value"] += value
            if not value:
                continue
            title = re.sub(r"^\*\s*", "", title)
            rate = re.search(r"\b(\d+\.\d+)%", title)
            maturity = re.search(r"\b(\d\d/\d\d/\d{4})\b", title)
            mapped_rows.append({
                "issuer_name": "",
                "investment_description": title,
                "asset_type": asset_type,
                "asset_type_source": source,
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": "",
                "asset_identifier": "",
                "interest_rate": rate.group(1) if rate else "",
                "maturity_date": maturity.group(1) if maturity else "",
                "section_heading": category,
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            })
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    sections = list(by_category.values())
    for section in sections:
        section["cost_diff"] = section["value_diff"] = section["reconciled"] = None
    rows = sum(s["rows"] for s in sections)
    tol = max(1.0, 0.5 * rows) if options.get("row_rounding") else 1.0
    grand_reconciled = False if unparsed or grand_total is None else (
        (grand_total[0] is None or abs(sum(s["cost"] for s in sections) - grand_total[0]) <= tol)
        and abs(sum(s["value"] for s in sections) - grand_total[1]) <= tol)
    return {"pages": pages, "sections": sections, "unparsed": unparsed, "grand_total": grand_total,
            "grand_reconciled": grand_reconciled}


def _parse_category_rate_totals(pdf_path: str, page_nums: List[int],
                                options: Optional[Dict] = None) -> Optional[Dict]:
    """parse_schedule_4i_pages for the category_rate_totals layout. Rows are
    "<description> <category> [<rate> <maturity>] <cost> <value>"; "TOTAL
    <category> <cost> <value>" or a bare "<cost> <value>" closes the rows above
    it, and "TOTAL INVESTMENTS <cost> <value>" is the grand total.
    options is the plan's registry entry:
      categories: pattern matching every category printed on a row.
      category_asset_types: [(pattern, asset_type)] tried on a row's category;
        an untyped category ("Other") is typed by description.
      subtotal_cost_errata: {heading: amount} a printed cost subtotal exceeds
        its rows' costs by, its value still tying; the grand total includes it
        (MassMutual's private equity subtotal, $25,882 over its rows, likely
        CIREP V AIV's cost printed "—").
    Amounts print in whole dollars: half a dollar of rounding per row."""
    options = options or {}
    category_re = re.compile(r"^(.*?)\s+(" + options["categories"] + r")(?:\s+(.*))?$")
    category_types = options.get("category_asset_types", [])
    cost_errata = {k.lower(): v for k, v in options.get("subtotal_cost_errata", {}).items()}
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        if not all(1 <= p <= len(pdf.pages) for p in page_nums):
            return None
        bodies, title_block = {}, set()
        for k, page_num in enumerate(page_nums):
            lines = [l.strip() for l in _upright_text(pdf.pages[page_num - 1]).splitlines()]
            start = next((j + 1 for j, l in enumerate(lines) if _CRT_COLUMNS_RE.match(l)), None)
            if k == 0:
                if start is None:
                    return None
                # Every page repeats the first page's title block (plan name, EIN, plan number).
                title_block = set(lines[:start])
            bodies[page_num] = [l for l in lines[start or 0:] if l and not l.isdigit()
                                and l not in title_block and not _CRT_FOOTER_RE.match(l)]

    sections, unparsed, pages, open_rows = [], [], [], []
    grand_total = None

    def close(heading: str, subtotal: List[float]) -> None:
        cost = sum(r["cost"] for r in open_rows)
        value = sum(r["value"] for r in open_rows)
        section = {"heading": heading, "asset_type": "", "partial": False, "rows": len(open_rows),
                   "cost": cost, "value": value, "subtotal": subtotal,
                   "cost_diff": cost + cost_errata.get(heading.lower(), 0.0) - subtotal[0],
                   "value_diff": value - subtotal[1]}
        tol = max(1.0, 0.5 * len(open_rows))
        section["reconciled"] = abs(section["cost_diff"]) <= tol and abs(section["value_diff"]) <= tol
        types = {r["row"]["asset_type"] for r in open_rows if r["row"]}
        if len(types) == 1:
            section["asset_type"] = types.pop()
        for r in open_rows:
            if r["row"]:
                r["row"]["section_heading"] = heading
        sections.append(section)
        open_rows.clear()

    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            if grand_total is not None:
                break
            t = _trailing_nums(line, 2)
            if not t:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            text, (cost, value) = t
            if _CRT_GRAND_RE.match(line):
                grand_total = [cost, value]
                continue
            if text.startswith("TOTAL ") or not text:
                if not open_rows:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                close(text[len("TOTAL "):].strip() if text else open_rows[-1]["category"], [cost, value])
                continue
            m = category_re.match(text)
            if not m:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            title, category, rest = m.group(1), m.group(2), m.group(3) or ""
            rate = re.search(r"\b(\d+(?:\.\d+)?)%", rest)
            maturity = re.search(r"\b(\d{1,2}/\d{1,2}/\d{4})\b", rest)
            row = None
            if value:
                asset_type = next((label for p, label in category_types if re.search(p, category, re.I)), "")
                source = "row"
                if not asset_type:
                    asset_type = detect_asset_type(title)
                    source = "row" if asset_type else "none"
                row = {
                    "issuer_name": "",
                    "investment_description": re.sub(r"^\*\s*", "", title),
                    "asset_type": asset_type,
                    "asset_type_source": source,
                    "par_value": "",
                    "cost": _fmt(cost),
                    "current_value": _fmt(value),
                    "units_or_shares": "",
                    "asset_identifier": "",
                    "interest_rate": rate.group(1) if rate else "",
                    "maturity_date": maturity.group(1) if maturity else "",
                    "section_heading": "",
                    "extraction_method": "sch4i_text",
                    "page_number": page_num,
                    "row_id": len(mapped_rows) + 1,
                }
                mapped_rows.append(row)
            open_rows.append({"cost": cost or 0.0, "value": value or 0.0, "row": row, "category": category})
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })
    for r in open_rows:
        # Rows the grand total reached with no closing total.
        unparsed.append({"page_number": r["row"]["page_number"] if r["row"] else None, "line": r["category"]})

    tol = max(1.0, 0.5 * sum(s["rows"] for s in sections))
    grand_reconciled = False if unparsed or grand_total is None else (
        abs(sum(s["cost"] for s in sections) + sum(cost_errata.values()) - grand_total[0]) <= tol
        and abs(sum(s["value"] for s in sections) - grand_total[1]) <= tol)
    return {"pages": pages, "sections": sections, "unparsed": unparsed, "grand_total": grand_total,
            "grand_reconciled": grand_reconciled}


def _parse_typed_column_totals(pdf_path: str, page_nums: List[int], section_types=(),
                               scale: float = 1) -> Optional[Dict]:
    """parse_schedule_4i_pages for the typed_column_totals layout. Every page
    must carry the column header.
    section_types: [(pattern, asset_type)] searched in each category's heading.
    scale: rows' amounts are in this many dollars; totals reconcile as printed."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            lines = [l.strip() for l in _page_text(pdf.pages[page_num - 1], "typed_column_totals").splitlines()]
            start = next((i + 1 for i, l in enumerate(lines) if _TCT_COLUMNS_RE.match(l)), None)
            if start is None:
                return None
            bodies[page_num] = [l for l in lines[start:] if l and not _TCT_FOOTER_RE.match(l)]

    sections, unparsed, pages = [], [], []
    current = None
    part_totals = []

    def is_open():
        return current is not None and current["subtotal"] is None

    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            clean = _TCT_NOTE_RE.sub("", " ".join(tok for tok in line.split() if tok != "$"))
            t2 = _trailing_nums(clean, 2)
            if t2 is None:
                if is_open():
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                asset_type = next((t for pat, t in section_types if re.search(pat, clean, re.I)), "")
                current = {"heading": clean, "asset_type": asset_type, "partial": False, "rows": 0,
                           "cost": 0.0, "value": 0.0, "subtotal": None, "cost_diff": None,
                           "value_diff": None, "reconciled": None}
                sections.append(current)
                continue
            if _TCT_TOTAL_RE.match(t2[0]):
                if is_open():
                    cost, value = t2[1]
                    current["subtotal"] = [cost, value]
                    current["cost_diff"] = current["cost"] - cost
                    current["value_diff"] = current["value"] - value
                    current["reconciled"] = abs(current["cost_diff"]) <= 1.0 and abs(current["value_diff"]) <= 1.0
                else:
                    part_totals.append(t2[1])   # "Total directly held investments $ c $ v"
                continue
            # The row repeats its category in the type column ("Debt Securities").
            title = re.sub(r"\s+" + r"\s+".join(map(re.escape, current["heading"].split())) + "$", "",
                           t2[0], flags=re.I) if is_open() else t2[0]
            # "#" on loan, "@" securities lending collateral, "*" party in interest.
            title = re.sub(r"^[#@*]+\s*", "", title)
            if not is_open() or not title:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            cost, value = t2[1]
            current["rows"] += 1
            current["cost"] += cost or 0.0
            current["value"] += value or 0.0
            mapped_rows.append({
                "issuer_name": "",
                "investment_description": title,
                "asset_type": current["asset_type"],
                "asset_type_source": "heading" if current["asset_type"] else "none",
                "par_value": "",
                "cost": _fmt(None if cost is None else cost * scale),
                "current_value": _fmt(None if value is None else value * scale),
                "units_or_shares": "",
                "asset_identifier": "",
                "interest_rate": "",
                "maturity_date": "",
                "section_heading": current["heading"],
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            })
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })
    if is_open():
        unparsed.append({"page_number": page_nums[-1], "line": f"unclosed section {current['heading']}"})
    grand_total = [sum(t[0] for t in part_totals), sum(t[1] for t in part_totals)] if part_totals else None
    grand_reconciled = grand_total is not None and not unparsed and all(s["reconciled"] for s in sections) \
        and abs(sum(s["value"] for s in sections) - grand_total[1]) <= 1.0 \
        and abs(sum(s["cost"] for s in sections) - grand_total[0]) <= 1.0
    return {"pages": pages, "sections": sections, "unparsed": unparsed,
            "grand_total": grand_total, "grand_reconciled": grand_reconciled}


def _parse_cusip_column_totals(pdf_path: str, page_nums: List[int], section_types=()) -> Optional[Dict]:
    """parse_schedule_4i_pages for the cusip_column_totals layout. Every page
    must carry the title.
    section_types: [(pattern, asset_type)] searched in each category's heading."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            lines = [l.strip() for l in (pdf.pages[page_num - 1].extract_text() or "").splitlines()]
            start = next((i + 1 for i, l in enumerate(lines) if _CCT_TITLE_RE.match(l)), None)
            if start is None:
                return None
            bodies[page_num] = [l for l in lines[start:] if l and not _CCT_COLUMNS_RE.match(l)]

    sections, unparsed, pages = [], [], []
    current = None
    grand_total = None

    def is_open():
        return current is not None and current["subtotal"] is None

    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            if grand_total is not None:
                break
            tokens = [tok for tok in line.split() if tok != "$"]
            cusip = tokens.pop() if len(tokens) > 3 and _CCT_CUSIP_RE.match(tokens[-1]) else ""
            clean = " ".join(tokens)
            t2 = _trailing_nums(clean, 2)
            if t2 is None and not cusip:
                if is_open():
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                asset_type = next((t for pat, t in section_types if re.search(pat, clean, re.I)), "")
                current = {"heading": clean, "asset_type": asset_type, "partial": False, "rows": 0,
                           "cost": 0.0, "value": 0.0, "subtotal": None, "cost_diff": None,
                           "value_diff": None, "reconciled": None}
                sections.append(current)
                continue
            if t2 is None:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            total = None if cusip else _CCT_TOTAL_RE.match(t2[0])
            if total and re.match(r"INVESTMENTS$", total.group(1), re.I) and not is_open():
                grand_total = t2[1]
                continue
            if total and is_open():
                cost, value = t2[1]
                current["subtotal"] = [cost, value]
                current["cost_diff"] = current["cost"] - cost
                current["value_diff"] = current["value"] - value
                current["reconciled"] = abs(current["cost_diff"]) <= 1.0 and abs(current["value_diff"]) <= 1.0
                continue
            t3 = _trailing_nums(clean, 3)
            title, units, (cost, value) = (t3[0], t3[1][0], t3[1][1:]) if t3 else (t2[0], None, t2[1])
            title = re.sub(r"^\*+\s*", "", title)   # "*" marks a party in interest
            if not is_open() or not title:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            current["rows"] += 1
            current["cost"] += cost or 0.0
            current["value"] += value or 0.0
            mapped_rows.append({
                "issuer_name": "",
                "investment_description": title,
                "asset_type": current["asset_type"],
                "asset_type_source": "heading" if current["asset_type"] else "none",
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": _fmt(units) if units is not None else "",
                "asset_identifier": cusip,
                "interest_rate": "",
                "maturity_date": "",
                "section_heading": current["heading"],
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            })
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })
    if is_open():
        unparsed.append({"page_number": page_nums[-1], "line": f"unclosed section {current['heading']}"})
    grand_reconciled = grand_total is not None and not unparsed and all(s["reconciled"] for s in sections) \
        and abs(sum(s["value"] for s in sections) - grand_total[1]) <= 1.0 \
        and abs(sum(s["cost"] for s in sections) - grand_total[0]) <= 1.0
    return {"pages": pages, "sections": sections, "unparsed": unparsed,
            "grand_total": grand_total, "grand_reconciled": grand_reconciled}


def _parse_colon_heading_totals(pdf_path: str, page_nums: List[int], section_types=()) -> Optional[Dict]:
    """parse_schedule_4i_pages for the colon_heading_totals layout. Every page
    must carry the column header.
    section_types: [(pattern, asset_type)] searched in each category's name."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            # Padding spaces split "$ 1 4,377,264".
            lines = [l.strip() for l in
                     (_drop_overlapping_spaces(pdf.pages[page_num - 1]).extract_text() or "").splitlines()]
            start = next((i + 1 for i, l in enumerate(lines) if _CHT_COLUMNS_RE.match(l)), None)
            if start is None:
                return None
            bodies[page_num] = [l for l in lines[start:] if l and not _CHT_FOOTER_RE.match(l)]

    sections, unparsed, pages = [], [], []
    current = None
    grand_total = None
    wrapped = []   # description lines of a row whose amounts follow

    def close(cost, value):
        current["subtotal"] = [cost, value]
        current["cost_diff"] = current["cost"] - cost
        current["value_diff"] = current["value"] - value
        current["reconciled"] = abs(current["cost_diff"]) <= 1.0 and abs(current["value_diff"]) <= 1.0

    def is_open():
        return current is not None and current["subtotal"] is None

    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            if grand_total is not None:
                break
            clean = " ".join(tok for tok in line.split() if tok != "$")
            t2 = _trailing_nums(clean, 2)
            if t2 is not None and "." in clean.split()[-1]:
                t2 = None   # amounts are whole dollars: "... TRANCHE # TR 10 3.7" is a wrapped description
            if t2 is None and clean.endswith(":") and not _CHT_WRAPPED_RE.search(clean):
                if wrapped:
                    unparsed.extend({"page_number": page_num, "line": w} for w in wrapped)
                    wrapped = []
                if is_open() and current["rows"] == 1:
                    close(current["cost"], current["value"])   # a one-row category prints no total
                if is_open():
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                heading = clean[:-1].strip()
                asset_type = next((t for pat, t in section_types if re.search(pat, heading, re.I)), "")
                current = {"heading": heading, "asset_type": asset_type, "partial": False, "rows": 0,
                           "cost": 0.0, "value": 0.0, "subtotal": None, "cost_diff": None,
                           "value_diff": None, "reconciled": None}
                sections.append(current)
                continue
            if t2 is None:
                if is_open():
                    wrapped.append(clean)   # the row's amounts print on a later line
                else:
                    unparsed.append({"page_number": page_num, "line": line})
                continue
            # A total prints two amounts; "TOTAL CAPITAL INTL SA ..." is a row with three.
            if _CHT_TOTAL_RE.match(t2[0]) and is_open() and not wrapped and _trailing_nums(clean, 3) is None:
                close(*t2[1])
                continue
            if (t2[0] == "" or _CHT_GRAND_LABEL_RE.match(t2[0])) and not is_open() and not wrapped:
                grand_total = t2[1]
                continue
            if wrapped:
                clean = " ".join(wrapped + [clean])
                wrapped = []
            t3 = _trailing_nums(clean, 3)
            title, units, (cost, value) = (t3[0], t3[1][0], t3[1][1:]) if t3 else (t2[0], None, t2[1])
            # "*" marks a party in interest, "^" an investment the trustee does not certify.
            title = re.sub(r"^[*^]+\s*", "", title)
            ident = _CHT_ID_RE.search(title)
            if ident:
                title = title[:ident.start()].strip()
            if not is_open() or not title:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            current["rows"] += 1
            current["cost"] += cost or 0.0
            current["value"] += value or 0.0
            mapped_rows.append({
                "issuer_name": "",
                "investment_description": title,
                "asset_type": current["asset_type"],
                "asset_type_source": "heading" if current["asset_type"] else "none",
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": _fmt(units) if units is not None else "",
                "asset_identifier": ident.group(1) if ident else "",
                "interest_rate": "",
                "maturity_date": "",
                "section_heading": current["heading"],
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            })
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })
    if wrapped:
        unparsed.extend({"page_number": page_nums[-1], "line": w} for w in wrapped)
    if is_open():
        unparsed.append({"page_number": page_nums[-1], "line": f"unclosed section {current['heading']}"})
    grand_reconciled = grand_total is not None and not unparsed and all(s["reconciled"] for s in sections) \
        and abs(sum(s["value"] for s in sections) - grand_total[1]) <= 1.0 \
        and abs(sum(s["cost"] for s in sections) - grand_total[0]) <= 1.0
    return {"pages": pages, "sections": sections, "unparsed": unparsed,
            "grand_total": grand_total, "grand_reconciled": grand_reconciled}


def _parse_bold_heading_totals(pdf_path: str, page_nums: List[int], section_types=()) -> Optional[Dict]:
    """parse_schedule_4i_pages for the bold_heading_totals layout. The first
    page must carry the column header.
    section_types: [(pattern, asset_type)] searched in each category's name."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            tls = pdf.pages[page_num - 1].extract_text_lines(return_chars=True)
            start = next((i + 1 for i, tl in enumerate(tls) if _BHT_COLUMNS_RE.match(tl["text"].strip())), None)
            if start is None and page_num == page_nums[0]:
                return None
            body = []
            for tl in tls[start or 0:]:
                line = tl["text"].strip()
                if not line or _BHT_PAGE_RE.match(line):
                    continue
                chars = [c for c in tl["chars"] if c["text"].strip()]
                body.append((line, bool(chars) and all("Bold" in c["fontname"] for c in chars)))
            bodies[page_num] = body

    sections, unparsed, pages = [], [], []
    current = None
    grand_total = None
    wrapped = ""         # a row's lines before its amounts
    pending_total = ""   # a "Total ..." line wrapping onto the next

    def type_of(name):
        return next((t for pat, t in section_types if re.search(pat, name, re.I)), "")

    def open_section(heading):
        section = {"heading": heading, "partial": False, "rows": 0, "cost": 0.0, "value": 0.0,
                   "subtotal": None, "cost_diff": None, "value_diff": None, "reconciled": None,
                   "asset_type": type_of(heading), "mapped": []}
        sections.append(section)
        return section

    def close(section, label, cost, value):
        section["subtotal"] = [cost, value]
        section["cost_diff"] = section["cost"] - cost
        section["value_diff"] = section["value"] - value
        section["reconciled"] = abs(section["cost_diff"]) <= 1.0 and abs(section["value_diff"]) <= 1.0
        if not section["heading"]:
            # Rows printed before any heading ("Securities on loan"): the total names them.
            section["heading"], section["asset_type"] = label, type_of(label)
        for row in section["mapped"]:
            row["section_heading"] = section["heading"]
            row["asset_type"] = section["asset_type"]
            row["asset_type_source"] = "heading" if section["asset_type"] else "none"

    def is_open():
        return current is not None and current["subtotal"] is None

    for page_num in page_nums:
        mapped_rows = []
        for line, bold in bodies[page_num]:
            if grand_total is not None:
                break
            clean = " ".join(tok for tok in line.split() if tok != "$")
            if pending_total:
                clean, pending_total = pending_total + " " + clean, ""
            t = _trailing_nums(clean, 2)
            total = _BHT_TOTAL_RE.match(clean)
            if total and not wrapped:
                if t is None:
                    pending_total = clean
                elif _BHT_GRAND_RE.match(clean):
                    grand_total = t[1]
                elif is_open():
                    close(current, t[0].split(None, 1)[1], *t[1])
                else:
                    unparsed.append({"page_number": page_num, "line": line})
                continue
            if t is None:
                one = _trailing_nums(clean, 1)
                if _BHT_LESS_RE.match(clean) and one and is_open() and not wrapped:
                    # "Less: Securities on loan (208,825)": in the sum, not a row.
                    current["value"] += one[1][0]
                elif bold and not wrapped:
                    if is_open() and current["rows"] == 0:
                        current["heading"] += " " + clean   # wrapped heading
                        current["asset_type"] = type_of(current["heading"])
                    elif is_open():
                        unparsed.append({"page_number": page_num, "line": line})
                    else:
                        current = open_section(clean)
                else:
                    wrapped = (wrapped + " " + clean).strip()
                continue
            if t[0] == "" and not wrapped:
                # A sub-total before "Less: Securities on loan".
                if not (is_open() and abs(current["cost"] - t[1][0]) <= 1.0
                        and abs(current["value"] - t[1][1]) <= 1.0):
                    unparsed.append({"page_number": page_num, "line": line})
                continue
            title, (cost, value) = (wrapped + " " + t[0]).strip(), t[1]
            wrapped = ""
            if not is_open():
                current = open_section("")
            # "*" / "***" mark a party in interest, on any of a row's lines.
            title = " ".join(tok for tok in title.split() if tok.strip("*"))
            current["rows"] += 1
            current["cost"] += cost or 0.0
            current["value"] += value or 0.0
            row = {
                "issuer_name": "",
                "investment_description": title,
                "asset_type": current["asset_type"],
                "asset_type_source": "heading" if current["asset_type"] else "none",
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": "",
                "asset_identifier": "",
                "interest_rate": "",
                "maturity_date": "",
                "section_heading": current["heading"],
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            }
            current["mapped"].append(row)
            mapped_rows.append(row)
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })
    if wrapped or pending_total:
        unparsed.append({"page_number": page_nums[-1], "line": wrapped or pending_total})
    if is_open():
        unparsed.append({"page_number": page_nums[-1], "line": f"unclosed section {current['heading']}"})
    for section in sections:
        del section["mapped"]
    # Against the printed category totals: Marsh's cost totals sum $2 over
    # its grand total, each rounded.
    grand_reconciled = grand_total is not None and not unparsed and all(s["reconciled"] for s in sections) \
        and abs(sum(s["subtotal"][1] for s in sections) - grand_total[1]) <= len(sections) \
        and abs(sum(s["subtotal"][0] for s in sections) - grand_total[0]) <= len(sections)
    return {"pages": pages, "sections": sections, "unparsed": unparsed,
            "grand_total": grand_total, "grand_reconciled": grand_reconciled}


def _parse_cusip_type_blocks(pdf_path: str, page_nums: List[int], scale: float = 1) -> Optional[Dict]:
    """parse_schedule_4i_pages for the cusip_type_blocks layout. The first
    page must carry the column header.
    scale: rows' amounts are in this many dollars; totals reconcile as
    printed."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            lines = [l.strip() for l in
                     (_drop_overlapping_spaces(pdf.pages[page_num - 1]).extract_text() or "").splitlines()]
            start = next((i + 1 for i, l in enumerate(lines) if _CTB_COLUMNS_RE.match(l)), None)
            if start is None and page_num == page_nums[0]:
                return None
            bodies[page_num] = [l for l in lines[start or 0:] if l]

    sections, unparsed, pages = [], [], []
    current = None
    grand_total = None
    pending = None   # a "Total ..." line wrapping onto the next

    def block_type(heading):
        return next((t for pat, t in _CTB_BLOCK_TYPES if re.search(pat, heading, re.I)), "")

    def open_section(heading):
        section = {"heading": heading, "partial": False, "rows": 0, "cost": 0.0, "value": 0.0,
                   "subtotal": None, "cost_diff": None, "value_diff": None, "reconciled": None,
                   "asset_type": block_type(heading)}
        sections.append(section)
        return section

    def close(section, cost, value):
        section["subtotal"] = [cost, value]
        section["cost_diff"] = section["cost"] - cost
        section["value_diff"] = section["value"] - value
        # Rows rounded to thousands each.
        tol = 1.0 + 0.5 * section["rows"]
        section["reconciled"] = abs(section["value_diff"]) <= tol and abs(section["cost_diff"]) <= tol

    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            if grand_total is not None:
                break
            if pending is not None:
                line, pending = pending + " " + line, None
            t = _trailing_nums(line, 2)
            if line.startswith("*"):
                if current is not None and current["subtotal"] is None:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                if t is None:
                    current = open_section(line[1:])
                    continue
                current = open_section(t[0][1:])
                title, (cost, value), asset_type = t[0][1:], t[1], current["asset_type"]
                source = "heading" if asset_type else "none"
            elif _CTB_TOTAL_RE.match(line):
                if t is None:
                    pending = line
                    continue
                if _CTB_GRAND_RE.match(line):
                    grand_total = t[1]
                elif current is not None and current["subtotal"] is None:
                    close(current, *t[1])
                # Otherwise a rollup of the blocks above.
                continue
            elif t is None:
                # Category captions between blocks ("NON-POOLED SEPARATE ACCOUNTS").
                if current is not None and current["subtotal"] is None:
                    unparsed.append({"page_number": page_num, "line": line})
                continue
            elif current is None or current["subtotal"] is not None:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            elif t[0] == "":
                close(current, *t[1])
                continue
            else:
                text, (cost, value) = t
                m = _CTB_TYPE_RE.search(text)
                if m:
                    asset_type = next(at for pat, at in _CTB_ROW_TYPES if re.fullmatch(pat, m.group(1)))
                    source = "row"
                    text = text[:m.start()]
                    tokens = text.split()
                    cusip = tokens[-1] if tokens else ""
                    text = " ".join(tokens[:-1])
                else:
                    # A TYPE drawn over by another line (Cigna's money market
                    # row): the block's type, the text after the repeated cusip
                    # dropped.
                    asset_type = current["asset_type"]
                    source = "heading" if asset_type else "none"
                    tokens = text.split()
                    lead = re.match(r"[0-9A-Z#]+", text)
                    k = next((k for k in range(len(tokens) - 1, 0, -1) if lead and tokens[k] in
                              (lead.group(0), lead.group(0).lstrip("0"))), None)
                    if k is None:
                        unparsed.append({"page_number": page_num, "line": line})
                        continue
                    cusip, text = tokens[k], " ".join(tokens[:k])
                # The leading cusip, glued to the description or not
                # ("608919718FEDERATED", "32614117" for "032614117").
                for lead in (cusip, cusip.lstrip("0")):
                    if lead and text.startswith(lead):
                        text = text[len(lead):].strip()
                        break
                title = text
            current["rows"] += 1
            current["cost"] += cost or 0.0
            current["value"] += value or 0.0
            mapped_rows.append({
                "issuer_name": "",
                "investment_description": title,
                "asset_type": asset_type,
                "asset_type_source": source,
                "par_value": "",
                "cost": _fmt(None if cost is None else cost * scale),
                "current_value": _fmt(None if value is None else value * scale),
                "units_or_shares": "",
                "asset_identifier": "",
                "interest_rate": "",
                "maturity_date": "",
                "section_heading": current["heading"],
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            })
            if line.startswith("*"):
                close(current, cost, value)
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })
    if current is not None and current["subtotal"] is None:
        unparsed.append({"page_number": page_nums[-1], "line": f"unclosed section {current['heading']}"})
    tol = 1.0 + 0.5 * sum(s["rows"] for s in sections)
    grand_reconciled = grand_total is not None and not unparsed and all(s["reconciled"] for s in sections) \
        and abs(sum(s["value"] for s in sections) - grand_total[1]) <= tol
    return {"pages": pages, "sections": sections, "unparsed": unparsed,
            "grand_total": grand_total, "grand_reconciled": grand_reconciled}


def _parse_fmv_cost_summary(pdf_path: str, page_nums: List[int]) -> Optional[Dict]:
    """parse_schedule_4i_pages for the fmv_cost_summary layout. Every page
    must carry the "@ FMV @ COST" header."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            page = pdf.pages[page_num - 1]
            # The "Information Classification: Company Internal" stamp (its own
            # font size) runs through a fund name ("VANGUARD RInUfoSrmSaEtiLoLn ...").
            groups: Dict[tuple, str] = {}
            for c in page.chars:
                key = (c["fontname"], round(c["size"], 1))
                groups[key] = groups.get(key, "") + c["text"]
            stamp = {k for k, text in groups.items()
                     if "Information Classification:" in text and len(text) < 100}
            if stamp:
                page = page.filter(lambda o: o.get("object_type") != "char"
                                   or (o["fontname"], round(o["size"], 1)) not in stamp)
            lines = [l.strip() for l in (_drop_overlapping_spaces(page).extract_text() or "").splitlines()]
            start = next((i + 1 for i, l in enumerate(lines) if _FCS_COLUMNS_RE.match(l)), None)
            if start is None:
                return None
            bodies[page_num] = [l for l in lines[start:] if l]

    sections, unparsed, pages = [], [], []
    current = None

    def open_section(heading):
        section = {"heading": heading, "partial": False, "rows": 0, "cost": 0.0, "value": 0.0,
                   "subtotal": None, "cost_diff": None, "value_diff": None, "reconciled": None,
                   "asset_type": next((t for pat, t in _FCS_ASSET_TYPES if re.search(pat, heading, re.I)), "")}
        sections.append(section)
        return section

    def close(section, cost, value):
        section["subtotal"] = [cost, value]
        section["value_diff"] = section["value"] - value
        section["cost_diff"] = None if cost is None else section["cost"] - cost
        section["reconciled"] = abs(section["value_diff"]) <= 1.0 and (
            cost is None or abs(section["cost_diff"]) <= 1.0)

    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            attached = _FCS_ATTACHED_RE.search(line)
            if attached and _NUM_RE.match(attached.group(1).strip()):
                if current is not None and current["subtotal"] is None:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                title, cost, value = line[:attached.start()], None, _num(attached.group(1).strip())
                current = open_section(title)
            else:
                t = _trailing_nums(line, 2)
                if t is None:
                    if current is None or current["subtotal"] is not None:
                        current = open_section(line)
                    else:
                        unparsed.append({"page_number": page_num, "line": line})
                    continue
                title, (value, cost) = t
                if current is None or current["subtotal"] is not None:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                if title in ("", "TOTAL"):
                    close(current, cost, value)
                    continue
            current["rows"] += 1
            current["cost"] += cost or 0.0
            current["value"] += value or 0.0
            mapped_rows.append({
                "issuer_name": "",
                "investment_description": title,
                "asset_type": current["asset_type"],
                "asset_type_source": "heading" if current["asset_type"] else "none",
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": "",
                "asset_identifier": "",
                "interest_rate": "",
                "maturity_date": "",
                "section_heading": current["heading"],
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            })
            if attached:
                close(current, None, value)
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })
    if current is not None and current["subtotal"] is None:
        unparsed.append({"page_number": page_nums[-1], "line": f"unclosed section {current['heading']}"})
    # No grand total is printed: the category totals alone check it.
    grand_reconciled = None if not unparsed and all(s["reconciled"] for s in sections) else False
    return {"pages": pages, "sections": sections, "unparsed": unparsed,
            "grand_total": None, "grand_reconciled": grand_reconciled}


def _parse_leading_category(pdf_path: str, page_nums: List[int]) -> Optional[Dict]:
    """parse_schedule_4i_pages for the leading_category layout. Each manager's
    category is a section closed by its "TOTAL <CATEGORY>"; each block's
    "TOTAL INVESTMENTS" (or a value-only block's category total) checks the
    sections since the last block, and the summary's TOTAL line is the grand
    total (value only)."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        if not all(1 <= p <= len(pdf.pages) for p in page_nums):
            return None
        bodies = {}
        for page_num in page_nums:
            text = _upright_text(_drop_overlapping_spaces(pdf.pages[page_num - 1]))
            if not _LC_TITLE_RE.search(text):
                return None
            lines = [l.strip() for l in text.splitlines()]
            end = next((j for j, l in enumerate(lines) if _LC_HEADER_END_RE.match(l)), None)
            if end is None:
                return None
            # "revalued": <hist> <revalued> <market>; "plain": <units> <cost>
            # <value>; "value": <value>; "summary": manager rows and the TOTAL.
            header = " ".join(lines[:end + 1])
            kind = ("summary" if lines[end].startswith("Cash ") else "value" if "MARKET VALUE" in lines[end]
                    else "revalued" if "REVALUED" in header else "plain")
            bodies[page_num] = (kind, [l for l in lines[end + 1:] if l])

    def category_of(text):
        m = _LC_CATEGORY_RE.match(text)
        if not m:
            return None
        i = next(int(k[1:]) for k, v in m.groupdict().items() if v is not None)
        return _LC_CATEGORIES[i][0], _LC_CATEGORIES[i][1], m.groups()[-1].strip()

    def open_section(category, asset_type):
        section = {"heading": category, "asset_type": asset_type, "partial": False,
                   "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None}
        sections.append(section)
        return section

    sections, unparsed, pages, blocks = [], [], [], []
    current = None
    block_start = 0
    grand_total = None
    for page_num in page_nums:
        kind, body = bodies[page_num]
        mapped_rows = []
        for line in body:
            clean = " ".join(tok for tok in line.split() if tok != "$")
            if kind == "summary":
                t = _LC_TOTAL_RE.match(clean)
                if t:
                    nums = t.group(1).split()
                    if nums and all(_NUM_RE.match(n) for n in nums):
                        grand_total = [_num(nums[-1])]
                continue
            if _LC_BLOCK_TOTAL_RE.match(clean):
                t = _trailing_nums(clean, 1)
                if t is None:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                printed = t[1][0]
                got = sum(sec["value"] for sec in sections[block_start:])
                blocks.append({"page_number": page_num, "value": got, "printed": printed})
                block_start, current = len(sections), None
                continue
            total = _LC_TOTAL_RE.match(clean)
            if total:
                rest = total.group(1)
                n = 1 if kind == "value" else 3
                t = _trailing_nums(rest, n)
                cat = category_of(t[0]) if t else None
                if t is None or cat is None or current is None or cat[0].replace(" ", "")[:12] != \
                        current["heading"].replace(" ", "")[:12]:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                current["subtotal"] = [None if kind == "value" else t[1][0], t[1][-1]]
                if kind == "value":
                    # A value-only block ends on its category total.
                    blocks.append({"page_number": page_num, "value": current["value"], "printed": t[1][-1]})
                    block_start = len(sections)
                current = None
                continue
            cat = category_of(clean)
            n = 1 if kind == "value" else 3
            t = _trailing_nums(cat[2] if cat else clean, n)
            if cat and t is None and not cat[2]:
                # A category on its own line heads the rows below it.
                current = open_section(cat[0], cat[1])
                continue
            if t is None:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            text, amounts = t
            if cat:
                if current is None or current["heading"] != cat[0] or current["subtotal"] is not None:
                    current = open_section(cat[0], cat[1])
            elif current is None:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            if kind == "revalued":
                cost, value = amounts[0], amounts[2]
                rt = _trailing_nums(text, 1)
                units, text = (rt[1][0], rt[0]) if rt and rt[0] else (None, text)
            elif kind == "plain":
                units, cost, value = amounts
            else:
                units, cost, value = None, None, amounts[0]
            title = text or current["heading"]
            current["rows"] += 1
            current["cost"] += cost or 0.0
            current["value"] += value or 0.0
            if not value:
                continue
            asset_type = current["asset_type"] or detect_asset_type(title)
            row = {
                "issuer_name": "",
                "investment_description": title,
                "asset_type": asset_type,
                "asset_type_source": "heading" if current["asset_type"] else ("row" if asset_type else "none"),
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": _fmt(units) if units else "",
                "asset_identifier": "",
                "interest_rate": "",
                "maturity_date": "",
                "section_heading": current["heading"],
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            }
            mapped_rows.append(row)
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    for section in sections:
        sub = section["subtotal"]
        tol = max(1.0, 0.01 * section["rows"])
        section["value_diff"] = None if sub is None else section["value"] - sub[1]
        section["cost_diff"] = None if sub is None or sub[0] is None else section["cost"] - sub[0]
        section["reconciled"] = None if sub is None else (
            abs(section["value_diff"]) <= tol and (section["cost_diff"] is None or abs(section["cost_diff"]) <= tol))
    # Sections after the last block total were never checked.
    for section in sections[block_start:]:
        section["reconciled"] = False
    for block in blocks:
        if abs(block["value"] - block["printed"]) > 1.0:
            unparsed.append({"page_number": block["page_number"],
                             "line": f"TOTAL INVESTMENTS {block['printed']:,.2f} != rows {block['value']:,.2f}"})
    # The summary prints whole dollars, one rounding per manager and category.
    grand_reconciled = False if unparsed or grand_total is None else (
        abs(sum(s["value"] for s in sections) - grand_total[0]) <= max(1.0, 0.5 * len(sections)))
    return {"pages": pages, "sections": sections, "unparsed": unparsed, "grand_total": grand_total,
            "grand_reconciled": grand_reconciled}


def _parse_titled_category_pages(pdf_path: str, page_nums: List[int],
                                 options: Optional[Dict] = None) -> Optional[Dict]:
    """parse_schedule_4i_pages for the titled_category_pages layout. Each page
    names its category in a "<CATEGORY> AS OF <date>" line; rows end in
    "<cost> <value>" ("<rate> <maturity> <par> <cost> <value>" under a "PAR
    VALUE" column header), a line without them continues the row above, and
    "TOTAL <CATEGORY>" closes the category.
    options is the plan's registry entry:
      heading_asset_types: [(pattern, asset_type)] tried on each category;
        an untyped category's rows are typed by description."""
    options = options or {}
    heading_types = options.get("heading_asset_types", [])
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            lines = [l.strip() for l in (pdf.pages[page_num - 1].extract_text() or "").splitlines()]
            category = next((m.group(1) for m in map(_TCP_CATEGORY_RE.match, lines) if m), None)
            end = next((j for j, l in enumerate(lines) if _TCP_COLUMNS_END_RE.search(l)), None)
            if category is None or end is None:
                return None
            has_par = bool(re.search(r"\sPAR\s+VALUE\s+COST\s+FAIR\s+VALUE$", lines[end]))
            # "$" prints as its own token before a page's first amounts.
            body = [re.sub(r"(?:(?<=\s)|^)\$\s+", "", l) for l in lines[end + 1:]
                    if l and not _TCP_FOOTER_RE.match(l)]
            bodies[page_num] = (category, has_par, body)

    sections, unparsed, pages, open_rows = [], [], [], []
    heading, heading_type = None, ""
    last_row = None
    for page_num in page_nums:
        category, has_par, body = bodies[page_num]
        if category != heading:
            if open_rows:
                unparsed.append({"page_number": page_num, "line": f"{len(open_rows)} rows under {heading} unclosed"})
                open_rows.clear()
            heading, last_row = category, None
            heading_type = next((label for p, label in heading_types if re.search(p, heading, re.I)), "")
        n = 3 if has_par else 2
        mapped_rows = []
        for line in body:
            if line.startswith("TOTAL "):
                t = _trailing_nums(line, n)
                if not t or t[0] != f"TOTAL {heading}" or not open_rows:
                    unparsed.append({"page_number": page_num, "line": line})
                    open_rows.clear()
                    continue
                cost, value = t[1][-2:]
                rows_cost = sum(r["cost"] for r in open_rows)
                rows_value = sum(r["value"] for r in open_rows)
                section = {"heading": heading, "asset_type": heading_type, "partial": False,
                           "rows": len(open_rows), "cost": rows_cost, "value": rows_value,
                           "subtotal": [cost, value], "cost_diff": rows_cost - cost,
                           "value_diff": rows_value - value}
                section["reconciled"] = abs(section["cost_diff"]) <= 1.0 and abs(section["value_diff"]) <= 1.0
                sections.append(section)
                open_rows.clear()
                last_row = None
                continue
            t = _trailing_nums(line, n)
            if not t:
                if last_row is not None:
                    last_row["investment_description"] += f" {line}"
                else:
                    unparsed.append({"page_number": page_num, "line": line})
                continue
            text, amounts = t
            cost, value = amounts[-2:]
            open_rows.append({"cost": cost or 0.0, "value": value or 0.0})
            last_row = None
            ident, units, par, rate, maturity = "", None, None, "", ""
            if has_par:
                par = amounts[0]
                m = re.match(r"^(.*?)\s+(\d+(?:\.\d+)?)\s+(\d{1,2}/\d{1,2}/\d{4})$", text)
                if m:
                    text, rate, maturity = m.groups()
                parts = text.split(" ", 1)
                if len(parts) == 2 and re.fullmatch(r"[0-9A-Z]{6,9}", parts[0]) and re.search(r"\d", parts[0]):
                    ident, text = parts
            else:
                parts = text.split(" ", 1)
                if len(parts) == 2 and _NUM_RE.match(parts[0]):
                    units, text = _num(parts[0]), parts[1]
            text = re.sub(r"^\*\s*", "", text)
            if not value:
                continue
            asset_type, source = heading_type, "section"
            if not asset_type:
                asset_type = detect_asset_type(text)
                source = "row" if asset_type else "none"
            last_row = {
                "issuer_name": "",
                "investment_description": text,
                "asset_type": asset_type,
                "asset_type_source": source,
                "par_value": _fmt(par),
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": _fmt(units),
                "asset_identifier": ident,
                "interest_rate": rate,
                "maturity_date": maturity,
                "section_heading": heading,
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            }
            mapped_rows.append(last_row)
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })
    if open_rows:
        unparsed.append({"page_number": page_nums[-1], "line": f"{len(open_rows)} rows under {heading} unclosed"})
    return {"pages": pages, "sections": sections, "unparsed": unparsed, "grand_total": None,
            "grand_reconciled": None}


def _parse_year_end_package(pdf_path: str, page_nums: List[int],
                            options: Optional[Dict] = None) -> Optional[Dict]:
    """parse_schedule_4i_pages for the year_end_package layout. Rows end in
    "<price> <shares/par> <adjusted cost> <historical cost> <current value>
    <g/l> <g/l>"; a line without amounts after a "=====" rule is a category
    heading ("<category> (CONT.)" atop a page repeats it), any other continues
    the row above; "TOTAL FOR <category> <six amounts>" closes the category and
    "TOTAL ASSETS" is the grand total. The pages must start at the schedule's
    first page.
    options is the plan's registry entry:
      heading_asset_types: [(pattern, asset_type)] tried on each category;
        an untyped category's rows are typed by description."""
    options = options or {}
    heading_types = options.get("heading_asset_types", [])
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            lines = [l.strip() for l in (pdf.pages[page_num - 1].extract_text() or "").splitlines()]
            start = next((j + 1 for j, l in enumerate(lines) if _YEP_COLUMNS_RE.match(l)), None)
            if start is None:
                return None
            bodies[page_num] = [l for l in lines[start:] if l and not _YEP_FOOTER_RE.match(l)]

    sections, unparsed, pages, open_rows = [], [], [], []
    grand_total = None
    heading, heading_type = "", ""
    expect_heading = True   # the first line, and any after a "=====" rule
    last_row = None
    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            if grand_total is not None:
                break
            if _YEP_RULE_RE.match(line):
                if line.startswith("="):
                    expect_heading, last_row = True, None
                continue
            if line.startswith("TOTAL "):
                t = _trailing_nums(line, 6)
                if not t:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                text, (_, cost, _, value, _, _) = t
                if _YEP_GRAND_RE.match(line):
                    grand_total = [cost, value]
                    continue
                name = text[len("TOTAL FOR "):].strip() if text.startswith("TOTAL FOR ") else text
                if not open_rows or not heading.startswith(name):
                    unparsed.append({"page_number": page_num, "line": line})
                    open_rows.clear()
                    continue
                rows_cost = sum(r["cost"] for r in open_rows)
                rows_value = sum(r["value"] for r in open_rows)
                section = {"heading": heading, "asset_type": heading_type, "partial": False,
                           "rows": len(open_rows), "cost": rows_cost, "value": rows_value,
                           "subtotal": [cost, value], "cost_diff": rows_cost - cost,
                           "value_diff": rows_value - value}
                section["reconciled"] = abs(section["cost_diff"]) <= 1.0 and abs(section["value_diff"]) <= 1.0
                sections.append(section)
                open_rows.clear()
                last_row = None
                continue
            t = _trailing_nums(line, 7)
            if not t:
                if re.search(r"\(CONT\.\)$", line):
                    expect_heading = False
                elif expect_heading:
                    heading, expect_heading = line, False
                    heading_type = next((label for p, label in heading_types if re.search(p, heading, re.I)), "")
                elif last_row is not None:
                    last_row["investment_description"] += f" {line}"
                elif not open_rows:
                    unparsed.append({"page_number": page_num, "line": line})
                # else: the wrapped name of a zero-value row, dropped with it.
                continue
            expect_heading = False
            title, (price, units, cost, _, value, _, _) = t
            open_rows.append({"cost": cost or 0.0, "value": value or 0.0})
            last_row = None
            if not value:
                continue
            asset_type, source = heading_type, "section"
            if not asset_type:
                asset_type = detect_asset_type(title)
                source = "row" if asset_type else "none"
            rate = re.search(r"(\d+(?:\.\d+)?)%", title)
            maturity = re.search(r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b", title)
            last_row = {
                "issuer_name": "",
                "investment_description": re.sub(r"^\*\s*", "", title),
                "asset_type": asset_type,
                "asset_type_source": source,
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": _fmt(units),
                "asset_identifier": "",
                "interest_rate": rate.group(1) if rate else "",
                "maturity_date": maturity.group(1) if maturity else "",
                "section_heading": heading,
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            }
            mapped_rows.append(last_row)
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })
    if open_rows:
        # Rows the grand total reached with no closing total.
        unparsed.append({"page_number": page_nums[-1], "line": f"{len(open_rows)} rows under {heading} unclosed"})

    grand_reconciled = False if unparsed or grand_total is None else (
        abs(sum(s["cost"] for s in sections) - grand_total[0]) <= 1.0
        and abs(sum(s["value"] for s in sections) - grand_total[1]) <= 1.0)
    return {"pages": pages, "sections": sections, "unparsed": unparsed, "grand_total": grand_total,
            "grand_reconciled": grand_reconciled}


def _parse_summary_attached(pdf_path: str, page_nums: List[int], options: Optional[Dict] = None) -> Optional[Dict]:
    """parse_schedule_4i_pages for the summary_attached layout. The first page
    lists funds as "<party> <description> <cost> <value>" up to "Total <cost>
    <value>"; its "Various - See Attached" line stands for the securities on the
    following pages ("<shares> <description> <cost> <value>"), which end on a
    bare "<cost> <value>" line. The summary has no category column, so options'
    row_asset_types (regex on the line -> type) types its funds, ahead of
    detect_asset_type."""
    options = options or {}
    row_types = options.get("row_asset_types", [])
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        if not all(1 <= p <= len(pdf.pages) for p in page_nums):
            return None
        bodies = {}
        for k, page_num in enumerate(page_nums):
            lines = [l.strip() for l in _page_text(pdf.pages[page_num - 1], "summary_attached").splitlines()]
            columns_re = _SA_COLUMNS_RE if k == 0 else _SA_DETAIL_COLUMNS_RE
            start = next((j + 1 for j, l in enumerate(lines) if columns_re.match(l)), None)
            if start is None:
                return None
            bodies[page_num] = [l for l in lines[start:] if l and not _SA_PAGE_NUMBER_RE.match(l)
                                and not l.startswith("* Indicates")]

    summary = {"heading": "Summary", "asset_type": "", "partial": False,
               "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None}
    attached = {"heading": "See Attached", "asset_type": "", "partial": False,
                "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None}
    sections, unparsed, pages = [summary, attached], [], []
    grand_total = placeholder = closing = None
    prefix = ""
    for k, page_num in enumerate(page_nums):
        mapped_rows = []
        for line in bodies[page_num]:
            clean = " ".join(tok for tok in line.split() if tok != "$")
            t2 = _trailing_nums(clean, 2)
            if t2 is None:
                if k == 0 and grand_total is None:
                    # A party name printed on its own line ("JP Morgan Chase Bank
                    # Securities Lending:") heads the next row.
                    prefix = f"{prefix} {clean}".strip()
                else:
                    unparsed.append({"page_number": page_num, "line": line})
                continue
            text, (cost, value) = t2
            if k == 0:
                if grand_total is not None:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                if re.match(r"^Total$", text):
                    grand_total = [cost, value]
                    continue
                text, prefix = f"{prefix} {text}".strip(), ""
                if _SA_ATTACHED_RE.search(text):
                    placeholder = [cost, value]
                    continue
                section, shares = summary, ""
            else:
                if not text:
                    closing = [cost, value]
                    continue
                if closing is not None:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                tokens = text.split()
                shares = tokens[0] if len(tokens) > 1 and _NUM_RE.match(tokens[0]) else ""
                text = " ".join(tokens[1:]) if shares else text
                section = attached
            section["rows"] += 1
            section["cost"] += cost or 0.0
            section["value"] += value or 0.0
            if not value:
                continue
            asset_type = next((label for p, label in row_types if re.search(p, text, re.I)), "") if k == 0 else ""
            source = "row"
            if not asset_type:
                asset_type = detect_asset_type(text)
                source = "row" if asset_type else "none"
            mapped_rows.append({
                "issuer_name": "",
                "investment_description": text,
                "asset_type": asset_type,
                "asset_type_source": source,
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": _fmt(_num(shares)) if shares else "",
                "asset_identifier": "",
                "interest_rate": "",
                "maturity_date": "",
                "section_heading": section["heading"],
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            })
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    # The attached detail ties to its closing line, which must repeat the
    # summary's "See Attached" amounts.
    attached["subtotal"] = closing
    if closing is not None and placeholder != closing:
        unparsed.append({"page_number": page_nums[-1], "line": f"closing {closing} != See Attached {placeholder}"})
    summary["subtotal"] = None if grand_total is None or placeholder is None else [
        grand_total[0] - placeholder[0], grand_total[1] - placeholder[1]]
    for section in sections:
        sub = section["subtotal"]
        section["cost_diff"] = None if sub is None else section["cost"] - sub[0]
        section["value_diff"] = None if sub is None else section["value"] - sub[1]
        tol = max(1.0, 0.5 * section["rows"])
        section["reconciled"] = None if sub is None else (
            abs(section["value_diff"]) <= tol and abs(section["cost_diff"]) <= tol)
    grand_reconciled = False if unparsed or grand_total is None else (
        abs(summary["value"] + attached["value"] - grand_total[1])
        <= max(1.0, 0.5 * (summary["rows"] + attached["rows"])))
    return {"pages": pages, "sections": sections, "unparsed": unparsed, "grand_total": grand_total,
            "grand_reconciled": grand_reconciled}


def _parse_cpa_total_lines(pdf_path: str, page_nums: List[int]) -> Optional[Dict]:
    """parse_schedule_4i_pages for the cpa_total_lines layout. The pages must
    start at the first category heading so every section opens in range."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            text = _page_text(pdf.pages[page_num - 1], "cpa_total_lines")
            if not _CTL_HEADER_RE.search(text):
                return None
            bodies[page_num] = _ctl_body(text)

    current = None
    sections, unparsed, pages = [], [], []
    grand_parts = []

    def new_section(heading):
        section = {"heading": heading, "asset_type": next(
            (label for pattern, label in _CTL_ASSET_TYPES if re.search(pattern, heading, re.I)), ""),
            "partial": False, "rows": 0, "cost": 0.0, "value": 0.0, "subtotal": None}
        sections.append(section)
        return section

    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            # "$" prints as its own token on a section's first row and total.
            clean = " ".join(tok for tok in line.split() if tok != "$")
            row = _trailing_nums(clean, 3)
            if clean.startswith("TOTAL ") and not row:
                t = _trailing_nums(clean, 2)
                if not t:
                    unparsed.append({"page_number": page_num, "line": line})
                elif _CTL_GRAND_RE.match(t[0]):
                    grand_parts.append(t[1])
                elif current is not None and current["subtotal"] is None:
                    current["subtotal"] = t[1]
                # Otherwise a rollup of totals already taken; the grand total checks it.
                continue
            if not row:
                # Partnership rows print cost and value only.
                row = _trailing_nums(clean, 2)
                if row:
                    row = (row[0], [None] + row[1])
            if row and row[0]:
                if current is None or current["subtotal"] is not None:
                    # One-line section ("INTEREST-BEARING CASH $ <cost> $ <value>"; Bank
                    # of America's "* PARTICIPANT LOANS ..." right after a total, 2026-10-16).
                    current = new_section(row[0])
                title = row[0]
                shares, cost, value = row[1]
                current["rows"] += 1
                current["cost"] += cost or 0.0
                current["value"] += value or 0.0
                if not value:
                    continue
                m = _CTL_RATE_DATE_RE.search(title)
                rate, maturity = ((m.group(2), m.group(1)) if m.group(1) else (m.group(3), m.group(4))) \
                    if m else ("", "")
                mapped = {
                    "issuer_name": "",
                    "investment_description": title,
                    "asset_type": current["asset_type"],
                    "asset_type_source": "heading" if current["asset_type"] else "none",
                    "par_value": "",
                    "cost": _fmt(cost),
                    "current_value": _fmt(value),
                    "units_or_shares": _fmt(shares),
                    "asset_identifier": "",
                    "interest_rate": rate,
                    "maturity_date": maturity,
                    "section_heading": current["heading"],
                    "extraction_method": "sch4i_text",
                    "page_number": page_num,
                    "row_id": len(mapped_rows) + 1,
                }
                if not current["asset_type"]:
                    asset_type = detect_asset_type(f"{current['heading']} {title}")
                    mapped["asset_type"] = asset_type
                    mapped["asset_type_source"] = "row" if asset_type else "none"
                mapped_rows.append(mapped)
                continue
            if not _trailing_nums(clean, 1):
                current = new_section(line.rstrip(":").strip())
                continue
            unparsed.append({"page_number": page_num, "line": line})
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    for section in sections:
        sub = section["subtotal"]
        section["cost_diff"] = None if sub is None else section["cost"] - sub[0]
        section["value_diff"] = None if sub is None else section["value"] - sub[1]
        if sub is None:
            section["reconciled"] = None if section["rows"] else True
        else:
            # Printed in whole dollars, so allow a dollar of rounding per row.
            tol = max(1.0, section["rows"])
            section["reconciled"] = abs(section["value_diff"]) <= tol and abs(section["cost_diff"]) <= tol
    grand_total = [sum(p[0] or 0.0 for p in grand_parts), sum(p[1] or 0.0 for p in grand_parts)] \
        if grand_parts else None
    grand_reconciled = None
    if grand_total:
        grand_reconciled = abs(sum(s["value"] for s in sections) - grand_total[1]) <= max(
            1.0, sum(s["rows"] for s in sections))
    return {"pages": pages, "sections": sections, "unparsed": unparsed,
            "grand_total": grand_total, "grand_reconciled": grand_reconciled}


def is_trustworthy(result: Optional[Dict]) -> bool:
    """Every reconcilable section and the grand total (if checkable) tie out."""
    if result is None or not any(p["mapped_rows"] for p in result["pages"]):
        return False
    if any(s["reconciled"] is False for s in result["sections"]):
        return False
    return result["grand_reconciled"] is not False


def print_parse_report(result: Dict) -> None:
    rows = sum(len(p["mapped_rows"]) for p in result["pages"])
    value = sum(s["value"] for s in result["sections"])
    print(f"    [sch4i_text_parser] {rows} row(s), market value {value:,.0f}")
    for s in result["sections"]:
        status = {True: "OK", False: "MISMATCH", None: "n/a (section starts before requested pages)"
                  if s["partial"] else "n/a (no subtotal in range)"}[s["reconciled"]]
        diffs = "" if s["partial"] else (
            f" value_diff={s['value_diff']:+,.0f}" if s["value_diff"] is not None else "") + (
            f" cost_diff={s['cost_diff']:+,.0f}" if s["cost_diff"] is not None and not s["partial"] else "")
        sub = s["subtotal"][-1] if s["subtotal"] else None
        print(f"      {s['heading'] or '<no heading>'!r:28} -> {s['asset_type'] or '<blank>':30} "
              f"rows={s['rows']:4d} value={s['value']:>16,.0f} subtotal="
              f"{'-' if sub is None else format(sub, ',.0f'):>16} {status}{diffs}")
    for u in result["unparsed"]:
        print(f"      UNPARSED p.{u['page_number']}: {u['line']}")
    if result["grand_total"]:
        status = {True: "OK", False: "MISMATCH", None: "n/a (partial schedule)"}[result["grand_reconciled"]]
        print(f"      schedule grand total printed: {result['grand_total']} {status}")
