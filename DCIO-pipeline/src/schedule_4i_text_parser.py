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
pp.22-46), JV section $2.85B.

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
certified JV $3.08B plus real estate $0.50B.

An eleventh layout, "bare_subtotals" (2026-10-15), is titled "Schedule H -
Line 4i - Schedule of Assets (Held at End of Year)" on its first page only;
later pages carry just the auditor's footer. A Schedule H category heading,
"<description> [units] <cost> <value>" rows (units may be blank or "n/a"),
then a bare "<cost> <value>" subtotal; a one-holding category prints no
subtotal. A "<fund>, LLC: ASSETS DETAILED BELOW:" block after a subtotal
belongs to the category above it. "Grand Total <cost> <value>" ends it.
pdfplumber's default x_tolerance runs each line's words together here, so
this layout extracts at 1. First seen on National Electrical Benefit Fund
(ack 20260730200559NAL0029346880001, pp.26-69): the two partnership/JV
categories total $3,349,872,727, its certified JV to the dollar.

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

An eighteenth layout, "investments_held_summary" (2026-10-16), is a
"Schedule H, Line 4i ... Investments Held Summary" that rolls an attached
custodial statement up to one line per fund or asset class: category
headings, rows "<custodian> Page <n> - <name> <statement balance>
<adjustments> <market value> <cost | See Attached>", a bare-amount subtotal
closing each category, and a bare grand total. Padding spaces split digits
("1 2,169") and are dropped first. First seen on Chicago & Vicinity
Laborers' District Council Pension Plan (ack 20260311155722NAL0005772992003,
pp.30-31), partnership/joint venture interests $1.25B.

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
    # MGB (Mass General Brigham): the whole held-at-end-of-year composite (2026-10-15).
    "043294527-004": {"name": "MGB ERISA Master Trust", "layout": "bny_h30t",
                      "start_heading": r"^INTEREST\s+BEARING\s+CASH$"},
    # GE Pension Trust: same BNY composite, report id GEAC (2026-10-15).
    "140689340-001": {"name": "GE Pension Trust", "layout": "bny_h30t",
                      "start_heading": r"^INTEREST\s+BEARING\s+CASH$"},
    # Northrop Grumman DB Master Trust: the Alternative Investments Fund composite (2026-10-15).
    "136692740-007": {"name": "Northrop Grumman DB Master Trust", "layout": "bny_h30t",
                      "start_heading": r"^INTEREST\s+BEARING\s+CASH$"},
    # FCA US Master Retirement Trust Balanced Pool: the whole composite (2026-10-15).
    "133112458-059": {"name": "FCA US Master Retirement Trust Balanced Pool", "layout": "bny_h30t",
                      "start_heading": r"^INTEREST\s+BEARING\s+CASH$"},
    # American Airlines (AMR) Master Fixed Benefit Pension Trust: BNY composite 5Y2C (2026-10-15).
    "131502798-020": {"name": "American Airlines Master Fixed Benefit Pension Trust", "layout": "bny_h30t",
                      "start_heading": r"^INTEREST\s+BEARING\s+CASH$"},
    # Boeing: the whole Investment Control Pool list (2026-10-15).
    "910425694-062": {"name": "Boeing Employee Retirement Plans Master Trust", "layout": "flat_cost_value",
                      "start_heading": r"^INVESTMENT\s+CONTROL\s+POOL$"},
    # Koch DB Master Trust: pp.2-4, no total; every row no pattern types is a
    # private fund (Dyal, Kayne, CD&R, Starwood, co-invests) (2026-10-16).
    "256263968-001": {"name": "Koch Companies Defined Benefit Master Trust", "layout": "flat_cost_value",
                      "start_heading": r"^Koch\s+Companies\s+Defined\s+Benefit\s+Master\s+Trust\b",
                      "untyped_as": "Partnership Interest"},
    # UFCW Consolidated Pension Fund: pp.58-67, every section ties (2026-10-16).
    "586101602-001": {"name": "UFCW Consolidated Pension Fund", "layout": "typed_maturity_rate",
                      "start_heading": r"^UFCW\s+CONSOLIDATED\s+PENSION\s+FUND$"},
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
    # Laborers Pension Trust Fund for Northern California: pp.22-42, every total
    # ties; limited partnerships = cert JV (2026-10-16).
    "946277608-001": {"name": "Laborers Pension Trust Fund for Northern California", "layout": "closing_totals",
                      "start_heading": r"^Laborers\s+Pension\s+Trust\s+Fund\s+for\s+Northern\s+California$",
                      "notes": ["2026-10-16: the $6.8M of Laborers Funds Admin notes receivable print under "
                                "Pooled separate accounts and are typed Separate Account."]},
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
    # Honeywell: the whole Northern Trust list, pp.4-21; alt_span is its private
    # partnership/real estate run, 2024 filing (2026-10-15).
    "366968926-001": {"name": "Honeywell International Inc. Master Retirement Trust",
                      "layout": "nt_par_cost_value", "start_heading": r"^Schedule\s+H,\s+Line\s+4I\b",
                      "alt_span": (r"^4702\s+PARK\s+370$", r"^JOHN\s+HANCOCK\s+GAC\s+25$")},
    "362382580-010": {"name": "John Deere Pension Trust", "layout": "inline_category",
                      "start_heading": r"^5500\s+Schedule\s+of\s+Investments\s+at\s+End\s+of\s+Plan\s+Year$"},
    # NEBF (National Electrical Benefit Fund): the whole schedule, pp.26-69 (2026-10-15).
    "530181657-001": {"name": "National Electrical Benefit Fund", "layout": "bare_subtotals",
                      "start_heading": r"^Schedule\s+H\s*-\s*Line\s+4i\b"},
    # Dow: the first of two printed copies, pp.51-183 of the 2024 filing (2026-10-15).
    "381285128-001": {"name": "Dow Employees' Pension Plan", "layout": "nt_held_regions",
                      "start_heading": r"^Schedule\s+of\s+Assets\s+Held\s+for\s+Investment\s+Purposes$"},
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
    "360698440-337": {"name": "Abbott Laboratories Annuity Retirement Trust", "layout": "nt_held_regions",
                      "start_heading": r"^Account\s+Name\s+ABBOTT\s+CONSOLIDATION$"},
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
_JPM_HEADER_RE = re.compile(r"SCHEDULE\s+OF\s+ASSETS\s+HELD\s+FOR\s+INVESTMENT\s+PURPOSES\s+AT\s+END\s+OF\s+YEAR")
# Last line of each page's title block.
_JPM_COLUMNS_RE = re.compile(r"^SHARES\s*/\s*PAR$")
_JPM_FOOTER_RE = re.compile(r"^JPMorgan\s+Investor\s+Services$")
_JPM_RULE_RE = re.compile(r"^[-=]{3,}[-= ]*$")
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
                            r"|^a\)\s+b\)\s+Name\s+c\)\s+Description\s+d\)\s+Cost\s+e\)\s+Current\s+Value$", re.M)
_FCV_TOTAL_RE = re.compile(r"^Total\s+investments\b", re.I | re.M)
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
                            r"|SCHEDULE\s+H\s+TO\s+FORM\s+5500,\s+LINE\s+4i\s+-\s+SCHEDULE\s+OF\s+ASSETS\s+\(HELD")
# Last line of the title block; holdings start below it.
_CTL_COLUMNS_RE = re.compile(r"^\(a\)\s+(?:\(b\)\s+)?Identity\s+of\s+issuer|^SIMILAR\s+PARTY\b.*\bVALUE$", re.I)
# Page footer "- 89 - (Continued...)".
_CTL_FOOTER_RE = re.compile(r"^-\s*\d+\s*-")
# Grand-total lines: IUOE prints held and loaned separately; IAM's held line
# already includes its loaned block ("TOTAL ASSETS ON HAND" / "TOTAL
# SECURITIES ON LOAN" are intermediate rollups and are skipped).
_CTL_GRAND_RE = re.compile(r"^TOTAL\s+(?:ASSETS\s+\(HELD\b|SECURITIES\s+LOANED\b|INVESTMENTS$)")
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
_GLT_HEADER_RE = re.compile(r"SCHEDULE\s+OF\s+ASSETS\s+AS\s+OF\b.{0,40}\bSCH\s+H\s+PART\s+IV\s+LINE\s+4\(i\)")
# Last line of the title block; holdings start below it.
_GLT_COLUMNS_RE = re.compile(r"^GAIN/LOSS$")
_GLT_GRAND_RE = re.compile(r"-TOTAL\s+ASSETS\s+HELD\s+AT\s+THE\s+END\s+OF\s+THE\s+YEAR$")
# Column header, the last line of every page's title block.
_NTP_HEADER_RE = re.compile(r"^\(a\)\s+\(c\)\s+Description\s+of\s+Investment\b.*\bShare/Par\s+Value\s+Cost\s+Value$",
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
    r"^(INTEREST-BEARING\s+CASH|GOVERNMENT\s+SECURITIES|CORPORATE\s+(DEBT|STOCK)|LOANS\b|"
    r"PARTNERSHIP/JOINT\s+VENTURE|REAL\s+ESTATE|103-12|COMMON/COLLECTIVE|POOLED\s+SEPARATE|"
    r"REGISTERED\s+INVESTMENT|OTHER\s+INVESTMENTS|EMPLOYER|INSURANCE|MASTER\s+TRUST)", re.I)
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
_NHR_HEADER_RE = re.compile(r"^Schedule\s+of\s+Assets\s+Held\s+for\s+Investment\s+Purposes$", re.M)
# Column header, the last line of every page's title block.
_NHR_COLUMNS_RE = re.compile(r"^Security\s+Description\s+/\s+Asset\s+ID\s+Shares/Par\s+Value\s+"
                             r"Historical\s+Cost\s+Current\s+Value$", re.M)
# The unnamed grand total; category and sub-group totals carry a name.
_NHR_GRAND_RE = re.compile(r"^Total\s+\(?-?[\d,]+\.\d{2}\)?\s+\(?-?[\d,]+\.\d{2}\)?$", re.M)
# Securities-lending legend and Northern Trust's page footer.
_NHR_FOOTER_RE = re.compile(r"^\*\*\s+All\s+or\s+a\s+portion\b|^Northern\s+Trust\s+-|^Generated\s+by\s+Northern\s+Trust\b")
# Identifier lines under a holding ("CUSIP: ...", "SWU020HB3 SEDOL: ...", a bare id).
_NHR_ID_RE = re.compile(r"\b(?:CUSIP|SEDOL|ISIN|TICKER)\s*:\s*(\S+)|^((?=[A-Z0-9]*\d)[A-Z0-9]{9})$", re.I)
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
                             r"par\s+or\s+maturity\s+value\s+Cost\s+Value$", re.M)
_TMR_SUBTOTAL_RE = re.compile(r"^(?:Sub)?total\s+(?:-\s+)?(.+)$", re.I)
_TMR_GRAND_RE = re.compile(r"^Total\s+assets\s+\(held\s+at\s+end\s+of\s+year\)"
                           r"|^Total\s+Investments,\s+at\s+Fair\s+Value\b|^Grand\s+Total\b", re.I | re.M)
# Citigroup's umbrella heading over every section, and its "(Continued)" page
# footers; 3M's "Schedule 1" and FirstEnergy's "Page 17" page footers.
_TMR_SKIP_RE = re.compile(r"^Investments,\s+at\s+fair\s+value:$|^\d+\s+\(Continued\)$|^Schedule\s+\d+$|^Page\s+\d+$",
                          re.I)
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
                            r"\(d\)\s*Cost\s+\(e\)\s*Current\s+Value$", re.M)
_CT_TOTAL_RE = re.compile(r"^Total\s+(.+)$", re.I)
_CT_GRAND_RE = re.compile(r"^Total\s+investments\s+\$?\s*[\d,]+", re.I | re.M)
_CT_FOOTER_RE = re.compile(r"^See\s+Independent\s+Auditor's\s+Report|^\d+$")
# Lines at or right of the identity column are rows or wrapped identities;
# headings sit in column (a).
_CT_ROW_X = 80.0
_CT_ASSET_TYPES = [
    (r"money\s+market", "Short-Term Investment Fund"),
    (r"government|bonds?\b", "Bond"),
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
_IHS_COLUMNS_RE = re.compile(r"^Balance\s+Adjustments\s+Value\s+Value$")
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


def _upright_text(page) -> str:
    """page.extract_text(), but a page whose text is mostly drawn rotated 90°
    counter-clockwise on an upright page (DuPont's schedule) is read with its
    characters turned back first; otherwise pdfplumber returns one reversed
    word per line (2026-10-16)."""
    chars = page.chars
    rotated = [c for c in chars if not c["upright"] and c["matrix"][1] > 0 > c["matrix"][2]]
    if len(rotated) * 2 <= len(chars):
        return page.extract_text() or ""
    turned = []
    for c in rotated:
        t = dict(c)
        t["x0"], t["x1"] = page.height - c["bottom"], page.height - c["top"]
        t["top"], t["bottom"] = c["x0"], c["x1"]
        t["doctop"], t["upright"] = t["top"], True
        turned.append(t)
    return pdfplumber.utils.extract_text(turned) or ""


def _page_text(page, layout: str) -> str:
    if layout in ("issue_par_rate", "cpa_total_lines", "gain_loss_totals"):
        page = _drop_overlapping_spaces(page)
    return _upright_text(page)


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
    return [l for l in lines[start:] if l and not _JPM_FOOTER_RE.match(l)]


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
    # The default tolerance runs the words of every line together.
    return page.extract_text(x_tolerance=1) or ""


def _bs_body(text: str) -> List[str]:
    """bare_subtotals lines below the first page's column header, footer dropped."""
    lines = [l.strip() for l in text.splitlines()]
    start = next((i + 1 for i, l in enumerate(lines) if _BS_COLUMNS_RE.match(l)), 0)
    return [l for l in lines[start:] if l and not _BS_FOOTER_RE.match(l)]


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
                  "typed_maturity_rate", "closing_totals", "investments_held_summary"):
        # Title-block lines above the column header (pool name among them).
        columns_re = {"flat_cost_value": _FCV_HEADER_RE, "inline_category": _IC_COLUMNS_RE,
                      "nt_held_regions": _NHR_COLUMNS_RE, "titled_appendix": _TA_COLUMNS_RE,
                      "typed_description": _TD_COLUMNS_RE, "typed_maturity_rate": _TMR_COLUMNS_RE,
                      "closing_totals": _CT_COLUMNS_RE, "investments_held_summary": _IHS_COLUMNS_RE}[layout]
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
                  "typed_maturity_rate", "closing_totals"):
        # Run from the first page with the header to the page with the total
        # (nt_held_regions repeats the header, and its schedule is printed twice).
        header_re, total_re = {
            "flat_cost_value": (_FCV_HEADER_RE, _FCV_TOTAL_RE),
            "inline_category": (_IC_COLUMNS_RE, re.compile(r"^Grand\s+Total\s", re.I | re.M)),
            "nt_held_regions": (_NHR_COLUMNS_RE, _NHR_GRAND_RE),
            "titled_appendix": (_TA_TITLE_RE, _TA_TOTAL_RE),
            "typed_description": (_TD_COLUMNS_RE, _TD_END_RE),
            "typed_maturity_rate": (_TMR_COLUMNS_RE, _TMR_GRAND_RE),
            "closing_totals": (_CT_COLUMNS_RE, _CT_GRAND_RE)}[layout]
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                if layout == "typed_maturity_rate":
                    _decode_calibri_cids(page)
                text = page.extract_text() or ""
                if start is None and header_re.search(text) and any(
                        re.match(start_heading, h, re.I) for h in _headings(text, layout)):
                    start = page_num
                if start is not None:
                    if layout == "flat_cost_value" and page_num > start and _FCV_NEXT_SCHEDULE_RE.search(text):
                        return run
                    run.append(page_num)
                    if total_re.search(text):
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
        return _parse_gain_loss_totals(pdf_path, page_nums)
    if layout == "nt_par_cost_value":
        return _parse_nt_par_cost_value(pdf_path, page_nums, PLAN_PARSERS.get(plan_id, {}).get("alt_span"))
    if layout == "bare_subtotals":
        return _parse_bare_subtotals(pdf_path, page_nums)
    if layout == "inline_category":
        return _parse_inline_category(pdf_path, page_nums)
    if layout == "nt_held_regions":
        return _parse_nt_held_regions(pdf_path, page_nums)
    if layout == "titled_appendix":
        return _parse_titled_appendix(pdf_path, page_nums)
    if layout == "typed_description":
        return _parse_typed_description(pdf_path, page_nums, PLAN_PARSERS.get(plan_id, {}).get("exclude_rows"))
    if layout == "typed_maturity_rate":
        return _parse_typed_maturity_rate(pdf_path, page_nums,
                                          PLAN_PARSERS.get(plan_id, {}).get("row_rounding", False))
    if layout == "closing_totals":
        return _parse_closing_totals(pdf_path, page_nums)
    if layout == "investments_held_summary":
        return _parse_investments_held_summary(pdf_path, page_nums)
    if layout == "detail_report":
        return _parse_detail_report(pdf_path, page_nums)
    if layout == "soi_revalued_cost":
        return _parse_soi_revalued_cost(pdf_path, page_nums)
    if layout == "jpm_held":
        return _parse_jpm_held(pdf_path, page_nums)
    if layout == "bny_h30t":
        return _parse_bny_h30t(pdf_path, page_nums)
    if layout == "flat_cost_value":
        return _parse_flat_cost_value(pdf_path, page_nums, PLAN_PARSERS.get(plan_id, {}).get("untyped_as", ""))
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
            text = pdf.pages[page_num - 1].extract_text() or ""
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


def _parse_jpm_held(pdf_path: str, page_nums: List[int]) -> Optional[Dict]:
    """parse_schedule_4i_pages for the jpm_held layout. The pages must start
    at the first section heading so every section opens in range."""
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
            if _JPM_RULE_RE.match(line) or _JPM_SHARES_RE.match(line) or line.startswith("RATINGS:"):
                continue
            nxt = body[i + 1] if i + 1 < len(body) else ""
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
                        and name.startswith(current["heading"])):
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
        grand_reconciled = abs(invest_value - grand_total[-1]) <= 1.0 + 0.5 * invest_rows
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


def _parse_flat_cost_value(pdf_path: str, page_nums: List[int], untyped_as: str = "") -> Optional[Dict]:
    """parse_schedule_4i_pages for the flat_cost_value layout. The first page
    must carry the column header and the last the "Total investments" line.
    untyped_as (registry) types rows no name pattern matched, for a list whose
    only remaining holdings are private funds (Koch)."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        texts = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            texts[page_num] = pdf.pages[page_num - 1].extract_text() or ""
    if not page_nums or not _FCV_HEADER_RE.search(texts[page_nums[0]]):
        return None

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
            if not line or _FCV_SKIP_RE.match(line):
                continue
            # "$" prints as its own token on the first and total lines.
            clean = " ".join(tok for tok in line.split() if tok != "$")
            t = _trailing_nums(clean, 2)
            if _FCV_TOTAL_RE.match(line):
                if t:
                    grand_total = t[1]
                continue
            if grand_total is not None:
                continue
            if not t or not t[0]:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            title = t[0].lstrip("* ").strip()
            cost, value = t[1]
            section["rows"] += 1
            section["cost"] += cost or 0.0
            section["value"] += value or 0.0
            if not value:
                continue
            m = _FCV_RATE_DATE_RE.search(title)
            asset_type = next((label for pattern, label in _FCV_ASSET_TYPES if re.search(pattern, title)), untyped_as)
            mapped_rows.append({
                "issuer_name": "",
                "investment_description": title,
                "asset_type": asset_type,
                "asset_type_source": "row" if asset_type else "none",
                "par_value": "",
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": "",
                "asset_identifier": "",
                "interest_rate": m.group(1) if m else "",
                "maturity_date": m.group(2) if m else "",
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


def _parse_gain_loss_totals(pdf_path: str, page_nums: List[int]) -> Optional[Dict]:
    """parse_schedule_4i_pages for the gain_loss_totals layout. The pages must
    start at the schedule's first page so every section opens in range."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            text = _page_text(pdf.pages[page_num - 1], "gain_loss_totals")
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

    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            row = _trailing_nums(line, 4)
            if not row:
                t = _trailing_nums(line, 3)
                if t and _GLT_GRAND_RE.search(t[0]):
                    grand_total = t[1][:2]
                    continue
                if t and t[0].startswith("TOTAL "):
                    heading = t[0][len("TOTAL "):].strip()
                    section = current or open_section()
                    asset_type = next((label for pattern, label in _CTL_ASSET_TYPES
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
            title = " ".join(name_lines + ([row[0]] if row[0] else []))
            name_lines = []
            shares, cost, value, gain = row[1]
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
                "asset_identifier": "",
                "interest_rate": rate,
                "maturity_date": maturity,
                "section_heading": "",
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            }
            current["row_dicts"].append(mapped)
            mapped_rows.append(mapped)
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
                             alt_span: Optional[tuple] = None) -> Optional[Dict]:
    """parse_schedule_4i_pages for the nt_par_cost_value layout. alt_span is
    (first-row pattern, last-row pattern) of the plan's partnership run."""
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
                span_row = True
                if re.match(alt_span[1], title, re.I):
                    in_span = False
            if not value:
                continue
            if span_row:
                asset_type, source = "Partnership Interest", "span"
            elif line.startswith("* ") and re.search(r"\bCOM\b", title):
                asset_type, source = "Employer Stock", "row"
            else:
                asset_type = next((label for pattern, label in _NTP_ASSET_TYPES + _FCV_ASSET_TYPES
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
    """nt_held_regions lines below the column header, footer dropped."""
    lines = [l.strip() for l in text.splitlines()]
    start = next((i + 1 for i, l in enumerate(lines) if _NHR_COLUMNS_RE.match(l)), len(lines))
    return [l for l in lines[start:] if l and not _NHR_FOOTER_RE.match(l)]


def _parse_nt_held_regions(pdf_path: str, page_nums: List[int]) -> Optional[Dict]:
    """parse_schedule_4i_pages for the nt_held_regions layout. The last page
    must carry the unnamed grand "Total" line."""
    pdf_stem = pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    page_nums = sorted(set(page_nums))
    with pdfplumber.open(pdf_path) as pdf:
        bodies = {}
        for page_num in page_nums:
            if not 1 <= page_num <= len(pdf.pages):
                return None
            text = pdf.pages[page_num - 1].extract_text() or ""
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
            # Private funds carry their own category here, so a name never makes one.
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
                               row_rounding: bool = False) -> Optional[Dict]:
    """parse_schedule_4i_pages for the typed_maturity_rate layout. The pages
    must start at the first section heading and end at the grand total.
    row_rounding allows half a dollar per row when a filer rounds each row
    but totals the unrounded amounts (3M)."""
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

    sections, unparsed, pages = [], [], []
    current = parent = None
    grand_total = None
    for page_num in page_nums:
        lines = [tl["text"].strip() for tl in text_lines[page_num]]
        start = next((i + 1 for i, l in enumerate(lines) if _TMR_COLUMNS_RE.match(l)), 0)
        mapped_rows = []
        for line, tl in zip(lines[start:], text_lines[page_num][start:]):
            if not line or re.fullmatch(r"\d+", line) or grand_total is not None or _TMR_SKIP_RE.match(line):
                continue
            clean = " ".join(tok for tok in line.split() if tok != "$")
            if _TMR_GRAND_RE.match(line):
                t = _trailing_nums(clean, 2)
                grand_total = t[1] if t else None
                continue
            m = _TMR_SUBTOTAL_RE.match(clean)
            if m:
                t = _trailing_nums(m.group(1), 2)
                if current is None or not t:
                    unparsed.append({"page_number": page_num, "line": line})
                    continue
                if current["subtotal"] is not None:
                    # 3M rolls sub-headings up under their category ("Total corporate
                    # stock" after "Total corporate stock - preferred"); the grand
                    # total still checks every row.
                    continue
                current["subtotal"] = t[1]
                current["cost_diff"] = current["cost"] - t[1][0]
                current["value_diff"] = current["value"] - t[1][1]
                tol = 1.0 + (0.5 * current["rows"] if row_rounding else 0.0)
                current["reconciled"] = abs(current["value_diff"]) <= tol and abs(current["cost_diff"]) <= tol
                continue
            t = _trailing_nums(clean, 3)
            if t:
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
                    elif t and t[0].endswith("**"):
                        # FirstEnergy's written-down Russia fund: a value and no cost.
                        title, (value,), cost, par = t[0], t[1], None, None
                    else:
                        t = None
            if not t:
                # A heading line opens the next section; "(continued)" reopens one.
                base = re.sub(r"\s*\(continued\)$", "", line, flags=re.I).rstrip(":")
                current = next((s for s in sections if s["heading"].lower() == base.lower()), None)
                if current is None:
                    current = {"heading": base,
                               "asset_type": next((label for p, label in _TMR_ASSET_TYPES
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
            if current is None:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            # UFCW repeats its section heading as column (c); the name is what precedes it.
            type_re = r"[\s\-]+".join(map(re.escape, re.split(r"[\s\-]+", current["heading"])))
            tm = re.search(r"\s" + type_re + r"(?=\s|$)", title, re.I)
            if not title:
                name, rest = current["heading"], ["", ""]
            elif tm:
                name = title[:tm.start()].strip()
                # "<maturity> <rate>", where the rate may be "VARIABLE RATE".
                rest = (title[tm.end():].split(None, 1) + [""])[:2]
            else:
                # Western States: "<name> <type> <collateral> <maturity> <rate>", the
                # type a separate column; the name ends at the first column gap.
                chars = [c for c in tl["chars"] if c["text"].strip()]
                cut = next((i for i in range(1, len(chars)) if chars[i]["x0"] - chars[i - 1]["x1"] > 6), len(chars))
                name = " ".join(pdfplumber.utils.extract_text(chars[:cut], x_tolerance=1).split()) if chars else title
                fields = title.split()
                rest = [next((f for f in fields if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", f)), ""),
                        next((f for f in fields if re.fullmatch(r"\d+(?:\.\d+)?%", f)), "")]
            current["rows"] += 1
            current["cost"] += cost or 0.0
            current["value"] += value or 0.0
            if not value:
                continue
            mapped_rows.append({
                "issuer_name": "",
                "investment_description": name.lstrip("* ").strip(),
                "asset_type": current["asset_type"],
                "asset_type_source": "heading" if current["asset_type"] else "none",
                "par_value": _fmt(par),
                "cost": _fmt(cost),
                "current_value": _fmt(value),
                "units_or_shares": "",
                "asset_identifier": "",
                "interest_rate": "" if set(rest[1].split()) <= {"N/A"} else rest[1],
                "maturity_date": "" if rest[0] == "N/A" else rest[0],
                "section_heading": current["heading"],
                "extraction_method": "sch4i_text",
                "page_number": page_num,
                "row_id": len(mapped_rows) + 1,
            })
        pages.append({
            "pdf": pdf_path, "pdf_stem": pdf_stem, "page_number": page_num,
            "mapped_rows": mapped_rows, "ocr_cells": [], "normalized_path": pdf_path,
        })

    grand_reconciled = None
    if grand_total is not None:
        tol = 1.0 + (0.5 * sum(s["rows"] for s in sections) if row_rounding else 0.0)
        grand_reconciled = (abs(sum(s["value"] for s in sections) - grand_total[1]) <= tol
                            and abs(sum(s["cost"] for s in sections) - grand_total[0]) <= tol)
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


def _parse_investments_held_summary(pdf_path: str, page_nums: List[int]) -> Optional[Dict]:
    """parse_schedule_4i_pages for the investments_held_summary layout. The
    pages must run from the summary's first page to its last."""
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
        t = _trailing_nums(line, 4)
        if t and t[0] and _NUM_RE.match(t[0].split()[-1]):
            t = None   # five amounts: not a row
        if t:
            return t[0], t[1][0], t[1][1], t[1][2], t[1][3]
        t = _trailing_nums(line, 3)
        return (t[0], t[1][0], t[1][1], t[1][2], None) if t else None

    for page_num in page_nums:
        mapped_rows = []
        for line in bodies[page_num]:
            a = amounts(line)
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
                heading = None
                continue
            if heading is None:
                unparsed.append({"page_number": page_num, "line": line})
                continue
            row = {
                "issuer_name": "",
                "investment_description": _IHS_REF_RE.sub("", text),
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
                if current is None:
                    # One-line section ("INTEREST-BEARING CASH $ <cost> $ <value>").
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
