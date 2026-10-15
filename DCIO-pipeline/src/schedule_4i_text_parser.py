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
}

# Every requested page must carry this column-header line, else we don't own the layout.
_HEADER_RE = re.compile(r"Identifier\s+Security\s+Title\b.*\bValue\b", re.I)
_IPR_HEADER_RE = re.compile(r"Identity\s+of\s+Issue\b.*\bPar\s+Value\b.*\bCurrent\s+Value\b", re.I)
_IPR_HEADING_RE = re.compile(r"^[A-Z][A-Z ,&/()'\-]*[A-Z)]$")
_DR_HEADER_RE = re.compile(r"Assets\s+and\s+Liabilities\s+Detail\s+Report", re.I)
# Column-date line ("1/1/2024 12/31/2024") that ends each page's title block.
_DR_DATES_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}/\d{1,2}/\d{4}$")
# Footer: "4/29/2025 7:43:38 PM EDT Page 1 of 203" (pdfplumber garbles some to "o2f03").
_DR_FOOTER_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}:\d{2}\s+[AP]M\b")
_DR_LETTER_RE = re.compile(r"^([A-Z])\.\s+(.+)$")
_DR_ITEM_RE = re.compile(r"^\((\d{1,2})\)\s+(.+)$")
_DR_SUBITEM_RE = re.compile(r"^\(([A-Z])\)\s+(.+)$")
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
_FCV_HEADER_RE = re.compile(r"^Security\s+Name,\s+Maturity\s+Date,\s+and\s+Rate\s+Cost\s+Fair\s+Value$", re.M)
_FCV_TOTAL_RE = re.compile(r"^Total\s+investments\b", re.I | re.M)
# Page number "- 27 -" and the party-in-interest legend.
_FCV_SKIP_RE = re.compile(r"^-\s*\d+\s*-$|^\*\s*Party-in-interest$", re.I)
_FCV_RATE_DATE_RE = re.compile(r"(\d{1,3}\.\d+)%\s+(\d{2}/\d{2}/\d{4})$")
# Row name -> asset_type, first match wins. There are no category headings, and
# detect_asset_type is DC-tuned (any "COMMON STOCK" -> Employer Stock), so this
# list is the layout's own. Securities are matched before funds so an issuer
# named "... PARTNERS LP CALLABLE NOTES" stays a bond.
_FCV_ASSET_TYPES = [
    (r"\bSWAPS?\b|\bTRS\b|\bCDS\b|\bCDI\b|\bFUTURES?\b|\bFORWARD\b|\bOPTIONS?\b", "Derivative"),
    (r"LIQUIDITY\s+FUND|MONEY\s+MARKET|\bSTIF\b|COMMERCIAL\s+PAPER|CERTIFICATE\s+OF\s+DEPOSIT", "Short-Term Investment Fund"),
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


def _page_text(page, layout: str) -> str:
    if layout in ("issue_par_rate", "cpa_total_lines"):
        page = _drop_overlapping_spaces(page)
    return page.extract_text() or ""


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


def _headings(text: str, layout: str) -> List[str]:
    if layout == "cpa_total_lines":
        return [l for l in _ctl_body(text) if not l.startswith("TOTAL ") and not _trailing_nums(l, 2)]
    if layout == "flat_cost_value":
        # Title-block lines above the column header (pool name among them).
        lines = [l.strip() for l in text.splitlines()]
        end = next((i for i, l in enumerate(lines) if _FCV_HEADER_RE.match(l)), 0)
        return [l for l in lines[:end] if l]
    if layout == "bny_h30t":
        body = _h30t_body(text)
        return [l for l, nxt in zip(body, body[1:]) if _H30T_RULE_RE.match(nxt) and not l.startswith("-")]
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
    if layout == "flat_cost_value":
        # Header on the first page only: run from it to the page with the total.
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                if start is None and _FCV_HEADER_RE.search(text) and any(
                        re.match(start_heading, h, re.I) for h in _headings(text, layout)):
                    start = page_num
                if start is not None:
                    run.append(page_num)
                    if _FCV_TOTAL_RE.search(text):
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


def parse_schedule_4i_pages(pdf_path: str, page_nums: List[int], layout: str = "default") -> Optional[Dict]:
    """Parse the requested pages. Returns None if any page lacks the 4i header.

    Result: {"pages": [<run_pipeline page_data dicts>], "sections": [...],
             "unparsed": [...], "grand_total": [cost, value] or None}
    """
    if layout == "detail_report":
        return _parse_detail_report(pdf_path, page_nums)
    if layout == "soi_revalued_cost":
        return _parse_soi_revalued_cost(pdf_path, page_nums)
    if layout == "jpm_held":
        return _parse_jpm_held(pdf_path, page_nums)
    if layout == "bny_h30t":
        return _parse_bny_h30t(pdf_path, page_nums)
    if layout == "flat_cost_value":
        return _parse_flat_cost_value(pdf_path, page_nums)
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
            text = pdf.pages[page_num - 1].extract_text() or ""
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
                if current is None or not name_lines:
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
                holding["accounts"] += t[1][2] or 0.0
                holding["n_accounts"] += 1
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


def _parse_flat_cost_value(pdf_path: str, page_nums: List[int]) -> Optional[Dict]:
    """parse_schedule_4i_pages for the flat_cost_value layout. The first page
    must carry the column header and the last the "Total investments" line."""
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
        if page_num == page_nums[0]:
            lines = lines[next(i + 1 for i, l in enumerate(lines) if _FCV_HEADER_RE.match(l)):]
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
            asset_type = next((label for pattern, label in _FCV_ASSET_TYPES if re.search(pattern, title)), "")
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
