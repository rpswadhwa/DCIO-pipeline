"""
junk_detect.py  --  identify rows that were never a holding at all (pure junk).

Junk != CIT. A CIT is a real vehicle we exclude for scope reasons; junk is a total
line, a section header, a carry-forward balance, a date, or a bond fragment that got
scraped as if it were a fund. Removing junk is ALWAYS safe -- there is no real fund to
lose -- so we do it independently of the certified anchor, and then use certified only
to VALIDATE (did junk removal get us to the audited MF total?).

IDENTIFICATION (three tiers)
  Tier 1 structural (name-independent): exact duplicate rows; near-duplicate rows
       (same fund once share-class/boilerplate wording is stripped, value within 0.5%);
       grand-total row (value ~= sum of all other rows in the plan).
  Tier 2 lexicon (bounded accounting vocabulary, matched on the NORMALIZED whole
       string -- never a loose substring): totals, balances, section headers,
       bookkeeping scraps.
  Tier 3 shape: no alphabetic content; a date; a bond fragment ("950 maturing ...");
       a lone generic word.

TWO TRAPS the matching must respect (found in the real data):
  * junk label glued onto a real fund -> "Investments at fair value State St Equity 500
    Index K". Do NOT delete -> PREFIX_CONTAMINATED (protect the fund; value-repair later).
  * same word spans junk and real vehicles -> "Commingled Trust" (junk header) vs
    "Fidelity Contrafund Commingled Pool" (real CIT). Whole-string equality avoids this;
    substring-contains would not.

norm(): lowercase, drop everything non-alphanumeric. Collapses spacing/punct/case/glyph
variants so "Self-Directed" == "self directed" == "SelfDirected" -> "selfdirected", and
"Tota l" -> "total".
"""

from __future__ import annotations
import re
from typing import List, Dict, Tuple

_NONALNUM = re.compile(r"[^a-z0-9]")


def norm(s: str) -> str:
    return _NONALNUM.sub("", (s or "").lower())


# Share-class / boilerplate words stripped before the NEAR-duplicate name comparison
# (Tier 1a2). These are the words that vary between two extractions of the SAME fund
# ("... Admiral Shares" vs "... Institutional Shares", "... Fund" vs "... Fund Class R6")
# without changing what the fund actually is.
_SHARE_CLASS_WORDS = re.compile(
    r'\b(class\s*[a-z0-9]{1,3}|institutional|inst|admiral|investor|retirement|'
    r'advisor|adv|retail|premier|select|r[1-6]|shares?|units?|fund|series)\b',
    re.IGNORECASE,
)


def fuzzy_norm(s: str) -> str:
    """Aggressive normalization for near-duplicate detection: strips share-class /
    boilerplate words on top of norm()'s case/punctuation collapse, so 'Vanguard 500
    Index Fund Admiral Shares' and 'Vanguard 500 Index Fund Institutional Shares'
    both reduce to the same key. Intentionally more aggressive than norm() -- only
    ever used PAIRED with a value-closeness check (see Tier 1a2), never alone, so it
    can't merge two genuinely different funds that happen to share a family name.
    """
    return norm(_SHARE_CLASS_WORDS.sub(' ', s or ''))


# --- Tier 2 lexicon: NORMALIZED whole-string labels that are never a security --------
# (matched by exact-equality against norm(name); see is_junk_name)
JUNK_EXACT = {
    # totals / subtotals
    "total", "subtotal", "grandtotal", "totalinvestments",
    "totalinvestmentsatfairvalue", "totalassets", "totalfairvalue",
    "totalinvestmentsatcost", "totalheldforinvestment",
    "totalparticipantdirectedfunds", "subtotalforward",  # user-added 2026-07-07
    "totalparticipantdirected", "totalparticipantdirectedinvestments",
    # value-basis header lines
    "investmentsatfairvalue", "investmentsatcost", "investmentsatcontractvalue",
    "atfairvalue", "atcost", "atcontractvalue", "fairvalue",
    # assets-held headers
    "assetsheldforinvestment", "assetsheldforinvestmentpurpose",
    "assetsheldforinvestmentpurposes", "assetsheldforinvestmentpurposesatendofyear",
    "assetsheldatendofyear", "assetsheldforinvestmentpurposeatendofyear",
    "assetsheldforinvestmentpurposesatend", "assetsheldforinvestmentatendofyear",
    # balances / carry-forward
    "balanceforward", "beginningbalance", "endingbalance", "balanceprevious",
    "nextinvestments", "beginningofyear", "endofyear",
    # section headers scraped as rows
    "commonstocks", "commonstock", "preferredstock", "mutualfunds",
    "registeredinvestmentcompanies", "registeredinvestmentcompany",
    "collectivetrust", "collectivetrustfund", "collectivetrustfunds",
    "commingledtrust", "commingledpool", "commingledfunds",
    "selfdirected", "selfdirectedaccount", "selfdirectedaccounts",
    "selfdirectedbrokerage", "selfdirectedbrokerageaccount",
    "selfdirectedbrokerageaccounts",
    "participantdirected", "participantdirectedinvestments",
    "participantdirectedbrokerage", "participantdirectedaccount",
    "participantdirectedaccounts",
    # bookkeeping scraps
    "statements", "perthefinancialstatements", "ein", "seenotes", "notes",
    "various", "other", "miscellaneous", "misc", "cash",
    # user-reviewed 2026-07-15
    "commoninvestmenttrustfunds", "sharesofcommonstock", "shares",
    "carriedforward", "emp", "investmentsperfinancialstatements",
    # user-reviewed 2026-07-16 (bare labels / fragments)
    "balances", "balancebroughtforward", "contracts", "companyshares",
    "purchasedshare", "partygainor", "involvedloss", "loans",
    "dividendsinterestreinvested", "tiaa", "charlesschwabtrustbank",
    "greatgreytrust", "greatgraytrust",
}

# --- plan_mf_history_v3 cleanup denylist (2026-09-08/09) ------------------------------
# Confirmed contaminants found by manual review of live plan_mf_history_v3, deleted from
# that table directly. Added here so future extractions don't reintroduce the same rows.
# Checked by exact norm() equality only (never substring) -- these are literal strings
# observed in the data, not patterns.
#
# Bucket 1: pure boilerplate/accounting-language junk with no recoverable fund identity
# (generic phrases that also happen to appear, verbatim, as "fund names" in bad extractions).
V3_CLEANUP_BOILERPLATE_EXACT = {norm(n) for n in [
    "Assets Investments at fair value Mutual funds",
    "Reconciliation to the financial statements:",
    "Fidelity Mutual funds - see attachment",
    "Interest held in Master Trust at fair value Mutual funds",
    "Adjustment going from Fair Value to Contract Value",
    "otal Investments per the financial T statements",
    "JPM DAILY MARKET VALUE SUNDRY",
    "contract value",
    "Brought forward",
    "Investments brought forward",
    "/ETFs (continued) Balance brought forward",
    "AT FAIR VALUE Baird",
    "Net assets available for benefits",
    "Investments at fair value",
    "Investments at fair market value",
    "Net appreciation in fair value of investments",
    "Carried Forward",
    "Net assets available for benefits per the financial statements",
    "per the financial statements",
    "the financial statements",
    "Investment contract at fair value",
    # Bucket 2 (2026-09-09): boilerplate + raw Treasury securities confirmed live in v3
    # and deleted directly; plus a few preventive entries with no current live row
    # (flagged by the user, kept here so a future extraction can't reintroduce them).
    "Responsive",
    "Allocated",
    "account balance",
    "Investments measured at NAV",
    "accompanying Statements of Changes in Net Assets Available for Benefits were",
    "accounts at fair value Vanguard",
    "US TREASURY N/B",
    "US Treasury",
    "UNITED STATES OF AMER TREAS NOTES",
    "UNITED STATES OF AMER TREAS NOTES 3875",  # preventive -- no live row under this exact string
    "Investments at fair value per financial statements",  # preventive
    "Forwarded",  # preventive
    "Investments at contract value",  # preventive
    "from 4.25 to 9.50 percent)",  # preventive
    # 2026-09-09: confirmed live, deleted from v3 -- narrative transfer-between-plans line
    "Transfer to the Cornerstone Building Brands 401k Profit Sharing Plan",
]}

# Bucket 3: individual stocks manually confirmed present in plan_mf_history_v3 (a stock
# is never a mutual fund; these are specific, provably-wrong names, not a general
# stock-detection heuristic -- broader stock contamination is still an open, unresolved
# problem tracked separately).
V3_CLEANUP_STOCK_EXACT = {norm(n) for n in [
    "BOEING CO",
    "AbbVie Inc Com 4375",
    "MARRIOTT INTERNATIONAL INC/MD",
    "WELLS FARGO CO",
    "SCHWAB CHARLES CORP",
    "ARTHUR J GALLAGHAR AND CO",
    "Webster Financial Corporation",
    "Elliot International Ltd.",
    # 2026-09-09: confirmed live, deleted from v3
    "Ryder System, Inc.",
    # 2026-09-09: preventive -- confirmed NOT currently live under these exact strings,
    # added defensively since the user flagged them by name
    "GENERAL MOTORS CO",
    "EVERSOURCE ENERGY COM",
    "GLACIER BANCORP INC MONTANA",
    "DEVON ENERGY CORPORATION",
    "MEDTRONIC PLC",
    "DIAGEO PLC ADR",
    "Abbott Laboratories common shares",
    "McDonalds Corp Com",
    "ATT INC",
]}

# Bucket 2 shape patterns (2026-09-09): three families of extraction artifacts found by
# regex sweep of live v3, none recoverable to a fund identity. Checked against nn
# (norm() already strips spaces/punctuation, so "EIN 59", "EIN94", "EIN – 45-" all
# collapse to the same "ein<digits>" shape).
_EIN_FRAGMENT_RE = re.compile(r"^ein\d+$")               # isolated EIN digit-group, e.g. "EIN 59"
_FORM_ID_CODE_RE = re.compile(r"^\d[a-z]\d{4}[a-z]$")     # form/schedule code, e.g. "1P1211A"
_MULTI_SEDOL_RE = re.compile(r"sedol.*sedol")             # 2+ SEDOLs spliced into one "name"

# Bucket 6 (2026-09-09): interest-rate-swap / CDS derivative contract legs, e.g.
# "99S273OA7 SWU02FMZ1 IRS EUR P V 06MEURIB SWUV2FMZ3 CCPVANILLA". A dummy "99S..."
# identifier prefix, a swap-leg counterparty code (BWU/SWU/BWPC/SWPC), and an IRS/CDS
# instrument tag are jointly distinctive enough that no real fund name collides with the
# shape -- confirmed against all 279 live matches (both pay and receive legs, several
# currencies/reference-rate variants, IRS and CDS) before this pattern was added. These
# are derivative positions, never a mutual fund.
_SWAP_CDS_LEG_RE = re.compile(r"^99s\w*[bs]w(u|pc)\w*(irs|cds)")

# Participant loans / notes receivable -- never a fund. Matched as a normalized substring
# because the phrasing is bounded and never occurs inside a real fund name.
_LOAN_RE = re.compile(
    r"participantloan|loanstoparticipant|notesreceivablefromparticipant|"
    r"participantnotesreceivable|loanreceivable|promissorynote"
)

# --- user-reviewed junk forms (2026-07-15) ---------------------------------------------
_CID_RE = re.compile(r"cid\d+")                                    # OCR glyph artifact "cid98"
_ENDS_TOTAL_RE = re.compile(r"(?i)\b(sub)?totals?\s*$")            # "... Total" (manager/section subtotal)
_SUBTOTAL_NORM_RE = re.compile(r"^(sub)?total.*(investments|funds|assets)$")  # norm: "T otal ... funds"
_SHARE_FRAG_RE = re.compile(r"(?i)^\s*[\d,]+\s+shares?\b")         # "9848 Shares", "99381 shares 1"
_SHARE_DBL_RE = re.compile(r"(?i)\bshares?\s+[\d,]+\s+shares?\b")  # "Investor Shares 35532 shares a"
_PARTY_RE = re.compile(r"partyininterest")                        # "* indicates party in interest"
_YEAR_RANGE_RE = re.compile(r"(?i)^\s*(from\s+)?(19|20)\d\d\s+to\s+(19|20)\d\d\s*$")  # "from 2025 to 2038"
# interest-rate range fragments: "325 to 950 maturing", "Rates from 425 to 1050", "rate 425 950"
_RATE_RANGE_RE = re.compile(r"(?i)^\s*(?:rates?\s+)?(?:from\s+)?\d[\d.,]*\s+(?:to|through|-)\s+\d[\d.,]*\s*(?:percent|%|maturing)?\s*$")
_RATE_WORDS_RE = re.compile(r"(?i)^\s*rates?\s+\d[\d.,]*\s+\d[\d.,]*\s*$")
# "through October 2027", "dates through December 7, 2029"
_THROUGH_DATE_RE = re.compile(r"(?i)^\s*(?:dates?\s+)?through\s+\S.*\d{4}\s*$")
_PENSION_RE = re.compile(r"(?i)pension\s+identification\s+number")
_AGG_ASSETS_RE = re.compile(r"(?i)^\s*aggregate\b.*\bassets?\s*$")   # "Aggregate SBDA Assets"
_SUBACCT_RE = re.compile(r"(?i)^\s*sub\s?account\s+of\b")            # "SubAccount of John Hancock"

# Mode A (accounting income / balance lines scraped as holdings). Bounded phrasings that
# never occur inside a real fund name -> safe to match as a normalized substring. NOTE:
# bare "interest"/"dividend" are NOT here (they appear in real fund names, e.g. "Vanguard
# Dividend Growth"); only the receivable/accrued/income-statement forms.
_ACCTG_RE = re.compile(
    r"netinvestmentincome|netappreciation|netdepreciation|netrealized|netunrealized|"
    r"dividendsreceivable|dividendreceivable|interestreceivable|accruedincome|"
    r"accruedinterest|accruedexpense|accrueddividend|pendingsettle|pendingtrade|"
    r"unsettledtrade|duefrombroker|duetobroker|contributionsreceivable|"
    r"employercontributionreceivable|dividendsandinterestreceivable"
)

# Mode E (subtotal by NAME): a line that starts with (sub)total and ends in the plural
# aggregation word "investments" / "funds" / "assets" -> a per-manager or per-section
# subtotal, never a real fund (real funds end in Fund/Index/Trust/Shares, and "Total
# Return Fund" ends in 'fund' singular preceded by a real name, not 'Total ... funds').
_SUBTOTAL_NAME_RE = re.compile(r"(?i)^\s*(sub[\s-]*)?total\b.*\b(investments|funds|assets)\s*$")

# the word "total"/"subtotal" anywhere in a name -> a candidate subtotal row for the
# structural block-sum check (Tier 1c). Real funds may contain "total" (e.g. "PIMCO
# Total Return"), so this alone never deletes -- the block-sum math must also match.
_TOTAL_WORD_RE = re.compile(r"(?i)\b(sub[\s-]*)?totals?\b")

# Normalized PREFIXES: if norm(name) == prefix -> junk (bare line); if it STARTS WITH the
# prefix but has a real remainder -> PREFIX_CONTAMINATED (protect, don't delete).
JUNK_PREFIXES = (
    "netassetsavailableforbenefits",
    "netincreaseinnetassetsavailableforbenefits",
    "investmentsatfairvalue",
    "investmentsatcontractvalue",
    "assetsheldforinvestment",
    "totalinvestments",
)

# Tier 3 shape
_DATE_RE = re.compile(r"^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s*\d{1,2}",
                      re.I)
_BOND_FRAG_RE = re.compile(r"^\d[\d,\. ]*\s*(maturing|matures|due|%|percent)", re.I)
_MATURING_RE = re.compile(r"maturing (at various|through|on)", re.I)
_GENERIC_WORDS = {"statements", "investments", "total", "other", "various", "cash",
                  "notes", "misc", "miscellaneous", "fund", "funds", "account", "accounts"}


def is_junk_name(name: str) -> Tuple[bool, str, str]:
    """Return (is_junk, reason, disposition).
    disposition: 'DELETE' (remove whole row) | 'PREFIX' (protect: junk glued to a fund)
                 | '' (not junk)."""
    raw = (name or "").strip()
    nn = norm(raw)
    if not nn:
        return True, "empty/non-alphanumeric name", "DELETE"
    # no letters at all (pure digits / punctuation) -> junk
    if not re.search(r"[a-z]", nn):
        return True, "no alphabetic content", "DELETE"
    # plan_mf_history_v3 cleanup denylist (2026-09-08/09) -- checked before the prefix
    # logic below, since some of these boilerplate strings share a prefix with a real
    # fund-name pattern and would otherwise be wrongly classified PREFIX (protect).
    if nn in V3_CLEANUP_BOILERPLATE_EXACT:
        return True, "v3 cleanup: boilerplate/accounting phrase (confirmed 2026-09-08)", "DELETE"
    if nn in V3_CLEANUP_STOCK_EXACT:
        return True, "v3 cleanup: confirmed individual stock, not a fund (2026-09-08)", "DELETE"
    if _EIN_FRAGMENT_RE.match(nn):
        return True, "v3 cleanup: isolated EIN digit-group (2026-09-09)", "DELETE"
    if _FORM_ID_CODE_RE.match(nn):
        return True, "v3 cleanup: form/schedule ID code, e.g. '1P1211A' (2026-09-09)", "DELETE"
    if _MULTI_SEDOL_RE.search(nn):
        return True, "v3 cleanup: multiple SEDOLs spliced into one name (2026-09-09)", "DELETE"
    if _SWAP_CDS_LEG_RE.match(nn):
        return True, "v3 cleanup: interest-rate-swap/CDS contract leg, not a fund (2026-09-09)", "DELETE"
    # Participant loans / notes receivable (bounded phrasing, safe as substring)
    if _LOAN_RE.search(nn):
        return True, "participant loan / notes receivable", "DELETE"
    # Mode A: accounting income / balance line (bounded phrasing, safe as substring)
    if _ACCTG_RE.search(nn):
        return True, "accounting income/balance line", "DELETE"
    # Mode E: subtotal by name ("Total <manager> Investments/Funds/Assets")
    if _SUBTOTAL_NAME_RE.match(raw):
        return True, "subtotal line (Total ... investments/funds/assets)", "DELETE"
    # Tier 2 exact whole-string
    if nn in JUNK_EXACT:
        return True, "accounting/header label (exact)", "DELETE"
    # Tier 2 prefixes
    for p in JUNK_PREFIXES:
        if nn == p:
            return True, f"bare label '{p}'", "DELETE"
        if nn.startswith(p):
            # real remainder after the label -> junk glued to a real fund; protect it
            return False, f"label-prefixed fund ('{p}' + name)", "PREFIX"
    # Tier 3 shape
    if _DATE_RE.match(raw):
        return True, "date parsed as fund", "DELETE"
    if _BOND_FRAG_RE.match(raw) or _MATURING_RE.search(raw):
        return True, "bond/maturity fragment", "DELETE"
    # user-reviewed junk forms (2026-07-15)
    if _CID_RE.search(nn):
        return True, "OCR glyph artifact (cidNN)", "DELETE"
    if _SUBTOTAL_NORM_RE.match(nn) or _ENDS_TOTAL_RE.search(raw):
        return True, "subtotal line ('... total' / 'total ... funds')", "DELETE"
    if _SHARE_FRAG_RE.match(raw) or _SHARE_DBL_RE.search(raw):
        return True, "share-count fragment (no fund name)", "DELETE"
    if _PARTY_RE.search(nn):
        return True, "party-in-interest footnote marker", "DELETE"
    if _YEAR_RANGE_RE.match(raw) or _RATE_RANGE_RE.match(raw) or _RATE_WORDS_RE.match(raw):
        return True, "rate/date-range fragment", "DELETE"
    if _THROUGH_DATE_RE.match(raw):
        return True, "trailing date fragment ('through <date>')", "DELETE"
    if _PENSION_RE.search(raw) or _AGG_ASSETS_RE.match(raw) or _SUBACCT_RE.match(raw):
        return True, "metadata/subtotal/subaccount fragment", "DELETE"
    if nn in {norm(w) for w in _GENERIC_WORDS}:
        return True, "lone generic word", "DELETE"
    return False, "", ""


def _name(row: Dict) -> str:
    # works in the pipeline (raw_entity_name) and in offline analysis (fund_name)
    return row.get("fund_name") or row.get("raw_entity_name") or ""


def _val(row: Dict) -> float:
    try:
        v = float(row.get("plan_investment_amt") or 0.0)
    except (TypeError, ValueError):
        v = 0.0
    return v if v == v else 0.0


def clean_plan(rows: List[Dict], grand_total_tol: float = 0.02) -> Dict:
    """Remove junk from one plan's rows. Structural + lexicon + shape.
    Returns dict(keep, removed_junk, prefix_contaminated, dedup_removed, near_dup_removed)."""
    n = len(rows)
    vals = [_val(r) for r in rows]
    total = sum(vals)
    keep, removed, prefix_bad = [], [], []

    # Tier 1a: exact duplicates (same normalized name + same value) -> keep first only
    seen = set()
    dedup_removed = []
    dedup_mask = [False] * n
    for i, r in enumerate(rows):
        key = (norm(_name(r)), round(vals[i], 2))
        if key in seen and vals[i] > 0:
            dedup_mask[i] = True
            dedup_removed.append(r)
        else:
            seen.add(key)

    # Tier 1a2: NEAR-duplicate (same fund, share-class/boilerplate wording differs, value
    # within a tight tolerance) -> keep the larger-value row, drop the other. Requires BOTH
    # signals (fuzzy name match AND close value) so two genuinely distinct real positions in
    # the same fund family (e.g. participant deferral vs employer match, legitimately
    # different dollar amounts) are never merged -- only a near-identical value alongside a
    # near-identical name indicates the same holding got captured twice.
    _NEAR_DUP_VALUE_TOL = 0.005  # 0.5% relative difference
    fuzzy_removed = []
    fuzzy_mask = [False] * n
    _fuzzy_groups: Dict[str, List[int]] = {}
    for i, r in enumerate(rows):
        if dedup_mask[i] or vals[i] <= 0:
            continue
        key = fuzzy_norm(_name(r))
        if not key:
            continue
        _fuzzy_groups.setdefault(key, []).append(i)
    for idxs in _fuzzy_groups.values():
        if len(idxs) < 2:
            continue
        idxs_sorted = sorted(idxs, key=lambda i: -vals[i])
        kept_idx = idxs_sorted[0]
        for j in idxs_sorted[1:]:
            if fuzzy_mask[j]:
                continue
            denom = max(vals[kept_idx], vals[j])
            if denom > 0 and abs(vals[kept_idx] - vals[j]) <= _NEAR_DUP_VALUE_TOL * denom:
                fuzzy_mask[j] = True
                fuzzy_removed.append(rows[j])

    # Tier 1b: grand-total row -> value ~= sum of all OTHER rows
    gt_mask = [False] * n
    if total > 0:
        for i in range(n):
            others = total - vals[i]
            if others > 0 and abs(vals[i] - others) <= grand_total_tol * others:
                gt_mask[i] = True

    # Tier 1c (mode E structural): per-manager / per-section SUBTOTAL row -> value ~= sum
    # of a preceding run of rows, gated only on the name containing "total"/"subtotal".
    # DISABLED 2026-07-13. In practice it fires on REAL funds -- "Vanguard Total Stock
    # Market", "PIMCO Total Return", "SP Total Market Index", "Metropolitan West Total
    # Return Bond" -- whose value coincidentally equals a preceding block. The block math
    # is only meaningful in true reading order, but plan_mf_history_v3 has no page/row_id
    # key, so rows arrive in arbitrary order and the coincidences are spurious. On the
    # over-capture dry-run it removed 268 rows / $1.96B, mostly real holdings. Re-enable
    # ONLY with a reliable reading-order key AND a name guard that excludes fund-name
    # "total" (Total Stock Market / Total Bond / Total Return / Total Market Index).
    st_mask = [False] * n
    # for i in range(n):
    #     if gt_mask[i] or dedup_mask[i]:
    #         continue
    #     if not _TOTAL_WORD_RE.search(_name(rows[i])) or vals[i] <= 0:
    #         continue
    #     run = 0.0
    #     for j in range(i - 1, -1, -1):
    #         if gt_mask[j] or dedup_mask[j] or st_mask[j] or _TOTAL_WORD_RE.search(_name(rows[j])):
    #             break                      # block ends at a prior total / flagged row
    #         run += vals[j]
    #         if j < i - 1 and abs(vals[i] - run) <= grand_total_tol * vals[i]:
    #             st_mask[i] = True          # matched a >=2-row preceding block
    #             break

    for i, r in enumerate(rows):
        if dedup_mask[i] or fuzzy_mask[i]:
            continue
        if gt_mask[i]:
            removed.append((r, "grand-total (== sum of other rows)"))
            continue
        if st_mask[i]:
            removed.append((r, "section/manager subtotal (== sum of preceding block)"))
            continue
        junk, reason, disp = is_junk_name(_name(r))
        if junk and disp == "DELETE":
            removed.append((r, reason))
        elif disp == "PREFIX":
            prefix_bad.append((r, reason))
            keep.append(r)          # protected: kept for now, flagged for value-repair
        else:
            keep.append(r)
    return {
        "keep": keep,
        "removed_junk": removed,
        "prefix_contaminated": prefix_bad,
        "dedup_removed": dedup_removed,
        "near_dup_removed": fuzzy_removed,
        "keep_sum": sum(_val(r) for r in keep),
        "removed_sum": (sum(_val(r) for r, _ in removed) + sum(_val(r) for r in dedup_removed)
                        + sum(_val(r) for r in fuzzy_removed)),
    }


# --------------------------------------------------------------- pipeline stage ---
def is_enabled() -> bool:
    """OFF by default. Mirrors section_typing's env-flag gating (JUNK_FILTER=1)."""
    import os
    return os.environ.get("JUNK_FILTER", "0") == "1"


def apply_junk_stage(records: List[Dict]) -> Tuple[List[Dict], Dict[str, int]]:
    """Pipeline entry point. Given one plan's MF records (each with raw_entity_name /
    plan_investment_amt), return (kept_records, stats). Pure; callers substitute the
    returned list for the plan's rows. Removals here are provable non-holdings only --
    duplicates, grand-total lines, accounting/header labels, dates, bond fragments --
    so this never drops a real fund. Rows where a junk label is glued onto a real fund
    are KEPT and reported in stats['prefix_contaminated'] for the value-repair pass.
    """
    res = clean_plan(records)
    stats = {
        "removed_junk": len(res["removed_junk"]),
        "dedup_removed": len(res["dedup_removed"]),
        "near_dup_removed": len(res["near_dup_removed"]),
        "prefix_contaminated": len(res["prefix_contaminated"]),
        "removed_amt": int(res["removed_sum"]),
    }
    return res["keep"], stats
