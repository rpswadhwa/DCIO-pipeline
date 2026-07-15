"""
junk_detect.py  --  identify rows that were never a holding at all (pure junk).

Junk != CIT. A CIT is a real vehicle we exclude for scope reasons; junk is a total
line, a section header, a carry-forward balance, a date, or a bond fragment that got
scraped as if it were a fund. Removing junk is ALWAYS safe -- there is no real fund to
lose -- so we do it independently of the certified anchor, and then use certified only
to VALIDATE (did junk removal get us to the audited MF total?).

IDENTIFICATION (three tiers)
  Tier 1 structural (name-independent): exact duplicate rows; grand-total row
       (value ~= sum of all other rows in the plan).
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
}

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
    if _YEAR_RANGE_RE.match(raw):
        return True, "date-range fragment", "DELETE"
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
    Returns dict(keep, removed_junk, prefix_contaminated, dedup_removed)."""
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
        if dedup_mask[i]:
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
        "keep_sum": sum(_val(r) for r in keep),
        "removed_sum": sum(_val(r) for r, _ in removed) + sum(_val(r) for r in dedup_removed),
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
        "prefix_contaminated": len(res["prefix_contaminated"]),
        "removed_amt": int(res["removed_sum"]),
    }
    return res["keep"], stats
