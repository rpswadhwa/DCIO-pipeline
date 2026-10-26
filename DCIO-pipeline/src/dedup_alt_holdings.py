"""Cross-table de-duplication of alternative holdings (2026-10-21).

One holding must live in exactly one table:
  mutual fund -> plan_mf_history_v3
  CIT         -> plan_cit_history
  anything else (LPs, pooled separate accounts, 103-12 IEs, ...) -> plan_alternatives_history ("RH")

The same Schedule H/D line can land in more than one of them: the CITs Glue job loads every
Schedule D Part I row whatever its entity code, the alternatives router copies staging rows into
RH, and MF classification can mark a fund 'Alternatives' that RH or CIT already holds. This step
finds those copies within a filing (ack_id) and keeps exactly one.

Matching (same rules as the DC ALTA deck, build_ge100m.match_pairs), within an ack_id, greedy 1:1,
pair order RH-vs-CIT, RH-vs-MF, CIT-vs-MF:
  tier 1  amount within $1 and names match
  tier 2  amount within $1, names differ
  tier 3  amount within 1%, names match, and same fund kind/vintage (same_fund_kind, 2026-10-23)
  tier 4  names only -> NOT treated as the same holding (never acted on)

Which copy stays (Schedule D Part I dfe_p1_entity_code of the CIT row; C=CCT, P=pooled separate
account, E=103-12 IE, M=master trust):
  RH  vs CIT  code C       -> drop RH copy
  RH  vs CIT  code P/E/M   -> drop CIT copy
  CIT vs MF   code C       -> drop MF copy
  CIT vs MF   code E/M, or code P with an LP/LLC/separate-account style name (NOT_MF)
                           -> drop both, insert into RH (classification_method 'schedule_d_move:v1')
  CIT vs MF   code P, mutual-fund name -> drop CIT copy (insurer separate account wrapping an MF)
  RH  vs MF   NOT_MF name  -> drop MF copy, else drop RH copy
Non-Alternatives CIT vs MF (2026-10-22, pair citx_mfx): a fund the filer lists on Schedule D Part I
with code C (a bank collective trust) that MF extraction also wrote to plan_mf_history_v3 under an
Equity/Fixed Income/Target Date class (Timken: "WESTERN ASSET CORE PLUS BOND R1" $31.6M in both).
Same matching tiers; code C acts (drop the MF copy), any other code is reported and left alone.
2026-10-24: code P (insurer pooled separate account, "T. Rowe Price Retirement 2040 SA", "Prin LargeCap
S&P 500 Index SA-Z") drops the MF copy too, but only for a filing where that brings its MF total closer
to the certified Schedule H MF total (plan_asset_allocation.amt_mutual_funds): filers differ on whether
they count a separate account as MF. No certified figure -> left alone.
Non-MF names in plan_mf_history_v3 (2026-10-26, pair mfx_name): rows whose name says they can't be a
mutual fund (asset_type_patterns.non_mf_override_type -- the pipeline's always-on pass since 9a93b016,
so this catches rows written before it). Bond / participant loan / company stock -> dropped from MF.
Collective trust / brokerage window -> dropped only for a filing where that brings its MF total closer
to certified (filers often count both as MF on Schedule H); no certified figure -> left alone. Stable
value is reported and left alone: the stable value router skips staging rows that already sit in MF, so
dropping them here would take them out of the SV tables too. Nothing is inserted anywhere: a pipeline
rerun likewise only keeps such rows out of MF (they stay in plan_holdings_staging; CITs reach
plan_cit_history from Schedule D).
TODO (2026-10-26, must do): the always-on pass in post_extract_validator still re-types collective
trust / brokerage names with NO certified check, so rerunning a filing whose filer counts them as MF
strips them anyway. Fix: limit that pass to bond / loan / company stock (keep the NL Ser / Non-Lending /
JPMCB CIT tags always-on for plans with no certified row) and leave collective trust / brokerage to
mf_reconcile's certified-aware CIT-by-name stage.
Rows under $100 are never matched ($1 placeholder lines pair up on amount alone).
A CIT row with no Schedule D code is left alone, and so is any delete whose (ack, name, amount) key
matches more live rows than it targets (twins we can't tell apart).

Every row it deletes or inserts is written to default.alt_holding_dedup_log (full row as JSON)
BEFORE any change, so a run can be undone from the log alone. 'schedule_d_move:v1' is outside the
alternatives router's ALT_ROUTER_METHOD_REGEX, so later re-routes keep the moved rows.

Usage:
  python3.11 -m src.dedup_alt_holdings --ack-ids A,B,C          # dry run (default)
  ACK_IDS=A,B python3.11 -m src.dedup_alt_holdings --apply
  python3.11 -m src.dedup_alt_holdings --ack-file acks.txt --apply
  python3.11 -m src.dedup_alt_holdings --all                    # whole-table sweep
  python3.11 -m src.dedup_alt_holdings --undo-file rows.csv --apply   # put logged deletes back
"""
import argparse
import json
import math
import os
import re
import sys
import uuid
from datetime import datetime, timezone

import awswrangler as wr
import pandas as pd

from .asset_type_patterns import non_mf_override_type

DB = "default"
WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")
S3_OUTPUT = os.environ.get("ATHENA_RESULTS", "s3://retirementinsights-silver/athena-results/")
LOG_TABLE = "alt_holding_dedup_log"
MOVE_METHOD = "schedule_d_move:v1"
ACK_CHUNK = 300
STMT_CHUNK = 200
MIN_AMT = 100
MAX_STMT_CHARS = 180_000

STOP = {"THE", "FUND", "FD", "FDS", "TRUST", "TR", "LP", "L", "P", "LLC", "INC", "CORP", "CLASS", "CL", "OF", "AND", "INST",
        "INSTITUTIONAL", "SHARES", "SHS", "ACCOUNT", "ACCT", "COLLECTIVE", "INVESTMENT", "INVESTMENTS", "PORTFOLIO", "CO",
        "COMPANY", "N", "A", "NA", "I", "II", "III", "R6", "R5", "K", "X", "Y", "Z", "MANAGEMENT", "CAPITAL", "PARTNERS",
        "GROUP", "SERIES", "UNITS", "UNIT", "INTEREST", "IN", "FOR", "ADVISORS", "GLOBAL", "US", "U", "S"}
CONFIRMED = {1, 2, 3}
# Names that say "not a mutual fund" even when MF classification tagged them Alternatives.
NOT_MF = re.compile(r"\bl\.?\s?p\.?\b|limited\s+partnership|\bllc\b|\bfeeder\b|\breit\s+ii\b|closely\s*held|\bpartners\b"
                    r"|separate\s+acc|tiaa\s+real\s+estate(?!\s+(sec|index|idx))|\bprisa\b|\bullico\b", re.I)
MOVE_ASSET_TYPE = {"P": "Separate Account", "E": "Private Fund - LP", "M": "Master Trust Interest"}

# Rows outside the Alternatives pass (NULL asset_class included).
NOT_ALT_SQL = "coalesce(asset_class, '') <> 'Alternatives'"

LOG_DDL = f"""
CREATE TABLE IF NOT EXISTS {DB}.{LOG_TABLE} (
    run_id               string,
    run_at               timestamp,
    action               string,
    table_name           string,
    ack_id               string,
    row_order            string,
    raw_entity_name      string,
    plan_investment_amt  double,
    pair                 string,
    match_tier           int,
    sch_d_code           string,
    kept_table           string,
    kept_name            string,
    row_json             string
)
LOCATION 's3://retirementinsights-silver/iceberg-warehouse/{LOG_TABLE}'
TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'PARQUET', 'write_compression' = 'ZSTD')
"""


def log(msg):
    print(f"[dedup_alt_holdings] {msg}", flush=True)


def q(s):
    return "NULL" if s is None or (isinstance(s, float) and math.isnan(s)) else "'" + str(s).replace("'", "''") + "'"


def read(sql):
    return wr.athena.read_sql_query(sql=sql, database=DB, workgroup=WORKGROUP, s3_output=S3_OUTPUT, ctas_approach=False)


def execute(sql):
    qid = wr.athena.start_query_execution(sql=sql, database=DB, workgroup=WORKGROUP, s3_output=S3_OUTPUT, wait=False)
    wr.athena.wait_query(query_execution_id=qid)


def amt_key(x):
    try:
        v = float(x)
        # Half away from zero like Athena's round(); Python's round() is half-to-even, so a
        # $1,497,096.50 row keyed ...096 here and ...097 in the DELETE (2026-10-22).
        return "nan" if math.isnan(v) else f"{math.copysign(math.floor(abs(v) + 0.5), v) + 0.0:.0f}"
    except (TypeError, ValueError):
        return "nan"


def name_key(ack, name, amt):
    """Same key the DELETE uses: ack|raw_entity_name|rounded amount."""
    return f"{ack}|{name if isinstance(name, str) else ''}|{amt_key(amt)}"


# SQL twin of name_key().
NAME_KEY_SQL = ("concat(ack_id,'|',coalesce(raw_entity_name,''),'|',"
                "cast(cast(round(cast(plan_investment_amt as double)) as bigint) as varchar))")


def toks(*parts):
    t = set()
    for p in parts:
        if isinstance(p, str):
            t |= {w for w in re.sub(r"[^A-Z0-9 ]", " ", p.upper()).split() if w not in STOP and not w.isdigit()}
    return t


def name_match(a, b):
    if not a or not b:
        return False
    i = len(a & b)
    return i / len(a | b) >= 0.34 or i / min(len(a), len(b)) >= 0.6


# Tier 3 (amount only within 1%) needs the two names to be the same KIND of fund too: a shared brand
# ("T. Rowe Price", "Vanguard") passed name_match and paired different funds of similar size
# (Retirement 2025 vs TRP Stable Value, Growth Index vs 500 Index, 2055 vs 2035) -- 103 rows / $1.5B
# wrongly dropped from MF in the 2026-10-22 cleanup.
VINTAGE = re.compile(r"(?<!\d)(20[0-7][05])(?!\d)")
FUND_KIND = {
    "tdf": r"RETIRE|TARGET|LIFETIME|FREEDOM|FRDM|LIFEPATH|TRGT|TGT|\bRET\b|BLEND|MYCOMPASS|FLEXPATH|(?<!\d)20[1-7][05](?!\d)",
    "sv": r"STABLE|SAVINGS TR|SVGS TR|SAV TR|PRESERV|\bGIC\b",
    "mm": r"MONEY|\bMMKT\b|GVMNT MON|CASH",
    "growth": r"GROWTH|\bGRW|\bGRTH|\bGR\b",
    "value": r"(?<!STABLE )(?<!ST )VALUE|\bVAL\b",
    "midsmall": r"\bMID|SMALL|SMID|\bSM ?CAP|SMMID",
    "intl": r"INT'?L|INTERNATIONAL|INTERNATL|FOREIGN|EMERG|EAFE|\bWORLD|EX[- ]?US",
    "sector": r"\bSCI|TECH|HEALTH|REAL ESTATE|\bREIT|ENERGY",
}
FUND_KIND = {k: re.compile(p) for k, p in FUND_KIND.items()}


def fund_kinds(name):
    s = name.upper() if isinstance(name, str) else ""
    return {k for k, p in FUND_KIND.items() if p.search(s)}


def same_fund_kind(a, b):
    """False when two names point at different funds: different vintage years, a vintage on only one of
    two target-date names, or a fund kind (stable value, money market, growth, ...) on only one side."""
    a = a.upper() if isinstance(a, str) else ""
    b = b.upper() if isinstance(b, str) else ""
    ya, yb = set(VINTAGE.findall(a)), set(VINTAGE.findall(b))
    if ya and yb and not ya & yb:
        return False
    ka, kb = fund_kinds(a), fund_kinds(b)
    if "tdf" in ka and "tdf" in kb and bool(ya) != bool(yb) and "INCOME" not in a + b:
        return False
    return ka == kb


def match_pairs(L, R_):
    """Greedy one-to-one match of rows in L to rows in R_ on the same ack_id. Returns index_l, index_r, tier."""
    j = L.reset_index().merge(R_.reset_index(), on="ack_id", suffixes=("_l", "_r"))
    if j.empty:
        return pd.DataFrame(columns=["index_l", "index_r", "tier"])
    j["diff"] = (j.amt_l - j.amt_r).abs()
    nm = pd.Series([name_match(a, b) for a, b in zip(j.tok_l, j.tok_r)], index=j.index)
    j["tier"] = 0
    j.loc[(j["diff"] <= 1) & nm, "tier"] = 1
    j.loc[(j["diff"] <= 1) & ~nm, "tier"] = 2
    kind = pd.Series([same_fund_kind(a, b) for a, b in zip(j.name_l, j.name_r)], index=j.index)
    j.loc[(j.tier == 0) & (j["diff"] <= 0.01 * j[["amt_l", "amt_r"]].max(axis=1)) & nm & kind, "tier"] = 3
    j.loc[(j.tier == 0) & nm, "tier"] = 4
    j = j[j.tier > 0].sort_values(["tier", "diff"])
    used_l, used_r, out = set(), set(), []
    for r in j.itertuples():
        if r.index_l in used_l or r.index_r in used_r:
            continue
        used_l.add(r.index_l)
        used_r.add(r.index_r)
        out.append((r.index_l, r.index_r, r.tier))
    return pd.DataFrame(out, columns=["index_l", "index_r", "tier"])


def ack_clause(acks, col="ack_id"):
    return "" if acks is None else f" AND {col} IN ({','.join(q(a) for a in acks)})"


def fetch(acks):
    """Live rows of the three tables for these acks (None = all). cit/mf = their Alternatives rows,
    citx/mfx = every other CIT/MF row (compared only with each other)."""
    chunks = [None] if acks is None else [acks[i:i + ACK_CHUNK] for i in range(0, len(acks), ACK_CHUNK)]
    parts = {"rh": [], "cit": [], "mf": [], "citx": [], "mfx": []}
    for ch in chunks:
        parts["rh"].append(read(f"SELECT * FROM {DB}.plan_alternatives_history WHERE true{ack_clause(ch)}"))
        parts["cit"].append(read(f"""
            SELECT c.*, coalesce(d.dfe_p1_entity_code, s.dfe_p1_entity_code) AS sch_d_code
            FROM {DB}.plan_cit_history c
            LEFT JOIN {DB}.schedule_d_part1 d ON d.ack_id = c.ack_id AND cast(d.row_order AS varchar) = cast(c.row_order AS varchar)
            LEFT JOIN {DB}.schedule_d_part1_stp s ON d.ack_id IS NULL AND s.ack_id = c.ack_id
                 AND cast(s.row_order AS varchar) = cast(c.row_order AS varchar)
            WHERE c.asset_class = 'Alternatives'{ack_clause(ch, 'c.ack_id')}"""))
        parts["mf"].append(read(f"SELECT * FROM {DB}.plan_mf_history_v3 WHERE asset_class = 'Alternatives'{ack_clause(ch)}"))
        parts["citx"].append(read(f"""
            SELECT c.*, coalesce(d.dfe_p1_entity_code, s.dfe_p1_entity_code) AS sch_d_code
            FROM {DB}.plan_cit_history c
            LEFT JOIN {DB}.schedule_d_part1 d ON d.ack_id = c.ack_id AND cast(d.row_order AS varchar) = cast(c.row_order AS varchar)
            LEFT JOIN {DB}.schedule_d_part1_stp s ON d.ack_id IS NULL AND s.ack_id = c.ack_id
                 AND cast(s.row_order AS varchar) = cast(c.row_order AS varchar)
            WHERE {NOT_ALT_SQL.replace('asset_class', 'c.asset_class')}{ack_clause(ch, 'c.ack_id')}"""))
        parts["mfx"].append(read(f"SELECT * FROM {DB}.plan_mf_history_v3 WHERE {NOT_ALT_SQL}{ack_clause(ch)}"))
    live = {k: pd.concat(v, ignore_index=True) for k, v in parts.items()}
    for k, d in live.items():
        d["ack_id"] = d["ack_id"].astype(str)
        d["_amt"] = pd.to_numeric(d["plan_investment_amt"], errors="coerce")
        d["_key"] = [name_key(a, n, x) for a, n, x in zip(d.ack_id, d.raw_entity_name, d.plan_investment_amt)]
    live["cit"]["_cit_key"] = cit_keys(live["cit"])
    live["citx"]["_cit_key"] = cit_keys(live["citx"])
    return live


def certified_mf(acks):
    """Schedule H certified mutual-fund total (plan_asset_allocation.amt_mutual_funds) per ack; NULL -> absent."""
    chunks = [None] if acks is None else [acks[i:i + ACK_CHUNK] for i in range(0, len(acks), ACK_CHUNK)]
    out = {}
    for ch in chunks:
        d = read(f"SELECT ack_id, max(TRY_CAST(nullif(trim(amt_mutual_funds), '') AS double)) cert "
                 f"FROM {DB}.plan_asset_allocation WHERE true{ack_clause(ch)} GROUP BY 1")
        out.update({str(a): float(c) for a, c in zip(d.ack_id, d.cert) if pd.notna(c)})
    return out


def cit_keys(cit):
    return [f"{a}|{r}" for a, r in zip(cit.ack_id, cit.row_order)]


def find_actions(live, cert=None):
    mgr_col = {"rh": "matched_manager_name", "cit": "normalized_manager", "mf": "normalized_sponsor_name",
               "citx": "normalized_manager", "mfx": "normalized_sponsor_name"}
    frames = []
    for src, d in live.items():
        f = pd.DataFrame({"src": src, "ack_id": d.ack_id, "amt": d._amt, "row": d.index,
                          "name": d.raw_entity_name, "mgr": d.get(mgr_col[src])})
        frames.append(f)
    H = pd.concat(frames, ignore_index=True)
    # $1/$10 placeholder lines (unvalued LP interests) "match" anything else at $1 on amount alone.
    H = H[H.amt >= MIN_AMT].copy()
    H["tok"] = [toks(a, b) for a, b in zip(H.name, H.mgr)]
    H["tier"] = 0
    pairs = []
    for l, r in [("rh", "cit"), ("rh", "mf"), ("cit", "mf"), ("citx", "mfx")]:
        free = ~H.tier.isin(CONFIRMED)
        A = H[(H.src == l) & free][["ack_id", "amt", "tok", "name"]]
        B = H[(H.src == r) & free][["ack_id", "amt", "tok", "name"]]
        A, B = A[A.ack_id.isin(set(B.ack_id))], B[B.ack_id.isin(set(A.ack_id))]
        mp = match_pairs(A, B)
        mp = mp[mp.tier.isin(CONFIRMED)]
        H.loc[mp.index_r, "tier"] = mp.tier.values
        for il, ir, t in mp.itertuples(index=False):
            pairs.append((f"{l}_{r}", int(t), H.loc[il], H.loc[ir]))

    acts = []  # dicts: op (delete/insert), table, row (live index), pair, tier, code, kept_table, kept_name
    skipped = []
    for pair, tier, a, b in pairs:
        cit = a if pair in ("cit_mf", "citx_mfx") else b if pair == "rh_cit" else None
        code = live[cit.src].loc[cit.row, "sch_d_code"] if cit is not None else None
        code = code if isinstance(code, str) and code.strip() else None
        base = dict(pair=pair, tier=tier, code=code)
        if pair in ("rh_cit", "cit_mf", "citx_mfx") and code is None:
            skipped.append((pair, a.ack_id, a["name"], "no Schedule D code on the CIT row"))
            continue
        if pair == "citx_mfx":
            if code in ("C", "P"):  # P (insurer separate account) added 2026-10-24: not a mutual fund either
                acts.append(dict(base, op="delete", table="mfx", row=b.row, kept_table="citx", kept_name=a["name"]))
            else:
                skipped.append((pair, a.ack_id, a["name"], f"Schedule D code {code}, not C"))
        elif pair == "rh_cit":
            if code == "C":
                acts.append(dict(base, op="delete", table="rh", row=a.row, kept_table="cit", kept_name=b["name"]))
            else:
                acts.append(dict(base, op="delete", table="cit", row=b.row, kept_table="rh", kept_name=a["name"]))
        elif pair == "cit_mf":
            if code == "C":
                acts.append(dict(base, op="delete", table="mf", row=b.row, kept_table="cit", kept_name=a["name"]))
            elif code == "P" and not NOT_MF.search(f"{a['name']} {b['name']}"):
                # P-coded insurer separate account that wraps a real mutual fund (Cohen & Steers RE, iShares
                # Gold, Principal RE Secs SA-Z, ...): the MF copy is the holding. E (103-12 IE) and M never are.
                acts.append(dict(base, op="delete", table="cit", row=a.row, kept_table="mf", kept_name=b["name"]))
            else:
                g = dict(base, group=len(acts), kept_table="rh", kept_name=a["name"])
                acts.append(dict(g, op="insert", table="rh", row=a.row))
                acts.append(dict(g, op="delete", table="cit", row=a.row))
                acts.append(dict(g, op="delete", table="mf", row=b.row))
        else:  # rh_mf
            if NOT_MF.search(f"{a['name']} {b['name']}"):
                acts.append(dict(base, op="delete", table="mf", row=b.row, kept_table="rh", kept_name=a["name"]))
            else:
                acts.append(dict(base, op="delete", table="rh", row=a.row, kept_table="mf", kept_name=b["name"]))

    # Code P is filer-dependent: some filers count an insurer separate account in their certified MF total,
    # some don't (2026-10-24 dry run: dropping every P copy helped 397 filings and broke 557). So a filing's
    # P copies go only when that brings its MF total closer to its certified MF total; no certified figure
    # -> left alone.
    if cert is not None:
        mf_tot = pd.concat([live["mf"][["ack_id", "_amt"]], live["mfx"][["ack_id", "_amt"]]]).groupby("ack_id")._amt.sum()
        c_rm, p_rm = {}, {}
        for x in acts:
            if x["pair"] == "citx_mfx":
                tgt = p_rm if x["code"] == "P" else c_rm
                a = live["mfx"].loc[x["row"], "ack_id"]
                tgt[a] = tgt.get(a, 0.0) + float(live["mfx"].loc[x["row"], "_amt"])
        drop = set()
        for a, rm in p_rm.items():
            v = float(mf_tot.get(a, 0.0)) - c_rm.get(a, 0.0)
            if a not in cert:
                why = "Schedule D code P, no certified MF total"
            elif abs(v - rm - cert[a]) < abs(v - cert[a]):
                continue
            else:
                why = f"Schedule D code P, filer counts it as MF (certified {cert[a]:,.0f}, MF {v:,.0f})"
            drop.add(a)
            skipped.append(("citx_mfx", a, None, why))
        acts = [x for x in acts if not (x["pair"] == "citx_mfx" and x["code"] == "P" and live["mfx"].loc[x["row"], "ack_id"] in drop)]
    else:
        acts = [x for x in acts if not (x["pair"] == "citx_mfx" and x["code"] == "P")]

    acts += name_actions(live, acts, cert, skipped)

    # Twin guard: a name-keyed DELETE removes every live row with that key, so only allow it when
    # the number of live rows with the key equals the number of rows we mean to delete.
    for tbl in ("rh", "mf", "mfx"):
        want = pd.Series([live[tbl].loc[x["row"], "_key"] for x in acts if x["op"] == "delete" and x["table"] == tbl]).value_counts()
        have = live[tbl]["_key"].value_counts()
        bad = {k for k, n in want.items() if have.get(k, 0) != n}
        if bad:
            for x in acts:
                if x["op"] == "delete" and x["table"] == tbl and live[tbl].loc[x["row"], "_key"] in bad:
                    x["op"] = "skip"
                    skipped.append((x["pair"], live[tbl].loc[x["row"], "ack_id"], live[tbl].loc[x["row"], "raw_entity_name"],
                                    f"{tbl} key has twins"))
    # A move (insert + CIT delete + MF delete) goes all together or not at all, so nothing is double counted.
    bad_groups = {x["group"] for x in acts if x["op"] == "skip" and "group" in x}
    return [x for x in acts if x["op"] != "skip" and x.get("group") not in bad_groups], skipped


NAME_HARD = {"Bond", "Participant Loan", "Employer Stock"}
NAME_SOFT = {"Common/Collective Trust Fund", "Self-Directed Brokerage Account"}


def name_actions(live, acts, cert, skipped):
    """mfx_name deletes for MF rows whose name says they aren't a mutual fund (see module doc)."""
    mfx = live["mfx"]
    gone = {x["row"] for x in acts if x["table"] == "mfx"}
    out, soft = [], {}
    for i, n in mfx.raw_entity_name.items():
        # blank amount: nothing to fix, and the delete key can't match a NULL amount
        t = non_mf_override_type(n) if isinstance(n, str) and i not in gone and pd.notna(mfx.loc[i, "_amt"]) else ""
        if not t:
            continue
        x = dict(op="delete", table="mfx", row=i, pair="mfx_name", tier=0, code=None, kept_table="stg", kept_name=t)
        if t in NAME_HARD:
            out.append(x)
        elif t in NAME_SOFT:
            soft.setdefault(mfx.loc[i, "ack_id"], []).append(x)
        else:
            skipped.append(("mfx_name", mfx.loc[i, "ack_id"], n, f"{t}: left for the stable value router"))
    if not soft:
        return out
    # MF total per filing (Alternatives rows too) after every other MF delete in this run
    v_now = pd.concat([live["mf"][["ack_id", "_amt"]], mfx[["ack_id", "_amt"]]]).groupby("ack_id")._amt.sum().to_dict()
    for x in acts + out:
        if x["op"] == "delete" and x["table"] in ("mf", "mfx"):
            r = live[x["table"]].loc[x["row"]]
            v_now[r.ack_id] = v_now.get(r.ack_id, 0.0) - (float(r._amt) if pd.notna(r._amt) else 0.0)
    for a, xs in soft.items():
        s = sum(float(mfx.loc[x["row"], "_amt"]) for x in xs if pd.notna(mfx.loc[x["row"], "_amt"]))
        v = float(v_now.get(a, 0.0))
        if cert is None or a not in cert:
            skipped.append(("mfx_name", a, None, "collective trust/brokerage name, no certified MF total"))
        elif abs(v - s - cert[a]) < abs(v - cert[a]):
            out += xs
        else:
            skipped.append(("mfx_name", a, None, f"collective trust/brokerage name, filer counts it as MF "
                                                  f"(certified {cert[a]:,.0f}, MF {v:,.0f})"))
    return out


def row_json(series):
    return json.dumps({k: (None if (isinstance(v, float) and math.isnan(v)) else
                           v.isoformat() if hasattr(v, "isoformat") else
                           v.item() if hasattr(v, "item") else v)
                       for k, v in series.items() if not str(k).startswith("_")}, default=str)


TABLES = {"rh": "plan_alternatives_history", "cit": "plan_cit_history", "mf": "plan_mf_history_v3",
          "citx": "plan_cit_history", "mfx": "plan_mf_history_v3", "stg": "plan_holdings_staging"}


def chunked(lst, n=STMT_CHUNK, max_chars=MAX_STMT_CHARS):
    """Up to n items per batch, and never more than max_chars of text (Athena caps a query at 256KB)."""
    batch, size = [], 0
    for item in lst:
        if batch and (len(batch) >= n or size + len(item) > max_chars):
            yield batch
            batch, size = [], 0
        batch.append(item)
        size += len(item) + 2
    if batch:
        yield batch


def build_statements(live, acts, run_id, run_at):
    log_rows, ins_rows, del_keys = [], [], {"rh": [], "cit": [], "mf": [], "mfx": []}
    for x in acts:
        src_tbl = "cit" if x["op"] == "insert" else x["table"]
        r = live[src_tbl].loc[x["row"]]
        ro = str(r["row_order"]) if "row_order" in r.index and pd.notna(r.get("row_order")) else None
        log_rows.append(
            f"({q(run_id)},TIMESTAMP {q(run_at)},{q(x['op'])},{q(TABLES[x['table']])},{q(r.ack_id)},{q(ro)},"
            f"{q(r.raw_entity_name)},{'NULL' if pd.isna(r._amt) else repr(float(r._amt))},{q(x['pair'])},{x['tier']},"
            f"{q(x['code'])},{q(TABLES[x['kept_table']])},{q(x['kept_name'])},{q(row_json(r))})")
        if x["op"] == "insert":
            mgr = r.get("proposed_manager_ai")
            mgr = mgr if isinstance(mgr, str) and mgr else r.get("normalized_manager")
            ins_rows.append(
                f"({q(r.ack_id)},{q(r.raw_entity_name)},{q(r.get('raw_sponsor_name'))},CAST({float(r._amt):.2f} AS decimal(18,2)),"
                f"{q(r.get('asset_sub_class'))},NULL,{q(MOVE_ASSET_TYPE.get(x['code'], 'Unknown'))},'HIGH',{q(MOVE_METHOD)},false,"
                f"current_timestamp,'Alternatives',{q(mgr if isinstance(mgr, str) else None)},NULL,'cit_history',NULL)")
        else:
            del_keys[x["table"]].append(r["_cit_key"] if x["table"] == "cit" else r["_key"])

    stmts = []
    for ch in chunked(log_rows):
        stmts.append(("log", f"INSERT INTO {DB}.{LOG_TABLE} VALUES\n" + ",\n".join(ch)))
    for ch in chunked(ins_rows):
        stmts.append(("insert_rh", f"INSERT INTO {DB}.plan_alternatives_history (ack_id,raw_entity_name,raw_sponsor_name,"
                      "plan_investment_amt,asset_sub_class,validation_status,asset_type,classification_confidence,"
                      "classification_method,manual_review_required,routed_at,asset_class,matched_manager_name,"
                      "manager_match_confidence,manager_match_method,provider_id) VALUES\n" + ",\n".join(ch)))
    for ch in chunked(sorted(set(del_keys["cit"]))):
        stmts.append(("delete_cit", f"DELETE FROM {DB}.plan_cit_history WHERE asset_class = 'Alternatives' AND "
                      f"concat(ack_id,'|',cast(row_order AS varchar)) IN ({','.join(q(k) for k in ch)})"))
    for tbl, extra in (("mf", "asset_class = 'Alternatives' AND "), ("mfx", NOT_ALT_SQL + " AND "), ("rh", "")):
        for ch in chunked(sorted(set(del_keys[tbl]))):
            stmts.append((f"delete_{tbl}", f"DELETE FROM {DB}.{TABLES[tbl]} WHERE {extra}{NAME_KEY_SQL} IN ({','.join(q(k) for k in ch)})"))
    return stmts, del_keys, len(ins_rows)


def sql_value(v, typ):
    """A row_json value as an Athena literal of column type typ."""
    if v is None or (isinstance(v, str) and v in ("<NA>", "NaT", "nan", "None")) or (isinstance(v, float) and math.isnan(v)):
        return "NULL"
    if typ.startswith("timestamp"):
        return f"CAST({q(str(v).replace('T', ' '))} AS timestamp)"
    if typ == "boolean":
        return f"CAST({q(str(v).lower())} AS boolean)"
    if typ == "varchar" or typ.startswith("varchar"):
        return q(v)
    return f"CAST({q(v)} AS {typ})"


def undo(path, apply):
    """Put deleted rows back from the log. path: CSV with run_id, ack_id, raw_entity_name, plan_investment_amt
    of logged deletes. Each comes back exactly as logged (row_json), only if no live row has its key."""
    want = pd.read_csv(path, dtype=str, keep_default_na=False)  # a fund literally named "N/A" stays "N/A"
    want["k"] = [f"{r}|{name_key(a, n, x)}" for r, a, n, x in zip(want.run_id, want.ack_id, want.raw_entity_name, want.plan_investment_amt)]
    runs = sorted(set(want.run_id))
    lg = read(f"SELECT * FROM {DB}.{LOG_TABLE} WHERE action = 'delete' AND run_id IN ({','.join(q(r) for r in runs)})"
              f"{ack_clause(sorted(set(want.ack_id)))}")
    lg["k"] = [f"{r}|{name_key(a, n, x)}" for r, a, n, x in zip(lg.run_id, lg.ack_id, lg.raw_entity_name, lg.plan_investment_amt)]
    lg = lg[lg.k.isin(set(want.k))].drop_duplicates("k")
    missing = set(want.k) - set(lg.k)
    if missing:
        sys.exit(f"{len(missing)} rows not in the log, nothing done: {sorted(missing)[:5]}")
    stmts, keys = [], {}
    for tbl, g in lg.groupby("table_name"):
        cols = read(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '{DB}' "
                    f"AND table_name = {q(tbl)} ORDER BY ordinal_position")
        live = read(f"SELECT {NAME_KEY_SQL} AS k FROM {DB}.{tbl} WHERE true{ack_clause(sorted(set(g.ack_id)))}")
        g = g.assign(nk=[name_key(a, n, x) for a, n, x in zip(g.ack_id, g.raw_entity_name, g.plan_investment_amt)])
        back = g.nk.isin(set(live.k))
        if back.any():
            log(f"{tbl}: {int(back.sum())} rows already live again, left alone")
        g = g[~back]
        keys[tbl] = list(g.nk)
        vals = []
        for rj in g.row_json:
            d = json.loads(rj)
            vals.append("(" + ",".join(sql_value(d.get(c), t) for c, t in zip(cols.column_name, cols.data_type)) + ")")
        log(f"{tbl}: restore {len(vals)} rows ${pd.to_numeric(g.plan_investment_amt).sum() / 1e6:,.2f}M")
        for ch in chunked(vals):
            stmts.append(f"INSERT INTO {DB}.{tbl} ({','.join(cols.column_name)}) VALUES\n" + ",\n".join(ch))
    if not apply or not stmts:
        return
    run_id = uuid.uuid4().hex[:12]
    run_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    restored = lg[lg.apply(lambda r: name_key(r.ack_id, r.raw_entity_name, r.plan_investment_amt) in set(keys.get(r.table_name, [])), axis=1)]
    log_rows = [f"({q(run_id)},TIMESTAMP {q(run_at)},'undo',{q(r.table_name)},{q(r.ack_id)},{q(r.row_order)},{q(r.raw_entity_name)},"
                f"{repr(float(r.plan_investment_amt))},{q(r.pair)},{int(r.match_tier)},{q(r.sch_d_code)},{q(r.kept_table)},"
                f"{q(r.kept_name)},{q(r.run_id)})" for r in restored.itertuples()]
    for ch in chunked(log_rows):  # row_json of an 'undo' row = the run_id it reverses
        execute(f"INSERT INTO {DB}.{LOG_TABLE} VALUES\n" + ",\n".join(ch))
    log(f"undo run_id={run_id} run_at={run_at} UTC; {len(stmts)} insert statements")
    for sql in stmts:
        execute(sql)
    for tbl, ks in keys.items():
        live = read(f"SELECT {NAME_KEY_SQL} AS k FROM {DB}.{tbl} WHERE true{ack_clause(sorted({k.split('|')[0] for k in ks}))}")
        n = int(pd.Series(ks).isin(set(live.k)).sum())
        log(f"verify {tbl}: {n}/{len(ks)} restored rows live")
        if n != len(ks):
            sys.exit(f"UNDO VERIFY FAILED run_id={run_id}")
    log(f"undo done run_id={run_id}")


def summarize(live, acts):
    rows = []
    for x in acts:
        src_tbl = "cit" if x["op"] == "insert" else x["table"]
        rows.append(dict(op=x["op"], table=TABLES[x["table"]], pair=x["pair"], code=x["code"] or "-",
                         usd=float(live[src_tbl].loc[x["row"], "_amt"])))
    if not rows:
        return "no duplicates found"
    s = pd.DataFrame(rows).groupby(["op", "table", "pair", "code"]).usd.agg(rows="size", usd_m=lambda v: round(v.sum() / 1e6, 2))
    return "\n" + s.to_string()


def load_acks(args):
    if args.all:
        return None
    acks = []
    if args.ack_ids:
        acks += args.ack_ids.split(",")
    if args.ack_file:
        acks += open(args.ack_file).read().split()
    if not acks:
        acks += os.environ.get("ACK_IDS", "").split(",")
    acks = sorted({a.strip() for a in acks if a.strip()})
    if not acks:
        sys.exit("No ack_ids given (use --ack-ids, --ack-file, ACK_IDS env, or --all)")
    return acks


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ack-ids", help="comma-separated ack_ids")
    ap.add_argument("--ack-file", help="file with whitespace-separated ack_ids")
    ap.add_argument("--all", action="store_true", help="sweep every ack in the three tables")
    ap.add_argument("--apply", action="store_true", help="write changes (default is a dry run)")
    ap.add_argument("--undo-file", help="CSV (run_id,ack_id,raw_entity_name,plan_investment_amt) of logged deletes to put back")
    args = ap.parse_args()
    if args.undo_file:
        log(f"undo from {args.undo_file}; mode: {'APPLY' if args.apply else 'dry run'}")
        return undo(args.undo_file, args.apply)

    acks = load_acks(args)
    log(f"scope: {'ALL acks' if acks is None else f'{len(acks)} ack_ids'}; mode: {'APPLY' if args.apply else 'dry run'}")
    live = fetch(acks)
    log("live rows: " + ", ".join(f"{TABLES[k]}={len(v)}" for k, v in live.items()))
    acts, skipped = find_actions(live, certified_mf(acks))
    log(f"actions: {len(acts)}{summarize(live, acts)}")
    for s in skipped[:50]:
        log(f"skipped {s[0]} ack={s[1]} name={s[2]!r}: {s[3]}")
    if len(skipped) > 50:
        log(f"... {len(skipped) - 50} more skipped")
    if not acts or not args.apply:
        return

    run_id = uuid.uuid4().hex[:12]
    run_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    stmts, del_keys, n_ins = build_statements(live, acts, run_id, run_at)
    execute(LOG_DDL)
    log(f"run_id={run_id} run_at={run_at} UTC (Iceberg time-travel point); {len(stmts)} statements")
    for kind, sql in stmts:  # log first, then RH inserts, then deletes -- money never leaves before its new home exists
        execute(sql)
        log(f"ok {kind}")

    # Verify: every deleted key is gone and the moved rows are in RH.
    after = fetch(acks)
    left = {t: int(after[t]["_cit_key" if t == "cit" else "_key"].isin(set(k)).sum()) for t, k in del_keys.items()}
    moved = read(f"SELECT count(*) n FROM {DB}.plan_alternatives_history WHERE classification_method = {q(MOVE_METHOD)} "
                 f"AND routed_at >= TIMESTAMP {q(run_at)}{ack_clause(acks)}").n.iloc[0] if n_ins else 0
    log(f"verify: delete targets still live {left}; moved rows inserted {moved}/{n_ins}")
    if any(left.values()) or moved != n_ins:
        sys.exit(f"VERIFY FAILED run_id={run_id}; see {DB}.{LOG_TABLE}")
    log(f"done run_id={run_id}")


if __name__ == "__main__":
    main()
