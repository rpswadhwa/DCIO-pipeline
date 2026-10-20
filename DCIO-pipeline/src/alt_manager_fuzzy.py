"""Fuzzy manager labels for alternatives rows that every exact pass left without
a manager (added 2026-10-20).

apply_alt_provider_ids only joins the ALT: aliases on the exact cleaned raw
name, so a filing's spelling variant ("ELLIOTT INTL LTD - CLASS C" vs the
alias "ELLIOTT INTERNATIONAL LIMITED") stays managerless: 10,784 rows / $140.7B
had no manager on 2026-10-19. This pass matches those names against the same
approved aliases in Python and MERGEs the answers back by cleaned raw name:

  T0 alias_exact_v1 (HIGH)   - the cleaned name is an alt_exact alias.
  T1 alias_norm_v1  (MEDIUM) - the name equals an alt_exact alias after
                               abbreviation expansion, noise-word removal and
                               doubled-letter folding, and that alias set
                               names exactly one manager.
  T2 alias_core_v1  (LOW)    - the name contains a manager's brand core (its
                               label / family key minus generic words). A
                               single-word core must open the name; cores in
                               CORE_BLOCK are never used; CORE_EXCLUDE vetoes
                               known lookalikes.

It never overwrites a manager or provider_id another pass set. stamp_family_ids
also gives provider_id to rows whose manager label (regex/LLM) is an
alt_family alias. The deployed router (HEAD e531f765) had no alias pass at all,
so re-routed rows lost their provider_id; T0 and the family stamp repeat the
uncommitted apply_alt_provider_ids work and are idempotent with it. The cores and
rules were reviewed against all 400 cores by $ on 2026-10-19 (scratchpad
mgr/core_rev.tsv).

CLI:  python -m src.alt_manager_fuzzy [--ack-ids a,b] [--dry-run out.tsv]
      python -m src.alt_manager_fuzzy --offline rows.tsv aliases.tsv labels.tsv out.tsv
"""
import csv
import logging
import os
import re
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

METHOD_EXACT = "alias_exact_v1"
METHOD_NORM = "alias_norm_v1"
METHOD_CORE = "alias_core_v1"
_CHUNK = 400  # VALUES rows per MERGE; well under Athena's 262144-char query cap

ABBR = {
    "INTL": "INTERNATIONAL", "INTERNATION": "INTERNATIONAL", "LTD": "LIMITED", "FD": "FUND", "FDS": "FUNDS",
    "PTNRS": "PARTNERS", "PRTNRS": "PARTNERS", "PTRS": "PARTNERS", "PTN": "PARTNERS", "PRTNR": "PARTNERS",
    "MGMT": "MANAGEMENT", "CAP": "CAPITAL", "GBL": "GLOBAL", "OPP": "OPPORTUNITIES", "OPPS": "OPPORTUNITIES",
    "RE": "REAL ESTATE", "INV": "INVESTORS", "INVS": "INVESTORS",
}
NOISE = set("LP L P LLC LIMITED PARTNERSHIP PARTNERSHIPS INTEREST INTERESTS THE CLASS CL SHARES UNITS INC CO "
            "COMPANY FUND FUNDS".split())
GEN = set("CAPITAL MANAGEMENT ASSET ASSETS INVESTMENT INVESTMENTS ADVISORS ADVISERS ADVISORY PARTNERS GROUP CO "
          "COMPANY LLC LP INC LIMITED LTD HOLDINGS THE AND OF GLOBAL CORPORATION CORP TRUST FUND FUNDS LLP".split())
STOP = set("""NORTH SOUTH EAST WEST AMERICAN AMERICA NATIONAL FIRST STATE PRIVATE CREDIT EQUITY REAL ESTATE
 INFRASTRUCTURE VENTURE VENTURES STRATEGIC OPPORTUNITIES INTERNATIONAL CAPITAL INVESTORS PARTNERS GROWTH VALUE
 INCOME HEDGE SPECIAL CORE PLUS SENIOR DEBT LENDING PACIFIC ATLANTIC EUROPE EUROPEAN ASIA ASIAN EMERGING MARKETS
 MARKET SECONDARY SECONDARIES MULTI STRATEGY ABSOLUTE RETURN TOTAL PROPERTY PROPERTIES REALTY LAND ENERGY
 RESOURCES""".split())
# Cores that are ordinary words in fund names ("ALPINVEST STRATEGIC PORTFOLIO",
# "NORTHERN TRUST ...") or that two unrelated firms share (TRIDENT, REDWOOD).
CORE_BLOCK = set("""PORTFOLIO INTERMEDIATE DIVERSIFIED CATALYST FRONTIER FINANCIAL NORTHERN BRIDGE NORDIC APARTMENT
 RESOURCE WASHINGTON BERKSHIRE ABBOTT STERLING MATRIX DURATION TRIDENT SOCIAL FOUNDATION PRINCIPAL FORTUNE
 CENTRE CORNERSTONE GRANITE VECTOR HELIOS ENDEAVOUR METROPOLITAN REDWOOD""".split()) | {"US REAL ESTATE"}
# core -> names it must not label (seen in the 2026-10-19 review)
CORE_EXCLUDE = {
    "MILLENNIUM": r"\bCORP(ORATE)? PARK\b",
    "MONROE": r"\bENGINEERING\b",
    "ARSENAL": r"\bRESOURCES\b",
    "CHATHAM": r"\bCHATHAM HILL\b",
    "SUMMIT": r"\bSOUTHERN RIDGES\b",
    "GRAHAM": r"\bGRAHAM PARTNERS\b",
}
# custodian prefixes before the fund name: "CF ELLIOTT ...", "380990317 GOLDMAN ..."
_PREFIX = re.compile(r"^((CF|HOLDBACK|LIMITED PARTNERSHIPS?|\d+)\s+)+")


def clean(s: Optional[str]) -> str:
    """Python twin of post_extract_validator._alt_clean_key_sql."""
    return re.sub(r"[^A-Z0-9]+", " ", (s or "").upper()).strip()


def norm(s: Optional[str]) -> str:
    toks = " ".join(ABBR.get(w, w) for w in clean(s).split()).split()
    return " ".join(re.sub(r"(.)\1+", r"\1", w) for w in toks if w not in NOISE)  # ELLIOT == ELLIOTT


class Matcher:
    def __init__(self, aliases: List[dict], label_rows: Dict[str, int]):
        """aliases: dicts with mapping_source, k, mgr, provider_id (approved ALT:
        aliases). label_rows: manager label -> alt-table row count, used to pick
        one label when several spellings of a firm share a core."""
        self.pid: Dict[str, str] = {}
        for a in aliases:
            if a["mgr"]:
                self.pid.setdefault(a["mgr"], a["provider_id"])
        self.exact = {clean(a["k"]): a["mgr"] for a in aliases if a["mapping_source"] == "alt_exact" and a["mgr"]}
        self.nexact = defaultdict(set)
        for a in aliases:
            if a["mapping_source"] == "alt_exact" and a["mgr"]:
                self.nexact[norm(a["k"])].add(a["mgr"])
        cores = defaultdict(set)
        for a in aliases:
            if not a["mgr"]:
                continue
            for src in (a["mgr"], a["k"] if a["mapping_source"] == "alt_family" else ""):
                toks = [w for w in clean(src).split() if w not in GEN]
                core = " ".join(toks)
                if not toks or core in CORE_BLOCK or all(w in STOP for w in toks):
                    continue
                if len(toks) == 1 and (len(core) < 6 or core in STOP):
                    continue
                cores[core].add(a["mgr"])
        self.label_rows = label_rows
        self.cores = cores
        self.corere = []
        for c in sorted(cores, key=len, reverse=True):
            # one word, or only initials ("A M"): must open the name
            lead = len(c.split()) == 1 or all(len(w) <= 2 for w in c.split())
            rx = re.compile((r"^" if lead else r"(^| )") + re.escape(c) + r"( |$)")
            ex = re.compile(CORE_EXCLUDE[c]) if c in CORE_EXCLUDE else None
            self.corere.append((c, rx, ex))

    def _one(self, labels: set) -> Optional[str]:
        """Several labels for one core are usually spellings of one firm
        ("Trident Capital" / "Trident Capital Management"): keep them only when
        they reduce to the same core, then take the label used most."""
        if len(labels) == 1:
            return next(iter(labels))
        reduced = {" ".join(w for w in norm(m).split() if w not in GEN) for m in labels}
        if len(reduced) != 1:
            return None
        return max(sorted(labels), key=lambda m: self.label_rows.get(m, 0))

    def match(self, name: str) -> Optional[Tuple[str, str, str, str]]:
        """-> (manager, provider_id, confidence, method) or None."""
        m = self.exact.get(clean(name))
        if m:
            return m, self.pid.get(m), "HIGH", METHOD_EXACT
        n = self.nexact.get(norm(name), set())
        if len(n) == 1:
            m = next(iter(n))
            return m, self.pid.get(m), "MEDIUM", METHOD_NORM
        k = _PREFIX.sub("", clean(name))
        hits = []
        for c, rx, ex in self.corere:
            if rx.search(k) and not (ex and ex.search(k)) and not any(c in h for h in hits):
                hits.append(c)
        if not hits:
            return None
        m = self._one(set().union(*(self.cores[h] for h in hits)))
        if not m:
            return None
        return m, self.pid.get(m), "LOW", METHOD_CORE


def _q(s) -> str:
    return "NULL" if s is None or s == "" else "'" + str(s).replace("'", "''") + "'"


def _athena():
    import awswrangler as wr
    return wr, os.getenv("ATHENA_WORKGROUP", "primary"), os.getenv("ATHENA_STAGING_S3")


def _read(sql: str, glue_db: str):
    wr, wg, s3 = _athena()
    return wr.athena.read_sql_query(sql=sql, database=glue_db, workgroup=wg, s3_output=s3, ctas_approach=False)


def _exec(sql: str, glue_db: str) -> None:
    wr, wg, s3 = _athena()
    qid = wr.athena.start_query_execution(sql=sql, database=glue_db, workgroup=wg, s3_output=s3)
    wr.athena.wait_query(query_execution_id=qid)


_NO_MANAGER = ("(t.matched_manager_name IS NULL OR trim(t.matched_manager_name) = '')"
               " AND (t.provider_id IS NULL OR trim(CAST(t.provider_id AS varchar)) = '')")


def load_matcher(glue_db: str, target_table: str) -> Matcher:
    al = _read(f"""
        SELECT mapping_source, substr(mapping_key, 5) AS k,
               trim(regexp_extract(notes, 'alt_manager=([^;]*)', 1)) AS mgr, provider_id
        FROM {glue_db}.provider_mapping_final
        WHERE mapping_key LIKE 'ALT:%' AND mapping_status = 'approved'""", glue_db)
    lab = _read(f"""
        SELECT matched_manager_name AS m, count(*) AS n FROM {glue_db}.{target_table}
        WHERE matched_manager_name IS NOT NULL GROUP BY 1""", glue_db)
    return Matcher(al.fillna("").to_dict("records"), dict(zip(lab["m"], lab["n"])))


def match_table(glue_db: str, target_table: str, ack_ids: Optional[List[str]] = None,
                matcher: Optional[Matcher] = None) -> List[tuple]:
    """-> [(cleaned raw name, manager, provider_id, confidence, method, rows, amt)]
    for managerless rows of ack_ids (whole table when None)."""
    ids = ""
    if ack_ids:
        ids = " AND t.ack_id IN (" + ", ".join(_q(a) for a in ack_ids) + ")"
    df = _read(f"""
        SELECT t.raw_entity_name AS n, count(*) AS rows_, sum(CAST(t.plan_investment_amt AS double)) AS amt
        FROM {glue_db}.{target_table} t WHERE {_NO_MANAGER}{ids} GROUP BY 1""", glue_db)
    if df.empty:
        return []
    matcher = matcher or load_matcher(glue_db, target_table)
    best: Dict[str, list] = {}
    for n, rows_, amt in zip(df["n"], df["rows_"], df["amt"]):
        hit = matcher.match(n or "")
        if hit:
            key = clean(n)
            cur = best.setdefault(key, [key, *hit, 0, 0.0])
            cur[5] += int(rows_)
            cur[6] += float(amt or 0)
    return [tuple(v) for v in best.values()]


def apply_fuzzy_managers(glue_db: str, target_table: str, ack_ids: Optional[List[str]] = None,
                         matcher: Optional[Matcher] = None) -> int:
    """MERGE match_table answers into managerless rows (keyed on the cleaned raw
    name, same as the alias_exact pass). Returns names written."""
    hits = match_table(glue_db, target_table, ack_ids, matcher)
    ids = ""
    if ack_ids:
        ids = " AND t.ack_id IN (" + ", ".join(_q(a) for a in ack_ids) + ")"
    for i in range(0, len(hits), _CHUNK):
        vals = ",\n".join(f"({_q(h[0])}, {_q(h[1])}, {_q(h[2])}, {_q(h[3])}, {_q(h[4])})"
                          for h in hits[i:i + _CHUNK])
        _exec(f"""
            MERGE INTO {glue_db}.{target_table} t
            USING (SELECT * FROM (VALUES {vals}) AS v(akey, mgr, pid, conf, method)) v
            ON v.akey = trim(regexp_replace(upper(coalesce(t.raw_entity_name, '')), '[^A-Z0-9]+', ' '))
               AND {_NO_MANAGER}{ids}
            WHEN MATCHED THEN UPDATE SET
                matched_manager_name = v.mgr,
                provider_id = v.pid,
                manager_match_confidence = v.conf,
                manager_match_method = v.method""", glue_db)
    logger.info("Fuzzy alt manager labels: %d names for %s acks", len(hits),
                "all" if ack_ids is None else len(ack_ids))
    return len(hits)


def stamp_family_ids(glue_db: str, target_table: str, ack_ids: Optional[List[str]] = None) -> None:
    """provider_id for rows that have a manager label but none yet, from the
    alt_family alias of that label (same join as apply_alt_provider_ids)."""
    ids = ""
    if ack_ids:
        ids = " AND t.ack_id IN (" + ", ".join(_q(a) for a in ack_ids) + ")"
    _exec(f"""
        MERGE INTO {glue_db}.{target_table} t
        USING (
            SELECT substr(mapping_key, 5) AS akey, max(provider_id) AS provider_id
            FROM {glue_db}.provider_mapping_final
            WHERE mapping_key LIKE 'ALT:%' AND mapping_status = 'approved' AND mapping_source = 'alt_family'
            GROUP BY 1
        ) a
        ON a.akey = trim(regexp_replace(upper(coalesce(t.matched_manager_name, '')), '[^A-Z0-9]+', ' '))
           AND (t.provider_id IS NULL OR trim(CAST(t.provider_id AS varchar)) = ''){ids}
        WHEN MATCHED THEN UPDATE SET provider_id = a.provider_id""", glue_db)


def fill(glue_db: str, target_table: str, ack_ids: Optional[List[str]] = None) -> None:
    """Router entry point: family provider_ids, then fuzzy managers."""
    stamp_family_ids(glue_db, target_table, ack_ids)
    apply_fuzzy_managers(glue_db, target_table, ack_ids)


def _write(hits: List[tuple], path: str) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["akey", "manager", "provider_id", "confidence", "method", "rows", "amt"])
        for h in sorted(hits, key=lambda h: -h[6]):
            w.writerow(h)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--glue-db", default="default")
    ap.add_argument("--table", default="plan_alternatives_history")
    ap.add_argument("--ack-ids", default="")
    ap.add_argument("--dry-run", metavar="OUT_TSV")
    ap.add_argument("--offline", nargs=4, metavar=("ROWS", "ALIASES", "LABELS", "OUT"),
                    help="TSVs: rows(n, amt), aliases(mapping_source, k, mgr, provider_id), labels(m, n)")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if a.offline:
        rd = lambda p: list(csv.DictReader(open(p, encoding="utf-8"), delimiter="\t"))
        mt = Matcher(rd(a.offline[1]), {r["m"]: int(r["n"]) for r in rd(a.offline[2])})
        best: Dict[str, list] = {}
        for r in rd(a.offline[0]):
            hit = mt.match(r["n"])
            if hit:
                cur = best.setdefault(clean(r["n"]), [clean(r["n"]), *hit, 0, 0.0])
                cur[5] += 1
                cur[6] += float(r["amt"] or 0)
        _write([tuple(v) for v in best.values()], a.offline[3])
    else:
        from dotenv import load_dotenv
        load_dotenv()
        ids = [x for x in a.ack_ids.split(",") if x] or None
        if a.dry_run:
            _write(match_table(a.glue_db, a.table, ids), a.dry_run)
        else:
            fill(a.glue_db, a.table, ids)
