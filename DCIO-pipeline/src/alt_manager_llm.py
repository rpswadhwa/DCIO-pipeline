"""LLM manager lookup for alternatives rows the regex/brand passes leave without
a matched_manager_name.

Name-keyed cache: default.alt_manager_llm_lookup holds one Gemini answer per
lower(trim(raw_entity_name)). A name is looked up at most once (any status), so
re-routing a plan or seeing the same fund in another plan's filing costs
nothing. Two consumers read status='ACCEPTED' rows:
  * _route_alternatives_chunk joins the cache in its final SELECT, so a
    DELETE+INSERT re-route keeps LLM-found managers instead of wiping them;
  * apply_lookup() MERGEs the cache into rows that are already routed (used
    right after new lookups, and for whole-table backfills).

Gemini's own confidence label is uncalibrated (289/291 HIGH on the Mayo
prototype, 2026-10-12), so the gate is evidence-based: an answer whose evidence
is a deduction ("likely", "indicates", "prefix") or that Gemini itself did not
rate HIGH is stored as REVIEW and never written to the alternatives table until
someone flips it to ACCEPTED.

Statuses: ACCEPTED / REVIEW / NO_MATCH (Gemini), NOT_DISCLOSED (reviewer: generic
label, no external manager), SUPERSEDED (older row replaced by a reviewer decision).

CLI (backfill):  python -m src.alt_manager_llm [--max N] [--ack-ids a,b,...]
CLI (sweep):     python -m src.alt_manager_llm --apply-only
CLI (review):    python -m src.alt_manager_llm --import-review decisions.csv [--dry-run]
"""
import json
import logging
import os
import re
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

LOOKUP_TABLE = "alt_manager_llm_lookup"
METHOD = "llm_lookup_v1"
MATCH_CONFIDENCE = "MEDIUM"
MODEL = os.getenv("ALT_MANAGER_LLM_MODEL", "gemini-2.5-flash")
URL = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent"

PROMPT = """You identify the investment manager (the asset management firm that sponsors/manages the fund) of a holding listed on a US retirement plan's Form 5500 Schedule H line 4i schedule of assets.

Plan: {plan}
Classified as: {section}
Holding name exactly as printed (may be truncated, abbreviated, or carry share-class/series/feeder suffixes): "{name}"

Use Google Search to verify. Rules:
- Return the management FIRM (e.g. "Elliott Investment Management", "Oaktree Capital Management", "Blackstone"), not the fund name.
- If the firm is a well-known subsidiary/affiliate of a larger firm, also give the parent (e.g. GSO -> Blackstone); otherwise parent = null.
- If you cannot identify the manager with real evidence, return manager = null. Do NOT guess from a generic word. Generic category labels ("Partnership interests", "Real estate funds") have no manager: return null.
- confidence: HIGH = the fund name is unambiguously tied to the firm by sources; MEDIUM = likely but some ambiguity (abbreviation, several similarly named funds); LOW = weak/indirect evidence.

Reply with ONLY a JSON object, no prose, no code fence:
{{"manager": string|null, "parent": string|null, "confidence": "HIGH"|"MEDIUM"|"LOW", "evidence": "one sentence"}}"""

INFERRED = re.compile(r"(?i)\b(indicat\w*|likely|suggest\w*|could|may be|might|prefix|presumabl\w*|appears? to)\b")

# Words that vary between spellings of one firm ("OrbiMed" / "OrbiMed Advisors
# LLC"); stripped only to build a comparison key, never from stored names.
_SUFFIX = re.compile(r"\b(l\.?l\.?c|l\.?p|inc|ltd|limited|corp(oration)?|co|plc|group|holdings?|management|"
                     r"capital|advisors?|advisers|partners|investments?|investors|asset|global|"
                     r"international|company|the|sa|ag|gmbh|lp)\b\.?")


def _key(s: Optional[str]) -> str:
    s = (s or "").lower().replace("&", " and ")
    s = _SUFFIX.sub(" ", s)
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def _q(s) -> str:
    return "NULL" if s is None else "'" + str(s).replace("'", "''") + "'"


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


def _ids_clause(ack_ids: Optional[List[str]], col: str) -> str:
    if not ack_ids:
        return ""
    return f" AND {col} IN (" + ", ".join(_q(a) for a in ack_ids) + ")"


def _ask(key: str, name: str, section: str, plan: str) -> dict:
    body = {
        "contents": [{"parts": [{"text": PROMPT.format(name=name, section=section or "alternative investment",
                                                       plan=plan or "US retirement plan")}]}],
        "tools": [{"google_search": {}}],
        "generationConfig": {"temperature": 0},
    }
    err = ""
    for attempt in range(4):
        try:
            r = requests.post(URL, params={"key": key}, json=body, timeout=120)
            if r.status_code in (429, 500, 503):
                time.sleep(5 * (attempt + 1))
                continue
            r.raise_for_status()
            cand = r.json()["candidates"][0]
            text = "".join(p.get("text", "") for p in cand.get("content", {}).get("parts", []))
            m = re.search(r"\{.*\}", text, re.S)
            ans = json.loads(m.group(0)) if m else {}
            chunks = cand.get("groundingMetadata", {}).get("groundingChunks", []) or []
            ans["sources"] = [c.get("web", {}).get("title") for c in chunks if c.get("web", {}).get("title")][:5]
            return ans
        except Exception as exc:  # noqa: BLE001 - one bad name must not stop the batch
            err = str(exc)
            time.sleep(5 * (attempt + 1))
    return {"error": err[:200]}


def _vocab(glue_db: str, target_table: str) -> Dict[str, str]:
    """Comparison key -> preferred spelling: names already in the alternatives
    table win, then manager_canonical, then earlier accepted LLM answers."""
    vk: Dict[str, str] = {}
    for sql in (
        f"SELECT DISTINCT matched_manager_name AS n FROM {glue_db}.{target_table} "
        f"WHERE matched_manager_name IS NOT NULL AND (manager_match_method IS NULL OR manager_match_method <> '{METHOD}')",
        f"SELECT DISTINCT canonical_manager AS n FROM {glue_db}.manager_canonical WHERE canonical_manager IS NOT NULL",
        f"SELECT DISTINCT manager AS n FROM {glue_db}.{LOOKUP_TABLE} WHERE status = 'ACCEPTED' AND manager IS NOT NULL",
    ):
        for n in _read(sql, glue_db)["n"].dropna().tolist():
            k = _key(n)
            if k:
                vk.setdefault(k, str(n))
    return vk


def lookup_new_names(glue_db: str, target_table: str, ack_ids: Optional[List[str]] = None,
                     max_names: Optional[int] = None) -> int:
    """Ask Gemini about managerless alternatives names not yet in the cache and
    store the answers. Returns the number of names looked up."""
    key = os.getenv("GEMINI_API_KEY")
    if not key:
        logger.warning("alt manager LLM lookup skipped: GEMINI_API_KEY not set")
        return 0
    max_names = max_names or int(os.getenv("ALT_MANAGER_LLM_MAX", "500"))
    df = _read(f"""
        SELECT lower(trim(h.raw_entity_name)) AS k, arbitrary(h.raw_entity_name) AS name,
               arbitrary(h.asset_sub_class) AS section, arbitrary(pm.plan_name_clean) AS plan,
               sum(h.plan_investment_amt) AS amt
        FROM {glue_db}.{target_table} h
        LEFT JOIN {glue_db}.plan_master pm ON pm.ack_id = h.ack_id
        WHERE h.matched_manager_name IS NULL
          AND h.raw_entity_name IS NOT NULL AND trim(h.raw_entity_name) <> ''{_ids_clause(ack_ids, 'h.ack_id')}
          AND NOT EXISTS (SELECT 1 FROM {glue_db}.{LOOKUP_TABLE} l WHERE l.name_key = lower(trim(h.raw_entity_name)))
        GROUP BY 1
        ORDER BY 5 DESC NULLS LAST
        LIMIT {int(max_names)}""", glue_db)
    if df.empty:
        logger.info("alt manager LLM lookup: no new names")
        return 0
    rows = df.to_dict("records")
    workers = int(os.getenv("ALT_MANAGER_LLM_WORKERS", "4"))
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        answers = list(ex.map(lambda r: _ask(key, r["name"], r["section"], r["plan"]), rows))

    vk = _vocab(glue_db, target_table)
    out = []
    for r, a in zip(rows, answers):
        if "error" in a:
            continue  # not stored: retried on the next run
        mgr = (a.get("manager") or "").strip() or None
        parent = (a.get("parent") or "").strip() or None
        conf = str(a.get("confidence") or "LOW").upper()
        ev = (a.get("evidence") or "").strip()
        inferred = bool(mgr) and (conf != "HIGH" or bool(INFERRED.search(ev)))
        status = "NO_MATCH" if not mgr else ("REVIEW" if inferred else "ACCEPTED")
        out.append({"k": r["k"], "name": r["name"], "mgr": mgr, "parent": parent, "conf": conf,
                    "inferred": inferred, "status": status, "ev": ev, "src": json.dumps(a.get("sources") or [])})

    # Spelling: an existing name with the same key wins; otherwise collapse this
    # batch's variants to the most common, then shortest, spelling.
    groups = defaultdict(list)
    for o in out:
        if o["mgr"]:
            groups[_key(o["mgr"]) or o["mgr"].lower()].append(o)
    for k, os_ in groups.items():
        cnt = Counter(o["mgr"] for o in os_)
        pick = vk.get(k) or sorted(cnt, key=lambda s: (-cnt[s], len(s)))[0]
        for o in os_:
            o["mgr"] = pick
    for o in out:
        if o["parent"]:
            o["parent"] = vk.get(_key(o["parent"]), o["parent"])

    for i in range(0, len(out), 100):
        vals = ", ".join(
            f"({_q(o['k'])}, {_q(o['name'])}, {_q(o['mgr'])}, {_q(o['parent'])}, {_q(o['conf'])}, "
            f"{'true' if o['inferred'] else 'false'}, {_q(o['status'])}, {_q(o['ev'])}, {_q(o['src'])})"
            for o in out[i:i + 100])
        _exec(f"""
            INSERT INTO {glue_db}.{LOOKUP_TABLE}
            SELECT name_key, raw_entity_name, manager, parent, llm_confidence, '{MATCH_CONFIDENCE}',
                   inferred, status, evidence, sources, '{MODEL}', '{METHOD}', CAST(current_timestamp AS timestamp(6))
            FROM (VALUES {vals}) AS v(name_key, raw_entity_name, manager, parent, llm_confidence,
                                      inferred, status, evidence, sources)""", glue_db)
    by = Counter(o["status"] for o in out)
    logger.info("alt manager LLM lookup: %d names in %.0fs -> %s (%d errors, retried next run)",
                len(rows), time.time() - t0, dict(by), len(rows) - len(out))
    return len(out)


def apply_lookup(glue_db: str, target_table: str, ack_ids: Optional[List[str]] = None) -> None:
    """Fill managerless alternatives rows from ACCEPTED cache answers. Never
    overwrites a manager a regex/brand/override pass already set."""
    _exec(f"""
        MERGE INTO {glue_db}.{target_table} t
        USING (
            SELECT name_key, manager, match_confidence, method FROM (
                SELECT name_key, manager, match_confidence, method,
                       row_number() OVER (PARTITION BY name_key ORDER BY looked_up_at DESC) AS rn
                FROM {glue_db}.{LOOKUP_TABLE} WHERE status = 'ACCEPTED' AND manager IS NOT NULL
            ) WHERE rn = 1
        ) l
        ON lower(trim(t.raw_entity_name)) = l.name_key AND t.matched_manager_name IS NULL{_ids_clause(ack_ids, 't.ack_id')}
        WHEN MATCHED THEN UPDATE SET
            matched_manager_name = l.manager,
            manager_match_confidence = l.match_confidence,
            manager_match_method = l.method""", glue_db)


REVIEW_METHOD = "human_review_v1"
REVIEW_STATUS = {"ACCEPT": "ACCEPTED", "CHANGE": "ACCEPTED", "NO MANAGER": "NOT_DISCLOSED"}


def import_review(glue_db: str, path: str, dry_run: bool = False,
                  target_table: str = "plan_alternatives_history") -> Counter:
    """Record reviewer decisions for cached names (2026-10-27).

    `path` is a CSV/XLSX with columns name_key, decision (ACCEPT | CHANGE |
    NO MANAGER; anything else, e.g. HOLD, is skipped and the name stays REVIEW),
    manager (required for CHANGE; ACCEPT keeps the cached answer unless one is
    given), parent (optional) and note (optional, stored as evidence).

    Each decided name gets a new human_review_v1 row; every older row for that
    name is set to SUPERSEDED so both cache consumers (apply_lookup and the
    router join, which take the latest ACCEPTED row) see only the decision.
    NOT_DISCLOSED = the filing gives a generic label with no external manager;
    nothing is written to the holdings table and the name is never re-asked."""
    import pandas as pd
    df = pd.read_excel(path) if path.lower().endswith((".xlsx", ".xls")) else pd.read_csv(path)
    df = df.astype(object).where(df.notna(), None)
    cur = _read(f"""
        SELECT name_key, raw_entity_name, manager, parent FROM (
            SELECT *, row_number() OVER (PARTITION BY name_key ORDER BY looked_up_at DESC) AS rn
            FROM {glue_db}.{LOOKUP_TABLE}) WHERE rn = 1""", glue_db)
    cur = cur.astype(object).where(cur.notna(), None).set_index("name_key").to_dict("index")
    out, by = [], Counter()
    for r in df.to_dict("records"):
        dec = str(r.get("decision") or "").strip().upper()
        k = str(r.get("name_key") or "").strip()
        if dec not in REVIEW_STATUS:
            by["skipped"] += 1
            continue
        if k not in cur:
            raise ValueError(f"name_key not in cache: {k!r}")
        c = cur[k]
        mgr = (str(r.get("manager") or "").strip() or None) if dec != "NO MANAGER" else None
        if dec == "ACCEPT":
            mgr = mgr or c["manager"]
        if dec != "NO MANAGER" and not mgr:
            raise ValueError(f"{dec} without a manager: {k!r}")
        parent = None if dec == "NO MANAGER" else (str(r.get("parent") or "").strip() or
                                                    (c["parent"] if dec == "ACCEPT" else None))
        out.append((k, c["raw_entity_name"], mgr, parent, REVIEW_STATUS[dec],
                    str(r.get("note") or "").strip() or f"reviewer decision: {dec}"))
        by[REVIEW_STATUS[dec]] += 1
    logger.info("review import %s: %s", path, dict(by))
    if dry_run or not out:
        return by
    for i in range(0, len(out), 100):
        part = out[i:i + 100]
        keys = ", ".join(_q(o[0]) for o in part)
        _exec(f"UPDATE {glue_db}.{LOOKUP_TABLE} SET status = 'SUPERSEDED' WHERE name_key IN ({keys})", glue_db)
        vals = ", ".join(f"({_q(o[0])}, {_q(o[1])}, {_q(o[2])}, {_q(o[3])}, {_q(o[4])}, {_q(o[5])})" for o in part)
        _exec(f"""
            INSERT INTO {glue_db}.{LOOKUP_TABLE}
            SELECT name_key, raw_entity_name, manager, parent, 'HUMAN', 'HIGH', false, status, evidence, '[]',
                   'human', '{REVIEW_METHOD}', CAST(current_timestamp AS timestamp(6))
            FROM (VALUES {vals}) AS v(name_key, raw_entity_name, manager, parent, status, evidence)""", glue_db)
        # A decision replaces an earlier cache answer already written to the holdings
        # table: clear only cache-sourced managers so apply_lookup re-fills them.
        _exec(f"""
            UPDATE {glue_db}.{target_table}
            SET matched_manager_name = NULL, manager_match_confidence = NULL, manager_match_method = NULL,
                manager_parent_name = NULL
            WHERE manager_match_method IN ('{METHOD}', '{REVIEW_METHOD}', 'llm_gemini_2.5_flash_v1')
              AND lower(trim(raw_entity_name)) IN ({keys})""", glue_db)
    return by


def fill_managers(glue_db: str, target_table: str, ack_ids: Optional[List[str]] = None,
                  max_names: Optional[int] = None) -> None:
    lookup_new_names(glue_db, target_table, ack_ids, max_names)
    apply_lookup(glue_db, target_table, ack_ids)


if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--glue-db", default="default")
    ap.add_argument("--table", default="plan_alternatives_history")
    ap.add_argument("--ack-ids", default="")
    ap.add_argument("--max", type=int, default=None)
    ap.add_argument("--apply-only", action="store_true")
    ap.add_argument("--import-review", default="", help="CSV/XLSX of reviewer decisions (see import_review)")
    ap.add_argument("--dry-run", action="store_true", help="with --import-review: count only, write nothing")
    a = ap.parse_args()
    ids = [x for x in a.ack_ids.split(",") if x] or None
    if a.import_review:
        import_review(a.glue_db, a.import_review, a.dry_run, a.table)
        if a.dry_run:
            raise SystemExit(0)
    elif not a.apply_only:
        lookup_new_names(a.glue_db, a.table, ids, a.max)
    apply_lookup(a.glue_db, a.table, ids)
