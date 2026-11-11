"""Sync per-plan notes between the Undercapture Desk and the pipeline (2026-11-10).

The Desk keeps notes in its own database (collection `plan_notes`, one doc per plan_id) and shows
"What got loaded" from collection `loaded_reports` (one doc per ack_id). Only a Claude session can
read/write that database, so the sync is two local steps around it:

  export  -- Desk notes dumped to a folder (ArtifactData list with out_dir) -> plan_notes.json on S3.
             Only the last *confirmed* rules are exported; a draft never reaches the pipeline.
  loaded  -- per-filing note reports on S3 + what actually landed in the history tables ->
             one JSON file per ack_id, written back to the Desk with ArtifactData batch/file_path.

Usage:
  python tools/plan_notes_sync.py export --notes-dir <dir> [--dry-run]
  python tools/plan_notes_sync.py loaded --out <dir> [--ack <ack_id> ...]

Matching report rows to table rows by ack + amount below is for display only. It never decides
what loads.
"""
import argparse
import glob
import json
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.plan_notes import DEFAULT_NOTES_URI, DEFAULT_REPORT_URI  # noqa: E402

WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")
S3_OUTPUT = os.getenv("ATHENA_STAGING_S3")

# Where each kind of row ends up, and the Desk label for it.
DEST_TABLES = [
    ("plan_mf_history_v3", "Mutual funds"),
    ("plan_alternatives_history", "Alternatives"),
    ("plan_insurance_history", "Stable value (insurance)"),
    ("plan_cit_history", "Stable value / CIT"),
    ("plan_separately_managed_history", "Stable value (SMA)"),
    ("plan_stable_value_unresolved_history", "Stable value (unresolved)"),
]
REVIEW_TOLERANCE = 0.02  # same 2% the undercapture view uses for "resolved"


def _s3():
    import boto3
    return boto3.client("s3")


def _split(uri):
    bucket, key = uri[5:].split("/", 1)
    return bucket, key


def _athena(sql):
    import awswrangler as wr
    return wr.athena.read_sql_query(sql=sql, database="default", workgroup=WORKGROUP,
                                    s3_output=S3_OUTPUT, ctas_approach=False)


def _amount(value):
    try:
        return round(float(str(value).replace(",", "").replace("$", "").strip()), 2)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------- export

def _desk_doc(path):
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    # ArtifactData may wrap the fields; accept either shape.
    return doc.get("data") if isinstance(doc.get("data"), dict) else doc


def export_notes(notes_dir, dry_run=False):
    files = sorted(glob.glob(os.path.join(notes_dir, "**", "*.json"), recursive=True))
    out = {}
    for path in files:
        doc = _desk_doc(path)
        pid = str(doc.get("plan_id") or "").strip()
        if not pid or not doc.get("rules") or not doc.get("version"):
            continue  # never confirmed
        out[pid] = {"plan_name": doc.get("plan_name", ""), "version": int(doc["version"]),
                    "note_text": doc.get("note_text", ""), "rules": doc["rules"]}
    print(f"{len(out)} confirmed note(s) from {len(files)} Desk doc(s)")
    for pid, n in sorted(out.items()):
        print(f"  {pid} v{n['version']}  {n['plan_name']}")
    if dry_run:
        return out
    uri = os.getenv("PLAN_NOTES_URI", DEFAULT_NOTES_URI)
    bucket, key = _split(uri)
    s3 = _s3()
    try:  # keep the previous file next to it, so a bad export is one copy away from undone
        s3.copy_object(Bucket=bucket, Key=key + ".prev", CopySource={"Bucket": bucket, "Key": key})
    except Exception:  # noqa: BLE001 - first export has nothing to keep
        pass
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(out, indent=1).encode("utf-8"),
                  ContentType="application/json")
    print(f"wrote {uri}")
    return out


# ---------------------------------------------------------------- loaded

def _reports(acks=None):
    uri = os.getenv("PLAN_NOTES_REPORT_URI", DEFAULT_REPORT_URI)
    bucket, prefix = _split(uri)
    s3 = _s3()
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".json")]
    if acks:
        keys = [k for k in keys if os.path.basename(k)[:-5] in set(acks)]
    for key in keys:
        yield json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())


def _landed(acks):
    """ack -> list of (label, name, amount) for every row in the destination tables."""
    ack_list = ", ".join(f"'{a}'" for a in acks)
    landed = defaultdict(list)
    for table, label in DEST_TABLES:
        try:
            df = _athena(f"SELECT ack_id, raw_entity_name, CAST(plan_investment_amt AS double) amt "
                         f"FROM {table} WHERE ack_id IN ({ack_list})")
        except Exception as exc:  # noqa: BLE001 - one missing table must not hide the others
            print(f"  [warn] {table}: {exc}")
            continue
        for a, n, amt in zip(df["ack_id"], df["raw_entity_name"], df["amt"]):
            landed[a].append((label, n, round(float(amt or 0), 2)))
    return landed


def _mf_certified(acks):
    ack_list = ", ".join(f"'{a}'" for a in acks)
    df = _athena(
        "SELECT ack_id, reference_amt_mutual_funds FROM ("
        " SELECT ack_id, reference_amt_mutual_funds,"
        " ROW_NUMBER() OVER (PARTITION BY ack_id ORDER BY run_ts DESC) rn"
        f" FROM plan_mf_history_validation_summary WHERE ack_id IN ({ack_list})) WHERE rn = 1")
    return {a: (float(v) if v == v and v is not None else None)
            for a, v in zip(df["ack_id"], df["reference_amt_mutual_funds"])}


def build_loaded(out_dir, acks=None):
    reports = list(_reports(acks))
    if not reports:
        print("no note reports on S3 yet")
        return []
    all_acks = sorted({r["ack_id"] for r in reports})
    landed = _landed(all_acks)
    mf_cert = _mf_certified(all_acks)
    os.makedirs(out_dir, exist_ok=True)
    written = []
    for rep in reports:
        ack = rep["ack_id"]
        pool = list(landed.get(ack, []))  # each table row is claimed by at most one report row
        rows = []
        for r in rep.get("rows", []):
            amt = _amount(r.get("amount", r.get("amount_printed")))
            dest = None
            if r.get("action") != "skipped" and amt is not None:
                hit = next((i for i, (_, _, a) in enumerate(pool) if abs(a - amt) <= 1.0), None)
                if hit is not None:
                    dest = pool.pop(hit)[0]
            rows.append({"page": r.get("page"), "name": (r.get("name") or "")[:200],
                         "heading": (r.get("heading") or "")[:120], "amount": r.get("amount", ""),
                         "amount_num": amt, "type": r.get("type") or r.get("extractor_type") or "",
                         "rule": r.get("rule", ""), "action": r.get("action", ""),
                         "destination": dest, "flagged": r.get("action") != "skipped" and dest is None})
        totals = defaultdict(float)
        for label, _, a in landed.get(ack, []):
            totals[label] += a
        by_type = [{"type": table, "label": label, "loaded": round(totals[label], 2),
                    "certified": mf_cert.get(ack) if label == "Mutual funds" else None}
                   for table, label in DEST_TABLES if totals.get(label)]
        if "Mutual funds" not in totals and mf_cert.get(ack):
            by_type.insert(0, {"type": DEST_TABLES[0][0], "label": "Mutual funds", "loaded": 0.0,
                               "certified": mf_cert[ack]})
        status = None
        cert = mf_cert.get(ack)
        if cert:
            status = "ok" if abs(totals.get("Mutual funds", 0) - cert) <= REVIEW_TOLERANCE * cert else "needs_review"
        doc = {"ack_id": ack, "plan_id": rep.get("plan_id"), "plan_name": rep.get("plan_name"),
               "note_version": rep.get("note_version"), "pages": rep.get("pages", []),
               "page_source": rep.get("page_source"), "written_at": rep.get("written_at"),
               "by_type": by_type, "rows": rows[:600], "status": status}
        path = os.path.join(out_dir, f"{ack}.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(doc, fh)
        written.append((ack, rep.get("plan_id"), status, path))
        print(f"  {ack} {rep.get('plan_id')} rows={len(rows)} status={status} -> {path}")
    return written


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("export")
    e.add_argument("--notes-dir", required=True)
    e.add_argument("--dry-run", action="store_true")
    l_ = sub.add_parser("loaded")
    l_.add_argument("--out", required=True)
    l_.add_argument("--ack", nargs="*")
    args = ap.parse_args()
    if args.cmd == "export":
        export_notes(args.notes_dir, args.dry_run)
    else:
        build_loaded(args.out, args.ack)


if __name__ == "__main__":
    main()
