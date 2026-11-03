"""Chunked batch extraction support for run.sh (2026-11-02).

A full DOL release can be 60k+ PDFs (2026-11-02: 60,658 PDFs, ~79 GB). run.sh used to sync the
whole batch onto the box and run src.run_pipeline once over all of it. That cannot work at that size:
  - the root volume has ~7 GB free, so the sync alone fills the disk;
  - run_pipeline holds every PDF's rows in memory and writes nothing until the end, so the 48h SSM
    timeout (or an OOM) loses the entire run;
  - the 5500-extraction Step Function gives up polling after 25h.

This module splits a batch into ordered chunks so each chunk is fetched, extracted, written, and
deleted on its own, and a rerun resumes after the last finished chunk.

  plan   -- list the batch's PDFs in S3, look each ack_id up in plan_master, drop excluded plan types
            (Health & Welfare by default), and write ordered chunk manifests:
              priority tier = DC/DB plans with assets >= CHUNK_PRIORITY_MIN_ASSETS, largest first
              rest tier     = everything else kept, largest first
  fetch  -- download one manifest's PDFs into a directory (emptied first).

  python3.11 -m src.batch_chunks plan  --batch-date 2026-11-02 --out data/chunks/2026-11-02
  python3.11 -m src.batch_chunks fetch --batch-date 2026-11-02 --manifest .../p0001.txt --dest data/inputs
"""
import argparse
import os
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor

import awswrangler as wr
import boto3
import pandas as pd

BUCKET = "retirementinsights-bronze"
# The EC2 role cannot create Athena's default results bucket; same results location as apply_firm_parents.
S3_OUTPUT = os.environ.get("ATHENA_RESULTS", "s3://retirementinsights-silver/athena-results/")
WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")
PRIORITY_PLAN_TYPES = {"401(k)", "403(b)", "Other DC Plan", "Other Defined Contribution",
                       "Hybrid (DB/DC)", "Defined Benefit"}


def batch_prefix(batch_date):
    return f"filings_5500_pdf/year={batch_date[:4]}/batch_date={batch_date}/"


def list_batch_acks(batch_date):
    s3 = boto3.client("s3")
    acks = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=batch_prefix(batch_date)):
        for obj in page.get("Contents", []):
            name = obj["Key"].rsplit("/", 1)[-1]
            if name.lower().endswith(".pdf"):
                acks.append(name[:-4])
    return acks


def plan(batch_date, out_dir, chunk_size, min_assets, exclude_types):
    acks = list_batch_acks(batch_date)
    print(f"[chunks] {len(acks)} PDFs in s3://{BUCKET}/{batch_prefix(batch_date)}")
    if not acks:
        return 0
    info = wr.athena.read_sql_query(
        "SELECT CAST(ack_id AS varchar) ack_id, plan_type, TRY_CAST(assets AS double) assets "
        "FROM default.plan_master WHERE ack_id IS NOT NULL",
        database="default", ctas_approach=False, s3_output=S3_OUTPUT, workgroup=WORKGROUP,
    ).drop_duplicates("ack_id").set_index("ack_id")
    rows, excluded = [], 0
    for ack in acks:
        ptype = info["plan_type"].get(ack) if ack in info.index else None
        assets = info["assets"].get(ack) if ack in info.index else None
        assets = 0.0 if pd.isna(assets) else float(assets)
        if ptype in exclude_types:
            excluded += 1
            continue
        tier = "p" if ptype in PRIORITY_PLAN_TYPES and assets >= min_assets else "r"
        rows.append((tier, -assets, ack))
    rows.sort()

    os.makedirs(out_dir, exist_ok=True)
    for old in os.listdir(out_dir):
        if old.endswith(".txt") and old[0] in "pr" and old[1:5].isdigit():
            os.remove(os.path.join(out_dir, old))
    counts = {}
    for tier in ("p", "r"):
        tier_acks = [a for t, _, a in rows if t == tier]
        for i in range(0, len(tier_acks), chunk_size):
            n = i // chunk_size + 1
            with open(os.path.join(out_dir, f"{tier}{n:04d}.txt"), "w") as fh:
                fh.write("\n".join(tier_acks[i:i + chunk_size]) + "\n")
        counts[tier] = len(tier_acks)
    print(f"[chunks] priority={counts['p']} rest={counts['r']} excluded({','.join(sorted(exclude_types))})={excluded} "
          f"chunk_size={chunk_size} -> {out_dir}")
    return 0


def fetch(batch_date, manifest, dest):
    with open(manifest) as fh:
        acks = [line.strip() for line in fh if line.strip()]
    shutil.rmtree(dest, ignore_errors=True)
    os.makedirs(dest, exist_ok=True)
    s3 = boto3.client("s3")
    prefix = batch_prefix(batch_date)

    def get(ack):
        try:
            s3.download_file(BUCKET, f"{prefix}{ack}.pdf", os.path.join(dest, f"{ack}.pdf"))
            return True
        except Exception as exc:
            print(f"[chunks] fetch failed {ack}: {exc}", file=sys.stderr)
            return False

    with ThreadPoolExecutor(max_workers=16) as pool:
        ok = sum(pool.map(get, acks))
    print(f"[chunks] fetched {ok}/{len(acks)} PDFs from {os.path.basename(manifest)} -> {dest}")
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--batch-date", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--chunk-size", type=int, default=int(os.getenv("CHUNK_SIZE", "500")))
    p.add_argument("--min-assets", type=float, default=float(os.getenv("CHUNK_PRIORITY_MIN_ASSETS", "100000000")))
    p.add_argument("--exclude-types", default=os.getenv("CHUNK_EXCLUDE_PLAN_TYPES", "Health & Welfare"))
    f = sub.add_parser("fetch")
    f.add_argument("--batch-date", required=True)
    f.add_argument("--manifest", required=True)
    f.add_argument("--dest", required=True)
    a = ap.parse_args()
    if a.cmd == "plan":
        excl = {t.strip() for t in a.exclude_types.split(",") if t.strip()}
        return plan(a.batch_date, a.out, a.chunk_size, a.min_assets, excl)
    return fetch(a.batch_date, a.manifest, a.dest)


if __name__ == "__main__":
    sys.exit(main())
