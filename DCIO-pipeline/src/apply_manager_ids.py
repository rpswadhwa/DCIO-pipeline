"""(2026-10-28) Fill manager_id (provider_master id) on the holdings tables for the given ack_ids.

Runs the same SQL the 5500-tables machine runs at its end (s3://retirementinsights-reference/sqls/, mirrored in
core-data-platform athena/sql/s3_reference_sqls/): the manager_id_map refresh (table-wide; it only adds lookup
rows for spellings that exactly match one provider_master manager record), then the 0q4a-g fills scoped to this
run's acks. Readers take the firm name from provider_master by id; the filed name columns are never changed.
The step is idempotent, so a rerun only touches rows whose id changed.

    python3.11 -m src.apply_manager_ids --ack-ids A,B        # or --ack-file f / --all
"""
import argparse
import os
import sys

import awswrangler as wr
import boto3

DB = "default"
WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")
S3_OUTPUT = os.environ.get("ATHENA_RESULTS", "s3://retirementinsights-silver/athena-results/")
SQL_BUCKET = "retirementinsights-reference"
MAP_REFRESH_KEY = "sqls/0q4_manager_id_map_refresh.sql"
SQL_KEYS = [
    "sqls/0q4a_manager_id_alt_redirect.sql",
    "sqls/0q4b_manager_id_cit.sql",
    "sqls/0q4c_manager_id_mf.sql",
    "sqls/0q4d_manager_id_sch_a_sv.sql",
    "sqls/0q4e_manager_id_insurance.sql",
    "sqls/0q4f_manager_id_sv_unresolved.sql",
    "sqls/0q4g_manager_id_separately_managed.sql",
]
SCOPE_T = "1=1 /*SCOPE_T*/"
ACK_CHUNK = 300


def log(msg):
    print(f"[apply_manager_ids] {msg}", flush=True)


def load_acks(args):
    if args.all:
        return None
    acks = []
    if args.ack_ids:
        acks += args.ack_ids.split(",")
    if args.ack_file:
        acks += open(args.ack_file).read().split()
    acks = sorted({a.strip() for a in acks if a.strip()})
    if not acks:
        sys.exit("No ack_ids given (use --ack-ids, --ack-file or --all)")
    return acks


def scoped(sql, chunk):
    if chunk is None:
        return sql
    assert sql.count(SCOPE_T) == 1, "scope token missing from S3 SQL"
    ids = ",".join("'" + a.replace("'", "''") + "'" for a in chunk)
    return sql.replace(SCOPE_T, f"t.ack_id IN ({ids})")


def run(sql, label):
    qid = wr.athena.start_query_execution(sql=sql, database=DB, workgroup=WORKGROUP, s3_output=S3_OUTPUT, wait=False)
    res = wr.athena.wait_query(query_execution_id=qid)
    n = res.get("Statistics", {}).get("DataManipulationQuery", {}).get("RowsUpdated") if isinstance(res, dict) else None
    log(f"ok {label} qid={qid}" + (f" rows={n}" if n is not None else ""))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ack-ids", help="comma-separated ack_ids")
    ap.add_argument("--ack-file", help="file with whitespace-separated ack_ids")
    ap.add_argument("--all", action="store_true", help="whole tables (same as the state machine)")
    args = ap.parse_args()
    acks = load_acks(args)
    chunks = [None] if acks is None else [acks[i:i + ACK_CHUNK] for i in range(0, len(acks), ACK_CHUNK)]
    log(f"scope: {'ALL acks' if acks is None else f'{len(acks)} ack_ids'}")
    s3 = boto3.client("s3")
    get = lambda key: s3.get_object(Bucket=SQL_BUCKET, Key=key)["Body"].read().decode("utf-8")
    run(get(MAP_REFRESH_KEY), MAP_REFRESH_KEY.split("/")[-1])
    for key in SQL_KEYS:
        sql = get(key)
        for ch in chunks:
            run(scoped(sql, ch), key.split("/")[-1])
    log("done")


if __name__ == "__main__":
    main()
