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
  run    -- (2026-11-03) process a tier's chunks end to end; see run_tier().

  python3.11 -m src.batch_chunks plan  --batch-date 2026-11-02 --out data/chunks/2026-11-02
  python3.11 -m src.batch_chunks fetch --batch-date 2026-11-02 --manifest .../p0001.txt --dest data/inputs
  python3.11 -m src.batch_chunks run   --batch-date 2026-11-02 --chunk-dir data/chunks/2026-11-02 --tier priority
"""
import argparse
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

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


# ---------------------------------------------------------------------------------------------
# run (2026-11-03)
#
# The first chunk-mode run (one run_pipeline per 500-filing chunk) was OOM-killed after 2.5h on
# p0001: run_pipeline keeps every page of every filing in memory and writes nothing until the end,
# and some master-trust filings alone peak above 20 GB (Altria, 360 pages). So:
#   - each chunk is split into micro-batches (default 25 filings); each is one run_pipeline process
#     that writes its own results, so a crash loses at most one micro-batch;
#   - a watchdog kills a process whose memory (whole process group) passes its cap, or that runs
#     past its time limit;
#   - micro-batches extract in parallel (default 3 workers, own input/output dirs); the table
#     writes (run_pipeline steps 9-13) take turns via PIPELINE_WRITE_LOCK;
#   - a failed micro-batch is retried one filing at a time, alone on the box, with a bigger cap;
#     a filing that still fails goes to <chunk-dir>/quarantine.tsv for review and is skipped on
#     reruns (delete its line to retry it);
#   - finished filings are recorded in done/acks/<chunk>.txt, so a rerun resumes mid-chunk;
#   - per chunk, classification + cross-table dedup run once over the chunk's finished filings.
# Re-running a filing is safe: the validator deletes and re-inserts by ack_id.
# ---------------------------------------------------------------------------------------------

_print_lock = threading.Lock()


def _log(msg):
    with _print_lock:
        print(f"{datetime.now(timezone.utc):%H:%M:%SZ} {msg}", flush=True)


def _group_rss_bytes(pgid):
    """Resident memory of every live process in process group pgid (Linux /proc)."""
    page = os.sysconf("SC_PAGE_SIZE")
    total = 0
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open(f"/proc/{pid}/stat") as fh:
                fields = fh.read().rsplit(")", 1)[1].split()
            if int(fields[2]) != pgid:  # fields[0]=state, [1]=ppid, [2]=pgrp
                continue
            with open(f"/proc/{pid}/statm") as fh:
                total += int(fh.read().split()[1]) * page
        except (OSError, IndexError, ValueError):
            continue
    return total


def _run_capped(cmd, env, log_path, mem_bytes, timeout_s):
    """Run cmd in its own process group; kill the group past mem_bytes or timeout_s.
    Returns (status, detail): status is ok | failed | oom | timeout."""
    start = time.time()
    peak = 0
    with open(log_path, "ab") as log:
        proc = subprocess.Popen(cmd, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        status = None
        while proc.poll() is None:
            rss = _group_rss_bytes(proc.pid)
            peak = max(peak, rss)
            if rss > mem_bytes:
                status = "oom"
            elif time.time() - start > timeout_s:
                status = "timeout"
            if status:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                proc.wait()
                break
            time.sleep(3)
    detail = f"exit={proc.returncode} peak={peak / 2**30:.1f}G {int(time.time() - start)}s"
    if status:
        return status, detail
    return ("ok" if proc.returncode == 0 else "failed"), detail


def _read_lines(path):
    if not os.path.exists(path):
        return []
    with open(path) as fh:
        return [line.strip() for line in fh if line.strip()]


def _append_lines(path, lines):
    with _print_lock:
        with open(path, "a") as fh:
            fh.writelines(f"{line}\n" for line in lines)


def _quarantined(chunk_dir):
    return {line.split("\t")[0] for line in _read_lines(os.path.join(chunk_dir, "quarantine.tsv"))}


def _drop_non_pdfs(inputs):
    """Remove fetched files that aren't PDFs (e.g. an EFAST HTML error page saved as .pdf);
    return their acks. One of these crashed run_pipeline and failed its whole batch."""
    bad = []
    for fname in sorted(os.listdir(inputs)):
        path = os.path.join(inputs, fname)
        with open(path, "rb") as fh:
            head = fh.read(1024)
        if b"%PDF-" not in head:
            os.remove(path)
            bad.append(fname.rsplit(".", 1)[0])
    return bad


def _pipeline_attempt(batch_date, acks, work_dir, log_path, mem_bytes, timeout_s, write_lock, not_pdf):
    """Fetch acks into work_dir/inputs and run run_pipeline on just them. Acks whose file isn't a
    PDF are left out of the run and appended to not_pdf for the caller to quarantine."""
    inputs = os.path.join(work_dir, "inputs")
    outputs = os.path.join(work_dir, "outputs")
    manifest = os.path.join(work_dir, "acks.txt")
    shutil.rmtree(outputs, ignore_errors=True)
    os.makedirs(outputs, exist_ok=True)
    with open(manifest, "w") as fh:
        fh.write("\n".join(acks) + "\n")
    if fetch(batch_date, manifest, inputs) != 0:
        return "fetch_failed", "no PDFs fetched"
    not_pdf.extend(_drop_non_pdfs(inputs))
    if not os.listdir(inputs):
        shutil.rmtree(inputs, ignore_errors=True)
        return "ok", "no readable PDFs left"
    env = {**os.environ, "INPUT_DIR": inputs, "OUTPUT_DIR": outputs, "SYNC_S3_INPUTS": "0",
           "PIPELINE_WRITE_LOCK": write_lock, "PYTHONUNBUFFERED": "1"}
    cmd = os.environ.get("BATCH_CHUNKS_PIPELINE_CMD", f"{sys.executable} -m src.run_pipeline").split()
    try:
        return _run_capped(cmd, env, log_path, mem_bytes, timeout_s)
    finally:
        shutil.rmtree(inputs, ignore_errors=True)


def _post_chunk(chunk, acks, chunk_dir):
    """Classification + cross-table dedup, scoped to the chunk's finished filings."""
    if not acks:
        return
    ack_file = os.path.join(chunk_dir, "done", "acks", f"{chunk}.txt")
    _log(f"[CHUNK {chunk}] classification ({len(acks)} filings)")
    rc = subprocess.run([sys.executable, "run_classification.py"], env={**os.environ, "ACK_IDS": ",".join(acks)}).returncode
    if rc != 0:
        _log(f"!!! WARNING: [CHUNK {chunk}] classification failed (exit {rc}) -- the end-of-run sweep retries")
    _log(f"[CHUNK {chunk}] cross-table dedup")
    rc = subprocess.run([sys.executable, "-m", "src.dedup_alt_holdings", "--ack-file", ack_file, "--apply"]).returncode
    if rc != 0:
        _log(f"!!! WARNING: [CHUNK {chunk}] dedup_alt_holdings failed (exit {rc}) -- rerun it by hand for {ack_file}")


def _quarantine_not_pdf(chunk, acks, not_pdf, quarantine_path):
    """Quarantine acks whose fetched file wasn't a PDF; return the rest of acks."""
    for ack in not_pdf:
        _append_lines(quarantine_path, [f"{ack}\t{chunk}\tnot_a_pdf\tfetched file is not a PDF -- re-download it"
                                        f"\t{datetime.now(timezone.utc):%FT%TZ}"])
        _log(f"!!! [CHUNK {chunk}] {ack} is not a PDF -- QUARANTINED (see {quarantine_path})")
    return [a for a in acks if a not in not_pdf]


def run_tier(batch_date, chunk_dir, tier, workers, micro, priority_micro, retry_micro, mem_gb, solo_mem_gb,
             timeout_min, solo_timeout_min):
    pattern = {"priority": "p", "rest": "r", "all": "pr"}[tier]
    manifests = sorted(f for f in os.listdir(chunk_dir)
                       if f.endswith(".txt") and f[0] in pattern and f[1:5].isdigit())
    os.makedirs(os.path.join(chunk_dir, "done", "acks"), exist_ok=True)
    log_dir = os.path.join(chunk_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    write_lock = os.path.abspath(os.path.join(chunk_dir, ".write.lock"))
    work_root = os.path.abspath(os.path.join("data", "work"))
    quarantine_path = os.path.join(chunk_dir, "quarantine.tsv")
    gib = 2**30

    for name in manifests:
        chunk = name[:-4]
        if os.path.exists(os.path.join(chunk_dir, "done", f"{chunk}.ok")):
            _log(f"[CHUNK {chunk}] already done, skipping")
            continue
        chunk_start = time.time()
        all_acks = _read_lines(os.path.join(chunk_dir, name))
        done_path = os.path.join(chunk_dir, "done", "acks", f"{chunk}.txt")
        finished = set(_read_lines(done_path))
        held = _quarantined(chunk_dir)
        todo = [a for a in all_acks if a not in finished and a not in held]
        _log(f"[CHUNK {chunk}] start: {len(all_acks)} filings ({len(finished)} already done, "
             f"{len(held & set(all_acks))} quarantined, {len(todo)} to run)")
        # (2026-11-03) run_pipeline holds every filing's pages until it writes, so memory grows with the
        # batch: on the largest-plan chunk 3 of the first 4 batches of 25 hit the cap and all 75 filings
        # went to the serial one-by-one lane. A failed batch is now split into batches of retry_micro and
        # run again in parallel at the same cap; only filings failing there go one by one.
        retry = []

        def run_pass(batches, tag, fallback):
            def run_batch(job):
                idx, acks = job
                slot = threading.current_thread().name.rsplit("_", 1)[-1]
                work_dir = os.path.join(work_root, f"w{slot}")
                log_path = os.path.join(log_dir, f"{chunk}_{tag}{idx:03d}.log")
                not_pdf = []
                status, detail = _pipeline_attempt(batch_date, acks, work_dir, log_path,
                                                   mem_gb * gib, timeout_min * 60, write_lock, not_pdf)
                acks = _quarantine_not_pdf(chunk, acks, not_pdf, quarantine_path)
                if status == "ok":
                    _append_lines(done_path, acks)
                    _log(f"[CHUNK {chunk}] {tag}{idx:03d} ok: {len(acks)} filings, {detail}")
                else:
                    smaller = tag == "m" and len(acks) > retry_micro > 1
                    (fallback if smaller else retry).extend(acks)
                    _log(f"!!! [CHUNK {chunk}] {tag}{idx:03d} {status}: {detail} -- {len(acks)} filings retried "
                         + ("in smaller batches" if smaller else "one by one"))

            with ThreadPoolExecutor(max_workers=workers) as pool:
                list(pool.map(run_batch, list(enumerate(batches, 1))))

        # priority chunks hold the largest plans (master trusts, hundreds of pages each), so smaller batches
        size = priority_micro if chunk.startswith("p") else micro
        split = []
        run_pass([todo[i:i + size] for i in range(0, len(todo), size)], "m", split)
        if split:
            run_pass([split[i:i + retry_micro] for i in range(0, len(split), retry_micro)], "s", retry)

        for ack in retry:
            log_path = os.path.join(log_dir, f"{chunk}_solo_{ack}.log")
            not_pdf = []
            status, detail = _pipeline_attempt(batch_date, [ack], os.path.join(work_root, "solo"), log_path,
                                               solo_mem_gb * gib, solo_timeout_min * 60, write_lock, not_pdf)
            if not _quarantine_not_pdf(chunk, [ack], not_pdf, quarantine_path):
                continue
            if status == "ok":
                _append_lines(done_path, [ack])
                _log(f"[CHUNK {chunk}] solo {ack} ok: {detail}")
            else:
                _append_lines(quarantine_path, [f"{ack}\t{chunk}\t{status}\t{detail}\t{datetime.now(timezone.utc):%FT%TZ}"])
                _log(f"!!! [CHUNK {chunk}] solo {ack} {status}: {detail} -- QUARANTINED (see {quarantine_path})")

        finished = set(_read_lines(done_path))
        _post_chunk(chunk, [a for a in all_acks if a in finished], chunk_dir)
        open(os.path.join(chunk_dir, "done", f"{chunk}.ok"), "w").close()
        n_done = len([f for f in os.listdir(os.path.join(chunk_dir, "done")) if f.endswith(".ok")])
        n_held = len(_quarantined(chunk_dir) & set(all_acks))
        _log(f"[CHUNK {chunk}] done in {int(time.time() - chunk_start)}s: {len(finished & set(all_acks))} ok, "
             f"{n_held} quarantined; {n_done} chunks done in total")
    shutil.rmtree(work_root, ignore_errors=True)
    return 0


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
    r = sub.add_parser("run")
    r.add_argument("--batch-date", required=True)
    r.add_argument("--chunk-dir", required=True)
    r.add_argument("--tier", choices=["priority", "rest", "all"], default="priority")
    r.add_argument("--workers", type=int, default=int(os.getenv("CHUNK_WORKERS", "3")))
    r.add_argument("--micro", type=int, default=int(os.getenv("CHUNK_MICRO", "25")))
    r.add_argument("--priority-micro", type=int, default=int(os.getenv("CHUNK_PRIORITY_MICRO", "10")))
    r.add_argument("--retry-micro", type=int, default=int(os.getenv("CHUNK_RETRY_MICRO", "5")))
    r.add_argument("--mem-gb", type=float, default=float(os.getenv("CHUNK_MEM_GB", "8")))
    r.add_argument("--solo-mem-gb", type=float, default=float(os.getenv("CHUNK_SOLO_MEM_GB", "24")))
    r.add_argument("--timeout-min", type=float, default=float(os.getenv("CHUNK_TIMEOUT_MIN", "120")))
    r.add_argument("--solo-timeout-min", type=float, default=float(os.getenv("CHUNK_SOLO_TIMEOUT_MIN", "90")))
    a = ap.parse_args()
    if a.cmd == "plan":
        excl = {t.strip() for t in a.exclude_types.split(",") if t.strip()}
        return plan(a.batch_date, a.out, a.chunk_size, a.min_assets, excl)
    if a.cmd == "run":
        return run_tier(a.batch_date, a.chunk_dir, a.tier, a.workers, a.micro, a.priority_micro, a.retry_micro,
                        a.mem_gb, a.solo_mem_gb, a.timeout_min, a.solo_timeout_min)
    return fetch(a.batch_date, a.manifest, a.dest)


if __name__ == "__main__":
    sys.exit(main())
