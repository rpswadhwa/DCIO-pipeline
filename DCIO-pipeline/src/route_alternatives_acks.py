"""Route a list of filings' staging rows into the alternatives table (deferred ALT routing).

The chunk runner (src/batch_chunks.py) sets ALT_ROUTE_DEFER_FILE for its run_pipeline batches, so
each batch writes staging and MF rows and queues its filings in that file; this runs the same router
(post_extract_validator.route_alternatives_with_managers) once per chunk over the queued filings,
ALT_ROUTE_CHUNK_SIZE filings per query. Rerunning it for the same filings is safe: the
router's guarded DELETE + INSERT replaces their routed rows. (2026-11-05)

    python -m src.route_alternatives_acks --ack-file data/chunks/<batch>/alt_pending/p0002.txt
"""
import argparse
import logging
import os
import sys

from dotenv import load_dotenv


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ack-file", required=True, help="one ack_id per line")
    args = ap.parse_args()
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    with open(args.ack_file) as fh:
        acks = list(dict.fromkeys(line.strip() for line in fh if line.strip()))
    glue_db = os.getenv("VALIDATED_GLUE_DB", "default")
    staging_table = os.getenv("HOLDINGS_STAGING_TABLE", "").strip()
    alt_table = os.getenv("ALTERNATIVES_TABLE", "").strip()
    if not (staging_table and alt_table):
        print("HOLDINGS_STAGING_TABLE / ALTERNATIVES_TABLE not set -- nothing to route", flush=True)
        return 0
    if not acks:
        return 0

    from .post_extract_validator import route_alternatives_with_managers
    print(f"[ALT ROUTE] {len(acks)} filings, {os.getenv('ALT_ROUTE_CHUNK_SIZE') or 25} per query", flush=True)
    route_alternatives_with_managers(glue_db, staging_table, alt_table, acks)
    print(f"[ALT ROUTE] done: {len(acks)} filings", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
