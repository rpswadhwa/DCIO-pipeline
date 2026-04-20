# Glue Python Shell script: sqlite_to_parquet.py
# Purpose: Read SQLite table 'investments_clean' from S3 DB file and write Parquet dataset to S3 for Athena.

import os
import sqlite3
import tempfile
import boto3
import pandas as pd
import awswrangler as wr  # AWS SDK for pandas
from urllib.parse import urlparse

# --------- CONFIG (edit as needed) ----------
S3_SQLITE = "s3://retirementinsights-silver/tables/plan_fund_mapping/data/pipeline.db"
SQLITE_TABLE = "investments"
S3_PARQUET_BASE = "s3://retirementinsights-silver/tables/plan_fund_mapping/optimized_parquet/"  # dataset root
ATHENA_DB = "default"               # Athena/Glue database
ATHENA_TABLE = "plan_fund_mapping"  # Athena table name to register/update
PARTITION_COLS = ["plan_year", "sponsor_ein"]
WRITE_MODE = "overwrite"  # or "overwrite_partitions" / "append"
# -------------------------------------------

def s3_download(s3_uri: str, local_path: str):
    p = urlparse(s3_uri)
    s3 = boto3.client("s3")
    s3.download_file(p.netloc, p.path.lstrip("/"), local_path)

def main():
    # 1) Download the SQLite DB file to /tmp
    with tempfile.TemporaryDirectory() as tmpd:
        local_db = os.path.join(tmpd, "pipeline.db")
        s3_download(S3_SQLITE, local_db)

        # 2) Read from SQLite
        con = sqlite3.connect(local_db)
        # If the table is large, read in chunks with LIMIT/OFFSET or use an iterator; simple read first:
        df = pd.read_sql_query(f"SELECT * FROM {SQLITE_TABLE}", con)
        con.close()

        # 3) (Optional) Ensure dtypes you want in Parquet
        # Example: enforce numerics/ints
        to_int = ["plan_year", "page_number", "row_id"]
        to_float = ["par_value", "cost", "current_value", "units_or_shares", "confidence"]
        for c in to_int:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")  # nullable ints
        for c in to_float:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        # 4) Write Parquet dataset partitioned for Athena (+ auto-register in Glue)
        wr.s3.to_parquet(
            df=df,
            path=S3_PARQUET_BASE,
            dataset=True,
            mode=WRITE_MODE,
            partition_cols=[c for c in PARTITION_COLS if c in df.columns],
            compression="snappy",
            database=ATHENA_DB,           # auto-create/update table in Glue Catalog if not exists
            table=ATHENA_TABLE
        )
        print(f"Wrote Parquet dataset to {S3_PARQUET_BASE} and updated Glue table {ATHENA_DB}.{ATHENA_TABLE}")

if __name__ == "__main__":
    main()