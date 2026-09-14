import csv
import sys
import time

import boto3


DATABASE = "default"
OUTPUT = "s3://retirementinsights-silver/athena-results/"


def run(sql: str) -> list[list[str]]:
    ath = boto3.client("athena", region_name="us-east-1")
    resp = ath.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": OUTPUT},
    )
    qid = resp["QueryExecutionId"]
    while True:
        status = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(status.get("StateChangeReason", state))
        time.sleep(1)

    rows: list[list[str]] = []
    paginator = ath.get_paginator("get_query_results")
    for page in paginator.paginate(QueryExecutionId=qid):
        for row in page["ResultSet"]["Rows"]:
            rows.append([cell.get("VarCharValue", "") for cell in row["Data"]])
    return rows


if __name__ == "__main__":
    arg = sys.argv[1]
    if arg.startswith("@"):
        with open(arg[1:], encoding="utf-8") as f:
            sql = f.read()
    else:
        sql = arg
    writer = csv.writer(sys.stdout)
    for row in run(sql):
        writer.writerow(row)
