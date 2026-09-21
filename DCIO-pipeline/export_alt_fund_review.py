import csv
import sys
import time
from pathlib import Path

import boto3


DATABASE = "default"
OUTPUT = "s3://retirementinsights-silver/athena-results/"


def query_athena(sql: str) -> list[list[str]]:
    client = boto3.client("athena", region_name="us-east-1")
    response = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": OUTPUT},
    )
    query_id = response["QueryExecutionId"]

    while True:
        status = client.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(status.get("StateChangeReason", state))
        time.sleep(2)

    rows: list[list[str]] = []
    paginator = client.get_paginator("get_query_results")
    for page in paginator.paginate(QueryExecutionId=query_id):
        for row in page["ResultSet"]["Rows"]:
            rows.append([cell.get("VarCharValue", "") for cell in row["Data"]])
    return rows


def main() -> None:
    sql_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])
    rows = query_athena(sql_path.read_text(encoding="utf-8"))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerows(rows)
    print(output_path)


if __name__ == "__main__":
    main()
