import sys
import time

import boto3


DATABASE = "default"
OUTPUT = "s3://retirementinsights-silver/athena-results/"


def run(sql: str) -> None:
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
            print(qid)
            return
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(status.get("StateChangeReason", state))
        time.sleep(1)


if __name__ == "__main__":
    run(sys.argv[1])
