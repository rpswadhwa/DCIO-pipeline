import csv
import os
import time
from pathlib import Path

import boto3


BASE = Path(r"C:\Users\User\Documents\GitHub\DCIO-pipeline\DCIO-pipeline")
FOCUSED = BASE / "outputs" / "alt_cit_like_alternative_records.csv"
OUT = BASE / "outputs" / "alt_cit_like_staging_vs_cit_records.csv"
ATHENA_OUTPUT = "s3://retirementinsights-silver/athena-results/"


def quote_sql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def run_athena(sql: str) -> tuple[list[str], list[dict[str, str]], str]:
    client = boto3.client("athena", region_name="us-east-1")
    response = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": "default"},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
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

    raw_rows: list[list[str]] = []
    for page in client.get_paginator("get_query_results").paginate(QueryExecutionId=query_id):
        for row in page["ResultSet"]["Rows"]:
            raw_rows.append([cell.get("VarCharValue", "") for cell in row["Data"]])

    if not raw_rows:
        return [], [], query_id
    headers = raw_rows[0]
    return headers, [dict(zip(headers, row)) for row in raw_rows[1:]], query_id


def as_amount(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return -1.0


def main() -> None:
    staging = list(csv.DictReader(FOCUSED.open(encoding="utf-8", newline="")))
    ack_ids = sorted({row["ack_id"] for row in staging if row.get("ack_id")})
    ack_values = ", ".join(quote_sql(ack) for ack in ack_ids)

    sql = f"""
WITH cit_rows AS (
    SELECT
        c.ack_id,
        c.plan_id,
        pm.plan_name_clean,
        c.filing_year,
        c.raw_entity_name,
        c.raw_sponsor_name,
        c.normalized_manager,
        c.asset_class,
        c.asset_sub_class,
        c.plan_investment_amt,
        upper(trim(regexp_replace(regexp_replace(coalesce(c.raw_entity_name, ''), '[^A-Z0-9 ]+', ' '), '\\\\s+', ' '))) AS entity_norm,
        upper(trim(regexp_replace(regexp_replace(coalesce(c.raw_sponsor_name, ''), '[^A-Z0-9 ]+', ' '), '\\\\s+', ' '))) AS sponsor_norm,
        upper(trim(regexp_replace(regexp_replace(coalesce(c.normalized_manager, ''), '[^A-Z0-9 ]+', ' '), '\\\\s+', ' '))) AS manager_norm,
        upper(trim(regexp_replace(regexp_replace(coalesce(c.asset_class, ''), '[^A-Z0-9 ]+', ' '), '\\\\s+', ' '))) AS asset_class_norm,
        upper(trim(regexp_replace(regexp_replace(coalesce(c.asset_sub_class, ''), '[^A-Z0-9 ]+', ' '), '\\\\s+', ' '))) AS asset_sub_class_norm
    FROM default.plan_cit_history c
    LEFT JOIN default.plan_master pm
      ON pm.ack_id = c.ack_id
     AND pm.filing_year = c.filing_year
    WHERE c.ack_id IN ({ack_values})
),
scored AS (
    SELECT
        *,
        CASE
            WHEN regexp_like(asset_class_norm || ' ' || asset_sub_class_norm,
                'ALTERNATIVE|PRIVATE EQUITY|PRIVATE CREDIT|REAL ESTATE|INFRASTRUCTURE|HEDGE|NATURAL RESOURCES|TIMBER|FARMLAND|VENTURE|BUYOUT|MEZZANINE|DIRECT LENDING|OPPORTUNISTIC|SPECIAL SITUATIONS')
                THEN 'CLASSIFIED_ALT'
            WHEN regexp_like(entity_norm || ' ' || sponsor_norm || ' ' || manager_norm,
                'BLACKSTONE|STARWOOD|BROOKFIELD|APOLLO|ARES|KKR|CARLYLE|BAIN|TPG|OAKTREE|BLUE OWL|HAMILTON LANE|HARBOURVEST|PARTNERS GROUP|PANTHEON|ARDIAN|STEPSTONE|GCM GROSVENOR|NEUBERGER BERMAN|HINES|LASALLE|HEITMAN|AEW|CLARION|ANGELO GORDON|OWL ROCK|FS KKR')
                THEN 'MANAGER_ALT_SIGNAL'
            WHEN regexp_like(entity_norm || ' ' || sponsor_norm,
                'PRIVATE EQUITY|PRIVATE CREDIT|DIRECT LENDING|MIDDLE MARKET|MEZZANINE|DISTRESSED|OPPORTUNITY|OPPORTUNISTIC|SPECIAL SITUATIONS|SECONDAR|BUYOUT|GROWTH EQUITY|VENTURE|REAL ESTATE|INFRASTRUCTURE|NATURAL RESOURCES|TIMBER|FARMLAND|HEDGE FUND|FUND OF FUNDS|CO INVEST|CLO|BDC|REIT|INTERVAL FUND|TENDER OFFER|LIMITED PARTNERSHIP|PARTNERSHIP INTEREST|JOINT VENTURE|PRIVATE FUND')
                THEN 'NAME_ALT_SIGNAL'
        END AS cit_alt_signal
    FROM cit_rows
)
SELECT
    ack_id,
    plan_id,
    plan_name_clean,
    filing_year,
    raw_entity_name,
    raw_sponsor_name,
    normalized_manager,
    asset_class,
    asset_sub_class,
    plan_investment_amt,
    cit_alt_signal
FROM scored
WHERE cit_alt_signal IS NOT NULL
ORDER BY ack_id, try_cast(plan_investment_amt AS double) DESC NULLS LAST, raw_entity_name
"""

    _, cit_rows, query_id = run_athena(sql)

    combined: list[dict[str, str]] = []
    for row in staging:
        combined.append(
            {
                "ack_id": row.get("ack_id", ""),
                "plan_id": row.get("plan_id", ""),
                "plan_name_clean": row.get("plan_name_clean", ""),
                "filing_year": row.get("filing_year", ""),
                "record_group": "A_STAGING_CIT_LIKE_ALT_CANDIDATE",
                "source_table": "plan_holdings_staging",
                "raw_entity_name": row.get("raw_entity_name", ""),
                "raw_sponsor_name": row.get("raw_sponsor_name", ""),
                "normalized_manager": "",
                "source_asset_type": row.get("source_asset_type", ""),
                "asset_class": row.get("source_asset_class", ""),
                "asset_sub_class": row.get("source_asset_sub_class", ""),
                "plan_investment_amt": row.get("plan_investment_amt", ""),
                "proposed_manager": row.get("proposed_manager", ""),
                "proposed_asset_type": row.get("proposed_asset_type", ""),
                "proposed_asset_class": row.get("proposed_asset_class", ""),
                "proposed_asset_sub_class": row.get("proposed_asset_sub_class", ""),
                "confidence_level": row.get("confidence_level", ""),
                "signal": row.get("match_reason", ""),
            }
        )

    for row in cit_rows:
        combined.append(
            {
                "ack_id": row.get("ack_id", ""),
                "plan_id": row.get("plan_id", ""),
                "plan_name_clean": row.get("plan_name_clean", ""),
                "filing_year": row.get("filing_year", ""),
                "record_group": "B_EXISTING_CIT_ALT_SIGNAL",
                "source_table": "plan_cit_history",
                "raw_entity_name": row.get("raw_entity_name", ""),
                "raw_sponsor_name": row.get("raw_sponsor_name", ""),
                "normalized_manager": row.get("normalized_manager", ""),
                "source_asset_type": "CIT",
                "asset_class": row.get("asset_class", ""),
                "asset_sub_class": row.get("asset_sub_class", ""),
                "plan_investment_amt": row.get("plan_investment_amt", ""),
                "proposed_manager": row.get("normalized_manager", ""),
                "proposed_asset_type": "CIT",
                "proposed_asset_class": row.get("asset_class", ""),
                "proposed_asset_sub_class": row.get("asset_sub_class", ""),
                "confidence_level": "EXISTING_CIT_SIGNAL",
                "signal": row.get("cit_alt_signal", ""),
            }
        )

    combined.sort(
        key=lambda row: (
            row["plan_name_clean"],
            row["ack_id"],
            row["record_group"],
            -as_amount(row["plan_investment_amt"]),
        )
    )

    fields = [
        "ack_id",
        "plan_id",
        "plan_name_clean",
        "filing_year",
        "record_group",
        "source_table",
        "raw_entity_name",
        "raw_sponsor_name",
        "normalized_manager",
        "source_asset_type",
        "asset_class",
        "asset_sub_class",
        "plan_investment_amt",
        "proposed_manager",
        "proposed_asset_type",
        "proposed_asset_class",
        "proposed_asset_sub_class",
        "confidence_level",
        "signal",
    ]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(combined)

    print(OUT)
    print(f"staging_rows={len(staging)}")
    print(f"cit_alt_rows={len(cit_rows)}")
    print(f"combined_rows={len(combined)}")
    print(f"plans={len(ack_ids)}")
    print(f"athena_query_id={query_id}")


if __name__ == "__main__":
    main()
