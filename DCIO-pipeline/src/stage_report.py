"""
Stage report generator — produces stage_compare.csv and stage_pivot.csv.

Usage as CLI:
    python3 -m src.stage_report                   # both reports, default paths
    python3 -m src.stage_report --compare-only
    python3 -m src.stage_report --pivot-only
    python3 -m src.stage_report --output-dir /some/dir

Usage as library:
    from src.stage_report import generate_stage_pivot, generate_stage_compare
    pivot_path   = generate_stage_pivot(data_dir, output_dir)
    compare_path = generate_stage_compare(data_dir, output_dir)
"""
import csv
import os
import argparse

_STAGE_COLS = [
    "issuer_name", "investment_description", "asset_type",
    "current_value", "cost", "par_value",
]
_KEY_COLS = ["pdf_stem", "page_number", "row_id", "pdf_name", "plan_year"]
_FIXED_COLS = [
    "stage", "pdf_stem", "page_number", "row_id",
    "issuer_name", "investment_description", "asset_type",
    "current_value", "cost", "par_value", "units_or_shares",
    "plan_name", "plan_year", "plan_number", "pdf_name",
]


def _stage_files(data_dir: str) -> dict:
    return {
        "raw":     os.path.join(data_dir, "investments_raw.csv"),
        "process": os.path.join(data_dir, "investments_pre_llm.csv"),
        "clean":   os.path.join(data_dir, "investments_clean_llm.csv"),
    }


def generate_stage_compare(data_dir: str, output_dir: str) -> str:
    """Stack raw / process / clean rows with a 'stage' column.

    Returns the path to the written CSV.
    """
    files = _stage_files(data_dir)
    out_rows = []
    all_fields: set = set()

    for stage, path in files.items():
        if not os.path.exists(path):
            print(f"  [stage_report] MISSING: {path}")
            continue
        with open(path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                row["stage"] = stage
                out_rows.append(row)
                all_fields.update(row.keys())

    extra_cols = sorted(all_fields - set(_FIXED_COLS))
    fieldnames = _FIXED_COLS + extra_cols

    out_rows.sort(key=lambda r: (
        r.get("pdf_stem", ""),
        ["raw", "process", "clean"].index(r.get("stage", "raw")),
        int(r.get("page_number", 0) or 0),
        int(r.get("row_id", 0) or 0),
    ))

    out_path = os.path.join(output_dir, "stage_compare.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(out_rows)

    print(f"  [stage_report] stage_compare: {len(out_rows)} rows -> {out_path}")
    return out_path


def generate_stage_pivot(data_dir: str, output_dir: str) -> str:
    """Pivot to wide format: one row per investment, raw/process/clean columns side by side.

    Returns the path to the written CSV.
    """
    files = _stage_files(data_dir)
    data: dict = {}
    all_keys: dict = {}

    for stage, path in files.items():
        data[stage] = {}
        if not os.path.exists(path):
            print(f"  [stage_report] MISSING: {path}")
            continue
        with open(path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                key = (
                    row.get("pdf_stem", ""),
                    row.get("page_number", ""),
                    row.get("row_id", ""),
                )
                data[stage][key] = row
                if key not in all_keys:
                    all_keys[key] = (
                        row.get("pdf_stem", ""),
                        int(row.get("page_number", 0) or 0),
                        int(row.get("row_id", 0) or 0),
                    )

    sorted_keys = sorted(all_keys, key=lambda k: all_keys[k])

    header = _KEY_COLS[:]
    for stage in ("raw", "process", "clean"):
        for col in _STAGE_COLS:
            header.append(f"{stage}_{col}")
    header.append("asset_type_changed")

    out_path = os.path.join(output_dir, "stage_pivot.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for key in sorted_keys:
            ref = (
                data["raw"].get(key)
                or data["process"].get(key)
                or data["clean"].get(key)
            )
            row_out = [ref.get(c, "") for c in _KEY_COLS]
            for stage in ("raw", "process", "clean"):
                r = data[stage].get(key, {})
                for col in _STAGE_COLS:
                    row_out.append(r.get(col, ""))
            types = [
                data[s].get(key, {}).get("asset_type", "")
                for s in ("raw", "process", "clean")
            ]
            row_out.append("Y" if len({t for t in types if t}) > 1 else "")
            w.writerow(row_out)

    print(f"  [stage_report] stage_pivot:   {len(sorted_keys)} rows -> {out_path}")
    return out_path


def main():
    parser = argparse.ArgumentParser(description="Generate stage comparison reports")
    parser.add_argument(
        "--data-dir",
        default=os.path.join(
            os.path.dirname(__file__), "..", "data", "outputs"
        ),
        help="Directory containing investments_raw/pre_llm/clean_llm CSVs",
    )
    parser.add_argument(
        "--output-dir",
        default="/tmp",
        help="Directory to write output CSVs (default: /tmp)",
    )
    parser.add_argument("--compare-only", action="store_true")
    parser.add_argument("--pivot-only", action="store_true")
    args = parser.parse_args()

    data_dir = os.path.realpath(args.data_dir)
    output_dir = os.path.realpath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    if not args.pivot_only:
        generate_stage_compare(data_dir, output_dir)
    if not args.compare_only:
        generate_stage_pivot(data_dir, output_dir)


if __name__ == "__main__":
    main()