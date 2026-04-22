import csv
import json
import os
from typing import Dict, List

from dotenv import load_dotenv

from .classify_pages import classify_pages
from .data_cleaner import clean_investment_data
from .detect_tables import detect_tables
from .ingest import ingest_pdfs
from .llm_map import map_rows_with_llm
from .load_db import load_cleaned_pipeline_results, reset_db
from .normalize_images import normalize_pages
from .ocr_passes import run_ocr
from .text_extract import classify_pages_text, extract_tables_and_map
from .utils import ensure_dir, read_env
from .validate import validate_pages

from cleanup_investment_names import cleanup_investments
from enhance_asset_types import enhance_asset_types
from llm_enhance_investments import (
    check_and_fix_asset_type_consistency,
    export_enhanced_csv,
    llm_enhance_investments,
)

_VALIDATION_ENABLED = os.getenv("VALIDATION_ENABLED", "0") == "1"
if _VALIDATION_ENABLED:
    from .post_extract_validator import run_post_extract_validation


def upload_to_s3(file_path: str, s3_path: str):
    if not os.path.exists(file_path):
        print(f"  File not found: {file_path}")
        return False

    try:
        if not s3_path.startswith("s3://"):
            print(f"  Invalid S3 path: {s3_path}")
            return False

        s3_path = s3_path.replace("s3://", "", 1)
        parts = s3_path.split("/", 1)
        bucket = parts[0]
        key_prefix = parts[1] if len(parts) > 1 else ""

        if key_prefix.endswith("/"):
            key_prefix += os.path.basename(file_path)
        elif not key_prefix:
            key_prefix = os.path.basename(file_path)

        import boto3

        s3_client = boto3.client("s3")
        print(f"  Uploading {os.path.basename(file_path)} to s3://{bucket}/{key_prefix}")
        s3_client.upload_file(file_path, bucket, key_prefix)
        return True
    except Exception as exc:
        print(f"  S3 upload failed: {exc}")
        return False


def _write_csv(path: str, rows: List[Dict], preferred_fields: List[str] = None) -> None:
    if not rows:
        return

    preferred_fields = preferred_fields or []
    fieldnames = list(preferred_fields)
    seen = set(fieldnames)

    for row in rows:
        for key in row.keys():
            if key not in seen:
                fieldnames.append(key)
                seen.add(key)

    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _collect_extracted_rows(pages: List[Dict], plan_info_map: Dict[str, Dict], plan_year: int) -> List[Dict]:
    rows = []
    for page in pages:
        pdf_path = page.get("pdf", "")
        pdf_name = os.path.basename(pdf_path) if pdf_path else ""
        pdf_stem = page.get("pdf_stem", "")
        plan_info = plan_info_map.get(pdf_stem, {})

        for mapped_row in page.get("mapped_rows", []):
            row = dict(mapped_row)
            row["pdf_name"] = pdf_name
            row["pdf_stem"] = pdf_stem
            row["sponsor_ein"] = plan_info.get("ein", "")
            row["plan_number"] = plan_info.get("plan_number", "001")
            row["plan_year"] = plan_year
            row["sponsor"] = plan_info.get("sponsor", "")
            row["plan_name"] = plan_info.get("plan_name", "")
            rows.append(row)
    return rows


def main():
    load_dotenv()

    input_dir = read_env("INPUT_DIR", "data/inputs")
    output_dir = read_env("OUTPUT_DIR", "data/outputs")
    images_dir = os.path.join(output_dir, "images")
    ensure_dir(output_dir)
    ensure_dir(images_dir)

    dpi = int(read_env("DPI", "350"))
    model = read_env("OPENAI_MODEL", "gpt-4.1-mini")
    use_llm = read_env("USE_LLM", "1") != "0"
    use_post_llm = read_env("USE_POST_LLM", "1") != "0"
    use_ocr = read_env("USE_OCR", "0") == "1"
    llm_batch_size = int(read_env("POST_LLM_BATCH_SIZE", "10"))
    llm_max_batches_raw = read_env("POST_LLM_MAX_BATCHES", "")
    llm_max_batches = int(llm_max_batches_raw) if llm_max_batches_raw else None
    plan_year = int(read_env("PLAN_YEAR", "2024"))

    keywords_yml = read_env("KEYWORDS_YML", "config/keywords.yml")
    schema_yml = read_env("SCHEMA_YML", "config/schema.yml")
    schema_sql = read_env("SCHEMA_SQL", "sql/schema.sql")

    db_path = os.path.join(output_dir, "pipeline.db")
    qa_report_path = os.path.join(output_dir, "qa_report.json")
    raw_csv_path = os.path.join(output_dir, "investments_raw.csv")
    clean_csv_path = os.path.join(output_dir, "investments_clean.csv")
    llm_csv_path = os.path.join(output_dir, "investments_clean_llm.csv")
    legacy_csv_path = os.path.join(output_dir, "investments.csv")
    removed_totals_path = os.path.join(output_dir, "removed_total_rows.csv")

    print("=" * 60)
    print("FORM 5500 PIPELINE")
    print("=" * 60)

    if use_ocr:
        print("\n[STEP 1] OCR extraction")
        pages = ingest_pdfs(input_dir, images_dir, dpi=dpi)
        pages = classify_pages(pages, keywords_yml)
        supplemental_pages = [p for p in pages if p.get("is_supplemental") == 1]
        supplemental_pages = normalize_pages(supplemental_pages)
        supplemental_pages = detect_tables(supplemental_pages)
        supplemental_pages = run_ocr(supplemental_pages)
        supplemental_pages = map_rows_with_llm(supplemental_pages, schema_yml, model, use_llm=use_llm)
        plan_info_map = {}
    else:
        print("\n[STEP 1] Text/table extraction")
        pages = []
        supplemental_pages = []
        plan_info_map = {}

        for fname in sorted(os.listdir(input_dir)):
            if not fname.lower().endswith(".pdf"):
                continue

            pdf_path = os.path.join(input_dir, fname)
            pdf_stem = fname.rsplit(".", 1)[0]
            print(f"  Processing {fname}")

            classified = classify_pages_text(pdf_path, keywords_yml)
            pages.extend(classified)

            supp_nums = [p["page_number"] for p in classified if p.get("is_supplemental") == 1]
            print(f"    Supplemental pages: {supp_nums}")

            plan_info, page_data = extract_tables_and_map(
                pdf_path,
                supp_nums,
                schema_yml,
                model,
                use_llm=use_llm,
            )
            if plan_info:
                plan_info_map[pdf_stem] = plan_info
            supplemental_pages.extend(page_data)

    print("\n[STEP 2] QA report")
    qa = validate_pages(supplemental_pages, schema_yml)
    with open(qa_report_path, "w", encoding="utf-8") as handle:
        json.dump(qa, handle, ensure_ascii=True, indent=2)

    print("\n[STEP 3] Raw extraction export")
    raw_rows = _collect_extracted_rows(supplemental_pages, plan_info_map, plan_year)
    _write_csv(
        raw_csv_path,
        raw_rows,
        preferred_fields=[
            "issuer_name",
            "investment_description",
            "asset_type",
            "par_value",
            "cost",
            "current_value",
            "units_or_shares",
            "page_number",
            "row_id",
            "pdf_name",
            "pdf_stem",
            "sponsor_ein",
            "plan_number",
            "plan_year",
            "sponsor",
            "plan_name",
        ],
    )
    print(f"  Extracted rows: {len(raw_rows)}")

    print("\n[STEP 4] Rule-based cleanup")
    clean_rows, removed_totals = clean_investment_data(
        raw_rows,
        preserve_loans=True,
        remove_dupes=True,
        verbose=True,
    )
    _write_csv(removed_totals_path, removed_totals)

    print("\n[STEP 5] Rebuild database")
    reset_db(db_path, schema_sql)
    load_cleaned_pipeline_results(db_path, supplemental_pages, pages, clean_rows, plan_info_map, plan_year=plan_year)

    print("\n[STEP 6] Rule-based post-processing")
    cleanup_investments(db_path, verbose=True)
    enhance_asset_types(db_path, verbose=True)

    llm_updates = 0
    if use_post_llm:
        print("\n[STEP 7] LLM enhancement")
        if os.getenv("OPENAI_API_KEY"):
            llm_updates = llm_enhance_investments(
                db_path,
                batch_size=llm_batch_size,
                max_batches=llm_max_batches,
                verbose=True,
            )
        else:
            print("  OPENAI_API_KEY not set. Skipping late-stage LLM enhancement.")
    else:
        print("\n[STEP 7] LLM enhancement skipped (USE_POST_LLM=0)")

    print("\n[STEP 8] Consistency check and exports")
    check_and_fix_asset_type_consistency(db_path, verbose=True)
    export_enhanced_csv(db_path, clean_csv_path, verbose=True)
    export_enhanced_csv(db_path, legacy_csv_path, verbose=True)
    if llm_updates > 0:
        export_enhanced_csv(db_path, llm_csv_path, verbose=True)

    s3_bucket = read_env("S3_BUCKET_PATH", "")
    if s3_bucket:
        print("\n[STEP 9] Upload database to S3")
        upload_to_s3(db_path, s3_bucket)
    else:
        print("\n[STEP 9] S3 upload skipped (S3_BUCKET_PATH not set)")

    if _VALIDATION_ENABLED:
        print("\n[STEP 10] Post-extraction validation")
        counts = run_post_extract_validation(
            db_path=db_path,
            glue_db=read_env("VALIDATION_REF_DB", "default"),
            ref_table=read_env("VALIDATION_REF_TABLE", "plan_master_index_universe"),
            workgroup=read_env("ATHENA_WORKGROUP", "primary"),
            s3_staging=read_env("ATHENA_STAGING_S3", ""),
            tolerance=float(read_env("VALIDATION_TOLERANCE", "0.05")),
            validated_s3=read_env("VALIDATED_S3_PATH",
                "s3://retirementinsights-silver/tables/plan_mf_history_v3/"),
            error_s3=read_env("VALIDATION_ERROR_S3_PATH",
                "s3://retirementinsights-silver/tables/plan_mf_history_validation_errors/"),
            validated_glue_db=read_env("VALIDATED_GLUE_DB", "default"),
            validated_table=read_env("VALIDATED_TABLE", "plan_mf_history_v3"),
            error_table=read_env("VALIDATION_ERROR_TABLE", "plan_mf_history_validation_errors"),
        )
        print(f"  {counts['passed']} passed, {counts['failed']} failed, {counts['skipped']} skipped")
    else:
        print("\n[STEP 10] Validation skipped (VALIDATION_ENABLED not set)")

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
