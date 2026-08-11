import csv
import json
import os
import subprocess
from typing import Dict, List

import pytesseract
from dotenv import load_dotenv

from .classify_pages import classify_pages
from .data_cleaner import clean_investment_data
from .detect_tables import detect_tables
from .ingest import ingest_pdfs
from .llm_map import map_rows_with_llm
from .load_db import load_cleaned_pipeline_results, reset_db
from .normalize_images import normalize_pages
from .ocr_passes import run_ocr
from .text_extract import classify_pages_text, extract_tables_and_map, _has_see_attachment, find_attachment_pages, find_structural_investment_pages, find_simple_investment_pages, _SEE_ATTACHMENT_RE, _DETAIL_REFERENCE_RE, expand_continuation_pages
from .utils import ensure_dir, read_env
from .validate import validate_pages

from cleanup_investment_names import cleanup_investments
from enhance_asset_types import enhance_asset_types
from llm_enhance_investments import (
    export_enhanced_csv,
    llm_enhance_investments,
)
from .mf_mapping_enrichment import enrich_mf_classes


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


def sync_s3_inputs(s3_input_path: str, input_dir: str) -> None:
    """Sync PDF inputs from S3 into the local input folder when configured."""
    if not s3_input_path:
        return
    if not s3_input_path.startswith("s3://"):
        print(f"  Invalid S3 input path, skipping sync: {s3_input_path}")
        return

    ensure_dir(input_dir)
    print(f"  Syncing PDF inputs from {s3_input_path} to {input_dir}")
    cmd = [
        "aws",
        "s3",
        "sync",
        s3_input_path,
        input_dir,
        "--exclude",
        "*",
        "--include",
        "*.pdf",
        "--region",
        read_env("AWS_REGION", "us-east-1"),
    ]
    subprocess.run(cmd, check=True)
    pdf_count = sum(
        1 for name in os.listdir(input_dir)
        if name.lower().endswith(".pdf")
    )
    print(f"  Local PDF inputs ready: {pdf_count}")


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



def _has_useful_extracted_rows(page_data: List[Dict]) -> bool:
    for page in page_data:
        for row in page.get("mapped_rows", []):
            issuer = str(row.get("issuer_name", "") or "").strip()
            desc = str(row.get("investment_description", "") or "").strip()
            value = str(row.get("current_value", "") or "").strip()
            if (issuer or desc) and value and value not in {"**", "-", "nan"}:
                return True
    return False


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
    validation_enabled = read_env("VALIDATION_ENABLED", "0") == "1"

    input_dir = read_env("INPUT_DIR", "data/inputs")
    output_dir = read_env("OUTPUT_DIR", "data/outputs")
    images_dir = os.path.join(output_dir, "images")
    ensure_dir(input_dir)
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
    plan_year = int(read_env("PLAN_YEAR", "2025"))

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

    s3_input_path = read_env(
        "S3_INPUT_PATH",
        "s3://retirementinsights-bronze/filings_5500_pdf/year=2025/raw/",
    )
    if read_env("SYNC_S3_INPUTS", "1") != "0":
        sync_s3_inputs(s3_input_path, input_dir)

    if use_ocr:
        print("\n[STEP 1] OCR extraction")
        tesseract_cmd = read_env("TESSERACT_CMD", "")
        if tesseract_cmd:
            pytesseract.pytesseract.tesseract_cmd = tesseract_cmd
        tessdata_prefix = read_env("TESSDATA_PREFIX", "")
        if tessdata_prefix:
            os.environ["TESSDATA_PREFIX"] = tessdata_prefix
        os.environ["OMP_THREAD_LIMIT"] = read_env("OMP_THREAD_LIMIT", "1")
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
            expanded_supp_nums = expand_continuation_pages(pdf_path, supp_nums)
            if expanded_supp_nums != supp_nums:
                added_pages = [p for p in expanded_supp_nums if p not in supp_nums]
                print(f"    Continuation pages added: {added_pages}")
            supp_nums = expanded_supp_nums
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

            if not _has_useful_extracted_rows(page_data):
                structural_nums = find_structural_investment_pages(pdf_path)
                structural_nums = [p for p in structural_nums if p not in supp_nums]
                if structural_nums:
                    print(f"    Structural fallback pages found: {structural_nums}")
                    fallback_plan_info, fallback_data = extract_tables_and_map(
                        pdf_path,
                        structural_nums,
                        schema_yml,
                        model,
                        use_llm=use_llm,
                    )
                    if fallback_plan_info and not plan_info:
                        plan_info = fallback_plan_info
                        plan_info_map[pdf_stem] = fallback_plan_info
                    if _has_useful_extracted_rows(fallback_data):
                        page_data = fallback_data
                        supp_nums = structural_nums

            # Third-tier fallback: only runs if the keyword classifier AND the structural
            # fallback above have BOTH already found zero usable rows for this PDF. Catches
            # simplified 2-column Schedule-of-Assets formats without loosening the primary
            # detection logic used on every filing.
            if not _has_useful_extracted_rows(page_data):
                simple_nums = find_simple_investment_pages(pdf_path)
                simple_nums = [p for p in simple_nums if p not in supp_nums]
                if simple_nums:
                    print(f"    Simple-format fallback pages found: {simple_nums}")
                    fallback_plan_info, fallback_data = extract_tables_and_map(
                        pdf_path,
                        simple_nums,
                        schema_yml,
                        model,
                        use_llm=use_llm,
                    )
                    if fallback_plan_info and not plan_info:
                        plan_info = fallback_plan_info
                        plan_info_map[pdf_stem] = fallback_plan_info
                    if _has_useful_extracted_rows(fallback_data):
                        page_data = fallback_data
                        supp_nums = simple_nums

            # Attachment page handling: if any row says "see attachment",
            # scan pages after the last supplemental page for the actual data
            if supp_nums and _has_see_attachment(page_data):
                # Use the earliest page where "see attachment" was found as the
                # starting point — attachment data follows immediately after that page
                see_attach_page_nums = [
                    page["page_number"] for page in page_data
                    if any(
                        _SEE_ATTACHMENT_RE.search(str(row.get("issuer_name", "") or ""))
                        or _SEE_ATTACHMENT_RE.search(str(row.get("investment_description", "") or ""))
                        for row in page.get("mapped_rows", [])
                    )
                ]
                last_sup = min(see_attach_page_nums) if see_attach_page_nums else max(supp_nums)
                attachment_nums = find_attachment_pages(pdf_path, last_sup, keywords_yml, ignore_negatives=True)
                if attachment_nums:
                    print(f"    Attachment pages found: {attachment_nums}")
                    _, attach_data = extract_tables_and_map(
                        pdf_path, attachment_nums, schema_yml, model, use_llm=use_llm,
                    )
                    # Drop the summary "see attachment" rows — detail is now in attach_data
                    for page in page_data:
                        page["mapped_rows"] = [
                            r for r in page.get("mapped_rows", [])
                            if not _SEE_ATTACHMENT_RE.search(str(r.get("issuer_name", "") or ""))
                        ]
                    supplemental_pages.extend(attach_data)

            # Drop rollup rows that point to detail exhibits already selected for extraction,
            # such as "refer to Exhibit A - investments". This mirrors see-attachment
            # behavior without triggering a broad forward scan.
            for page in page_data:
                page["mapped_rows"] = [
                    r for r in page.get("mapped_rows", [])
                    if not _DETAIL_REFERENCE_RE.search(
                        " ".join(
                            str(r.get(field, "") or "")
                            for field in ("issuer_name", "investment_description", "asset_type", "current_value")
                        )
                    )
                ]

            # Section-typing correction stage (src/section_typing.py): re-type rows by
            # their PDF section and drop CIT / subtotal / duplicate junk -- AFTER
            # extraction, BEFORE the final validator -- so non-MF holdings (bonds,
            # stocks, treasuries, collective trusts) are excluded from plan_mf_history_v3.
            # EXCEPTION PATH ONLY: DEFAULT OFF. The normal full run does NOT touch the
            # ~44K good plans with this. It is enabled (SECTION_TYPING=1) only for the
            # targeted re-run over flagged over-capture / quarantine plans, then swapped
            # in per-ACK. Conservative (only downgrades non-MF).
            if read_env("SECTION_TYPING", "0") == "1":
                try:
                    from .section_typing import apply_section_typing_stage
                    _st_stats = apply_section_typing_stage(page_data, pdf_path)
                    if any(_st_stats.values()):
                        print(f"    Section-typing: {_st_stats}")
                except Exception as _st_exc:
                    print(f"    [section-typing] stage skipped: {_st_exc}")

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

    pre_llm_csv_path = os.path.join(output_dir, "investments_pre_llm.csv")
    export_enhanced_csv(db_path, pre_llm_csv_path, verbose=True)
    print(f"  Pre-LLM snapshot saved: {pre_llm_csv_path}")

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

    print("\n[STEP 8] Exports")
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

    if validation_enabled:
        from .post_extract_validator import run_post_extract_validation
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
            summary_table=read_env("VALIDATION_SUMMARY_TABLE", "plan_mf_history_validation_summary"),
            summary_glue_db=read_env("VALIDATION_SUMMARY_GLUE_DB",
                read_env("VALIDATED_GLUE_DB", "default")),
            manual_review_tolerance=float(read_env("MANUAL_REVIEW_TOLERANCE", "0.10")),
        )
        print(f"  {counts['passed']} passed, {counts['failed']} failed, {counts['skipped']} skipped")

        # Step 11: Copy asset_class/sub_asset_class into MF table and backfill mapping
        if read_env("ENRICH_MF_ENABLED", "1") != "0":
            print("\n[STEP 11] MF asset class copy + mapping backfill")
            try:
                mf_db = read_env("VALIDATED_GLUE_DB", "default")
                mf_table = read_env("VALIDATED_TABLE", "plan_mf_history_v3")
                mapping_db = os.getenv("FUND_MAPPING_GLUE_DB", mf_db)
                mapping_table = read_env("FUND_MAPPING_TABLE", "fund_intelligence_mapping_mf")
                overwrite = read_env("ENRICH_MF_OVERWRITE", "0") == "1"

                to_update, to_backfill = enrich_mf_classes(
                    mf_db=mf_db,
                    mf_table=mf_table,
                    mapping_db=mapping_db,
                    mapping_table=mapping_table,
                    workgroup=read_env("ATHENA_WORKGROUP", "primary"),
                    s3_staging=read_env("ATHENA_STAGING_S3", ""),
                    overwrite_existing=overwrite,
                )
                print(f"  ✓ Estimated rows eligible for update: {to_update}")
                print(f"  ✓ Unmapped names backfilled to mapping: {to_backfill}")
            except Exception as exc:
                print(f"  ⚠ MF enrichment step failed: {exc}")
        else:
            print("\n[STEP 11] MF enrichment skipped (ENRICH_MF_ENABLED=0)")
    else:
        print("\n[STEP 10] Validation skipped (VALIDATION_ENABLED not set)")


    if read_env("STAGE_REPORT_ENABLED", "0") == "1":
        print("\n[STEP 12] Stage comparison reports")
        try:
            from .stage_report import generate_stage_compare, generate_stage_pivot

            data_dir = os.path.join(os.path.dirname(__file__), "..", "data", "outputs")
            compare_path = generate_stage_compare(data_dir, "/tmp")
            pivot_path = generate_stage_pivot(data_dir, "/tmp")
            print(f"  stage_compare -> {compare_path}")
            print(f"  stage_pivot   -> {pivot_path}")
        except ImportError:
            print("  stage report skipped: stage_report module not available")
        except Exception as exc:
            print(f"  stage report failed: {exc}")
    else:
        print("\n[STEP 12] Stage comparison reports skipped (STAGE_REPORT_ENABLED=0)")

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
