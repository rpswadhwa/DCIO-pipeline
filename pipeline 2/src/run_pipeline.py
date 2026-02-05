import json
import os
from pathlib import Path

from .ingest import ingest_pdfs
from .classify_pages import classify_pages
from .normalize_images import normalize_pages
from .detect_tables import detect_tables
from .ocr_passes import run_ocr
from .llm_map import map_rows_with_llm
from .validate import validate_pages
from .load_db import init_db, load_to_db
from .export_csv import export_csv
from .utils import ensure_dir, read_env


def main():
    input_dir = read_env("INPUT_DIR", "data/inputs")
    output_dir = read_env("OUTPUT_DIR", "data/outputs")
    images_dir = os.path.join(output_dir, "images")
    ensure_dir(output_dir)
    ensure_dir(images_dir)

    dpi = int(read_env("DPI", "350"))
    model = read_env("OPENAI_MODEL", "gpt-4.1-mini")
    use_llm = read_env("USE_LLM", "1") != "0"

    keywords_yml = read_env("KEYWORDS_YML", "config/keywords.yml")
    schema_yml = read_env("SCHEMA_YML", "config/schema.yml")

    db_path = os.path.join(output_dir, "pipeline.db")
    qa_report_path = os.path.join(output_dir, "qa_report.json")
    csv_path = os.path.join(output_dir, "investments.csv")
    schema_sql = read_env("SCHEMA_SQL", "sql/schema.sql")

    pages = ingest_pdfs(input_dir, images_dir, dpi=dpi)
    pages = classify_pages(pages, keywords_yml)
    supplemental_pages = [p for p in pages if p.get("is_supplemental") == 1]

    supplemental_pages = normalize_pages(supplemental_pages)
    supplemental_pages = detect_tables(supplemental_pages)
    supplemental_pages = run_ocr(supplemental_pages)
    supplemental_pages = map_rows_with_llm(supplemental_pages, schema_yml, model, use_llm=use_llm)

    qa = validate_pages(supplemental_pages, schema_yml)
    with open(qa_report_path, "w", encoding="utf-8") as f:
        json.dump(qa, f, ensure_ascii=True, indent=2)

    init_db(db_path, schema_sql)
    load_to_db(db_path, supplemental_pages, pages)

    export_csv(supplemental_pages, schema_yml, csv_path)


if __name__ == "__main__":
    main()
