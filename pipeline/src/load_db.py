import sqlite3
from typing import Dict, List

from .utils import to_json


def init_db(db_path: str, schema_sql_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        with open(schema_sql_path, "r", encoding="utf-8") as f:
            conn.executescript(f.read())


def load_to_db(db_path: str, pages: List[Dict], pdfs: List[Dict]) -> None:
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        plan_ids = {}
        for pdf in pdfs:
            cur.execute(
                "INSERT INTO plans(plan_name, sponsor, plan_year, source_pdf) VALUES (?, ?, ?, ?)",
                (None, None, 2024, pdf["pdf"],),
            )
            plan_ids[pdf["pdf_stem"]] = cur.lastrowid

        for page in pages:
            plan_id = plan_ids.get(page["pdf_stem"], None)
            cur.execute(
                "INSERT INTO source_pages(plan_id, page_number, is_supplemental, image_path) VALUES (?, ?, ?, ?)",
                (plan_id, page["page_number"], page.get("is_supplemental", 0), page["normalized_path"],),
            )

            for cell in page.get("ocr_cells", []):
                cur.execute(
                    "INSERT INTO ocr_cells(page_number, row_id, cell_id, bbox, text, confidence) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        page["page_number"],
                        cell.get("row_id", 0),
                        cell.get("cell_id", 0),
                        to_json(cell.get("bbox", [])),
                        cell.get("text", ""),
                        float(cell.get("confidence", 0.0)),
                    ),
                )

            for row in page.get("mapped_rows", []):
                cur.execute(
                    "INSERT INTO investments(plan_id, page_number, row_id, issuer_name, investment_description, asset_type, par_value, cost, current_value, units_or_shares, confidence) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        plan_id,
                        row["page_number"],
                        row["row_id"],
                        row.get("issuer_name"),
                        row.get("investment_description"),
                        row.get("asset_type"),
                        row.get("par_value"),
                        row.get("cost"),
                        row.get("current_value"),
                        row.get("units_or_shares"),
                        None,
                    ),
                )

        conn.commit()
