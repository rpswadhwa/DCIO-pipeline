import sqlite3
from typing import Dict, List

from .utils import to_json


def init_db(db_path: str, schema_sql_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        with open(schema_sql_path, "r", encoding="utf-8") as f:
            conn.executescript(f.read())


def load_to_db(db_path: str, pages: List[Dict], pdfs: List[Dict], plan_info_map: Dict[str, Dict] = None) -> None:
    """
    Load pages and investment data to database.
    
    Args:
        db_path: Path to SQLite database
        pages: List of page data with mapped_rows
        pdfs: List of PDF metadata
        plan_info_map: Dict mapping pdf_stem to plan info (ein, plan_number, etc.)
    """
    if plan_info_map is None:
        plan_info_map = {}
    
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        plan_ids = {}
        ein_to_plan_id = {}
        
        for pdf in pdfs:
            pdf_stem = pdf.get("pdf_stem")
            plan_info = plan_info_map.get(pdf_stem, {})
            
            sponsor_ein = plan_info.get('ein')
            plan_name = plan_info.get('plan_name')
            plan_number = plan_info.get('plan_number', '001')
            sponsor = plan_info.get('sponsor')
            
            if not sponsor_ein:
                print(f"  ⚠ Warning: No EIN found for {pdf_stem}, skipping plan creation")
                continue
            
            try:
                cur.execute(
                    "INSERT INTO plans(sponsor_ein, plan_name, plan_number, sponsor, plan_year, source_pdf) VALUES (?, ?, ?, ?, ?, ?)",
                    (sponsor_ein, plan_name, plan_number, sponsor, 2024, pdf["pdf"],),
                )
                plan_id = cur.lastrowid
                plan_ids[pdf_stem] = plan_id
                ein_to_plan_id[sponsor_ein] = plan_id
            except sqlite3.IntegrityError as e:
                print(f"  ⚠ Warning: Could not insert plan for {pdf_stem}: {e}")
                # Try to get existing plan by EIN
                cur.execute("SELECT id FROM plans WHERE sponsor_ein = ?", (sponsor_ein,))
                row = cur.fetchone()
                if row:
                    plan_id = row[0]
                    plan_ids[pdf_stem] = plan_id
                    ein_to_plan_id[sponsor_ein] = plan_id

        for page in pages:
            pdf_stem = page.get("pdf_stem")
            plan_id = plan_ids.get(pdf_stem)
            
            if not plan_id:
                continue
            
            cur.execute(
                "INSERT INTO source_pages(plan_id, page_number, is_supplemental, image_path) VALUES (?, ?, ?, ?)",
                (plan_id, page["page_number"], page.get("is_supplemental", 0), page.get("normalized_path", page.get("image_path")),),
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

            # Find the EIN for this pdf_stem
            plan_info = plan_info_map.get(pdf_stem, {})
            sponsor_ein = plan_info.get('ein')
            
            if not sponsor_ein:
                continue
            
            for row in page.get("mapped_rows", []):
                cur.execute(
                    "INSERT INTO investments(sponsor_ein, page_number, row_id, issuer_name, investment_description, asset_type, par_value, cost, current_value, units_or_shares, confidence) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        sponsor_ein,
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
