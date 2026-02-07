#!/usr/bin/env python3
"""
Complete pipeline: Extract all Form 5500 documents, clean tables, and update database
"""
import os
import csv
import sqlite3
import json
from pathlib import Path
from dotenv import load_dotenv
from src.text_extract import classify_pages_text, extract_tables_and_map
from src.utils import load_yaml, read_env, ensure_dir
from src.data_cleaner import clean_investment_data

load_dotenv()

# Configuration
input_dir = read_env("INPUT_DIR", "data/inputs")
output_dir = read_env("OUTPUT_DIR", "data/outputs")
schema_yml = read_env("SCHEMA_YML", "config/schema.yml")
db_path = os.path.join(output_dir, "pipeline.db")
raw_csv_path = os.path.join(output_dir, "investments_raw.csv")
clean_csv_path = os.path.join(output_dir, "investments_clean.csv")

use_llm = False  # Disable to avoid quota issues
use_ocr = False

print("=" * 60)
print("FORM 5500 PIPELINE - FULL EXTRACTION AND CLEANUP")
print("=" * 60)

# Step 1: Extract from all PDFs
print("\n[STEP 1] Extracting data from all Form 5500 documents...")
all_investments = []
plan_info = {}

for fname in sorted(os.listdir(input_dir)):
    if not fname.lower().endswith(".pdf"):
        continue
    
    pdf_path = os.path.join(input_dir, fname)
    pdf_stem = fname.rsplit(".", 1)[0]
    
    print(f"\n  Processing: {fname}")
    
    # Classify pages
    classified = classify_pages_text(pdf_path, "config/keywords.yml")
    supp_nums = [p["page_number"] for p in classified if p.get("is_supplemental") == 1]
    
    print(f"    Found {len(supp_nums)} supplemental pages: {supp_nums}")
    
    if supp_nums:
        # Extract tables from supplemental pages
        plan_info_dict, extracted = extract_tables_and_map(
            pdf_path,
            supp_nums,
            schema_yml,
            "gpt-4-mini",
            use_llm=use_llm,
        )
        
        # Store plan info for later use
        if plan_info_dict:
            plan_info[pdf_stem] = plan_info_dict
        
        # Flatten the mapped_rows into individual records
        for page_data in extracted:
            for row in page_data.get("mapped_rows", []):
                row["pdf_name"] = fname
                row["pdf_stem"] = pdf_stem
                all_investments.append(row)
        
        print(f"    Extracted {sum(len(p.get('mapped_rows', [])) for p in extracted)} investment records")

print(f"\n  Total records extracted: {len(all_investments)}")

# Step 2: Save raw CSV
print("\n[STEP 2] Saving raw extracted data...")
if all_investments:
    fieldnames = list(all_investments[0].keys())
    with open(raw_csv_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_investments)
    print(f"  ✓ Raw CSV saved: {raw_csv_path}")

# Step 3: Clean the CSV (remove totals, metadata, duplicates)
print("\n[STEP 3] Cleaning investment data...")

clean_investments, removed_totals = clean_investment_data(
    all_investments,
    preserve_loans=True,
    remove_dupes=True,
    verbose=True
)

# Save removed total rows for verification
if removed_totals:
    removed_totals_path = os.path.join(output_dir, "removed_total_rows.csv")
    with open(removed_totals_path, 'w', encoding='utf-8', newline='') as f:
        if removed_totals:
            fieldnames = removed_totals[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(removed_totals)
    print(f"  ✓ Removed total rows saved: {removed_totals_path}")

# Step 4: Save clean CSV
print("\n[STEP 4] Saving cleaned data...")
if clean_investments:
    fieldnames = list(clean_investments[0].keys())
    with open(clean_csv_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(clean_investments)
    print(f"  ✓ Clean CSV saved: {clean_csv_path}")

# Step 5: Update database
print("\n[STEP 5] Updating database with cleaned data...")

with sqlite3.connect(db_path) as conn:
    cur = conn.cursor()
    
    # Delete all old investment data
    cur.execute("DELETE FROM investments")
    print(f"  Deleted old investment records")
    
    # Get or create plans for each PDF with EIN
    inserted_count = 0
    plan_id_by_stem = {}
    
    for pdf_stem, info in plan_info.items():
        sponsor_ein = info.get('ein')
        plan_name = info.get('plan_name')
        plan_number = info.get('plan_number', '001')
        sponsor = info.get('sponsor')
        
        if not sponsor_ein:
            print(f"  ⚠ Warning: No EIN found for {pdf_stem}, skipping")
            continue
        
        # Check if plan exists
        cur.execute("SELECT id FROM plans WHERE sponsor_ein = ?", (sponsor_ein,))
        result = cur.fetchone()
        
        if result:
            plan_id = result[0]
            # Update plan info
            cur.execute(
                "UPDATE plans SET plan_name = ?, plan_number = ?, sponsor = ? WHERE id = ?",
                (plan_name, plan_number, sponsor, plan_id)
            )
        else:
            # Find the source PDF name for this stem
            pdf_name = next((r.get('pdf_name') for r in clean_investments if r.get('pdf_stem') == pdf_stem), None)
            
            cur.execute(
                "INSERT INTO plans(sponsor_ein, plan_name, plan_number, sponsor, plan_year, source_pdf) VALUES (?, ?, ?, ?, ?, ?)",
                (sponsor_ein, plan_name, plan_number, sponsor, 2024, pdf_name)
            )
            plan_id = cur.lastrowid
        
        plan_id_by_stem[pdf_stem] = {'plan_id': plan_id, 'ein': sponsor_ein}
        plan_id_by_stem[pdf_stem] = {'plan_id': plan_id, 'ein': sponsor_ein}
        
        # Insert investments for this plan
        for row in clean_investments:
            if row.get('pdf_stem') != pdf_stem:
                continue
            
            # Parse numeric fields safely
            def safe_float(value, default=None):
                """Convert string to float, handling special cases"""
                if not value or not isinstance(value, str):
                    return default
                value = value.strip().replace(',', '').replace('**', '')
                try:
                    return float(value)
                except ValueError:
                    return default
            
            par_value = safe_float(row.get('par_value', ''))
            cost = safe_float(row.get('cost', ''))
            current_value = safe_float(row.get('current_value', ''))
            units_or_shares = safe_float(row.get('units_or_shares', ''))
            
            page_number = int(row.get('page_number', 0)) if row.get('page_number', '') else 0
            row_id = int(row.get('row_id', 0)) if row.get('row_id', '') else 0
            
            cur.execute("""
                INSERT INTO investments 
                (sponsor_ein, page_number, row_id, issuer_name, investment_description, 
                 asset_type, par_value, cost, current_value, units_or_shares, confidence)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sponsor_ein,
                page_number,
                row_id,
                row.get('issuer_name', ''),
                row.get('investment_description', ''),
                row.get('asset_type', ''),
                par_value,
                cost,
                current_value,
                units_or_shares,
                1.0
            ))
            inserted_count += 1
    
    conn.commit()
    
    # Show summary
    cur.execute("SELECT COUNT(DISTINCT sponsor_ein) FROM investments")
    plan_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM investments")
    total_count = cur.fetchone()[0]
    
    print(f"  ✓ Database updated!")
    print(f"    Plans: {plan_count}")
    print(f"    Total investments: {total_count}")
    
    # Summary by PDF
    cur.execute("""
        SELECT p.plan_name, p.sponsor, COUNT(*) as count, SUM(i.current_value) as total_value
        FROM investments i
        JOIN plans p ON i.sponsor_ein = p.sponsor_ein
        GROUP BY p.sponsor_ein
    """)
    
    print(f"\n  Summary by plan:")
    for plan_name, sponsor, count, total_value in cur.fetchall():
        total_value = total_value or 0
        sponsor_display = sponsor or plan_name or 'Unknown'
        print(f"    {sponsor_display}: {count} holdings, ${total_value:,.2f}")

# Step 6: Enhancement - Populate missing asset_type fields
print("\n[STEP 6] Enhancement: Populating missing asset_type fields...")
from enhance_asset_types import enhance_asset_types

try:
    updated = enhance_asset_types(db_path, verbose=True)
    if updated > 0:
        print(f"  ✓ Enhanced {updated} records with asset_type")
    else:
        print(f"  ✓ All records already have asset_type")
except Exception as e:
    print(f"  ⚠ Enhancement failed: {e}")

print("\n" + "=" * 60)
print("✓ PIPELINE COMPLETE!")
print("=" * 60)
