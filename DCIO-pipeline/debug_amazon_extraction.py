#!/usr/bin/env python3
"""
Debug Amazon PDF extraction to see what's happening
"""
import pdfplumber
from src.text_extract import classify_pages_text, extract_tables_and_map

pdf_path = "data/inputs/AMAZON_401K_FORM5500_2024.pdf"

print("=" * 60)
print("DEBUGGING AMAZON PDF EXTRACTION")
print("=" * 60)

# Step 1: Classify pages
print("\n[STEP 1] Classifying pages...")
classified = classify_pages_text(pdf_path, "config/keywords.yml")
supp_nums = [p["page_number"] for p in classified if p.get("is_supplemental") == 1]

print(f"  Found {len(supp_nums)} supplemental pages: {supp_nums}")

# Step 2: Extract some sample text from these pages
print("\n[STEP 2] Examining supplemental pages...")
with pdfplumber.open(pdf_path) as pdf:
    for page_num in supp_nums[:3]:  # Check first 3 supplemental pages
        page = pdf.pages[page_num - 1]  # pdfplumber uses 0-based indexing
        text = page.extract_text()
        print(f"\n--- Page {page_num} (first 500 chars) ---")
        print(text[:500] if text else "[No text extracted]")

# Step 3: Try extracting tables
print("\n[STEP 3] Attempting table extraction...")
plan_info, extracted = extract_tables_and_map(
    pdf_path,
    supp_nums,
    "config/schema.yml",
    "gpt-4-mini",
    use_llm=False,
)

print(f"\nTotal pages processed: {len(extracted)}")
total_rows = sum(len(p.get('mapped_rows', [])) for p in extracted)
print(f"Total rows extracted: {total_rows}")

if extracted:
    print("\n[STEP 4] Sample extracted data:")
    for i, page_data in enumerate(extracted[:3]):  # Show first 3 pages
        mapped_rows = page_data.get('mapped_rows', [])
        print(f"\n  Page {page_data.get('page_number', '?')}: {len(mapped_rows)} rows")
        if mapped_rows:
            for j, row in enumerate(mapped_rows[:3]):  # Show first 3 rows
                issuer = row.get('issuer_name', '')[:40]
                desc = row.get('investment_description', '')[:40]
                value = row.get('current_value', '')
                print(f"    Row {j+1}: {issuer} | {desc} | ${value}")
