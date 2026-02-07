#!/usr/bin/env python3
import sys
import camelot
from src.utils import normalize_whitespace, load_yaml

pdf_path = "./data/inputs/GOOGLE_401K_Form5500_2024.pdf"

# Get all supplemental pages
import pdfplumber
from src.text_extract import classify_pages_text

pages = classify_pages_text(pdf_path, "config/keywords.yml")
supp_nums = [p["page_number"] for p in pages if p.get("is_supplemental") == 1]
print(f"Supplemental pages: {supp_nums}")

if supp_nums:
    pages_arg = ",".join(str(p) for p in supp_nums[:3])  # Test first 3
    print(f"\nReading tables from pages: {pages_arg}")
    
    tables = camelot.read_pdf(pdf_path, pages=pages_arg, flavor="stream")
    print(f"Found {len(tables)} tables")
    
    for i, table in enumerate(tables):
        print(f"\n--- Table {i} (Page {table.page}) ---")
        df = table.df
        print(f"Shape: {df.shape}")
        print("Header row:")
        header = [normalize_whitespace(h) for h in df.iloc[0].tolist()]
        for j, h in enumerate(header):
            print(f"  Col {j}: '{h}'")
        
        print("\nFirst 3 data rows:")
        for row_idx in range(1, min(4, df.shape[0])):
            row = df.iloc[row_idx].tolist()
            print(f"  Row {row_idx}: {[str(c)[:30] for c in row]}")
        
        # Check header matching
        cfg = load_yaml("config/schema.yml")
        synonyms = cfg["schema"]["header_synonyms"]
        from src.text_extract import _best_header_match
        
        print("\nHeader mapping:")
        for j, h in enumerate(header):
            field, score = _best_header_match(h, synonyms)
            print(f"  Col {j} '{h}' -> {field if field else 'NO MATCH'} (score: {score})")
