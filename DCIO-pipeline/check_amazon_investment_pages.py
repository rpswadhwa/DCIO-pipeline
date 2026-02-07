#!/usr/bin/env python3
"""
Check Amazon Schedule H Line 4(i) pages specifically
"""
import pdfplumber

pdf_path = "data/inputs/AMAZON_401K_FORM5500_2024.pdf"
investment_pages = [380, 381]  # Schedule H Line 4(i) pages

print("=" * 80)
print("CHECKING AMAZON SCHEDULE H LINE 4(i) PAGES")
print("=" * 80)

with pdfplumber.open(pdf_path) as pdf:
    for page_num in investment_pages:
        print(f"\n{'=' * 80}")
        print(f"PAGE {page_num}")
        print("=" * 80)
        
        page = pdf.pages[page_num - 1]
        text = page.extract_text()
        
        # Show first 1200 characters
        if text:
            print("\n--- TEXT CONTENT (first 1200 chars) ---")
            print(text[:1200])
            
            # Check for investment-related keywords
            keywords = ["Schedule H", "Line 4", "Assets", "Current Value", "Identity of Issue", 
                       "EMPLOYER IDENTIFICATION NUMBER", "EIN"]
            found_keywords = [kw for kw in keywords if kw in text]
            if found_keywords:
                print(f"\n✓ Found keywords: {', '.join(found_keywords)}")
        else:
            print("\n⚠ NO TEXT EXTRACTED")
        
        # Try to extract tables
        tables = page.extract_tables()
        print(f"\n--- TABLES DETECTED: {len(tables)} ---")
        if tables:
            for i, table in enumerate(tables[:2]):  # Show first 2 tables
                print(f"\nTable {i+1}: {len(table)} rows x {len(table[0]) if table and table[0] else 0} cols")
                if table and len(table) > 0:
                    print("\nFirst few rows:")
                    for j, row in enumerate(table[:5]):  # Show first 5 rows
                        print(f"  Row {j+1}: {row}")
