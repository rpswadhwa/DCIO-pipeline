#!/usr/bin/env python3
"""
Quick inspection of Amazon supplemental pages to see what's there
"""
import pdfplumber

pdf_path = "data/inputs/AMAZON_401K_FORM5500_2024.pdf"
check_pages = [349, 353, 354, 355, 357, 358, 366, 380, 381]

print("=" * 80)
print("INSPECTING AMAZON SUPPLEMENTAL PAGES")
print("=" * 80)

with pdfplumber.open(pdf_path) as pdf:
    for page_num in check_pages[:5]:  # Check first 5 pages
        print(f"\n{'=' * 80}")
        print(f"PAGE {page_num}")
        print("=" * 80)
        
        page = pdf.pages[page_num - 1]
        text = page.extract_text()
        
        # Show first 800 characters
        if text:
            print("\n--- TEXT CONTENT (first 800 chars) ---")
            print(text[:800])
            
            # Check for keywords
            if "Schedule H" in text:
                print("\n✓ Contains 'Schedule H'")
            if "Assets" in text:
                print("✓ Contains 'Assets'")
            if "Current Value" in text:
                print("✓ Contains 'Current Value'")
        else:
            print("\n⚠ NO TEXT EXTRACTED")
        
        # Try to extract tables
        tables = page.extract_tables()
        print(f"\n--- TABLES DETECTED: {len(tables)} ---")
        if tables:
            for i, table in enumerate(tables[:2]):  # Show first 2 tables
                print(f"\nTable {i+1}: {len(table)} rows x {len(table[0]) if table else 0} cols")
                if table:
                    for row in table[:3]:  # Show first 3 rows
                        print(f"  {row}")
