#!/usr/bin/env python3
import camelot
from src.utils import normalize_whitespace, load_yaml
from src.text_extract import _best_header_match

pdf_path = './data/inputs/GOOGLE_401K_Form5500_2024.pdf'

cfg = load_yaml('config/schema.yml')
synonyms = cfg['schema']['header_synonyms']

tables = camelot.read_pdf(pdf_path, pages='85', flavor='stream')
table = tables[0]
df = table.df

print(f"Table has {df.shape[0]} rows\n")

# Check all first 5 rows
for idx in range(min(5, df.shape[0])):
    potential_header = [normalize_whitespace(h) for h in df.iloc[idx].tolist()]
    print(f"\n--- Row {idx} ---")
    print("Content:", [h[:30] for h in potential_header])
    
    match_count = 0
    for i, h in enumerate(potential_header):
        field, score = _best_header_match(h, synonyms)
        if score >= 70:
            print(f"  Col {i}: '{h[:40]}' -> {field} ({score:.1f})")
            match_count += 1
    print(f"Match count: {match_count}")
