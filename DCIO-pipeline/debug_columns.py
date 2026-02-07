#!/usr/bin/env python3
import camelot
from src.utils import normalize_whitespace

pdf_path = './data/inputs/GOOGLE_401K_Form5500_2024.pdf'

tables = camelot.read_pdf(pdf_path, pages='85', flavor='stream')
table = tables[0]
df = table.df

print("Column 0 (first 10 rows):")
for idx in range(min(10, df.shape[0])):
    val = str(df.iloc[idx, 0])
    print(f"  Row {idx}: '{normalize_whitespace(val)}'")

print("\nColumn 1 (first 10 rows):")
for idx in range(min(10, df.shape[0])):
    val = str(df.iloc[idx, 1])
    print(f"  Row {idx}: '{normalize_whitespace(val)[:50]}'")
