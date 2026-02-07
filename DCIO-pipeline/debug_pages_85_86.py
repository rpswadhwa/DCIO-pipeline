#!/usr/bin/env python3
import camelot
from src.utils import normalize_whitespace, load_yaml
from src.text_extract import _best_header_match

pdf_path = './data/inputs/GOOGLE_401K_Form5500_2024.pdf'

# Check pages 85 and 86
for page_num in [85, 86]:
    print(f'\n=== PAGE {page_num} ===')
    tables = camelot.read_pdf(pdf_path, pages=str(page_num), flavor='stream')
    print(f'Found {len(tables)} tables\n')
    
    for i, table in enumerate(tables):
        df = table.df
        print(f'Table {i}: {df.shape[0]} rows x {df.shape[1]} cols')
        
        if df.shape[0] > 0:
            header = [normalize_whitespace(h) for h in df.iloc[0].tolist()]
            print('Header:', header[:6])
            
            cfg = load_yaml('config/schema.yml')
            synonyms = cfg['schema']['header_synonyms']
            
            has_match = False
            for j, h in enumerate(header[:6]):
                field, score = _best_header_match(h, synonyms)
                if score > 70:
                    print(f'  Col {j}: "{h[:40]}" -> {field} ({score:.1f})')
                    has_match = True
            
            if not has_match:
                print('  (no strong matches in first 6 cols)')
            
            # Show first few data rows
            print('First 3 rows:')
            for row_idx in range(1, min(4, df.shape[0])):
                row = [str(c)[:30] for c in df.iloc[row_idx].tolist()[:6]]
                print(f'  {row}')
        print()
