#!/usr/bin/env python3
import csv

# Read raw and clean CSVs
raw_rows = list(csv.DictReader(open('./data/outputs/investments_raw.csv', 'r', encoding='utf-8')))
clean_rows = list(csv.DictReader(open('./data/outputs/investments_clean.csv', 'r', encoding='utf-8')))

print(f"Raw rows: {len(raw_rows)}")
print(f"Clean rows: {len(clean_rows)}")
print(f"Removed during cleaning: {len(raw_rows) - len(clean_rows)}")

# Total raw values
raw_total = 0
for row in raw_rows:
    cv = row.get('current_value', '').strip()
    if cv and cv != '**':
        try:
            raw_total += float(cv.replace(',', '').replace('**', ''))
        except:
            pass

# Total clean values
clean_total = 0
for row in clean_rows:
    cv = row.get('current_value', '').strip()
    if cv and cv != '**':
        try:
            clean_total += float(cv.replace(',', '').replace('**', ''))
        except:
            pass

print(f"\nRaw CSV total: ${raw_total:,.2f}")
print(f"Clean CSV total: ${clean_total:,.2f}")
print(f"Difference: ${raw_total - clean_total:,.2f}")
