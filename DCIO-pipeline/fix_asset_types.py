#!/usr/bin/env python3
"""
Fix asset types in investments_clean.csv by re-parsing issuer names
"""
import csv
import sys
sys.path.insert(0, '.')
from src.data_cleaner import parse_investment_row

input_path = './data/outputs/investments_clean.csv'
output_path = './data/outputs/investments_clean.csv'

print("=" * 80)
print("FIXING ASSET TYPES IN CLEAN CSV")
print("=" * 80)

# Read the clean CSV
rows = []
with open(input_path, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    rows = list(reader)

print(f"\nProcessing {len(rows)} rows...")

fixed_count = 0
for idx, row in enumerate(rows, 1):
    original_issuer = row.get('issuer_name', '')
    original_asset = row.get('asset_type', '')
    
    # Re-parse the row to extract asset types
    parsed = parse_investment_row(row)
    
    # Update if changes detected
    if (parsed['issuer_name'] != original_issuer or 
        parsed['asset_type'] != original_asset):
        print(f"  [{idx}] Fixed:")
        print(f"      Issuer: '{original_issuer}' → '{parsed['issuer_name']}'")
        print(f"      Asset Type: '{original_asset}' → '{parsed['asset_type']}'")
        
        row['issuer_name'] = parsed['issuer_name']
        row['asset_type'] = parsed['asset_type']
        row['investment_description'] = parsed['investment_description']
        fixed_count += 1

print(f"\n✓ Fixed {fixed_count} rows")

# Save updated CSV
with open(output_path, 'w', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f"✓ Updated file saved: {output_path}")
print()
