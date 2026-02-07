import pandas as pd
from src.data_cleaner import remove_total_rows, remove_metadata_rows

# Load raw data
df = pd.read_csv('data/outputs/investments_raw.csv')
print(f"Starting records: {len(df)}")

# Check if our target investments are there
pimco_total = df[df['issuer_name'].str.contains('PIMCO TOTAL', na=False)]
vg_tot = df[df['issuer_name'].str.contains('VG IS TOT', na=False)]

print(f"\nTarget investments in raw data:")
print(f"  PIMCO TOTAL: {len(pimco_total)}")
print(f"  VG IS TOT: {len(vg_tot)}")

if len(pimco_total) > 0:
    print(f"    - {pimco_total.iloc[0]['issuer_name']}")
if len(vg_tot) > 0:
    print(f"    - {vg_tot.iloc[0]['issuer_name']}")

# Step 1: Convert to list of dicts
rows = df.to_dict('records')

# Step 2: Remove total rows
print("\n=== Testing remove_total_rows ===")
filtered_rows, removed_totals = remove_total_rows(rows, verbose=True)
print(f"\nAfter remove_total_rows: {len(filtered_rows)} records")

# Check if targets are still there
pimco_count = sum(1 for row in filtered_rows if 'PIMCO TOTAL' in str(row.get('issuer_name', '')))
vg_count = sum(1 for row in filtered_rows if 'VG IS TOT' in str(row.get('issuer_name', '')))

print(f"\nTarget investments remaining:")
print(f"  PIMCO TOTAL: {pimco_count}")
print(f"  VG IS TOT: {vg_count}")

# Check removed totals for our targets
pimco_removed = sum(1 for row in removed_totals if 'PIMCO TOTAL' in str(row.get('issuer_name', '')))
vg_removed = sum(1 for row in removed_totals if 'VG IS TOT' in str(row.get('issuer_name', '')))

if pimco_removed > 0 or vg_removed > 0:
    print(f"\n❌ Found target investments in removed_totals:")
    for row in removed_totals:
        issuer = str(row.get('issuer_name', ''))
        if 'PIMCO TOTAL' in issuer or 'VG IS TOT' in issuer:
            print(f"    - {issuer}")
else:
    print(f"\n✓ Target investments were NOT removed by remove_total_rows")

# Step 3: Remove metadata rows
print("\n=== Testing remove_metadata_rows ===")
filtered_rows = remove_metadata_rows(filtered_rows, preserve_loans=True, verbose=False)
print(f"After remove_metadata_rows: {len(filtered_rows)} records")

pimco_count_after_metadata = sum(1 for row in filtered_rows if 'PIMCO TOTAL' in str(row.get('issuer_name', '')))
vg_count_after_metadata = sum(1 for row in filtered_rows if 'VG IS TOT' in str(row.get('issuer_name', '')))

print(f"\nTarget investments remaining:")
print(f"  PIMCO TOTAL: {pimco_count_after_metadata}")
print(f"  VG IS TOT: {vg_count_after_metadata}")

if pimco_count_after_metadata == 0 or vg_count_after_metadata == 0:
    print(f"\n❌ Some target investments were REMOVED by remove_metadata_rows!")

# Step 4: Check duplicates
from src.data_cleaner import remove_duplicates
print("\n=== Testing remove_duplicates ===")
filtered_rows = remove_duplicates(filtered_rows, verbose=False)
print(f"After remove_duplicates: {len(filtered_rows)} records")

pimco_count_final = sum(1 for row in filtered_rows if 'PIMCO TOTAL' in str(row.get('issuer_name', '')))
vg_count_final = sum(1 for row in filtered_rows if 'VG IS TOT' in str(row.get('issuer_name', '')))

print(f"\nTarget investments remaining:")
print(f"  PIMCO TOTAL: {pimco_count_final}")
print(f"  VG IS TOT: {vg_count_final}")

if pimco_count_final == 0 or vg_count_final == 0:
    print(f"\n❌ Some target investments were REMOVED by remove_duplicates!")
