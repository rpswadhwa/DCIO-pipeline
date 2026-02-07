#!/usr/bin/env python3
"""
Remove duplicates and "Total" rows from clean CSV and update database
Preserves participant loan entries for QA validation
"""
import csv
import sqlite3
from pathlib import Path
from src.data_cleaner import clean_investment_data

clean_csv_path = "./data/outputs/investments_clean.csv"
raw_csv_path = "./data/outputs/investments_raw.csv"
db_path = "./data/outputs/pipeline.db"

print("=" * 60)
print("DATA CLEANING: Removing Duplicates and 'Total' rows")
print("=" * 60)

# Read the clean CSV
rows = []
with open(clean_csv_path, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

# Apply comprehensive cleanup
deduplicated_rows, removed_total_rows = clean_investment_data(
    rows,
    preserve_loans=True,
    remove_dupes=True,
    verbose=True
)

# Save removed total rows to a separate file for manual verification
if removed_total_rows:
    removed_totals_path = "./data/outputs/removed_total_rows.csv"
    with open(removed_totals_path, 'w', encoding='utf-8', newline='') as f:
        fieldnames = removed_total_rows[0].keys() if removed_total_rows else []
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(removed_total_rows)
    
    print(f"\n✓ Removed total rows saved: {removed_totals_path}")
    print(f"  ({len(removed_total_rows)} rows for manual verification)")

# Save updated clean CSV
with open(clean_csv_path, 'w', encoding='utf-8', newline='') as f:
    fieldnames = deduplicated_rows[0].keys() if deduplicated_rows else []
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(deduplicated_rows)

print(f"\n✓ Clean CSV updated: {clean_csv_path}")

# Step 4: Update database
print(f"\nUpdating database...")

with sqlite3.connect(db_path) as conn:
    cur = conn.cursor()
    
    # Delete all investment records
    cur.execute("DELETE FROM investments")
    print("  Deleted old investment records")
    
    # Get all plans
    cur.execute("SELECT id, source_pdf FROM plans")
    plans = {row[1]: row[0] for row in cur.fetchall()}
    
    # Insert cleaned data
    inserted_count = 0
    
    # Helper function for safe numeric conversion
    def safe_float(value, default=None):
        """Convert string to float, handling special cases"""
        if not value or not isinstance(value, str):
            return default
        value = value.strip().replace(',', '').replace('**', '')
        try:
            return float(value)
        except ValueError:
            return default
    
    for row in deduplicated_rows:
        pdf_name = row.get('pdf_name', '')
        plan_id = plans.get(pdf_name)
        
        if not plan_id:
            print(f"  Warning: Plan not found for {pdf_name}")
            continue
        
        # Parse numeric fields safely
        par_value = safe_float(row.get('par_value', ''))
        cost = safe_float(row.get('cost', ''))
        current_value = safe_float(row.get('current_value', ''))
        units_or_shares = safe_float(row.get('units_or_shares', ''))
        
        page_number = int(row.get('page_number', 0)) if row.get('page_number', '') else 0
        row_id = int(row.get('row_id', 0)) if row.get('row_id', '') else 0
        
        cur.execute("""
            INSERT INTO investments 
            (plan_id, page_number, row_id, issuer_name, investment_description, 
             asset_type, par_value, cost, current_value, units_or_shares, confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            plan_id,
            page_number,
            row_id,
            row.get('issuer_name', ''),
            row.get('investment_description', ''),
            row.get('asset_type', ''),
            par_value,
            cost,
            current_value,
            units_or_shares,
            1.0
        ))
        inserted_count += 1
    
    conn.commit()
    
    # Show summary
    cur.execute("SELECT COUNT(*) FROM investments")
    final_count = cur.fetchone()[0]
    
    print(f"  Inserted: {inserted_count} records")
    print(f"  Total in database: {final_count}")
    
    # Summary by plan
    cur.execute("""
        SELECT p.source_pdf, COUNT(*) as count, SUM(i.current_value) as total_value
        FROM investments i
        JOIN plans p ON i.plan_id = p.id
        GROUP BY p.source_pdf
        ORDER BY total_value DESC
    """)
    
    print(f"\n  Summary by document:")
    total_value = 0
    for pdf_name, count, value in cur.fetchall():
        value = value or 0
        total_value += value
        print(f"    {pdf_name}: {count} holdings, ${value:,.2f}")
    
    print(f"\n  Portfolio total: ${total_value:,.2f}")

print("\n" + "=" * 60)
print("CLEANUP SUMMARY")
print("=" * 60)
print(f"Total rows removed:")
print(f"  - 'Total' rows: {removed_totals}")
print(f"  - Duplicates: {removed_duplicates}")
print(f"  - Net result: {len(rows)} → {len(deduplicated_rows)} records")
print("=" * 60)
print("✓ DATA CLEANING COMPLETE!")
print("=" * 60)

# ============================================================
# QA VALIDATION: Compare cleaned totals against raw CSV totals
# ============================================================
print("\n" + "=" * 60)
print("QA VALIDATION: Investment Totals by Document")
print("=" * 60)

# Extract totals from raw CSV (there are multiple total rows per document)
raw_totals = {}
with open(raw_csv_path, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        issuer = row.get('issuer_name', '').strip()
        pdf_name = row.get('pdf_name', '').strip()
        
        # Look for "Total Investments" or standalone "Total" rows
        if (issuer.lower() == 'total investments' or 
            (issuer.lower() == 'total' and pdf_name)):
            current_value = row.get('current_value', '').strip()
            if current_value:
                try:
                    value = float(current_value.replace(',', '').replace('**', ''))
                    # Use the highest value found for each document (since duplicates exist)
                    if pdf_name not in raw_totals or value > raw_totals[pdf_name]:
                        raw_totals[pdf_name] = value
                except ValueError:
                    pass

# Calculate cleaned data total by document
cleaned_totals = {}
for row in deduplicated_rows:
    pdf_name = row.get('pdf_name', '').strip()
    current_value = row.get('current_value', '').strip()
    
    if pdf_name:
        if pdf_name not in cleaned_totals:
            cleaned_totals[pdf_name] = 0
        
        if current_value and current_value != '**':
            try:
                value = float(current_value.replace(',', '').replace('**', ''))
                cleaned_totals[pdf_name] += value
            except ValueError:
                pass

# Calculate loan total by document
loan_totals = {}
for loan_row in loan_entries:
    pdf_name = loan_row.get('pdf_name', '').strip()
    current_value = loan_row.get('current_value', '').strip()
    
    if pdf_name:
        if pdf_name not in loan_totals:
            loan_totals[pdf_name] = 0
        
        if current_value and current_value != '**':
            try:
                value = float(current_value.replace(',', '').replace('**', ''))
                loan_totals[pdf_name] += value
            except ValueError:
                pass

# Display comparison
print("\nDocument-Level Totals Comparison:")
print("-" * 60)

total_raw = 0
total_cleaned_with_loans = 0
all_match = True

for pdf_name in sorted(set(list(raw_totals.keys()) + list(cleaned_totals.keys()))):
    raw_val = raw_totals.get(pdf_name, 0)
    cleaned_val = cleaned_totals.get(pdf_name, 0)
    loan_val = loan_totals.get(pdf_name, 0)
    combined_val = cleaned_val + loan_val
    
    total_raw += raw_val
    total_cleaned_with_loans += combined_val
    
    print(f"\n{pdf_name}:")
    print(f"  Raw CSV total:              ${raw_val:,.2f}")
    print(f"  Cleaned investments:        ${cleaned_val:,.2f}")
    print(f"  Participant loans:          ${loan_val:,.2f}")
    print(f"  Combined total:             ${combined_val:,.2f}")
    
    if raw_val > 0:
        diff = abs(raw_val - combined_val)
        pct_diff = (diff / raw_val * 100) if raw_val != 0 else 0
        print(f"  Difference:                 ${diff:,.2f} ({pct_diff:.2f}%)")
        
        if diff > 1 and pct_diff > 0.01:
            all_match = False
            print(f"  ⚠ MISMATCH")
        else:
            print(f"  ✓ MATCH")

print("\n" + "-" * 60)
print(f"GRAND TOTALS:")
print(f"  Raw CSV total:              ${total_raw:,.2f}")
print(f"  Cleaned data with loans:    ${total_cleaned_with_loans:,.2f}")

if total_raw > 0:
    diff = abs(total_raw - total_cleaned_with_loans)
    pct_diff = (diff / total_raw * 100) if total_raw != 0 else 0
    print(f"  Difference:                 ${diff:,.2f} ({pct_diff:.2f}%)")
    
    if all_match:
        print(f"\n✓ QA PASS: All totals match")
    else:
        print(f"\n⚠ QA WARNING: Some totals differ")

print("=" * 60)

