#!/usr/bin/env python3
"""
Remove "Total" rows from clean CSV and update database
"""
import csv
import sqlite3
from pathlib import Path

clean_csv_path = "./data/outputs/investments_clean.csv"
db_path = "./data/outputs/pipeline.db"

print("=" * 60)
print("CLEANING: Removing 'Total' rows")
print("=" * 60)

# Read the clean CSV
rows = []
with open(clean_csv_path, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

print(f"\nBefore: {len(rows)} records")

# Filter out "Total" rows
filtered_rows = []
removed_count = 0

for row in rows:
    issuer = row.get('issuer_name', '').strip()
    description = row.get('investment_description', '').strip()
    
    # Skip if issuer is "Total" with no description
    if issuer.lower() == 'total' and not description:
        print(f"  Removing: Total row")
        removed_count += 1
        continue
    
    filtered_rows.append(row)

print(f"After: {len(filtered_rows)} records")
print(f"Removed: {removed_count} Total rows")

# Save updated clean CSV
with open(clean_csv_path, 'w', encoding='utf-8', newline='') as f:
    fieldnames = filtered_rows[0].keys() if filtered_rows else []
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(filtered_rows)

print(f"\n✓ Clean CSV updated: {clean_csv_path}")

# Update database
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
    for row in filtered_rows:
        pdf_name = row.get('pdf_name', '')
        plan_id = plans.get(pdf_name)
        
        if not plan_id:
            print(f"  Warning: Plan not found for {pdf_name}")
            continue
        
        # Parse numeric fields
        par_value = row.get('par_value', '').strip()
        par_value = float(par_value.replace(',', '')) if par_value and par_value != '**' else None
        
        cost = row.get('cost', '').strip()
        cost = float(cost.replace(',', '')) if cost and cost != '**' else None
        
        current_value = row.get('current_value', '').strip()
        current_value = float(current_value.replace(',', '').replace('**', '')) if current_value else None
        
        units_or_shares = row.get('units_or_shares', '').strip()
        units_or_shares = float(units_or_shares.replace(',', '')) if units_or_shares and units_or_shares != '**' else None
        
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
print("✓ CLEANUP COMPLETE!")
print("=" * 60)
