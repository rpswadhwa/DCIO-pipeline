#!/usr/bin/env python3
"""
Restore missing investment descriptions from the raw CSV
"""
import sqlite3
import csv
from pathlib import Path


def restore_descriptions_from_raw(db_path, raw_csv_path, verbose=True):
    """
    Restore investment descriptions from raw CSV where they're missing
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    if verbose:
        print("\n[STEP 1] Reading raw CSV data...")
    
    # Read raw CSV to get original descriptions
    raw_data = {}
    with open(raw_csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Create key from identifying fields
            key = (
                row['sponsor_ein'],
                row['plan_number'],
                row['plan_year'],
                row['page_number'],
                row['row_id']
            )
            # Store issuer and description
            issuer = row.get('issuer_name', '').strip()
            desc = row.get('investment_description', '').strip()
            if issuer and desc and desc not in ['(a) or Similar Party', 's']:
                raw_data[key] = (issuer, desc)
    
    if verbose:
        print(f"  Found {len(raw_data)} entries with descriptions in raw CSV")
        print("\n[STEP 2] Finding investments needing description updates...")
    
    # Find investments where description is missing or same as issuer
    cursor.execute("""
        SELECT 
            sponsor_ein, plan_number, plan_year, page_number, row_id,
            issuer_name, investment_description
        FROM investments
        WHERE investment_description IS NULL 
           OR investment_description = '' 
           OR investment_description = issuer_name
    """)
    
    needs_update = cursor.fetchall()
    
    if verbose:
        print(f"  Found {len(needs_update)} investments needing updates")
        print("\n[STEP 3] Restoring descriptions from raw data...")
    
    updates = []
    for ein, pnum, year, page, row_id, issuer, desc in needs_update:
        key = (ein, pnum, str(year), str(page), str(row_id))
        if key in raw_data:
            raw_issuer, raw_desc = raw_data[key]
            # Update description if we have it
            if raw_desc and raw_desc != desc:
                updates.append((raw_desc, ein, pnum, year, page, row_id))
                if verbose and len(updates) <= 10:
                    print(f"  Row {row_id} (page {page}):")
                    print(f"    Issuer: {issuer}")
                    print(f"    Old Desc: {desc or '(none)'}")
                    print(f"    New Desc: {raw_desc}")
    
    if updates:
        if verbose:
            print(f"\n  Applying {len(updates)} updates...")
        
        cursor.executemany("""
            UPDATE investments
            SET investment_description = ?
            WHERE sponsor_ein = ? AND plan_number = ? AND plan_year = ?
              AND page_number = ? AND row_id = ?
        """, updates)
        
        conn.commit()
        
        if verbose:
            print(f"  ✓ Updated {len(updates)} descriptions")
    else:
        if verbose:
            print("  No updates needed")
    
    conn.close()
    return len(updates)


def export_updated_csv(db_path, output_path, verbose=True):
    """
    Export updated data to CSV
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    if verbose:
        print("\n[STEP 4] Exporting updated data to CSV...")
    
    cursor.execute("""
        SELECT 
            i.sponsor_ein,
            i.plan_number,
            i.plan_year,
            p.sponsor,
            p.plan_name,
            i.issuer_name,
            i.investment_description,
            i.asset_type,
            i.par_value,
            i.cost,
            i.current_value,
            i.units_or_shares,
            i.page_number,
            i.row_id,
            i.confidence
        FROM investments i
        JOIN plans p ON 
            i.sponsor_ein = p.sponsor_ein AND 
            i.plan_number = p.plan_number AND 
            i.plan_year = p.plan_year
        ORDER BY i.sponsor_ein, i.page_number, i.row_id
    """)
    
    rows = cursor.fetchall()
    
    fieldnames = [
        'sponsor_ein', 'plan_number', 'plan_year', 'sponsor', 'plan_name',
        'issuer_name', 'investment_description', 'asset_type',
        'par_value', 'cost', 'current_value', 'units_or_shares',
        'page_number', 'row_id', 'confidence'
    ]
    
    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))
    
    if verbose:
        print(f"  ✓ Exported {len(rows)} records to {output_path}")
    
    conn.close()
    return len(rows)


def main():
    db_path = Path('data/outputs/pipeline.db')
    raw_csv = Path('data/outputs/investments_raw.csv')
    output_csv = Path('data/outputs/investments_clean.csv')
    
    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        return
    
    if not raw_csv.exists():
        print(f"❌ Raw CSV not found: {raw_csv}")
        return
    
    print("=" * 80)
    print("RESTORE MISSING INVESTMENT DESCRIPTIONS FROM RAW DATA")
    print("=" * 80)
    
    # Restore descriptions
    updated = restore_descriptions_from_raw(db_path, raw_csv, verbose=True)
    
    # Export updated CSV
    if updated > 0:
        exported = export_updated_csv(db_path, output_csv, verbose=True)
    
    print("\n" + "=" * 80)
    print(f"✓ Complete: {updated} descriptions restored")
    print("=" * 80)


if __name__ == '__main__':
    main()
