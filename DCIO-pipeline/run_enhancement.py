#!/usr/bin/env python3
"""
Standalone script to enhance asset types and update CSV exports
Run this after complete_pipeline.py to enrich the dataset with inferred asset types
"""
import sqlite3
import csv
from pathlib import Path
from enhance_asset_types import enhance_asset_types


def export_to_csv(db_path, output_path, verbose=True):
    """
    Export investments from database to CSV with all composite key columns
    
    Args:
        db_path: Path to SQLite database
        output_path: Path to output CSV file
        verbose: Print progress
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Query investments with plan details using composite key join
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
        ORDER BY p.sponsor_ein, i.page_number, i.row_id
    """)
    
    rows = cursor.fetchall()
    
    if not rows:
        if verbose:
            print(f"  ⚠ No data to export")
        conn.close()
        return 0
    
    # Write to CSV
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
    clean_csv_path = Path('data/outputs/investments_clean.csv')
    
    print("=" * 70)
    print("ASSET TYPE ENHANCEMENT & CSV UPDATE")
    print("=" * 70)
    
    # Step 1: Enhance asset types in database
    print("\n[STEP 1] Enhancing asset types in database...")
    updated = enhance_asset_types(db_path, verbose=True)
    
    if updated > 0:
        print(f"\n  ✓ Enhanced {updated} records with asset_type")
    else:
        print(f"\n  ✓ All records already have asset_type")
    
    # Step 2: Export updated data to CSV
    print("\n[STEP 2] Exporting enhanced data to CSV...")
    exported = export_to_csv(db_path, clean_csv_path, verbose=True)
    
    print("\n" + "=" * 70)
    print(f"✓ Enhancement complete: {updated} records updated, {exported} exported")
    print("=" * 70)


if __name__ == '__main__':
    main()
