#!/usr/bin/env python3
"""
Extract mutual fund data from investments and store in separate table
Filters for asset_type = 'Mutual Fund' and creates a dedicated mutual_funds table
"""
import sqlite3
from pathlib import Path


def create_mutual_funds_table(cursor):
    """
    Create mutual_funds table with specified schema
    """
    cursor.execute("""
        DROP TABLE IF EXISTS mutual_funds
    """)
    
    cursor.execute("""
        CREATE TABLE mutual_funds (
            plan_year INTEGER NOT NULL,
            plan_sponsor_ein TEXT NOT NULL,
            plan_number TEXT NOT NULL,
            issuer_identity TEXT,
            asset_type TEXT,
            asset_sub_type TEXT,
            current_value REAL,
            PRIMARY KEY (plan_sponsor_ein, plan_number, plan_year, issuer_identity, asset_sub_type)
        )
    """)
    
    print("  ✓ Created mutual_funds table")


def extract_and_load_mutual_funds(db_path, verbose=True):
    """
    Extract mutual fund records from investments and load into mutual_funds table
    
    Args:
        db_path: Path to SQLite database
        verbose: Print progress
        
    Returns:
        Number of records inserted
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    if verbose:
        print("\n[STEP 1] Creating mutual_funds table...")
    create_mutual_funds_table(cursor)
    
    if verbose:
        print("\n[STEP 2] Extracting mutual fund records...")
    
    # Query investments for mutual funds only
    cursor.execute("""
        SELECT 
            i.plan_year,
            i.sponsor_ein,
            i.plan_number,
            i.issuer_name,
            i.asset_type,
            i.investment_description,
            i.current_value
        FROM investments i
        WHERE i.asset_type = 'Mutual Fund'
        ORDER BY i.plan_year, i.sponsor_ein, i.plan_number, i.issuer_name
    """)
    
    mutual_funds = cursor.fetchall()
    
    if verbose:
        print(f"  Found {len(mutual_funds)} mutual fund records")
    
    if not mutual_funds:
        if verbose:
            print("  ⚠ No mutual funds to extract")
        conn.close()
        return 0
    
    if verbose:
        print("\n[STEP 3] Loading mutual funds into dedicated table...")
    
    # Insert into mutual_funds table
    inserted = 0
    for record in mutual_funds:
        plan_year, sponsor_ein, plan_number, issuer, asset_type, description, value = record
        
        # Ensure plan_number is 3-digit numeric format
        if plan_number:
            try:
                plan_number_num = int(plan_number)
                plan_number = f"{plan_number_num:03d}"
            except ValueError:
                # Keep as is if not numeric
                pass
        
        try:
            cursor.execute("""
                INSERT INTO mutual_funds 
                (plan_year, plan_sponsor_ein, plan_number, issuer_identity, 
                 asset_type, asset_sub_type, current_value)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (plan_year, sponsor_ein, plan_number, issuer, asset_type, description, value))
            inserted += 1
        except sqlite3.IntegrityError as e:
            if verbose:
                print(f"  ⚠ Duplicate record skipped: {issuer} - {description}")
    
    conn.commit()
    
    if verbose:
        print(f"  ✓ Inserted {inserted} mutual fund records")
    
    # Summary statistics
    if verbose:
        print("\n[STEP 4] Summary statistics...")
        
        # Count by plan
        cursor.execute("""
            SELECT 
                p.sponsor,
                mf.plan_sponsor_ein,
                COUNT(*) as fund_count,
                SUM(mf.current_value) as total_value
            FROM mutual_funds mf
            JOIN plans p ON 
                mf.plan_sponsor_ein = p.sponsor_ein AND
                mf.plan_number = p.plan_number AND
                mf.plan_year = p.plan_year
            GROUP BY mf.plan_sponsor_ein, mf.plan_number, mf.plan_year
            ORDER BY total_value DESC
        """)
        
        print("\n  Mutual Funds by Plan:")
        for sponsor, ein, count, total in cursor.fetchall():
            sponsor_display = sponsor or ein
            print(f"    {sponsor_display}: {count} mutual funds, ${total:,.2f}")
        
        # Top 10 mutual funds by value
        cursor.execute("""
            SELECT 
                issuer_identity,
                asset_sub_type,
                current_value
            FROM mutual_funds
            WHERE current_value IS NOT NULL
            ORDER BY current_value DESC
            LIMIT 10
        """)
        
        print("\n  Top 10 Mutual Funds by Value:")
        for idx, (issuer, subtype, value) in enumerate(cursor.fetchall(), 1):
            subtype_display = subtype if subtype else "(No description)"
            print(f"    {idx:2d}. {issuer:40s} {subtype_display:30s} ${value:,.2f}")
    
    conn.close()
    return inserted


def main():
    db_path = Path('data/outputs/pipeline.db')
    
    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        print("   Run complete_pipeline.py first to generate the database.")
        return
    
    print("=" * 70)
    print("MUTUAL FUND EXTRACTION")
    print("=" * 70)
    
    inserted = extract_and_load_mutual_funds(db_path, verbose=True)
    
    print("\n" + "=" * 70)
    print(f"✓ Extraction complete: {inserted} mutual fund records loaded")
    print("=" * 70)
    print("\nQuery the mutual_funds table:")
    print("  sqlite3 data/outputs/pipeline.db 'SELECT * FROM mutual_funds LIMIT 10'")


if __name__ == '__main__':
    main()
