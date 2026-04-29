"""
Enhancement script to populate missing asset_type fields
"""
import sqlite3
import re
import sys
import os
from pathlib import Path

# Import shared patterns — works whether run standalone or as part of the package
try:
    from src.asset_type_patterns import detect_asset_type
except ImportError:
    sys.path.insert(0, os.path.dirname(__file__))
    from asset_type_patterns import detect_asset_type


def infer_asset_type(issuer, description):
    return detect_asset_type(f"{description} {issuer}")


def enhance_asset_types(db_path, verbose=True):
    """
    Populate missing asset_type fields in the database
    
    Args:
        db_path: Path to SQLite database
        verbose: Print progress
    
    Returns:
        Number of records updated
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get investments with missing asset_type
    cursor.execute("""
        SELECT id, issuer_name, investment_description, asset_type
        FROM investments
        WHERE asset_type IS NULL OR asset_type = ''
    """)
    
    missing_records = cursor.fetchall()
    
    if verbose:
        print(f"\n[ENHANCEMENT] Populating missing asset_type fields...")
        print(f"  Found {len(missing_records)} records with missing asset_type")
    
    updates = 0
    inferred_types = {}
    
    for record_id, issuer, description, current_type in missing_records:
        issuer = issuer or ''
        description = description or ''
        
        # Infer asset type
        inferred_type = infer_asset_type(issuer, description)
        
        if inferred_type:
            # Update the database
            cursor.execute("""
                UPDATE investments 
                SET asset_type = ?
                WHERE id = ?
            """, (inferred_type, record_id))
            
            updates += 1
            
            # Track inferred types for reporting
            inferred_types[inferred_type] = inferred_types.get(inferred_type, 0) + 1
            
            if verbose:
                print(f"    [OK] {issuer[:50]:50} -> {inferred_type}")
    
    conn.commit()
    
    if verbose:
        print(f"\n  Summary:")
        print(f"    Total updated: {updates} records")
        if inferred_types:
            print(f"    Asset types assigned:")
            for asset_type, count in sorted(inferred_types.items(), key=lambda x: -x[1]):
                print(f"      - {asset_type}: {count}")
        
        # Check remaining gaps
        cursor.execute("""
            SELECT COUNT(*) 
            FROM investments 
            WHERE asset_type IS NULL OR asset_type = ''
        """)
        remaining = cursor.fetchone()[0]
        
        if remaining > 0:
            print(f"\n    [!] {remaining} records still missing asset_type")
            print(f"      (These may need manual classification)")
        else:
            print(f"\n    [OK] All records now have asset_type populated!")
    
    conn.close()
    
    return updates


if __name__ == '__main__':
    db_path = Path('data/outputs/pipeline.db')
    
    print("=" * 70)
    print("ASSET TYPE ENHANCEMENT")
    print("=" * 70)
    
    updated = enhance_asset_types(db_path, verbose=True)
    
    print("\n" + "=" * 70)
    print(f"[OK] Enhancement complete: {updated} records updated")
    print("=" * 70)
