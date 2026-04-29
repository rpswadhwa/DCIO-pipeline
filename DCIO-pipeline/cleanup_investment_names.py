#!/usr/bin/env python3
"""
Clean up investment data to properly separate issuer names from investment descriptions
"""
import sqlite3
import re
import sys
import os
from pathlib import Path
import csv

try:
    from src.asset_type_patterns import ASSET_TYPE_PATTERNS
except ImportError:
    sys.path.insert(0, os.path.dirname(__file__))
    from asset_type_patterns import ASSET_TYPE_PATTERNS


_SHARES_OF_RE = re.compile(r"^[\d,]+(?:\.\d+)?\s+shares?\s+of\s+", re.IGNORECASE)
# Strips trailing share counts like ", 5,770,653 shares" or "- 6,576,777" or "1,234,567"
# but NOT plain years like "2035" (no commas, no shares/units keyword).
# Uses two alternatives:
#   1. any number + explicit shares/units keyword (comma separator allowed)
#   2. comma-formatted count with internal commas (optional keyword, comma separator allowed)
_LABEL_TRAILING_RE = re.compile(
    r'[,\s\-]+\d[\d,]*\s+(?:shares?|units?)\s*$'
    r'|[,\s\-]+\d{1,3}(?:,\d{3})+\.?\d*\s*(?:shares?|units?)?\s*$',
    re.IGNORECASE,
)


def _is_asset_type_label(text: str) -> bool:
    """Return True if text is purely an asset type category label with no fund-specific content.
    Strips trailing share counts first so "Mutual Fund - 6,576,777 shares" is also caught,
    but preserves trailing years like "2035" so "Target Date Fund 2035" is NOT a pure label.
    """
    t = _LABEL_TRAILING_RE.sub('', (text or '').strip()).strip()
    for pattern, _ in ASSET_TYPE_PATTERNS:
        if re.fullmatch(pattern, t, re.IGNORECASE):
            return True
    return False


def parse_issuer_and_investment(issuer_name, investment_desc, asset_type):
    """
    Parse the combined issuer_name field to extract:
    1. Issuer name (the investment firm/asset manager)
    2. Investment description (the actual fund/investment name)

    Returns: (issuer, description)
    """
    if not issuer_name:
        return (None, None)

    issuer_name = issuer_name.strip()
    raw_desc = investment_desc.strip() if investment_desc else ""
    # Strip "X shares of" prefix from description only — keep everything else intact
    original_desc = _SHARES_OF_RE.sub("", raw_desc).strip()

    # Start with whatever col B had; fall back to full issuer when col B is empty
    desc = original_desc if original_desc else issuer_name

    # When col B is just an asset type category label (e.g. "Mutual Fund",
    # "Collective Investment Fund", "Target Date Fund"), the real fund name
    # lives in col A — use it before we standardize the issuer below.
    if _is_asset_type_label(desc):
        desc = issuer_name

    if issuer_name.upper().startswith('VANGUARD') or issuer_name.upper().startswith('VANG'):
        return ('Vanguard', desc)

    if issuer_name.upper().startswith('PIMCO'):
        return ('PIMCO', desc)

    if issuer_name.upper().startswith('BLACKROCK') or 'LIFEPATH' in issuer_name.upper():
        return ('BlackRock', desc)

    if issuer_name.upper().startswith('AF ') or 'EUROPAC' in issuer_name.upper():
        return ('American Funds', desc)

    if 'FIDELITY' in issuer_name.upper() or issuer_name.upper().startswith('FID '):
        return ('Fidelity', desc)

    if issuer_name.upper().startswith('NUVEEN'):
        return ('Nuveen', desc)

    if 'T ROWE' in issuer_name.upper() or 'T. ROWE' in issuer_name.upper():
        return ('T. Rowe Price', desc)

    if 'STATE STREET' in issuer_name.upper() or issuer_name.upper().startswith('SSG'):
        return ('State Street', desc)

    if 'BNY' in issuer_name.upper() or 'MELLON' in issuer_name.upper():
        return ('BNY Mellon', desc)

    if 'BAILLIE' in issuer_name.upper() or 'GIFFORD' in issuer_name.upper():
        return ('Baillie Gifford', desc)

    if issuer_name.upper().startswith('AB '):
        return ('AllianceBernstein', desc)

    if 'JP' in issuer_name.upper() and 'MORGAN' in issuer_name.upper():
        return ('J.P. Morgan', desc)

    if 'BROKERAGE' in issuer_name.upper() or 'BROKERGE' in issuer_name.upper():
        return ('Self-Directed', desc)

    if issuer_name.startswith('*'):
        clean_issuer = re.sub(r',.*$|Inc\.?$|LLC$|Corp\.?$', '', issuer_name.replace('*', '')).strip()
        return (clean_issuer, desc)

    # Default: keep original issuer and description unchanged
    return (issuer_name, desc)


def cleanup_investments(db_path, verbose=True):
    """
    Clean up investment data in the database
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    if verbose:
        print("\n[STEP 1] Fetching all investments...")
    
    cursor.execute("""
        SELECT rowid, issuer_name, investment_description, asset_type
        FROM investments
        ORDER BY rowid
    """)
    
    investments = cursor.fetchall()
    
    if verbose:
        print(f"  Found {len(investments)} investments to process")
        print("\n[STEP 2] Parsing and cleaning investment names...")
    
    updates = []
    
    for rowid, issuer_name, investment_desc, asset_type in investments:
        new_issuer, new_description = parse_issuer_and_investment(
            issuer_name, investment_desc, asset_type
        )
        
        if new_issuer or new_description:
            updates.append((new_issuer, new_description, rowid))
            
            if verbose and len(updates) <= 10:  # Show first 10 examples
                print(f"\n  Row {rowid}:")
                print(f"    Original Issuer: {issuer_name}")
                print(f"    → New Issuer: {new_issuer}")
                print(f"    → New Description: {new_description}")
    
    if verbose:
        print(f"\n  Processing {len(updates)} updates...")
    
    # Update the database
    cursor.executemany("""
        UPDATE investments
        SET issuer_name = ?, investment_description = ?
        WHERE rowid = ?
    """, updates)
    
    conn.commit()
    
    if verbose:
        print(f"\n[STEP 3] Summary of updates...")
        print(f"  ✓ Updated {len(updates)} investment records")
        
        # Show sample of cleaned data
        cursor.execute("""
            SELECT issuer_name, investment_description, asset_type
            FROM investments
            LIMIT 15
        """)
        
        print("\n  Sample of cleaned data:")
        for issuer, desc, atype in cursor.fetchall():
            print(f"    {issuer:20} | {desc:50} | {atype}")
    
    conn.close()
    return len(updates)


def export_cleaned_csv(db_path, output_path, verbose=True):
    """
    Export cleaned investment data to CSV
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    if verbose:
        print("\n[STEP 4] Exporting cleaned data to CSV...")
    
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
    
    if not rows:
        if verbose:
            print("  ⚠ No data to export")
        conn.close()
        return 0
    
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
    output_csv = Path('data/outputs/investments_clean.csv')
    
    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        return
    
    print("=" * 80)
    print("CLEANUP INVESTMENT NAMES: SEPARATE ISSUERS FROM INVESTMENT DESCRIPTIONS")
    print("=" * 80)
    
    # Clean up investment data
    updated = cleanup_investments(db_path, verbose=True)
    
    # Export cleaned CSV
    if updated > 0:
        exported = export_cleaned_csv(db_path, output_csv, verbose=True)
    
    print("\n" + "=" * 80)
    print(f"✓ Complete: {updated} investments cleaned, CSV updated")
    print("=" * 80)


if __name__ == '__main__':
    main()
