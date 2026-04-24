#!/usr/bin/env python3
"""
Clean up investment data to properly separate issuer names from investment descriptions
"""
import sqlite3
import re
from pathlib import Path
import csv


_SHARES_OF_RE = re.compile(r"^[\d,]+(?:\.\d+)?\s+shares?\s+of\s+", re.IGNORECASE)

def _best_desc(extracted, issuer, fallback_desc):
    """Return fallback_desc if extracted is just the manager name or empty."""
    e = (extracted or "").strip()
    if not e or e.lower() == (issuer or "").strip().lower():
        return fallback_desc or e
    return e

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
    original_desc = _SHARES_OF_RE.sub("", raw_desc).strip()
    
    # Pattern 1: Already properly formatted with asterisk
    # "* The Vanguard Group, Inc." → Issuer: "Vanguard"
    if issuer_name.startswith('*'):
        clean_issuer = issuer_name.replace('*', '').strip()
        if 'vanguard' in clean_issuer.lower():
            issuer = 'Vanguard'
        elif 'fidelity' in clean_issuer.lower():
            issuer = 'Fidelity'
        elif 'blackrock' in clean_issuer.lower():
            issuer = 'BlackRock'
        else:
            # Extract company name before comma or Inc/LLC/Corp
            issuer = re.sub(r',.*$|Inc\.?$|LLC$|Corp\.?$', '', clean_issuer).strip()
        return (issuer, original_desc)
    
    # Pattern 2: VANGUARD prefix
    if issuer_name.upper().startswith('VANGUARD') or issuer_name.upper().startswith('VANG'):
        issuer = 'Vanguard'
        description = issuer_name
        
        # Clean up common Vanguard fund patterns
        if 'TARGET' in description.upper():
            # VANGUARD TARGET 2020 → Target Retirement 2020
            match = re.search(r'TARGET\s+(\w+)', description, re.IGNORECASE)
            if match:
                year_or_name = match.group(1)
                if year_or_name.upper() == 'INC':
                    description = 'Target Retirement Income Fund'
                elif year_or_name.isdigit():
                    description = f'Target Retirement {year_or_name} Fund'
                else:
                    description = f'Target Retirement {year_or_name}'
        elif 'RET SVNG' in description.upper() or 'RETIREMENT SAVINGS' in description.upper():
            description = 'Retirement Savings Trust'
        elif 'SM VAL' in description.upper():
            description = 'Small-Cap Value Index Fund'
        elif 'FTSE SOC' in description.upper():
            description = 'FTSE Social Index Fund'
        elif 'TOT BD MKT' in description.upper() or 'TOTAL BOND MARKET' in description.upper():
            description = 'Total Bond Market Index Fund'
        elif 'TOT STK MK' in description.upper() or 'TOTAL STOCK MARKET' in description.upper():
            description = 'Total Stock Market Index Fund'
        elif 'INTL STK' in description.upper() or 'INTERNATIONAL STOCK' in description.upper():
            description = 'Total International Stock Market Index Fund'
        else:
            # Remove "VANGUARD" or "VANG" prefix
            description = re.sub(r'^VANG(UARD)?\s+', '', description, flags=re.IGNORECASE).strip()
            # Replace common abbreviations
            description = description.replace('IDX', 'Index').replace('INST', 'Institutional')
            description = description.replace('IS', '').strip()
        
        return (issuer, _best_desc(description, issuer, original_desc))
    
    # Pattern 3: PIMCO funds
    if issuer_name.upper().startswith('PIMCO'):
        issuer = 'PIMCO'
        description = re.sub(r'^PIMCO\s+', '', issuer_name, flags=re.IGNORECASE).strip()
        if 'TOTAL RTN' in description.upper():
            description = re.sub(r'TOTAL\s+RTN', 'Total Return Fund', description, flags=re.IGNORECASE)
        return (issuer, _best_desc(description, issuer, original_desc))
    
    # Pattern 4: BlackRock funds
    if issuer_name.upper().startswith('BLACKROCK') or 'LIFEPATH' in issuer_name.upper():
        issuer = 'BlackRock'
        description = re.sub(r'^BLACKROCK\s+', '', issuer_name, flags=re.IGNORECASE).strip()
        # Clean up LifePath descriptions
        if 'LIFEPATH' in description.upper():
            description = re.sub(r'ACCOUNT\s+[A-Z]$', '', description, flags=re.IGNORECASE).strip()
            description = description.replace('GLOBAL ', '').strip()
        return (issuer, _best_desc(description, issuer, original_desc))
    
    # Pattern 5: American Funds
    if issuer_name.upper().startswith('AF ') or 'EUROPAC' in issuer_name.upper():
        issuer = 'American Funds'
        description = re.sub(r'^AF\s+', '', issuer_name, flags=re.IGNORECASE).strip()
        return (issuer, _best_desc(description, issuer, original_desc))
    
    # Pattern 6: Fidelity funds
    if 'FIDELITY' in issuer_name.upper() or issuer_name.upper().startswith('FID '):
        issuer = 'Fidelity'
        description = re.sub(r'^FID(ELITY)?\s+', '', issuer_name, flags=re.IGNORECASE).strip()
        return (issuer, _best_desc(description, issuer, original_desc))
    
    # Pattern 7: Nuveen funds
    if issuer_name.upper().startswith('NUVEEN'):
        issuer = 'Nuveen'
        description = re.sub(r'^NUVEEN\s+', '', issuer_name, flags=re.IGNORECASE).strip()
        return (issuer, _best_desc(description, issuer, original_desc))
    
    # Pattern 8: T. Rowe Price funds
    if 'T ROWE' in issuer_name.upper() or 'T. ROWE' in issuer_name.upper():
        issuer = 'T. Rowe Price'
        description = re.sub(r'T\.?\s*ROWE\s+PRICE\s+', '', issuer_name, flags=re.IGNORECASE).strip()
        return (issuer, _best_desc(description, issuer, original_desc))
    
    # Pattern 9: State Street funds
    if 'STATE STREET' in issuer_name.upper() or issuer_name.upper().startswith('SSG'):
        issuer = 'State Street'
        description = re.sub(r'STATE\s+STREET\s+', '', issuer_name, flags=re.IGNORECASE).strip()
        description = re.sub(r'^SSG\s+', '', description, flags=re.IGNORECASE).strip()
        return (issuer, _best_desc(description, issuer, original_desc))
    
    # Pattern 10: BNY Mellon funds
    if 'BNY' in issuer_name.upper() or 'MELLON' in issuer_name.upper():
        issuer = 'BNY Mellon'
        description = re.sub(r'^BNY\s+MELLON\s+', '', issuer_name, flags=re.IGNORECASE).strip()
        return (issuer, _best_desc(description, issuer, original_desc))
    
    # Pattern 11: Baillie Gifford
    if 'BAILLIE' in issuer_name.upper() or 'GIFFORD' in issuer_name.upper():
        issuer = 'Baillie Gifford'
        description = re.sub(r'BAILLIE\s+GIFFORD\s+', '', issuer_name, flags=re.IGNORECASE).strip()
        return (issuer, _best_desc(description, issuer, original_desc))
    
    # Pattern 12: AB (AllianceBernstein)
    if issuer_name.upper().startswith('AB '):
        issuer = 'AllianceBernstein'
        description = re.sub(r'^AB\s+', '', issuer_name).strip()
        return (issuer, _best_desc(description, issuer, original_desc))
    
    # Pattern 13: JP Morgan
    if 'JP' in issuer_name.upper() and 'MORGAN' in issuer_name.upper():
        issuer = 'J.P. Morgan'
        description = re.sub(r'JP\s+MORGAN\s+', '', issuer_name, flags=re.IGNORECASE).strip()
        return (issuer, _best_desc(description, issuer, original_desc))
    
    # Pattern 14: Brokerage/Self-Directed accounts
    if 'BROKERAGE' in issuer_name.upper() or 'BROKERGE' in issuer_name.upper():
        if 'LINK' in issuer_name.upper():
            return ('Fidelity', 'BrokerageLink Self-Directed Account')
        else:
            return ('Self-Directed', 'Brokerage Account')
    
    # Pattern 15: Company Stock Funds
    if 'STOCK FUND' in issuer_name.upper() or 'COMPANY STOCK' in issuer_name.upper():
        # Extract company name before "STOCK"
        match = re.match(r'^(.+?)\s+(?:COMPANY\s+)?STOCK', issuer_name, re.IGNORECASE)
        if match:
            company = match.group(1).strip().title()
            return (company, f'{company} Company Stock Fund')
        return (issuer_name, 'Company Stock Fund')
    
    # Pattern 16: Individual stocks (usually have INC, CORP, LLC, CO in name)
    # For individual securities, the issuer IS the description
    stock_indicators = ['INC', 'CORP', 'CO ', 'LLC', 'LTD', 'PLC', 'CVR', 'CL A', 'CL B', 'CLASS A']
    if any(indicator in issuer_name.upper() for indicator in stock_indicators):
        # This is likely a stock - keep the full name as the description
        # The issuer in this case is the company itself
        clean_name = issuer_name.strip()
        return (clean_name, clean_name)
    
    # Pattern 17: Currency
    if issuer_name.upper().endswith('DOLLAR') or issuer_name.upper().endswith('CURRENCY'):
        return ('Currency', issuer_name)
    
    # Default: Keep original if we can't parse it
    return (issuer_name, original_desc if original_desc else issuer_name)


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
