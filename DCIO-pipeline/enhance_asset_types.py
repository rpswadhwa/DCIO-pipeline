"""
Enhancement script to populate missing asset_type fields
"""
import sqlite3
import re
from pathlib import Path


def infer_asset_type(issuer, description):
    """
    Infer asset type from issuer name and description
    
    Returns:
        Asset type string or None if cannot be determined
    """
    # Combine issuer and description for pattern matching
    combined = f"{issuer} {description}".lower()
    
    # Asset type patterns (ordered by specificity)
    patterns = [
        # Specific patterns first
        (r'collective trust fund|common/collective trust', 'Common/Collective Trust Fund'),
        (r'self[-\s]?directed brokerage', 'Self-Directed Brokerage Account'),
        (r'separately managed account|sma\b', 'Separately Managed Account'),
        (r'commingled fund', 'Commingled Fund'),
        (r'stable value fund', 'Stable Value Fund'),
        (r'money market fund|mm fund|federal mm', 'Money Market Fund'),
        
        # Mutual fund patterns
        (r'mutual fund|index fund|target retirement|target date', 'Mutual Fund'),
        (r'income fund|growth fund|value fund|blend fund', 'Mutual Fund'),
        (r'bond fund|equity fund|balanced fund', 'Mutual Fund'),
        
        # Stock patterns
        (r'company stock fund|employer stock', 'Common Stock'),
        (r'\bstock\b.*\bfund\b', 'Common Stock'),
        (r'common stock', 'Common Stock'),
        (r'preferred stock', 'Preferred Stock'),
        
        # Other patterns
        (r'participant loan|notes receivable from participants', 'Participant Loan'),
        (r'brokerage account|brokerge account', 'Self-Directed Brokerage Account'),
        (r'partnership interest', 'Partnership Interest'),
        (r'etf\b|exchange traded', 'ETF'),
        (r'currency', 'Currency'),
        (r'wrapper|wrap contract', 'Other'),
    ]
    
    for pattern, asset_type in patterns:
        if re.search(pattern, combined, re.IGNORECASE):
            return asset_type
    
    # Additional heuristics for fund companies
    # If issuer contains known fund companies and no asset type matched
    fund_companies = ['vanguard', 'fidelity', 'pimco', 'blackrock', 'ssga', 
                      'nuveen', 'metropolitan west', 'metwest', 'jpmorgan',
                      'american funds', 't. rowe', 'spdr', 'ishares', 
                      'earnest partners', 'dimensional', 'dodge cox']
    
    if any(company in combined for company in fund_companies):
        # Default to mutual fund for known fund companies
        return 'Mutual Fund'
    
    # Fallback: if description contains "fund" and we haven't classified it yet
    # it's likely a mutual fund or investment fund
    if re.search(r'\bfund\b', description.lower()):
        return 'Mutual Fund'
    
    return None


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
