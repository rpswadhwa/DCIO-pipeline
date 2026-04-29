#!/usr/bin/env python3
"""
LLM-Enhanced Investment Cleanup
Uses OpenAI to review and improve issuer_name, investment_description, and asset_type fields
beyond what rule-based logic can capture.
"""
import sqlite3
import os
import json
import time
import re
from pathlib import Path
from openai import OpenAI
from dotenv import load_dotenv
import csv
import sys
sys.path.insert(0, os.path.dirname(__file__))
from src.data_cleaner import is_meaningful_description

# Load environment variables
load_dotenv()

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

# Default model
MODEL = os.getenv('OPENAI_MODEL', 'gpt-5.2')
MISSING_EIN_PREFIX = 'MISSING_EIN::'


def clean_issuer_name(name: str) -> str:
    if not name:
        return ''
    cleaned = re.sub(r"[^A-Za-z0-9 ]+", "", name)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def clean_plan_name(name: str) -> str:
    if not name:
        return ''
    cleaned = re.sub(r"[^A-Za-z0-9 ]+", "", name)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    # Keep title casing for plan names for readability
    return cleaned.title()


def infer_asset_type(issuer: str, description: str) -> str:
    issuer_text = (issuer or '').lower()
    desc_text = (description or '').lower()
    combined = f"{issuer_text} {desc_text}"

    patterns = [
        (r'common/collective trust|collective trust', 'Common/Collective Trust Fund'),
        (r'target retirement|target date', 'Target Date Fund'),
        (r'self[-\s]?directed brokerage', 'Self-Directed Brokerage Account'),
        (r'\btarget\b', 'Target Date Fund'),
        (r'stock', 'Common Stock'),
        (r'preferred stock', 'Preferred Stock'),
        (r'money market', 'Money Market Fund'),
        (r'bond', 'Corporate Bond'),
        (r'government', 'Government Bond'),
        (r'partnership interest', 'Partnership Interest'),
        (r'real estate|realestate', 'Real Estate'),
        (r'currency', 'Currency'),
        (r'fund', 'Mutual Fund'),
        (r'etf|exchange traded', 'ETF'),
    ]

    for pattern, asset_type in patterns:
        if re.search(pattern, combined):
            return asset_type

    # Fallback strategy
    if 'vanguard' in combined or 'fidelity' in combined or 'blackrock' in combined or 'pimco' in combined:
        return 'Mutual Fund'

    return 'Other'


VALID_ASSET_TYPES = {
    'Common Stock', 'Preferred Stock', 'Mutual Fund', 'Common/Collective Trust Fund',
    'Index Fund', 'Money Market Fund', 'Self-Directed Brokerage Account',
    'Corporate Bond', 'Government Bond', 'Partnership Interest', 'Real Estate',
    'Currency', 'ETF', 'Separately Managed Account', 'Commingled Fund',
    'Stable Value Fund', 'Participant Loan', 'Other', 'Target Date Fund'
}


def infer_morningstar_ticker(issuer: str, description: str, asset_type: str) -> str:
    issuer_text = (issuer or '').lower()
    desc_text = (description or '').lower()

    if asset_type in {'Mutual Fund', 'Target Date Fund'}:
        # Heuristic: use issuer abbreviation + first words of fund name
        key = ''
        if 'vanguard' in issuer_text:
            key = 'VFIAX' if '500' in desc_text or '500 index' in desc_text else 'VTSAX'
        elif 'fidelity' in issuer_text:
            key = 'FSKAX' if 'index' in desc_text else 'FCNTX'
        elif 'pimco' in issuer_text:
            key = 'PODIX'
        elif 'blackrock' in issuer_text:
            key = 'AOKIX'
        else:
            key = ''

        # If this heuristic is of low confidence, leave blank
        return key

    return ''


def check_and_fix_asset_type_consistency(db_path, verbose=True):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT rowid, issuer_name, investment_description, asset_type
        FROM investments
    """)
    rows = cursor.fetchall()

    mismatch_count = 0
    missing_count = 0
    fixed_count = 0

    for rowid, issuer, description, asset_type in rows:
        issuer = issuer or ''
        description = description or ''
        asset_type = (asset_type or '').strip()

        inferred_type = infer_asset_type(issuer, description)

        if not asset_type:
            missing_count += 1
            cursor.execute(
                "UPDATE investments SET asset_type = ? WHERE rowid = ?",
                (inferred_type, rowid)
            )
            fixed_count += 1
            continue

        if asset_type not in VALID_ASSET_TYPES or asset_type != inferred_type:
            mismatch_count += 1
            if verbose:
                print(
                    f"    ⚠ row {rowid}: asset_type='{asset_type}' vs inferred='{inferred_type}'"
                )
            cursor.execute(
                "UPDATE investments SET asset_type = ? WHERE rowid = ?",
                (inferred_type, rowid)
            )
            fixed_count += 1

    conn.commit()
    conn.close()

    if verbose:
        print("\n[ASSET TYPE CONSISTENCY CHECK]")
        print(f"  rows checked: {len(rows)}")
        print(f"  missing asset_type fixed: {missing_count}")
        print(f"  mismatched asset_type fixed: {mismatch_count}")
        print(f"  total corrected: {fixed_count}")

    return fixed_count


def review_investment_with_llm(batch_data, verbose=False):
    """
    Send a batch of investments to LLM for review and enhancement
    
    Args:
        batch_data: List of tuples (rowid, issuer_name, investment_description, asset_type)
        
    Returns:
        List of tuples (rowid, new_issuer, new_description, new_asset_type)
    """
    
    if not batch_data:
        return []
    
    # Prepare the batch for LLM review
    investments = []
    for rowid, issuer, desc, atype in batch_data:
        investments.append({
            'id': rowid,
            'issuer_name': issuer or '',
            'investment_description': desc or '',
            'asset_type': atype or ''
        })
    
    prompt = f"""You are a financial data analyst specializing in Form 5500 investment data cleanup.

Review these investment records and improve the data quality by:

1. **Issuer Name**: Extract and standardize the asset manager/firm name only
   - If issuer contains only the manager name, standardize it: "The Vanguard Group, Inc." → "Vanguard"
   - If issuer contains the full fund name, extract just the manager: "Vanguard Total Bond Market Index Fund" → "Vanguard"
   - Common abbreviations: "VG"/"VANG" → "Vanguard", "AF" → "American Funds", "FID" → "Fidelity", "BLK" → "BlackRock"
   - For individual stocks (INC, CORP, LLC, CO, PLC, LTD), keep the full company name unchanged
   - Always return a value — never leave blank

2. **Investment Description**: Expand abbreviations ONLY — never shorten
   - Keep the full text exactly as-is, only expanding clear abbreviations in-place
   - Expand: "INST" → "Institutional", "IDX" → "Index", "IS" → "Institutional Shares"
   - Expand: "INTL" → "International", "STK" → "Stock", "MKT" → "Market", "BD" → "Bond"
   - Expand: "FD" → "Fund", "RET" → "Retirement", "TR" → "Trust", "CL" → "Class"
   - Expand: "TRGTDT" / "TRGT DT" → "Target Date", "SMCAP" → "Small Cap", "LGCAP" → "Large Cap"
   - Do NOT remove anything — manager name, asset type words, years, share classes
   - Do NOT reorder or restructure
   - CRITICAL: if the description contains a fund name (even if it also appears in the issuer field), preserve the FULL fund name — never reduce it to just the manager name
   - If description is blank, a pure asset type label ("Mutual Fund"), or financial details ("4.25% due 2027") — leave it exactly as-is, do not change it

3. **Asset Type**: Only change if clearly wrong or blank
   - Valid values: Common Stock, Preferred Stock, Mutual Fund, Common/Collective Trust Fund, Index Fund, Money Market Fund, Self-Directed Brokerage Account, Corporate Bond, Government Bond, Partnership Interest, Real Estate, Currency, ETF, Target Date Fund, Participant Loan, Other
   - If already set to a valid value that is plausible — leave it unchanged
   - Never expand an abbreviation that is already spelled out elsewhere in the same string

EXAMPLES:
- issuer="VG", desc="VG IS TL INTL STK MK IDX FD" → Issuer: "Vanguard", Desc: "Vanguard Institutional Total International Stock Market Index Fund"
- issuer="Vanguard Total Bond Market Index Fund", desc="" → Issuer: "Vanguard", Desc: "" (leave blank)
- issuer="Fidelity Management Trust Company", desc="Freedom Blend 2035 Mutual Fund" → Issuer: "Fidelity", Desc: "Freedom Blend 2035 Mutual Fund"
- issuer="ALPHABET INC CL A", desc="ALPHABET INC CL A" → Issuer: "ALPHABET INC CL A", Desc: "ALPHABET INC CL A" (stock — unchanged)
- issuer="Northern Trust", desc="Mutual Fund" → Issuer: "Northern Trust", Desc: "Mutual Fund" (leave as-is)
- issuer="American Funds TRGTDT 2010 R6", desc="American Funds TRGTDT 2010 R6" → Issuer: "American Funds", Desc: "American Funds Target Date 2010 R6" (expand abbr in desc, keep full name — do NOT reduce to just "American Funds")
- issuer="AF TRGTDT 2025 R6", desc="AF TRGTDT 2025 R6" → Issuer: "American Funds", Desc: "American Funds Target Date 2025 R6"
- issuer="Vanguard Cash Reserves Federal MM Fund Admiral Shares", desc="Vanguard Cash Reserves Federal MM Fund Admiral Shares" → Issuer: "Vanguard", Desc: "Vanguard Cash Reserves Federal Money Market Fund Admiral Shares"

Here are the investments to review:

{json.dumps(investments, indent=2)}

Respond with a JSON array containing the improved records. For each record, include:
- "id": the record ID
- "issuer_name": improved issuer name
- "investment_description": improved investment description
- "asset_type": standardized asset type
- "changed": boolean indicating if any field was modified

Example response format:
[
  {{
    "id": 1,
    "issuer_name": "Vanguard",
    "investment_description": "Target Retirement 2025 Fund",
    "asset_type": "Common/Collective Trust Fund",
    "changed": true
  }},
  ...
]

Respond ONLY with the JSON array, no additional text."""

    try:
        if verbose:
            print(f"  Sending {len(investments)} investments to LLM for review...")
        
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "You are a financial data analyst. Respond only with valid JSON."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.1,
        )
        
        response_text = response.choices[0].message.content.strip()
        
        # Parse JSON response
        if response_text.startswith('```json'):
            response_text = response_text.split('```json')[1].split('```')[0].strip()
        elif response_text.startswith('```'):
            response_text = response_text.split('```')[1].split('```')[0].strip()
        
        results = json.loads(response_text)
        
        # Convert to update tuples
        updates = []
        for result in results:
            if result.get('changed', False):
                updates.append((
                    result['id'],
                    result['issuer_name'],
                    result['investment_description'],
                    result['asset_type']
                ))
        
        if verbose:
            print(f"  ✓ LLM identified {len(updates)} records needing improvement")
        
        return updates
    
    except Exception as e:
        print(f"  ⚠ Error calling LLM: {e}")
        return []


def llm_enhance_investments(db_path, batch_size=10, max_batches=None, verbose=True):
    """
    Use LLM to enhance investment data quality
    
    Args:
        db_path: Path to SQLite database
        batch_size: Number of records per LLM call (default 10)
        max_batches: Maximum number of batches to process (None = all)
        verbose: Print progress
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    if verbose:
        print("\n[STEP 1] Fetching investments for LLM review...")
    
    # Get all investments
    cursor.execute("""
        SELECT rowid, issuer_name, investment_description, asset_type
        FROM investments
        ORDER BY rowid
    """)
    
    all_investments = cursor.fetchall()
    
    if not all_investments:
        if verbose:
            print("  ⚠ No investments found")
        conn.close()
        return 0
    
    if verbose:
        print(f"  Found {len(all_investments)} investments")
        print(f"\n[STEP 2] Processing in batches of {batch_size}...")
        print(f"  Model: {MODEL}")
    
    total_updates = 0
    batch_num = 0
    
    # Process in batches
    for i in range(0, len(all_investments), batch_size):
        batch = all_investments[i:i + batch_size]
        batch_num += 1
        
        if max_batches and batch_num > max_batches:
            if verbose:
                print(f"\n  Reached maximum batch limit ({max_batches})")
            break
        
        if verbose:
            print(f"\n  Batch {batch_num}/{(len(all_investments) + batch_size - 1) // batch_size}:")
        
        # Get LLM recommendations
        updates = review_investment_with_llm(batch, verbose=verbose)
        
        if updates:
            # Show examples
            if verbose and updates:
                print(f"\n  Sample changes in this batch:")
                for idx, (rowid, issuer, desc, atype) in enumerate(updates[:3]):
                    # Get original data
                    original = next((b for b in batch if b[0] == rowid), None)
                    if original:
                        print(f"\n    Record {rowid}:")
                        if original[1] != issuer:
                            print(f"      Issuer: '{original[1]}' → '{issuer}'")
                        if original[2] != desc:
                            print(f"      Description: '{original[2]}' → '{desc}'")
                        if original[3] != atype:
                            print(f"      Asset Type: '{original[3]}' → '{atype}'")
            
            # Apply updates
            for rowid, issuer, desc, atype in updates:
                clean_issuer = clean_issuer_name(issuer)
                clean_desc = desc.strip() if desc else ''
                clean_asset_type = atype.strip() if atype else ''

                cursor.execute("""
                    UPDATE investments
                    SET issuer_name = ?, investment_description = ?, asset_type = ?
                    WHERE rowid = ?
                """, (clean_issuer, clean_desc, clean_asset_type, rowid))
                total_updates += 1
            
            conn.commit()
        
        # Rate limiting - be nice to the API
        if i + batch_size < len(all_investments):
            time.sleep(1)
    
    if verbose:
        print(f"\n[STEP 3] Summary...")
        print(f"  ✓ Enhanced {total_updates} investment records using LLM")
        
        # Show sample of final data
        cursor.execute("""
            SELECT issuer_name, investment_description, asset_type
            FROM investments
            LIMIT 10
        """)
        
        print("\n  Sample of enhanced data:")
        for issuer, desc, atype in cursor.fetchall():
            print(f"    {issuer:20} | {desc[:45]:45} | {atype}")
    
    conn.close()
    return total_updates


def export_enhanced_csv(db_path, output_path, verbose=True):
    """
    Export enhanced investment data to CSV
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    if verbose:
        print("\n[STEP 4] Exporting enhanced data to CSV...")
    
    cursor.execute("""
        SELECT 
            i.sponsor_ein,
            p.plan_number,
            p.plan_year,
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
            i.confidence,
            p.source_pdf
        FROM investments i
        JOIN plans p ON 
            i.sponsor_ein = p.sponsor_ein
        ORDER BY i.sponsor_ein, i.page_number, i.row_id
    """)
    
    rows = cursor.fetchall()
    
    fieldnames = [
        'sponsor_ein', 'plan_number', 'plan_year', 'sponsor', 'plan_name',
        'sponsor_plan_key',
        'issuer_name', 'investment_description', 'asset_type', 'morningstar_ticker',
        'par_value', 'cost', 'current_value', 'units_or_shares',
        'page_number', 'row_id', 'confidence', 'pdf_stem'
    ]
    
    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        seen_rows = set()

        for row in rows:
            row_data = dict(row)

            # Hide synthetic internal plan keys from exported CSVs.
            sponsor_ein = row_data.get('sponsor_ein') or ''
            if sponsor_ein.startswith(MISSING_EIN_PREFIX):
                row_data['sponsor_ein'] = ''

            # Ensure plan_name and issuer_name are cleaned (no special chars)
            row_data['plan_name'] = clean_plan_name(row_data.get('plan_name', ''))
            row_data['issuer_name'] = clean_issuer_name(row_data.get('issuer_name', ''))
            # Derive pdf_stem from source_pdf filename
            source_pdf = row_data.pop('source_pdf', '') or ''
            import os as _os
            row_data['pdf_stem'] = _os.path.splitext(_os.path.basename(source_pdf))[0] if source_pdf else ''

            # Fund name selection: use investment_description if meaningful,
            # otherwise fall back to issuer_name
            desc = (row_data.get('investment_description') or '').strip()
            issuer = (row_data.get('issuer_name') or '').strip()
            if not is_meaningful_description(desc):
                row_data['investment_description'] = issuer

            # Ensure asset_type is always present and standardized
            asset_type = (row_data.get('asset_type') or '').strip()
            row_data['asset_type'] = asset_type

            # Infer Morningstar ticker for mutual funds/target date funds
            row_data['morningstar_ticker'] = infer_morningstar_ticker(
                row_data['issuer_name'],
                row_data.get('investment_description', ''),
                asset_type
            )

            # Deduplicate across full row values
            row_tuple = tuple(row_data.get(key, '') for key in fieldnames)
            if row_tuple in seen_rows:
                continue
            seen_rows.add(row_tuple)

            writer.writerow(row_data)
    
    if verbose:
        print(f"  ✓ Exported {len(rows)} records to {output_path}")
    
    conn.close()
    return len(rows)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='LLM-enhanced cleanup of investment data'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10,
        help='Number of records per LLM call (default: 10)'
    )
    parser.add_argument(
        '--max-batches',
        type=int,
        default=None,
        help='Maximum number of batches to process (default: all)'
    )
    parser.add_argument(
        '--db',
        type=str,
        default='data/outputs/pipeline.db',
        help='Path to database (default: data/outputs/pipeline.db)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='data/outputs/investments_clean.csv',
        help='Output CSV path (default: data/outputs/investments_clean.csv)'
    )
    
    args = parser.parse_args()
    
    db_path = Path(args.db)
    output_csv = Path(args.output)
    
    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        return
    
    # Check for API key
    if not os.getenv('OPENAI_API_KEY'):
        print("❌ OPENAI_API_KEY environment variable not set")
        print("   Set it with: export OPENAI_API_KEY='your-key-here'")
        return
    
    print("=" * 80)
    print("LLM-ENHANCED INVESTMENT DATA CLEANUP")
    print("=" * 80)
    print(f"Model: {MODEL}")
    print(f"Batch size: {args.batch_size}")
    if args.max_batches:
        print(f"Max batches: {args.max_batches}")
    
    # Enhance with LLM
    updated = llm_enhance_investments(
        db_path,
        batch_size=args.batch_size,
        max_batches=args.max_batches,
        verbose=True
    )

    # Consistency check and correction for asset_type
    corrected = check_and_fix_asset_type_consistency(db_path, verbose=True)

    # Export enhanced CSV
    if updated >= 0:
        exported = export_enhanced_csv(db_path, output_csv, verbose=True)
    
    print("\n" + "=" * 80)
    print(f"✓ Complete: {updated} investments enhanced with LLM")
    print("=" * 80)


if __name__ == '__main__':
    main()
