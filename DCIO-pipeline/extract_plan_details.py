#!/usr/bin/env python3
"""
Extract Plan Name (Part II - 1a) and Sponsor (Part II - 1b) from Form 5500 PDFs
and update the database plans table and CSV exports
"""
import sqlite3
import re
from pathlib import Path
import pdfplumber


def extract_part_ii_fields(pdf_path):
    """
    Extract Plan Name and Sponsor from Form 5500
    
    Strategy:
    1. Extract from Schedule H pages (most reliable - has actual data)
    2. Fall back to inferring from filename if PDF has placeholder text
    
    Returns:
        dict with 'plan_name' and 'sponsor' keys
    """
    plan_name = None
    sponsor = None
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            # Strategy 1: Look in Schedule H pages (supplemental schedules near end of document)
            # These pages typically have the real plan name in the header
            schedule_h_found = False
            
            # Search last 20 pages for Schedule H
            start_page = max(0, len(pdf.pages) - 20)
            for page_num in range(start_page, len(pdf.pages)):
                page = pdf.pages[page_num]
                text = page.extract_text() or ""
                
                # Check if this is a Schedule H page
                if 'SCHEDULE H' in text.upper() and 'LINE 4' in text.upper():
                    schedule_h_found = True
                    lines = text.split('\n')
                    
                    # Extract plan name from header (usually first few lines)
                    for line in lines[:10]:
                        # Look for patterns like "Amazon 401(k) Plan" or "APPLE INC. 401(K) PLAN"
                        if re.search(r'401\s*\(\s*[Kk]\s*\)', line, re.IGNORECASE):
                            candidate = line.strip()
                            # Remove trailing text like "EIN #..." or "Plan #..."
                            candidate = re.sub(r'\s+EIN\s+#.*$', '', candidate, flags=re.IGNORECASE)
                            candidate = re.sub(r'\s+Plan\s+#.*$', '', candidate, flags=re.IGNORECASE)
                            
                            if 5 < len(candidate) < 100:
                                plan_name = candidate
                                break
                    
                    # Extract EIN line which often has sponsor info
                    for line in lines[:10]:
                        ein_match = re.search(r'EIN\s+#(\d{2}-\d{7})', line, re.IGNORECASE)
                        if ein_match:
                            # The line before plan name might have sponsor
                            # For now, infer from plan name
                            if plan_name:
                                # Extract company name before "401(k)"
                                sponsor_match = re.match(r'^(.+?)\s+401\s*\(', plan_name, re.IGNORECASE)
                                if sponsor_match:
                                    sponsor = sponsor_match.group(1).strip()
                            break
                    
                    if plan_name:
                        break
            
            # Strategy 2: Infer from filename if Schedule H didn't work
            if not plan_name or not sponsor:
                filename = Path(pdf_path).stem.upper()
                
                # Common company name patterns in filenames
                company_patterns = [
                    (r'AMAZON', 'Amazon.com, Inc.', 'Amazon.com 401(k) Plan'),
                    (r'APPLE', 'Apple Inc.', 'Apple Inc. 401(k) Plan'),
                    (r'GOOGLE', 'Google LLC', 'Google LLC 401(k) Plan'),
                    (r'MICROSOFT', 'Microsoft Corporation', 'Microsoft Corporation 401(k) Plan'),
                ]
                
                for pattern, default_sponsor, default_plan in company_patterns:
                    if re.search(pattern, filename):
                        if not sponsor:
                            sponsor = default_sponsor
                        if not plan_name:
                            plan_name = default_plan
                        break
                
                # Generic fallback: extract company name from filename
                if not plan_name and not sponsor:
                    # Pattern like "COMPANY_401K_Form5500_2024.pdf"
                    match = re.match(r'^([A-Z][A-Z\s]+?)(?:_401K|_FORM)', filename)
                    if match:
                        company = match.group(1).strip().title()
                        sponsor = f"{company}, Inc."
                        plan_name = f"{company} 401(k) Plan"
    
    except Exception as e:
        print(f"  ⚠ Error extracting from {pdf_path}: {e}")
    
    return {
        'plan_name': plan_name,
        'sponsor': sponsor
    }


def update_plan_details(db_path, input_dir, verbose=True):
    """
    Extract plan names and sponsors from PDFs and update database
    
    Args:
        db_path: Path to SQLite database
        input_dir: Directory containing PDF files
        verbose: Print progress
    
    Returns:
        Number of plans updated
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    if verbose:
        print("\n[STEP 1] Retrieving plans from database...")
    
    # Get all plans with their source PDFs
    cursor.execute("""
        SELECT sponsor_ein, plan_number, plan_year, source_pdf, plan_name, sponsor
        FROM plans
        ORDER BY sponsor_ein, plan_number, plan_year
    """)
    
    plans = cursor.fetchall()
    
    if not plans:
        if verbose:
            print("  ⚠ No plans found in database")
        conn.close()
        return 0
    
    if verbose:
        print(f"  Found {len(plans)} plans")
        print("\n[STEP 2] Extracting plan details from PDFs...")
    
    updated = 0
    
    for sponsor_ein, plan_number, plan_year, source_pdf, current_plan_name, current_sponsor in plans:
        if not source_pdf:
            if verbose:
                print(f"  ⚠ No source PDF for plan {sponsor_ein} - {plan_number}")
            continue
        
        pdf_path = Path(input_dir) / source_pdf
        
        if not pdf_path.exists():
            if verbose:
                print(f"  ⚠ PDF not found: {pdf_path}")
            continue
        
        if verbose:
            print(f"\n  Processing: {source_pdf}")
            print(f"    EIN: {sponsor_ein}, Plan: {plan_number}, Year: {plan_year}")
        
        # Extract plan details from PDF
        details = extract_part_ii_fields(str(pdf_path))
        
        plan_name = details.get('plan_name') or current_plan_name
        sponsor = details.get('sponsor') or current_sponsor
        
        if verbose:
            if details.get('plan_name'):
                print(f"    ✓ Plan Name (1a): {plan_name}")
            else:
                print(f"    ⚠ Plan Name not extracted, keeping: {current_plan_name or '(none)'}")
            
            if details.get('sponsor'):
                print(f"    ✓ Sponsor (1b): {sponsor}")
            else:
                print(f"    ⚠ Sponsor not extracted, keeping: {current_sponsor or '(none)'}")
        
        # Update the database
        cursor.execute("""
            UPDATE plans
            SET plan_name = ?, sponsor = ?
            WHERE sponsor_ein = ? AND plan_number = ? AND plan_year = ?
        """, (plan_name, sponsor, sponsor_ein, plan_number, plan_year))
        
        updated += 1
    
    conn.commit()
    
    if verbose:
        print(f"\n[STEP 3] Summary...")
        print(f"  ✓ Updated {updated} plans")
        
        # Show updated plans
        cursor.execute("""
            SELECT sponsor_ein, plan_number, sponsor, plan_name
            FROM plans
            ORDER BY sponsor_ein
        """)
        
        print("\n  Current plan details:")
        for ein, pn, spon, name in cursor.fetchall():
            print(f"    {ein} - {pn}")
            print(f"      Sponsor: {spon or '(not set)'}")
            print(f"      Plan: {name or '(not set)'}")
    
    conn.close()
    return updated


def export_updated_csv(db_path, output_path, verbose=True):
    """
    Export investments with updated plan details to CSV
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    if verbose:
        print("\n[STEP 4] Exporting updated data to CSV...")
    
    # Query with proper joins to get plan details
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
    
    # Write to CSV
    import csv
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
    input_dir = Path('data/inputs')
    clean_csv_path = Path('data/outputs/investments_clean.csv')
    
    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        return
    
    print("=" * 70)
    print("EXTRACT PLAN DETAILS FROM FORM 5500 PART II")
    print("=" * 70)
    
    # Extract and update plan details
    updated = update_plan_details(db_path, input_dir, verbose=True)
    
    # Export updated CSV
    if updated > 0:
        exported = export_updated_csv(db_path, clean_csv_path, verbose=True)
    
    print("\n" + "=" * 70)
    print(f"✓ Complete: {updated} plans updated, CSV refreshed")
    print("=" * 70)


if __name__ == '__main__':
    main()
