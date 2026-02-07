#!/usr/bin/env python3
import sqlite3
import csv
import re
from pathlib import Path
import pdfplumber
from datetime import datetime

clean_csv_path = "./data/outputs/investments_clean_llm.csv"
db_path = "./data/outputs/pipeline.db"
input_dir = "./data/inputs"


def extract_plan_info_from_pdf(pdf_path):
    """Extract Form 5500 Part II plan information from PDF."""
    plan_info = {
        'plan_name': None,
        'plan_number': None,
        'sponsor': None,
        'sponsor_ein': None,
        'administrator_name': None,
        'plan_type': None,
        'plan_year_begin': None,
        'plan_year_end': None,
    }
    
    # Check if PDF is using placeholder text (common in redacted forms)
    filename = Path(pdf_path).stem
    is_redacted = False
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            # Search first 5 pages for plan information
            text_all = ""
            for page_num, page in enumerate(pdf.pages[:5], 1):
                text = page.extract_text() or ""
                text_all += text + "\n"
                
            # Check if this is a redacted/sample form
            if 'ABCDEFGHI' in text_all:
                is_redacted = True
            
            if not is_redacted:
                # Extract plan name - improved pattern
                patterns = [
                    r'([A-Z][A-Za-z\s&,\.\-\']+?)\s+401[Kk\()\s]+(?:SAVINGS\s+)?PLAN',
                    r'(?:^|\n)([A-Z][A-Z\s&,\.\-\']{10,60})\s+401[Kk]',
                    r'Name of [Pp]lan[:\s]+([A-Za-z][A-Za-z0-9\s&,\.\-\']{5,80}?)(?:\s+\d{3}\s+plan|\n)',
                ]
                
                for pattern in patterns:
                    plan_name_match = re.search(pattern, text_all, re.MULTILINE)
                    if plan_name_match:
                        candidate = plan_name_match.group(1).strip()
                        if len(candidate) > 10:
                            plan_info['plan_name'] = candidate
                            break
                
                # Extract sponsor name
                sponsor_patterns = [
                    r'(?:Plan [Ss]ponsor[:\s]+|[Ss]ponsor[:\s]+)([A-Z][A-Za-z\s&,\.\-\']{5,50}?)(?:\s+(?:EIN|Employer Identification|\d{2}-\d{7})|$)',
                    r'(?:^|\n)([A-Z][A-Z\s&,\.\-\']{10,50})(?:\s+\d{2}-\d{7})',
                ]
                
                for pattern in sponsor_patterns:
                    sponsor_match = re.search(pattern, text_all, re.MULTILINE)
                    if sponsor_match:
                        candidate = sponsor_match.group(1).strip()
                        if len(candidate) > 5:
                            plan_info['sponsor'] = candidate
                            break
                
                # Extract plan number (3 digits)
                plan_num_match = re.search(r'(?:plan number|PN)[:\s]*(\d{3})', text_all, re.IGNORECASE)
                if plan_num_match:
                    plan_info['plan_number'] = plan_num_match.group(1)
                
                # Extract sponsor EIN
                ein_patterns = [
                    r'(?:EIN|Employer Identification Number)[:\s]*(\d{2}-?\d{7})',
                ]
                
                for pattern in ein_patterns:
                    ein_match = re.search(pattern, text_all)
                    if ein_match:
                        ein = ein_match.group(1)
                        # Validate it looks like a real EIN (not placeholder)
                        if ein not in ['00-0000000', '12-3456789', '01-2345678', '012345678']:
                            plan_info['sponsor_ein'] = ein
                            break
                
                # Extract plan administrator
                admin_match = re.search(r'(?:Plan Administrator|Administrator)[:\s]*([A-Z][A-Za-z0-9\s&,\.\-\']+?)(?:\n|$)', text_all, re.MULTILINE)
                if admin_match:
                    plan_info['administrator_name'] = admin_match.group(1).strip()
                
                # Extract plan year dates
                date_match = re.search(r'(?:beginning)\s+(\d{1,2}/\d{1,2}/\d{4})\s+and\s+(?:ending)\s+(\d{1,2}/\d{1,2}/\d{4})', text_all, re.IGNORECASE)
                if date_match:
                    plan_info['plan_year_begin'] = date_match.group(1)
                    plan_info['plan_year_end'] = date_match.group(2)
            
            # Determine plan type from text
            if '401(k)' in text_all or '401K' in text_all.upper():
                plan_info['plan_type'] = '401(k)'
            elif '403(b)' in text_all:
                plan_info['plan_type'] = '403(b)'
            
    except Exception as e:
        print(f"  Warning: Could not extract plan info from PDF: {e}")
    
    # Fallback to filename-based extraction for redacted forms or missing data
    if is_redacted or not plan_info['plan_name']:
        # Extract company name from filename (e.g., "APPLE_401K_Form5500_2024.pdf" -> "APPLE")
        company_match = re.search(r'^([A-Z][A-Za-z]+)_401[Kk]', filename)
        if company_match:
            company_name = company_match.group(1)
            plan_info['plan_name'] = f"{company_name} 401(k) Savings Plan"
            plan_info['sponsor'] = company_name.upper() + " INC."
            if not plan_info['plan_type']:
                plan_info['plan_type'] = '401(k)'
    
    return plan_info


def get_or_create_plan(cur, pdf_path, plan_year):
    """Get existing plan_id or create a new plan record with extracted information."""
    pdf_filename = Path(pdf_path).name
    
    # Check if plan already exists
    cur.execute(
        "SELECT id FROM plans WHERE source_pdf = ? OR source_pdf LIKE ? LIMIT 1",
        (pdf_filename, f"%{pdf_filename}")
    )
    result = cur.fetchone()
    
    if result:
        plan_id = result[0]
        print(f"  Found existing plan_id: {plan_id}")
        
        # Update with extracted information
        plan_info = extract_plan_info_from_pdf(pdf_path)
        cur.execute("""
            UPDATE plans 
            SET plan_name = COALESCE(?, plan_name),
                plan_number = COALESCE(?, plan_number),
                sponsor = COALESCE(?, sponsor),
                sponsor_ein = COALESCE(?, sponsor_ein),
                administrator_name = COALESCE(?, administrator_name),
                plan_type = COALESCE(?, plan_type),
                plan_year_begin = COALESCE(?, plan_year_begin),
                plan_year_end = COALESCE(?, plan_year_end),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (
            plan_info['plan_name'],
            plan_info['plan_number'],
            plan_info['sponsor'],
            plan_info['sponsor_ein'],
            plan_info['administrator_name'],
            plan_info['plan_type'],
            plan_info['plan_year_begin'],
            plan_info['plan_year_end'],
            plan_id
        ))
        print(f"  Updated plan information for plan_id: {plan_id}")
        
    else:
        # Create new plan with extracted information
        plan_info = extract_plan_info_from_pdf(pdf_path)
        cur.execute("""
            INSERT INTO plans 
            (plan_name, plan_number, sponsor, sponsor_ein, administrator_name, 
             plan_type, plan_year, plan_year_begin, plan_year_end, source_pdf)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            plan_info['plan_name'],
            plan_info['plan_number'],
            plan_info['sponsor'],
            plan_info['sponsor_ein'],
            plan_info['administrator_name'],
            plan_info['plan_type'],
            plan_year,
            plan_info['plan_year_begin'],
            plan_info['plan_year_end'],
            pdf_filename
        ))
        plan_id = cur.lastrowid
        print(f"  Created new plan_id: {plan_id}")
    
    return plan_id


# Read the cleaned CSV
print("Reading cleaned investment data...")
rows = []
with open(clean_csv_path, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

print(f"Read {len(rows)} clean investment records")

# Group rows by PDF
pdf_groups = {}
for row in rows:
    pdf_name = row.get('pdf_name', '')
    if pdf_name:
        if pdf_name not in pdf_groups:
            pdf_groups[pdf_name] = []
        pdf_groups[pdf_name].append(row)

print(f"Found {len(pdf_groups)} unique PDF files")

# Connect to the database
with sqlite3.connect(db_path) as conn:
    cur = conn.cursor()
    
    total_inserted = 0
    
    for pdf_name, pdf_rows in pdf_groups.items():
        print(f"\n{'='*80}")
        print(f"Processing: {pdf_name}")
        print(f"{'='*80}")
        
        # Get full path to PDF
        pdf_path = Path(input_dir) / pdf_name
        
        # Extract year from first row
        plan_year = 2024  # default
        if pdf_rows:
            year_match = re.search(r'20\d{2}', pdf_name)
            if year_match:
                plan_year = int(year_match.group())
        
        # Get or create plan with extracted information
        plan_id = get_or_create_plan(cur, str(pdf_path), plan_year)
        
        # Delete existing investment data for this plan
        print(f"\nDeleting old investment data for plan_id {plan_id}...")
        cur.execute("DELETE FROM investments WHERE plan_id = ?", (plan_id,))
        deleted_count = cur.rowcount
        print(f"  Deleted {deleted_count} old investment records")
        
        # Insert new clean investment data
        print(f"\nInserting cleaned investment data...")
        inserted_count = 0
        
        for row in pdf_rows:
            # Skip total rows
            issuer = row.get('issuer_name', '').strip()
            if issuer.lower() == 'total' and not row.get('investment_description', '').strip():
                continue
            
            # Parse numeric fields
            def parse_numeric(value):
                if not value or value == '**':
                    return None
                try:
                    return float(str(value).replace(',', '').replace('**', ''))
                except:
                    return None
            
            par_value = parse_numeric(row.get('par_value', ''))
            cost = parse_numeric(row.get('cost', ''))
            current_value = parse_numeric(row.get('current_value', ''))
            units_or_shares = parse_numeric(row.get('units_or_shares', ''))
            
            page_number = int(row.get('page_number', 0)) if row.get('page_number', '').strip() else 0
            row_id = int(row.get('row_id', 0)) if row.get('row_id', '').strip() else 0
            
            cur.execute("""
                INSERT INTO investments 
                (plan_id, page_number, row_id, issuer_name, investment_description, 
                 asset_type, par_value, cost, current_value, units_or_shares, confidence)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                plan_id,
                page_number,
                row_id,
                issuer,
                row.get('investment_description', ''),
                row.get('asset_type', ''),
                par_value,
                cost,
                current_value,
                units_or_shares,
                1.0  # confidence = 1.0 for cleaned data
            ))
            inserted_count += 1
            
            if inserted_count % 20 == 0:
                print(f"  Inserted {inserted_count} records...", end='\r')
        
        print(f"  Inserted {inserted_count} investment records   ")
        total_inserted += inserted_count
    
    # Commit the transaction
    conn.commit()
    
    # Verify and report
    print(f"\n{'='*80}")
    print(f"DATABASE UPDATE SUMMARY")
    print(f"{'='*80}")
    
    cur.execute("SELECT COUNT(*) FROM plans WHERE plan_name IS NOT NULL")
    plans_with_names = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM plans")
    total_plans = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM investments")
    total_investments = cur.fetchone()[0]
    
    print(f"\n✓ Database updated successfully!")
    print(f"  Total plans in database: {total_plans}")
    print(f"  Plans with extracted names: {plans_with_names}")
    print(f"  Total investments in database: {total_investments}")
    print(f"  New investments inserted: {total_inserted}")
    
    # Show sample of plan data
    print(f"\n{'='*80}")
    print(f"SAMPLE PLAN DATA")
    print(f"{'='*80}")
    cur.execute("""
        SELECT id, plan_name, sponsor, plan_number, sponsor_ein, plan_year 
        FROM plans 
        WHERE plan_name IS NOT NULL 
        LIMIT 5
    """)
    for row in cur.fetchall():
        print(f"\nPlan ID: {row[0]}")
        print(f"  Name: {row[1]}")
        print(f"  Sponsor: {row[2]}")
        print(f"  Plan Number: {row[3]}")
        print(f"  Sponsor EIN: {row[4]}")
        print(f"  Year: {row[5]}")

    # Show summary
    cur.execute("""
        SELECT issuer_name, COUNT(*) as count, SUM(current_value) as total_value
        FROM investments 
        WHERE plan_id = ? AND issuer_name != ''
        GROUP BY issuer_name
        ORDER BY total_value DESC
        LIMIT 5
    """, (plan_id,))
    
    print(f"\nTop 5 issuers by total value:")
    for issuer, count, total_value in cur.fetchall():
        print(f"  {issuer}: {count} holdings, ${total_value:,.2f}")
