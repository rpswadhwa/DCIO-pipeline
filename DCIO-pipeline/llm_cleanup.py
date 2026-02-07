"""
LLM-based cleanup to separate investment descriptions and asset types
from issuer names and ensure consistency across rows.
"""
import csv
import os
from openai import OpenAI
import json
import time
import re
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

# Asset type keywords to detect and extract from investment descriptions
ASSET_TYPE_PATTERNS = [
    (r'\bcommon\s+stock\b', 'Common Stock'),
    (r'\bcollective\s+trust\s+fund\b', 'Collective Trust Fund'),
    (r'\bcollective\s+trust\b', 'Collective Trust'),
    (r'\bcommon[/\\]collective\s+trust\s+fund\b', 'Common/Collective Trust Fund'),
    (r'\bmutual\s+fund\b', 'Mutual Fund'),
    (r'\bmoney\s+market\s+fund\b', 'Money Market Fund'),
    (r'\bregistered\s+investment\s+company\b', 'Registered Investment Company'),
    (r'\bseparately\s+managed\s+account\b', 'Separately Managed Account'),
    (r'\bpartnership\s+interest\b', 'Partnership Interest'),
    (r'\bpreferred\s+stock\b', 'Preferred Stock'),
    (r'\bcorporate\s+bond\b', 'Corporate Bond'),
    (r'\bgovernment\s+bond\b', 'Government Bond'),
    (r'\bloan\b', 'Loan'),
]

def clean_investment_description(row_data):
    """
    Extract asset type information from investment_description and move it to asset_type.
    Returns updated row data.
    """
    description = row_data.get('investment_description', '').strip()
    asset_type = row_data.get('asset_type', '').strip()
    
    if not description:
        return row_data
    
    # Check for asset type patterns in description
    description_lower = description.lower()
    detected_asset_type = None
    matched_pattern = None
    
    for pattern, asset_name in ASSET_TYPE_PATTERNS:
        if re.search(pattern, description_lower, re.IGNORECASE):
            detected_asset_type = asset_name
            matched_pattern = pattern
            break
    
    # If asset type found in description, move it to asset_type column
    if detected_asset_type:
        # Remove the asset type text from description
        cleaned_description = re.sub(matched_pattern, '', description, flags=re.IGNORECASE).strip()
        # Clean up extra spaces, commas, dashes
        cleaned_description = re.sub(r'\s+', ' ', cleaned_description)
        cleaned_description = re.sub(r'^[,\-\s]+|[,\-\s]+$', '', cleaned_description)
        cleaned_description = cleaned_description.strip()
        
        # Always update asset_type when found in description
        row_data['asset_type'] = detected_asset_type
        
        # Always clear or update the description (remove the asset type text)
        row_data['investment_description'] = cleaned_description if cleaned_description else ''
    
    return row_data

def analyze_row_with_llm(row_data):
    """
    Use LLM to analyze if issuer_name contains investment description or asset type
    and separate them properly.
    """
    issuer = row_data.get('issuer_name', '').strip()
    description = row_data.get('investment_description', '').strip()
    asset_type = row_data.get('asset_type', '').strip()
    
    # Skip if issuer is empty
    if not issuer:
        return row_data
    
    prompt = f"""Analyze this investment row and determine if the issuer name contains embedded investment description information that should be separated.

Current data:
- Issuer Name: "{issuer}"
- Investment Description: "{description}"
- Asset Type: "{asset_type}"

Note: Asset types (Common Stock, Mutual Fund, Collective Trust, Loan, etc.) have already been extracted from the investment_description.

Investment descriptions typically include fund names, share classes, or additional details about the investment vehicle (but NOT asset types).

Task:
1. Identify if the issuer name contains investment description (like fund names, share classes)
2. Extract the pure issuer/company name
3. Ensure consistency: descriptions should be in investment_description field, NOT asset types

Return a JSON object with:
{{
  "issuer_name": "clean issuer/company name only",
  "investment_description": "investment description (combine with existing if needed, but NO asset types)",
  "asset_type": "asset type classification",
  "changes_made": true/false,
  "explanation": "brief explanation of changes"
}}

If no changes needed, return the original values with changes_made: false."""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an expert at parsing financial investment data and ensuring consistent data structure."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            response_format={"type": "json_object"}
        )
        
        result = json.loads(response.choices[0].message.content)
        return result
        
    except Exception as e:
        print(f"  ⚠ LLM error: {e}")
        return {
            "issuer_name": issuer,
            "investment_description": description,
            "asset_type": asset_type,
            "changes_made": False,
            "explanation": f"Error: {str(e)}"
        }

def main():
    input_file = 'data/outputs/investments_clean.csv'
    output_file = 'data/outputs/investments_clean_llm.csv'
    
    print("=" * 80)
    print("LLM-BASED DATA CLEANUP")
    print("=" * 80)
    print()
    print("Reading clean investment data...")
    
    # Read all rows
    rows = []
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    
    print(f"Processing {len(rows)} rows with LLM analysis...")
    print()
    
    cleaned_rows = []
    changes_count = 0
    description_changes = 0
    
    for idx, row in enumerate(rows, 1):
        # Rate limiting - process in batches to avoid API limits
        if idx > 1 and idx % 50 == 0:
            print(f"  Processed {idx}/{len(rows)} rows... (pausing for rate limit)")
            time.sleep(2)
        
        # First, clean investment_description to extract asset types
        original_description = row.get('investment_description', '')
        original_asset_type = row.get('asset_type', '')
        row = clean_investment_description(row)
        
        if (row.get('investment_description') != original_description or 
            row.get('asset_type') != original_asset_type):
            description_changes += 1
            print(f"  [{idx}] DESCRIPTION CLEANED:")
            print(f"      Original Description: {original_description[:60]}")
            print(f"      New Description: {row.get('investment_description', '')[:60]}")
            print(f"      Asset Type: {row.get('asset_type', '')}")
            print()
        
        # Analyze with LLM
        result = analyze_row_with_llm(row)
        
        if result.get('changes_made'):
            changes_count += 1
            print(f"  [{idx}] UPDATED: {row['issuer_name'][:50]}")
            print(f"      → Issuer: {result['issuer_name'][:50]}")
            print(f"      → Description: {result['investment_description'][:50]}")
            print(f"      → Asset Type: {result['asset_type']}")
            print(f"      → Reason: {result['explanation']}")
            print()
            
            # Update row with cleaned values
            row['issuer_name'] = result['issuer_name']
            row['investment_description'] = result['investment_description']
            row['asset_type'] = result['asset_type']
        
        cleaned_rows.append(row)
    
    # Save cleaned data
    print(f"\nSaving cleaned data to {output_file}...")
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(cleaned_rows)
    
    print()
    print("=" * 80)
    print(f"✓ LLM CLEANUP COMPLETE!")
    print("=" * 80)
    print(f"Total rows processed: {len(rows)}")
    print(f"Investment descriptions cleaned: {description_changes}")
    print(f"Rows modified by LLM: {changes_count}")
    print(f"Rows unchanged: {len(rows) - changes_count - description_changes}")
    print(f"Output file: {output_file}")
    print()

if __name__ == '__main__':
    main()
