#!/usr/bin/env python3
"""
Clean and extract investment data from LLM-enhanced CSV in S3 silver bucket.

DESCRIPTION:
    This script processes the LLM-enhanced investment CSV file from S3, performs
    comprehensive data cleaning, and extracts required fields into a structured output.

EXTRACTED FIELDS:
    - sponsor_ein        : Sponsor Employee Identification Number (from sponsor_plan_key)
    - plan_number        : Pension plan number (from sponsor_plan_key)
    - plan_year          : Tax year of the Form 5500 filing
    - issuer_name        : Asset manager/investment firm name (cleaned)
    - investment_description : Specific fund/investment name (cleaned)
    - asset_type         : Standardized asset classification
    - morningstar_ticker  : Morningstar ticker symbol (if available for funds)
    - current_value      : Current market value normalized to float

INPUT:
    - S3: s3://retirementinsights-silver/tables/investments_clean_llm.csv
    - Local fallback: data/outputs/investments_clean_llm.csv

OUTPUT:
    - Local: data/outputs/investments_extracted_clean.csv
    - S3: s3://retirementinsights-silver/tables/investments_extracted_clean.csv

DATA CLEANING:
    1. Removes rows with missing issuer_name or asset_type
    2. Deduplicates records based on issuer, description, and asset type
    3. Normalizes currency values to float format
    4. Cleans text fields by removing extra whitespace
    5. Extracts sponsor EIN and plan number from combined key
    6. Validates all required fields are present

DATA QUALITY:
    The script generates quality metrics including:
    - Count of records with each field populated
    - Distribution of asset types
    - Top asset managers
    - Duplicate removal statistics

DEPENDENCIES:
    - pandas: Data manipulation
    - python-dotenv: Environment configuration
    - AWS CLI: S3 file transfer (must be configured with AWS credentials)

USAGE:
    python3 clean_llm_investment_file.py

EXAMPLE OUTPUT:
    ================================================================================
    LLM INVESTMENT CLEANING & EXTRACTION
    ================================================================================
    
    [CHECK] Checking for input file...
      ✓ Found in S3: s3://retirementinsights-silver/tables/investments_clean_llm.csv
    
    [DOWNLOAD] Fetching tables/investments_clean_llm.csv from s3://retirementinsights-silver/
      ✓ Downloaded to data/outputs/investments_clean_llm.csv
    
    [LOAD] Loading investment data...
      ✓ Loaded 28 records
    
    [EXTRACT] Extracting required fields...
      ✓ Extracted all required fields
    
    [VALIDATE] Validating and cleaning records...
      ✓ Removed 0 invalid/duplicate records
      ✓ Final count: 28 records
    
    [QUALITY] Data quality summary:
      Records with sponsor_ein: 0
      Records with plan_number: 0
      Records with plan_year: 28
      Records with issuer_name: 28
      Records with investment_description: 28
      Records with asset_type: 28
      Records with morningstar_ticker: 18
      Records with current_value: 28
    
    [DISTRIBUTION] Asset types:
      • Target Date Fund: 12
      • Mutual Fund: 8
      • Common/Collective Trust Fund: 3
      • Other: 2
      • Common Stock: 1
      • Corporate Bond: 1
      • Money Market Fund: 1
    
    [TOP ISSUERS]:
      • Vanguard: 20 holdings
      • State Street Global Advisors: 3 holdings
      • BrokerageLink: 1 holdings
      • PIMCO: 1 holdings
      • Harris Associates: 1 holdings
    
    ✓ COMPLETE: Processed 28 investment records
"""

import os
import re
import subprocess
import pandas as pd
from io import StringIO
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Configuration
S3_BUCKET = "retirementinsights-silver"
S3_INPUT_FILE = "tables/investments_clean_llm.csv"
S3_OUTPUT_FILE = "tables/investments_extracted_clean.csv"
LOCAL_INPUT_PATH = "data/outputs/investments_clean_llm.csv"
LOCAL_OUTPUT_PATH = "data/outputs/investments_extracted_clean.csv"


def download_from_s3(bucket, key, local_path=None):
    """Download file from S3 using AWS CLI."""
    print(f"\n[DOWNLOAD] Fetching {key} from s3://{bucket}/")
    try:
        s3_url = f"s3://{bucket}/{key}"
        if local_path:
            result = subprocess.run(
                ["aws", "s3", "cp", s3_url, local_path],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                print(f"  ✓ Downloaded to {local_path}")
                return local_path
            else:
                print(f"  ✗ Error: {result.stderr}")
                return None
        else:
            result = subprocess.run(
                ["aws", "s3", "cp", s3_url, "-"],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                print(f"  ✓ Retrieved from S3 ({len(result.stdout)} bytes)")
                return result.stdout
            else:
                print(f"  ✗ Error: {result.stderr}")
                return None
    except Exception as e:
        print(f"  ✗ Error downloading file: {e}")
        return None


def upload_to_s3(bucket, key, file_path):
    """Upload file to S3 using AWS CLI."""
    print(f"\n[UPLOAD] Uploading {file_path} to s3://{bucket}/{key}")
    try:
        s3_url = f"s3://{bucket}/{key}"
        result = subprocess.run(
            ["aws", "s3", "cp", file_path, s3_url],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            print(f"  ✓ Successfully uploaded to S3")
        else:
            print(f"  ✗ Error uploading file: {result.stderr}")
    except Exception as e:
        print(f"  ✗ Error uploading file: {e}")


def normalize_currency(value):
    """Convert currency string to float."""
    if pd.isna(value) or value == "":
        return None
    
    value_str = str(value).strip()
    
    # Remove common currency symbols and formatting
    value_str = re.sub(r"[\$,\s]", "", value_str)
    value_str = re.sub(r"\((\d+)\)", r"-\1", value_str)  # Handle (1000) as -1000
    
    try:
        return float(value_str)
    except (ValueError, TypeError):
        return None


def extract_sponsor_ein(sponsor_plan_key):
    """Extract sponsor EIN from sponsor_plan_key."""
    if pd.isna(sponsor_plan_key) or sponsor_plan_key == "":
        return None
    
    # Try to extract EIN pattern (9 digits, usually in format XX-XXXXXXX)
    ein_match = re.search(r"(\d{2}-\d{7}|\d{9})", str(sponsor_plan_key))
    if ein_match:
        ein = ein_match.group(1)
        # Normalize to XX-XXXXXXX format
        if "-" not in ein:
            ein = f"{ein[:2]}-{ein[2:]}"
        return ein
    
    return None


def extract_plan_number(sponsor_plan_key):
    """Extract plan number from sponsor_plan_key."""
    if pd.isna(sponsor_plan_key) or sponsor_plan_key == "":
        return None
    
    # Try to extract plan number (typically 3-4 digits after EIN)
    plan_match = re.search(r"[\-\s]([0-9]{3,4})[\-\s]?$", str(sponsor_plan_key))
    if plan_match:
        return plan_match.group(1).zfill(3)
    
    return None


def clean_text_field(value):
    """Clean and normalize text fields."""
    if pd.isna(value) or value == "":
        return None
    
    text = str(value).strip()
    # Remove extra whitespace
    text = re.sub(r"\s+", " ", text)
    # Remove leading/trailing special characters
    text = text.strip("'\"")
    
    return text if text else None


def validate_row(row):
    """Validate that required fields are present."""
    required_fields = ["issuer_name", "asset_type"]
    
    for field in required_fields:
        if pd.isna(row.get(field)) or row.get(field) == "":
            return False
    
    return True


def clean_and_extract(input_file):
    """
    Clean and extract investment data.
    
    Args:
        input_file: Path to input CSV file
    
    Returns:
        DataFrame with extracted and cleaned data
    """
    print("\n[LOAD] Loading investment data...")
    
    try:
        # Load CSV from local file
        df = pd.read_csv(input_file)
        print(f"  ✓ Loaded {len(df)} records")
    except Exception as e:
        print(f"  ✗ Error loading file: {e}")
        return None
    
    print("\n[EXTRACT] Extracting required fields...")
    
    # Initialize result dataframe with required columns
    result = pd.DataFrame()
    
    # Extract sponsor_ein and plan_number from sponsor_plan_key if present
    if "sponsor_plan_key" in df.columns:
        result["sponsor_ein"] = df["sponsor_plan_key"].apply(extract_sponsor_ein)
        result["plan_number"] = df["sponsor_plan_key"].apply(extract_plan_number)
    else:
        result["sponsor_ein"] = df.get("sponsor_ein", None)
        result["plan_number"] = df.get("plan_number", None)
    
    # Extract basic fields
    result["plan_year"] = df.get("plan_year", None)
    result["issuer_name"] = df.get("issuer_name", "").apply(clean_text_field)
    result["investment_description"] = df.get("investment_description", "").apply(clean_text_field)
    result["asset_type"] = df.get("asset_type", "").apply(clean_text_field)
    result["morningstar_ticker"] = df.get("morningstar_ticker", "").apply(clean_text_field)
    
    # Extract current_value (asset value) and normalize
    result["current_value"] = df.get("current_value", 0).apply(normalize_currency)
    
    print(f"  ✓ Extracted all required fields")
    
    # Validation and cleaning
    print("\n[VALIDATE] Validating and cleaning records...")
    
    initial_count = len(result)
    
    # Remove rows with missing required fields
    result = result.dropna(subset=["issuer_name", "asset_type"])
    
    # Remove rows where asset_type is empty
    result = result[result["asset_type"].str.strip() != ""]
    
    # Remove duplicate rows
    result = result.drop_duplicates(subset=["issuer_name", "investment_description", "asset_type"])
    
    removed_count = initial_count - len(result)
    print(f"  ✓ Removed {removed_count} invalid/duplicate records")
    print(f"  ✓ Final count: {len(result)} records")
    
    # Data quality summary
    print("\n[QUALITY] Data quality summary:")
    print(f"  Records with sponsor_ein: {result['sponsor_ein'].notna().sum()}")
    print(f"  Records with plan_number: {result['plan_number'].notna().sum()}")
    print(f"  Records with plan_year: {result['plan_year'].notna().sum()}")
    print(f"  Records with issuer_name: {result['issuer_name'].notna().sum()}")
    print(f"  Records with investment_description: {result['investment_description'].notna().sum()}")
    print(f"  Records with asset_type: {result['asset_type'].notna().sum()}")
    print(f"  Records with morningstar_ticker: {result['morningstar_ticker'].notna().sum()}")
    print(f"  Records with current_value: {result['current_value'].notna().sum()}")
    
    # Asset type distribution
    print("\n[DISTRIBUTION] Asset types:")
    for asset_type, count in result["asset_type"].value_counts().items():
        print(f"  • {asset_type}: {count}")
    
    # Top issuers
    print("\n[TOP ISSUERS]:")
    for issuer, count in result["issuer_name"].value_counts().head(5).items():
        print(f"  • {issuer}: {count} holdings")
    
    return result


def save_to_local(df, output_path):
    """Save dataframe to local CSV file."""
    print(f"\n[SAVE LOCAL] Saving to {output_path}")
    try:
        # Create output directory if needed
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        df.to_csv(output_path, index=False, encoding="utf-8")
        print(f"  ✓ Saved successfully ({len(df)} records)")
        return output_path
    except Exception as e:
        print(f"  ✗ Error saving file: {e}")
        return None


def main():
    """Main execution."""
    print("=" * 80)
    print("LLM INVESTMENT CLEANING & EXTRACTION")
    print("=" * 80)
    print(f"Model: GPT-enhanced investment data from S3")
    print(f"Input: s3://{S3_BUCKET}/{S3_INPUT_FILE}")
    print(f"Output: s3://{S3_BUCKET}/{S3_OUTPUT_FILE}")
    
    # Determine input source (check S3 first, then local)
    input_source = None
    
    print("\n[CHECK] Checking for input file...")
    
    # Try S3 first
    s3_url = f"s3://{S3_BUCKET}/{S3_INPUT_FILE}"
    result = subprocess.run(
        ["aws", "s3", "ls", s3_url],
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print(f"  ✓ Found in S3: {s3_url}")
        # Download from S3 to local path first
        if download_from_s3(S3_BUCKET, S3_INPUT_FILE, LOCAL_INPUT_PATH):
            input_source = LOCAL_INPUT_PATH
    else:
        print(f"  ✗ Not found in S3, checking local path...")
        if os.path.exists(LOCAL_INPUT_PATH):
            print(f"  ✓ Found locally: {LOCAL_INPUT_PATH}")
            input_source = LOCAL_INPUT_PATH
        else:
            print(f"  ✗ File not found in S3 or local path")
            print(f"    Expected S3: {s3_url}")
            print(f"    Expected local: {LOCAL_INPUT_PATH}")
            return
    
    # Clean and extract
    df_cleaned = clean_and_extract(input_source)
    
    if df_cleaned is None or len(df_cleaned) == 0:
        print("\n✗ FAILED: No data to process")
        return
    
    # Save locally
    local_output = save_to_local(df_cleaned, LOCAL_OUTPUT_PATH)
    
    # Upload to S3
    if local_output and os.path.exists(local_output):
        upload_to_s3(S3_BUCKET, S3_OUTPUT_FILE, local_output)
    
    # Summary
    print("\n" + "=" * 80)
    print(f"✓ COMPLETE: Processed {len(df_cleaned)} investment records")
    print("=" * 80)
    print(f"\nOutput files:")
    print(f"  • Local: {LOCAL_OUTPUT_PATH}")
    print(f"  • S3: {s3_url.replace(S3_INPUT_FILE, S3_OUTPUT_FILE)}")
    
    return df_cleaned


if __name__ == "__main__":
    main()
