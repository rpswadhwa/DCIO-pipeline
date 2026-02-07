"""Quick test to verify asset type extraction from investment_description"""
import re

# Asset type patterns (same as in llm_cleanup.py)
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

def clean_investment_description(description, asset_type=''):
    """Extract asset type from description and return both cleaned values"""
    description = description.strip()
    
    if not description:
        return description, asset_type
    
    # Check for asset type patterns in description
    description_lower = description.lower()
    detected_asset_type = None
    matched_pattern = None
    
    for pattern, asset_name in ASSET_TYPE_PATTERNS:
        if re.search(pattern, description_lower, re.IGNORECASE):
            detected_asset_type = asset_name
            matched_pattern = pattern
            break
    
    # If asset type found in description, move it to asset_type
    if detected_asset_type:
        # Remove the asset type text from description
        cleaned_description = re.sub(matched_pattern, '', description, flags=re.IGNORECASE).strip()
        # Clean up extra spaces, commas, dashes
        cleaned_description = re.sub(r'\s+', ' ', cleaned_description)
        cleaned_description = re.sub(r'^[,\-\s]+|[,\-\s]+$', '', cleaned_description)
        cleaned_description = cleaned_description.strip()
        
        # Always update asset_type when found in description
        asset_type = detected_asset_type
        
        # Always clear or update the description
        description = cleaned_description if cleaned_description else ''
    
    return description, asset_type

# Test cases
test_cases = [
    ("Common Stock", ""),
    ("Mutual Fund - Class A", ""),
    ("Loan to ABC Corp", ""),
    ("Collective Trust", ""),
    ("Some Fund Name - Common Stock", ""),
    ("Money Market Fund", "Existing Type"),
]

print("=" * 80)
print("TESTING ASSET TYPE EXTRACTION")
print("=" * 80)
print()

for desc, asset in test_cases:
    new_desc, new_asset = clean_investment_description(desc, asset)
    print(f"Original:")
    print(f"  Description: '{desc}'")
    print(f"  Asset Type:  '{asset}'")
    print(f"After Cleaning:")
    print(f"  Description: '{new_desc}'")
    print(f"  Asset Type:  '{new_asset}'")
    print("-" * 80)
    print()
