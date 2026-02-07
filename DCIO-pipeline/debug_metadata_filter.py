import pandas as pd

df = pd.read_csv('data/outputs/investments_raw.csv')
rows = df.to_dict('records')

# Get just our target rows
target_rows = [row for row in rows if 'PIMCO TOTAL' in str(row.get('issuer_name', '')) or 'VG IS TOT' in str(row.get('issuer_name', ''))]

print("Target rows details:")
for row in target_rows:
    print(f"\nissuer_name: {row.get('issuer_name')}")
    print(f"investment_description: {row.get('investment_description')}")
    print(f"asset_type: {row.get('asset_type')}")
    print(f"current_value: {row.get('current_value')}")
    
    # Check what excluded keywords might match
    excluded_keywords = {
        "form 5500", "schedule", "omb no", "department", "plan number",
        "file as", "identity of", "issue lessor", "maturity date", "rate of",
        "collateral", "cost", "current value", "par value", "commingled",
        "notes receivable", "self-directed", "section", "notes:",
        "loans to", "interest rates", "maturities", "with", "to",
    }
    
    issuer = str(row.get('issuer_name', ''))
    description = str(row.get('investment_description', ''))  
    combined = (issuer + " " + description).lower()
    
    matched = [kw for kw in excluded_keywords if kw in combined]
    if matched:
        print(f"Matches excluded keywords: {matched}")
    else:
        print(f"No excluded keywords matched")
        
    # Check if it has current_value
    current_value = str(row.get('current_value','')).strip()
    has_value = current_value and current_value not in ["", "**", "-", "nan"]
    print(f"Has value: {has_value} (value={current_value})")
