#!/usr/bin/env python3
import csv
from pathlib import Path

csv_path = "./data/outputs/investments.csv"
output_path = "./data/outputs/investments_clean.csv"

# Read the CSV
rows = []
with open(csv_path, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

print(f"Total rows: {len(rows)}")

# Filter to keep only actual investment holdings
investment_rows = []
excluded_keywords = {
    "form 5500", "schedule", "omb no", "department", "plan number",
    "file as", "identity of", "issue lessor", "maturity date", "rate of",
    "collateral", "cost", "current value", "par value", "commingled",
    "notes receivable", "self-directed", "section", "total", "notes:",
    "loans to", "interest rates", "maturities", "with", "to",
}

for i, row in enumerate(rows):
    issuer = row.get("issuer_name", "").strip()
    description = row.get("investment_description", "").strip()
    current_value = row.get("current_value", "").strip()
    
    # Skip if both issuer and description are empty
    if not issuer and not description:
        print(f"✗ Row {i+2}: Empty issuer and description")
        continue
    
    # Skip if row appears to be a header/metadata
    combined = (issuer + " " + description).lower()
    if any(keyword in combined for keyword in excluded_keywords):
        # Exception: if it has a meaningful current_value, might still be an investment
        if current_value and current_value not in ["", "**", "-"]:
            if issuer and issuer != "Mutual Funds":  # Mutual Funds is a category header
                investment_rows.append(row)
                print(f"✓ Row {i+2}: {issuer[:40]}")
        else:
            print(f"✗ Row {i+2}: Excluded metadata - {issuer[:40]}")
        continue
    
    # Keep rows with non-empty issuer or description AND a current value
    if (issuer or description) and current_value and current_value not in ["", "**", "-"]:
        # Skip empty/placeholder issuers
        if issuer and issuer not in ["", "Mutual Funds"]:
            investment_rows.append(row)
            print(f"✓ Row {i+2}: {issuer[:40]}")
        elif description:
            investment_rows.append(row)
            print(f"✓ Row {i+2}: {description[:40]}")
    else:
        print(f"✗ Row {i+2}: No current value - {issuer[:30] if issuer else description[:30]}")

print(f"\nFound {len(investment_rows)} investment holdings")

# Write clean CSV
if investment_rows:
    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        fieldnames = investment_rows[0].keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(investment_rows)
    
    print(f"✓ Clean CSV saved to {output_path}")
    
    # Show summary
    total_value = 0
    for row in investment_rows:
        val_str = row.get("current_value", "").replace(",", "").replace("**", "").strip()
        try:
            total_value += float(val_str) if val_str else 0
        except:
            pass
    
    print(f"\nSummary:")
    print(f"  Total investment holdings: {len(investment_rows)}")
    print(f"  Total portfolio value: ${total_value:,.2f}")
else:
    print("No investment rows found!")
