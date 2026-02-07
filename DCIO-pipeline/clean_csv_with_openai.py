#!/usr/bin/env python3
import csv
import json
from pathlib import Path
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

csv_path = "./data/outputs/investments.csv"
output_path = "./data/outputs/investments_clean.csv"

# Read the CSV
rows = []
with open(csv_path, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

print(f"Total rows: {len(rows)}")

# Initialize OpenAI client
client = OpenAI()

# Process rows in batches to classify them
batch_size = 20
investment_rows = []

for i in range(0, len(rows), batch_size):
    batch = rows[i:i+batch_size]
    
    # Create a prompt for classification
    batch_data = []
    for row in batch:
        batch_data.append({
            "issuer_name": row.get("issuer_name", ""),
            "investment_description": row.get("investment_description", ""),
            "current_value": row.get("current_value", ""),
            "page_number": row.get("page_number", ""),
        })
    
    prompt = f"""Analyze these rows from a Form 5500 investment schedule. 
Determine which rows represent actual investment holdings (not metadata, headers, or section dividers).

Investment holdings should have:
1. An issuer name or fund name
2. An investment description or fund type
3. A current value (numeric)

Metadata/headers to exclude:
- Rows with empty issuer and description
- Rows that are just headers or labels
- Rows with "Notes", "Loans", "Self-Directed", "Total", "Section", etc. as standalone entries
- Rows that appear to be notes or explanatory text

For each row, respond with a JSON object with "is_investment": true/false

Rows to classify:
{json.dumps(batch_data, indent=2)}

Response format:
[
  {{"is_investment": true/false, "reason": "brief reason"}}
]

Respond ONLY with the JSON array, no other text."""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        max_tokens=1000,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )
    
    try:
        result_text = response.choices[0].message.content.strip()
        # Extract JSON from response
        if "```json" in result_text:
            result_text = result_text.split("```json")[1].split("```")[0]
        elif "```" in result_text:
            result_text = result_text.split("```")[1].split("```")[0]
        
        classifications = json.loads(result_text)
        
        for j, classification in enumerate(classifications):
            if classification.get("is_investment", False):
                investment_rows.append(batch[j])
                print(f"✓ Row {i+j+2}: {batch[j].get('issuer_name', 'N/A')[:30]}")
    except Exception as e:
        print(f"Error processing batch {i//batch_size + 1}: {e}")
        print(f"Response: {response.choices[0].message.content[:200]}")

print(f"\nFound {len(investment_rows)} investment holdings")

# Write clean CSV
if investment_rows:
    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        fieldnames = investment_rows[0].keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(investment_rows)
    
    print(f"✓ Clean CSV saved to {output_path}")
else:
    print("No investment rows found!")
