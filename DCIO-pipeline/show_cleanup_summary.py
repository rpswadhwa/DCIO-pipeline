#!/usr/bin/env python3
import sqlite3

db = sqlite3.connect('data/outputs/pipeline.db')
cursor = db.cursor()

print("=" * 80)
print("INVESTMENT DATA CLEANUP SUMMARY")
print("=" * 80)

# Overall stats
cursor.execute("SELECT COUNT(*) FROM investments")
total = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(DISTINCT issuer_name) FROM investments")
unique_issuers = cursor.fetchone()[0]

cursor.execute("""
    SELECT COUNT(*) FROM investments 
    WHERE investment_description IS NOT NULL AND investment_description != ''
""")
with_desc = cursor.fetchone()[0]

print(f"\n📊 Overall Statistics:")
print(f"  Total Investments: {total}")
print(f"  Unique Issuers: {unique_issuers}")
print(f"  With Descriptions: {with_desc} ({with_desc*100//total}%)")

# Top issuers
print(f"\n🏢 Top 10 Issuers by Investment Count:")
cursor.execute("""
    SELECT issuer_name, COUNT(*) as count 
    FROM investments 
    GROUP BY issuer_name 
    ORDER BY count DESC 
    LIMIT 10
""")
for issuer, count in cursor.fetchall():
    print(f"  {count:3d} - {issuer}")

# Sample from each company
print(f"\n📋 Sample Cleaned Data by Company:")

for sponsor in ['Amazon.com, Inc.', 'Google LLC', 'Apple Inc.']:
    print(f"\n  {sponsor}:")
    cursor.execute("""
        SELECT i.issuer_name, i.investment_description, i.asset_type
        FROM investments i
        JOIN plans p ON i.sponsor_ein = p.sponsor_ein 
            AND i.plan_number = p.plan_number 
            AND i.plan_year = p.plan_year
        WHERE p.sponsor = ?
        LIMIT 5
    """, (sponsor,))
    
    for issuer, desc, atype in cursor.fetchall():
        desc_short = (desc[:47] + '...') if desc and len(desc) > 50 else (desc or '')
        print(f"    • {issuer:15} → {desc_short:50} [{atype}]")

print("\n" + "=" * 80)
print("✓ Cleanup Complete - Data properly structured!")
print("=" * 80)

db.close()
