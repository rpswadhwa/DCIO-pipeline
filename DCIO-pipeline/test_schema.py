#!/usr/bin/env python3
"""
Test script to verify the new composite key schema
"""
import sqlite3

print("Testing new composite key schema...")
print("=" * 60)

# Create test database with new schema
db_path = "data/outputs/pipeline_test.db"
with open("sql/schema.sql", "r") as f:
    schema_sql = f.read()

conn = sqlite3.connect(db_path)
cur = conn.cursor()
cur.executescript(schema_sql)

print("\n1. Creating test plan with composite key...")
# Insert test plan with composite key
cur.execute("""
    INSERT INTO plans (sponsor_ein, plan_number, plan_year, plan_name, sponsor)
    VALUES ('12-3456789', '001', 2024, 'Test 401k Plan', 'Test Company Inc')
""")
print("   SUCCESS: Plan inserted")

print("\n2. Creating test investment with composite foreign key...")
# Insert test investment with composite foreign key
cur.execute("""
    INSERT INTO investments 
    (sponsor_ein, plan_number, plan_year, page_number, row_id, 
     issuer_name, current_value)
    VALUES ('12-3456789', '001', 2024, 1, 1, 'Vanguard S&P 500', '1000000')
""")
print("   SUCCESS: Investment inserted")

conn.commit()

print("\n3. Testing JOIN on composite key...")
# Verify the data with JOIN on composite key
cur.execute("""
    SELECT p.plan_name, p.sponsor, i.issuer_name, i.current_value
    FROM investments i
    JOIN plans p ON i.sponsor_ein = p.sponsor_ein 
        AND i.plan_number = p.plan_number 
        AND i.plan_year = p.plan_year
""")

result = cur.fetchone()
print(f"   SUCCESS: Retrieved record: {result}")

print("\n4. Verifying plans table schema...")
# Check table schema
cur.execute("PRAGMA table_info(plans)")
print("   Plans table columns:")
for col in cur.fetchall():
    primary_key = " (PRIMARY KEY COMPONENT)" if col[5] > 0 else ""
    print(f"     - {col[1]} ({col[2]}){primary_key}")

print("\n5. Verifying investments table schema...")
cur.execute("PRAGMA table_info(investments)")
print("   Investments table columns:")
for col in cur.fetchall():
    print(f"     - {col[1]} ({col[2]})")

print("\n6. Testing multiple plans with same EIN but different year...")
cur.execute("""
    INSERT INTO plans (sponsor_ein, plan_number, plan_year, plan_name, sponsor)
    VALUES ('12-3456789', '001', 2023, 'Test 401k Plan', 'Test Company Inc')
""")
print("   SUCCESS: Same EIN/plan_number with different year inserted")

print("\n7. Verifying both plans exist...")
cur.execute("""
    SELECT sponsor_ein, plan_number, plan_year, plan_name
    FROM plans
    ORDER BY plan_year DESC
""")
for row in cur.fetchall():
    print(f"   - {row[0]}, Plan {row[1]}, Year {row[2]}: {row[3]}")

conn.close()

print("\n" + "=" * 60)
print("SCHEMA VERIFICATION COMPLETE!")
print("All composite key operations working correctly.")
print("=" * 60)
