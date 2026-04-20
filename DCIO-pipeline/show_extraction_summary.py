#!/usr/bin/env python3
"""Display extraction summary of cleaned investment data."""

import pandas as pd

# Load the output file
df = pd.read_csv('data/outputs/investments_extracted_clean.csv')

print('INVESTMENT DATA EXTRACTION SUMMARY')
print('=' * 80)
print(f'\nTotal Records: {len(df)}')
print(f'Plan Year: {df["plan_year"].unique()[0] if len(df["plan_year"].unique()) > 0 else "N/A"}')
print(f'\nTotal Asset Value: ${df["current_value"].sum():,.2f}')
print(f'Average Value per Investment: ${df["current_value"].mean():,.2f}')

print(f'\nAsset Type Distribution:')
print('-' * 80)
for asset_type, count in df['asset_type'].value_counts().items():
    pct = (count / len(df)) * 100
    value = df[df['asset_type'] == asset_type]['current_value'].sum()
    print(f'  {asset_type:40} {count:3} ({pct:5.1f}%)  ${value:>15,.0f}')

print(f'\n\nTop 10 Holdings by Value:')
print('-' * 80)
top_10 = df.nlargest(10, 'current_value')[['issuer_name', 'investment_description', 'current_value']]
for idx, (_, row) in enumerate(top_10.iterrows(), 1):
    desc = row['investment_description'][:45] if pd.notna(row['investment_description']) else 'N/A'
    print(f'  {idx:2}. {row["issuer_name"]:20} | {desc:45} | ${row["current_value"]:>15,.0f}')

print(f'\n\nField Completeness:')
print('-' * 80)
fields = ['sponsor_ein', 'plan_number', 'plan_year', 'issuer_name', 'investment_description', 'asset_type', 'morningstar_ticker', 'current_value']
for field in fields:
    filled = df[field].notna().sum()
    pct = (filled / len(df)) * 100
    print(f'  {field:30} {filled:3}/{len(df)} ({pct:5.1f}%)')

print('\n' + '=' * 80)
