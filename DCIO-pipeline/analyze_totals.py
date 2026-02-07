import csv

# Read clean investments and identify loans
investments = {}
loans = {}
with open('data/outputs/investments_clean.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        pdf = row['pdf_name']
        if pdf not in investments:
            investments[pdf] = []
            loans[pdf] = []
        
        issuer = row.get('issuer_name', '').strip().lower()
        description = row.get('investment_description', '').strip().lower()
        value = row.get('current_value', '').strip()
        
        # Check if this is a loan entry
        is_loan = ('loan' in issuer or 'loan' in description) and \
                  ('participant' in issuer or 'participant' in description or 'receivable' in issuer)
        
        if value:
            try:
                val = float(value.replace(',', ''))
                investments[pdf].append(val)
                if is_loan:
                    loans[pdf].append(val)
            except:
                pass

# Read total rows (looking for main total row, not sub-totals)
totals = {}
with open('data/outputs/removed_total_rows.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        pdf = row['pdf_name']
        issuer = row.get('issuer_name', '').strip().lower()
        # Look for "Total Investments" or just "Total" at the end
        if issuer == 'total' or issuer == 'total investments':
            value = row.get('current_value', '').strip()
            if value:
                try:
                    val = float(value.replace(',', ''))
                    # Keep the largest total (in case of duplicates across pages)
                    if pdf not in totals or val > totals[pdf]:
                        totals[pdf] = val
                except:
                    pass

print('=' * 80)
print('INVESTMENT TOTALS ANALYSIS')
print('=' * 80)
print()

grand_sum = 0
grand_total = 0

for pdf in sorted(investments.keys()):
    sum_investments = sum(investments[pdf])
    sum_loans = sum(loans.get(pdf, []))
    count = len(investments[pdf])
    total_row = totals.get(pdf, 0)
    
    # Add loans to PDF total (loans are typically listed separately in PDF)
    total_with_loans = total_row + sum_loans
    
    diff = sum_investments - total_with_loans
    diff_pct = (diff / total_with_loans * 100) if total_with_loans else 0
    
    grand_sum += sum_investments
    grand_total += total_with_loans
    
    print(f'{pdf}:')
    print(f'  Investment rows count:    {count}')
    print(f'  Sum of investments:       ${sum_investments:,.2f}')
    print(f'  PDF Total row:            ${total_row:,.2f}')
    print(f'  PDF Loans:                ${sum_loans:,.2f}')
    print(f'  PDF Total + Loans:        ${total_with_loans:,.2f}')
    print(f'  Difference:               ${diff:,.2f} ({diff_pct:+.2f}%)')
    print()

print('-' * 80)
print(f'GRAND TOTALS:')
print(f'  Sum of all investments:   ${grand_sum:,.2f}')
print(f'  Sum of PDF Total + Loans: ${grand_total:,.2f}')
print(f'  Difference:               ${grand_sum - grand_total:,.2f} ({(grand_sum - grand_total) / grand_total * 100:+.2f}%)')
print('=' * 80)
