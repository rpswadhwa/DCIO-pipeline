"""Stable-value-only Schedule H line 4i scan for filings with no staged holdings.

Full extraction is expensive and fragile; this tool reads only the 4i pages of
each PDF and keeps lines that name a stable value vehicle and carry a dollar
amount. It writes a preview CSV first. `--apply` appends the accepted preview
rows to plan_holdings_staging, and only for ack_ids that have no staging rows
(it never deletes), so the stable value router can then pick them up:

    python extract_stable_value_lines.py --pdf-dir DIR --out preview.csv
    python extract_stable_value_lines.py --apply preview.csv
    python route_stable_value.py --ack-ids <same ids>            # dry run
    python route_stable_value.py --apply --ack-ids <same ids>
"""
import argparse
import csv
import os
import re
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from decimal import Decimal

import pdfplumber

from src.text_extract import _page_value_scale_factor

SV_RE = re.compile(
    r'stable\s*value|stable\s+return|stable\s+income|stable\s+asset|stable\s+principal|'
    r'managed\s+income|synthetic|wrap(?:per)?\s+(?:contract|agreement)|'
    r'guaranteed\s+investment\s+contract|\bgics?\b|general\s+account|fixed\s+account|'
    r'tiaa\s+traditional|guaranteed\s+(?:income|interest)\b|'
    r'interest\s+income\s+fund|capital\s+preservation|benefit[\s-]*responsive|'
    r'\bfixed\s+income\s+fund\s+(?:for|of)\s+retirement|retirement\s+savings\s+trust',
    re.I)
# Names that match SV_RE by accident.
NOT_SV_RE = re.compile(r'strategic\s+income|\bloan\b|money\s+market|short[\s-]+term|treasury\s+bill|'
                       r'wrap\s+fee|total\b|subtotal|fair\s+value\s+of|level\s+[123]\b|'
                       r'net\s+asset\s+value\s+per|contract\s+value\s+adjustment', re.I)
# \s* throughout: some PDFs extract with the spaces dropped ("ScheduleH,Line4i").
PAGE_4I_RE = re.compile(r'schedule\s*h.{0,40}(?:line\s*)?4\s*\(?i\)?|'
                        r'assets\s*\(?held\s*at\s*end\s*of\s*year\)?|identity\s*of\s*issue', re.I | re.S)
NOT_SCHEDULE_RE = re.compile(r'notes\s+to\s+(?:the\s+)?(?:financial\s+statements|statements)|'
                             r'statements?\s+of\s+(?:net\s+assets|changes)|'
                             r'report\s+of\s+independent|independent\s+auditor', re.I)
AMOUNT_RE = re.compile(r'\$?\s*\(?\d{1,3}(?:,\d{3})+(?:\.\d{2})?\)?')
STAGING_ASSET_TYPE = 'stable value fund'


def amount_tokens(line):
    # PDF text often splits a figure after its first digit ("$ 2 8,288,311").
    joined = re.sub(r'(?<=\b\d) (?=\d{1,2},\d{3})', '', line)
    joined = re.sub(r'\b(\d{1,3}) (,\d{3})', r'\1\2', joined)
    return joined, [m for m in AMOUNT_RE.finditer(joined)]


def to_number(token):
    text = token.replace('$', '').replace(',', '').replace(' ', '')
    negative = text.startswith('(')
    value = Decimal(text.strip('()'))
    return -value if negative else value


def schedule_pages(pdf):
    """1-based page numbers of the 4i schedule, plus its continuation pages."""
    pages, texts, inside = [], {}, False
    for index, page in enumerate(pdf.pages, 1):
        text = page.extract_text() or ''
        texts[index] = text
        head = text[:700]
        # The auditor's report and the notes cite "Schedule H, line 4i" too, so
        # only a 4i title at the top of the page, or the 4i column header,
        # opens the schedule; financial statements and notes close it.
        if NOT_SCHEDULE_RE.search(head):
            inside = False
        elif PAGE_4I_RE.search(head) or re.search(r'identity\s+of\s+(?:the\s+)?issue', text, re.I):
            inside = True
        elif inside and re.search(r'schedule\s+h.{0,30}line\s*4\s*j', head, re.I | re.S):
            inside = False
        if inside:
            pages.append(index)
    return pages, texts


def scan_pdf(path):
    ack_id = os.path.splitext(os.path.basename(path))[0]
    with pdfplumber.open(path) as pdf:
        pages, texts = schedule_pages(pdf)
        text_pages = sum(1 for t in texts.values() if len(t) > 200)
        total_pages = len(pdf.pages)
    rows, seen, seen_amounts = [], set(), set()
    for page in pages:
        text = texts[page]
        scale = _page_value_scale_factor(text)
        for line in text.split('\n'):
            if not SV_RE.search(line) or NOT_SV_RE.search(line):
                continue
            joined, tokens = amount_tokens(line)
            if not tokens:
                continue
            last = tokens[-1]
            name = re.sub(r'[\s*$"]+$', '', joined[:tokens[0].start()]).strip(' *"-')
            name = re.sub(r'\s+', ' ', name)
            amount = to_number(last.group()) * scale
            # A page declared "in thousands" can still print a full-dollar column.
            if scale > 1 and any(abs(to_number(t.group()) - amount) <= amount / 1000 for t in tokens[:-1]):
                amount = next(to_number(t.group()) for t in tokens[:-1]
                              if abs(to_number(t.group()) - amount) <= amount / 1000)
            if not name or amount <= 0:
                continue
            # Schedules are often printed twice; the same amount on another
            # page is a repeat (one copy may carry cents), while equal amounts
            # on one page are real.
            dollars = round(amount)
            if (name.lower(), dollars) in seen or any(a == dollars and p != page for p, a in seen_amounts):
                continue
            seen.add((name.lower(), dollars))
            seen_amounts.add((page, dollars))
            # A plan's interest in a master trust is counted at the trust; review before accepting.
            review = 'master_trust' if re.search(r'master\s*trust', line, re.I) else ''
            rows.append(dict(ack_id=ack_id, page=page, raw_entity_name=name, amount=f'{amount:.2f}',
                             amount_tokens=len(tokens), scale=scale, raw_line=line.strip(), review=review))
    status = 'no_text' if text_pages == 0 else 'no_4i_pages' if not pages else 'sv_found' if rows else 'no_sv_lines'
    return rows, dict(ack_id=ack_id, pages=total_pages, text_pages=text_pages,
                      schedule_pages=' '.join(map(str, pages)), status=status, sv_rows=len(rows))


def safe_scan(path):
    try:
        return scan_pdf(path)
    except Exception as exc:  # unreadable PDF: record and move on
        return [], dict(ack_id=os.path.basename(path)[:-4], status=f'error: {exc}'[:200])


def scan(args):
    files = sorted(f for f in os.listdir(args.pdf_dir) if f.lower().endswith('.pdf'))
    fields = ['ack_id', 'page', 'raw_entity_name', 'amount', 'amount_tokens', 'scale', 'raw_line', 'review', 'accept']
    summary_fields = ['ack_id', 'pages', 'text_pages', 'schedule_pages', 'status', 'sv_rows']
    with open(args.out, 'w', newline='', encoding='utf-8') as out, \
            open(args.summary or args.out.replace('.csv', '_files.csv'), 'w', newline='', encoding='utf-8') as summ:
        writer, summary = csv.DictWriter(out, fields), csv.DictWriter(summ, summary_fields)
        writer.writeheader()
        summary.writeheader()
        paths = [os.path.join(args.pdf_dir, name) for name in files]
        with ProcessPoolExecutor(args.workers) as pool:
            results = pool.map(safe_scan, paths)
            for number, (name, (rows, info)) in enumerate(zip(files, results), 1):
                for row in rows:
                    writer.writerow(dict(row, accept='N' if row['review'] else 'Y'))
                summary.writerow(info)
                print(f'[{number}/{len(files)}] {name} {info["status"]} {len(rows)}', flush=True)


def athena_run(client, sql, workgroup, output):
    qid = client.start_query_execution(QueryString=sql, WorkGroup=workgroup,
                                       ResultConfiguration={'OutputLocation': output})['QueryExecutionId']
    while True:
        state = client.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']
        if state['State'] in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            break
        time.sleep(2)
    if state['State'] != 'SUCCEEDED':
        raise RuntimeError(f'Athena {state["State"]}: {state.get("StateChangeReason")}')
    pages = client.get_paginator('get_query_results').paginate(QueryExecutionId=qid)
    rows = [r['Data'] for page in pages for r in page['ResultSet']['Rows']]
    return [[c.get('VarCharValue') for c in r] for r in rows[1:]]


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def drop_already_held(client, rows, acks, args):
    """Skip lines the ledger already counts. Filings without an extracted 4i
    can still carry their stable value fund from Schedule D (plan_cit_history),
    usually at a slightly different value than the 4i, so a line within 10% of
    an existing stable value holding on the same filing is that holding. Each
    holding absorbs at most one line."""
    held = {}
    for ack_id, amount in athena_run(
            client, f"SELECT ack_id, try_cast(plan_investment_amt AS double) FROM {args.database}."
                    f"v_plan_stable_value_holdings WHERE ack_id IN ({','.join(map(quote, acks))})",
            args.workgroup, args.athena_output):
        if amount is not None:
            held.setdefault(ack_id, []).append(Decimal(amount))
    kept, dropped = [], []
    for row in sorted(rows, key=lambda r: -Decimal(r['amount'])):
        amount, pool = Decimal(row['amount']), held.get(row['ack_id'], [])
        match = next((h for h in pool if abs(h - amount) <= max(amount, h) / 10), None)
        if match is None:
            kept.append(row)
        else:
            pool.remove(match)
            dropped.append(row)
    print(f'Skipped {len(dropped)} lines (${sum(Decimal(r["amount"]) for r in dropped):,.0f}) already held '
          f'on the same filing; inserting {len(kept)}')
    return kept


def apply(args):
    import boto3
    with open(args.apply, encoding='utf-8') as handle:
        rows = [r for r in csv.DictReader(handle) if r.get('accept', '').strip().upper() == 'Y']
    if not rows:
        sys.exit('No accepted rows')
    acks = sorted({r['ack_id'] for r in rows})
    if any(not re.fullmatch(r'[0-9A-Z]+', a) for a in acks):
        sys.exit('Unexpected ack_id format')
    client = boto3.client('athena', region_name=args.region)
    table = f'{args.database}.plan_holdings_staging'
    existing = athena_run(client, f"SELECT DISTINCT ack_id FROM {table} WHERE ack_id IN ({','.join(map(quote, acks))})",
                          args.workgroup, args.athena_output)
    if existing:
        sys.exit(f'Refusing: {len(existing)} ack_ids already have staging rows, e.g. {existing[0][0]}')
    rows = drop_already_held(client, rows, acks, args)
    acks = sorted({r['ack_id'] for r in rows})
    for start in range(0, len(rows), 200):
        values = ',\n'.join(
            f"({quote(r['ack_id'])},{quote(r['raw_entity_name'])},{quote(r['raw_entity_name'])},"
            f"CAST({Decimal(r['amount'])} AS decimal(18,2)),'PENDING_AI','PENDING_AI','UNVALIDATED',"
            f"{quote(STAGING_ASSET_TYPE)})" for r in rows[start:start + 200])
        athena_run(client, f'INSERT INTO {table} (ack_id, raw_entity_name, raw_sponsor_name, plan_investment_amt, '
                           f'asset_class, asset_sub_class, validation_status, asset_type) VALUES {values}',
                   args.workgroup, args.athena_output)
    count = athena_run(client, f"SELECT count(*), sum(plan_investment_amt) FROM {table} "
                               f"WHERE ack_id IN ({','.join(map(quote, acks))})", args.workgroup, args.athena_output)
    print(f'Inserted {len(rows)} rows for {len(acks)} filings; staging now has {count[0][0]} rows, ${count[0][1]}')
    with open(args.apply.replace('.csv', '_ack_ids.txt'), 'w') as handle:
        handle.write(','.join(acks))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--pdf-dir')
    parser.add_argument('--out', default='stable_value_lines_preview.csv')
    parser.add_argument('--summary')
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--apply', metavar='PREVIEW_CSV', help='Insert the accept=Y rows of a reviewed preview')
    parser.add_argument('--region', default='us-east-1')
    parser.add_argument('--database', default='default')
    parser.add_argument('--workgroup', default='primary')
    parser.add_argument('--athena-output', default='s3://retirementinsights-reference/athena-results/')
    args = parser.parse_args()
    if args.apply:
        apply(args)
    elif args.pdf_dir:
        scan(args)
    else:
        parser.error('--pdf-dir or --apply is required')


if __name__ == '__main__':
    main()
