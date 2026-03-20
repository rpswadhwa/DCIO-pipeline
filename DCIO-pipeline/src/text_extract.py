import json
import os
import re
from typing import Dict, List, Tuple, Optional

import camelot
import pdfplumber
from openai import OpenAI
from rapidfuzz import process, fuzz

import pandas as pd

from .data_cleaner import handle_split_rows, parse_investment_row
from .utils import load_yaml, normalize_whitespace


def _best_header_match(header: str, synonyms: Dict[str, List[str]]) -> Tuple[str, int]:
    header = header.lower()
    best_field = ""
    best_score = 0
    for field, terms in synonyms.items():
        match, score, _ = process.extractOne(
            header,
            terms,
            scorer=fuzz.partial_ratio,
        ) or ("", 0, None)
        if score > best_score:
            best_field = field
            best_score = score
    return best_field, best_score


def classify_pages_text(pdf_path: str, keywords_yml: str) -> List[Dict]:
    cfg = load_yaml(keywords_yml)
    keywords = [k.upper() for k in cfg.get("supplemental_schedule_keywords", [])]
    negatives = [k.upper() for k in cfg.get("negative_keywords", [])]
    min_hits = int(cfg.get("min_keyword_hits", 1))
    max_lines = int(cfg.get("header_scan_max_lines", 12))

    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            lines = [normalize_whitespace(l) for l in text.splitlines() if l.strip()]
            header_lines = lines[:max_lines]
            header_text = " ".join(header_lines).upper()
            hits = sum(1 for k in keywords if k in header_text)
            neg_hits = sum(1 for k in negatives if k in header_text)
            pages.append(
                {
                    "pdf": pdf_path,
                    "pdf_stem": pdf_path.split("/")[-1].rsplit(".", 1)[0],
                    "page_number": i,
                    "header_text": header_text,
                    "is_supplemental": 1 if hits >= min_hits and neg_hits == 0 else 0,
                }
            )
    return pages


def extract_ein_from_pdf(pdf_path: str, schedule_h_pages: List[int]) -> Optional[Dict[str, str]]:
    """
    Extract EIN, plan name, and sponsor from PDF.
    
    Priority:
    1. Extract from Schedule H pages (Line 4(i) - most reliable, has actual EIN)
    2. Validate/fallback to Part II (pages 1-3) if needed
    
    Returns dict with: ein, plan_number, plan_name, sponsor
    """
    # Patterns for EIN - handle different dash characters (hyphen -, en-dash ‐, etc.)
    ein_patterns = [
        re.compile(r'Employer\s+Identification\s+Number[:\s]*([0-9]{2}[\-\u2010-\u2015\s]?[0-9]{7})', re.IGNORECASE),
        re.compile(r'EIN[\s#:]*([0-9]{2}[\-\u2010-\u2015\s]?[0-9]{7})', re.IGNORECASE),
        re.compile(r'2b[:\s]+Employer[^0-9]*([0-9]{2}[\-\u2010-\u2015][0-9]{7})', re.IGNORECASE | re.DOTALL),  # Part II field 2b
    ]
    
    # Patterns for plan number
    plan_patterns = [
        re.compile(r'Plan[\s#:]*([0-9]{1,6})', re.IGNORECASE),
        re.compile(r'PN[:\s#]*([0-9]{1,6})', re.IGNORECASE),
        re.compile(r'\(PN\)[\s:]*([0-9]{1,6})', re.IGNORECASE),
    ]
    
    ein_schedule_h = None
    ein_part_ii = None
    plan_number = None
    plan_name = None
    sponsor = None
    
    with pdfplumber.open(pdf_path) as pdf:
        # STEP 1: Extract from Schedule H pages (PRIORITY - has actual EIN)
        if schedule_h_pages:
            for page_num in schedule_h_pages:
                if page_num < 1 or page_num > len(pdf.pages):
                    continue
                
                page = pdf.pages[page_num - 1]
                text = page.extract_text() or ""
                
                # Only process if this is actually a Schedule H page
                if 'SCHEDULE H' not in text.upper():
                    continue
                
                lines = text.split('\n')
                
                # Extract EIN from Schedule H
                if not ein_schedule_h:
                    for line in lines[:20]:  # Check first 20 lines of Schedule H
                        for pattern in ein_patterns:
                            match = pattern.search(line)
                            if match:
                                ein_raw = match.group(1)
                                # Normalize: remove all dash variants and spaces, then reformat
                                ein_clean = re.sub(r'[\-\u2010-\u2015\s]', '', ein_raw)
                                if len(ein_clean) == 9 and ein_clean.isdigit():
                                    ein_schedule_h = f"{ein_clean[:2]}-{ein_clean[2:]}"
                                    break
                        if ein_schedule_h:
                            break
                
                # Extract plan number from Schedule H
                if not plan_number:
                    for line in lines[:20]:
                        for pattern in plan_patterns:
                            match = pattern.search(line)
                            if match:
                                pn = match.group(1)
                                # Filter out obviously wrong numbers
                                if pn not in ['2024', '2025', '2023'] and len(pn) <= 6:
                                    plan_number = pn.lstrip('0') or '1'
                                    break
                        if plan_number:
                            break
                
                # Extract sponsor and plan name from Schedule H header
                if not sponsor or not plan_name:
                    for i, line in enumerate(lines[:15]):
                        line_stripped = line.strip()
                        # Look for plan name (first line with plan name pattern)
                        if not plan_name and ('401' in line or 'PLAN' in line.upper() or 'SAVINGS' in line.upper()):
                            if len(line_stripped) > 10 and 'SCHEDULE' not in line_stripped:
                                plan_name = line_stripped
                        
                        # Look for sponsor (typically says "Plan Sponsor:")
                        if not sponsor and 'PLAN SPONSOR' in line.upper():
                            sponsor_match = re.search(r'Plan\s+Sponsor[:\s]+(.+)', line, re.IGNORECASE)
                            if sponsor_match:
                                sponsor = sponsor_match.group(1).strip()
                
                # If we found EIN on this Schedule H page, we're done
                if ein_schedule_h:
                    break
        
        # STEP 2: Check Part II (pages 1-3) for validation or backup
        for page_num in range(1, min(4, len(pdf.pages) + 1)):
            page = pdf.pages[page_num - 1]
            text = page.extract_text() or ""
            lines = text.split('\n')
            
            # Look for Part II EIN (may be redacted, so we use Schedule H as priority)
            if not ein_part_ii:
                for line in lines:
                    for pattern in ein_patterns:
                        match = pattern.search(line)
                        if match:
                            ein_raw = match.group(1)
                            ein_clean = re.sub(r'[\-\u2010-\u2015\s]', '', ein_raw)
                            if len(ein_clean) == 9 and ein_clean.isdigit():
                                # Check if it's not a placeholder (00-0000000, 12-3456789, etc.)
                                if not re.match(r'^(00-?0000000|12-?3456789|01-?2345678)$', ein_raw.replace(' ', '')):
                                    ein_part_ii = f"{ein_clean[:2]}-{ein_clean[2:]}"
                                    break
                    if ein_part_ii:
                        break
            
            # Extract plan number from Part II if not found in Schedule H
            if not plan_number:
                for line in lines:
                    # Look for plan number near form field identifiers
                    if 'PN' in line or 'plan number' in line.lower():
                        for pattern in plan_patterns:
                            match = pattern.search(line)
                            if match:
                                pn = match.group(1)
                                if pn not in ['2024', '2025', '2023'] and len(pn) <= 6:
                                    plan_number = pn.lstrip('0') or '1'
                                    break
                    if plan_number:
                        break
    
    # Choose the EIN: prefer Schedule H (actual EIN), fallback to Part II if valid
    final_ein = ein_schedule_h or ein_part_ii
    
    # Validate: if both exist and differ, log warning but use Schedule H
    if ein_schedule_h and ein_part_ii and ein_schedule_h != ein_part_ii:
        print(f"    ⚠ EIN mismatch - Schedule H: {ein_schedule_h}, Part II: {ein_part_ii} (using Schedule H)")
    
    if final_ein:
        return {
            'ein': final_ein,
            'plan_number': plan_number or '001',
            'plan_name': plan_name,
            'sponsor': sponsor
        }
    return None


def _llm_normalize_headers(client: OpenAI, model: str, headers: List[str], schema_fields: List[str]) -> Dict[int, str]:
    prompt = {
        "headers": headers,
        "schema_fields": schema_fields,
        "instruction": "Map each header to the best matching schema field or null. Return JSON with keys as header index and value as schema field or null.",
    }
    response = client.responses.create(
        model=model,
        input=json.dumps(prompt),
    )
    text = response.output_text
    try:
        data = json.loads(text)
        return {int(k): v for k, v in data.items() if v}
    except Exception:
        return {}


def extract_text_based_investments(pdf_path: str, page_num: int) -> List[Dict]:
    """
    Extract investment data from text-based format (non-table).
    Used as fallback when camelot can't detect tables.
    
    Expected format:
    ISSUER NAME    DESCRIPTION    TYPE    ** $VALUE
    """
    investments = []
    
    with pdfplumber.open(pdf_path) as pdf:
        if page_num < 1 or page_num > len(pdf.pages):
            return investments
        
        page = pdf.pages[page_num - 1]
        text = page.extract_text() or ""
        
        # Check if this is a Schedule H Line 4(i) investment page
        if 'Schedule H, Line 4(i)' not in text and 'SCHEDULE OF ASSETS' not in text.upper():
            return investments
        
        lines = text.split('\n')
        
        # Find where investment data starts (after headers)
        data_start_idx = 0
        for i, line in enumerate(lines):
            if 'CURRENT VALUE' in line.upper() or 'MATURITY VALUE' in line.upper():
                data_start_idx = i + 1
                break
        
        # Parse each line as a potential investment
        for i in range(data_start_idx, len(lines)):
            line = lines[i].strip()
            
            # Skip empty lines, headers, and footer markers
            if not line or len(line) < 10:
                continue
            if any(skip in line.upper() for skip in ['SCHEDULE', 'PAGE', 'EIN #', 'PLAN #', '(In Thousands)', 'December']):
                continue
            
            # Look for investment lines that have a value at the end
            # Format: ISSUER    TYPE    ** $VALUE or ** VALUE
            # Value pattern: comma-separated numbers, possibly with $ or **
            value_pattern = r'\*\*\s*\$?\s*([\d,]+)'
            value_match = re.search(value_pattern, line)
            
            if not value_match:
                continue
            
            current_value = value_match.group(1).replace(',', '')
            
            # Check if values are reported "(In Thousands)" and need multiplication
            # Look for this indicator in the first few lines of the page
            multiply_by_1000 = '(In Thousands)' in '\n'.join(lines[:10])
            if multiply_by_1000:
                try:
                    current_value = str(int(current_value) * 1000)
                except ValueError:
                    pass  # Keep original if conversion fails
            
            # Everything before the ** separator is issuer/description/type
            issuer_description = line[:value_match.start()].strip()
            
            # Try to identify asset type keywords
            asset_type = ""
            asset_type_patterns = {
                'COLLECTIVE INVESTMENT TRUST': 'Common/Collective Trust Fund',
                'COMMON/COLLECTIVE TRUST': 'Common/Collective Trust Fund',
                'SEPARATELY MANAGED ACCOUNT': 'Separately Managed Account',
                'SELF DIRECTED BROKERAGE': 'Self-Directed Brokerage Account',
                'MUTUAL FUND': 'Mutual Fund',
                'COMMON STOCK': 'Common Stock',
            }
            
            for pattern, asset_name in asset_type_patterns.items():
                if pattern in issuer_description.upper():
                    asset_type = asset_name
                    # Remove asset type from issuer name
                    issuer_description = re.sub(pattern, '', issuer_description, flags=re.IGNORECASE).strip()
                    break
            
            # The remaining text is the issuer name (and possibly description)
            # Often issuer is before any asset type keywords
            issuer_name = issuer_description.strip()
            
            # Remove asterisk markers for party-in-interest
            issuer_name = issuer_name.lstrip('*').strip()
            
            investment = {
                'issuer_name': issuer_name,
                'investment_description': '',
                'asset_type': asset_type,
                'par_value': '',
                'cost': '**',  # Indicated by ** in format
                'current_value': current_value,
                'units_or_shares': '',
                'page_number': page_num,
                'row_id': i - data_start_idx + 1,
            }
            
            investments.append(investment)
    
    return investments


def extract_tables_and_map(
    pdf_path: str,
    supplemental_pages: List[int],
    schema_yml: str,
    model: str,
    use_llm: bool = True,
) -> Tuple[Optional[Dict[str, str]], List[Dict]]:
    cfg = load_yaml(schema_yml)
    fields = cfg["schema"]["fields"]
    synonyms = cfg["schema"]["header_synonyms"]

    if not supplemental_pages:
        return None, []
    
    # Extract EIN and plan info from Schedule H pages
    plan_info = extract_ein_from_pdf(pdf_path, supplemental_pages)

    pages_arg = ",".join(str(p) for p in supplemental_pages)
    tables = camelot.read_pdf(pdf_path, pages=pages_arg, flavor="stream")

    if use_llm:
        api_key = os.getenv("OPENAI_API_KEY")
        client = OpenAI(api_key=api_key) if api_key else None
    else:
        client = None
    mapped_pages: Dict[int, List[Dict]] = {}
    
    # Track which pages had tables extracted
    pages_with_tables = set()
    for table in tables:
        pages_with_tables.add(int(table.page))
    
    # Separate storage for text-extracted pages (no DataFrame processing needed)
    text_extracted_pages: Dict[int, List[Dict]] = {}

    for table in tables:
        df = table.df
        if df.shape[0] < 2:
            continue
        
        # Find the actual header rows - headers may span multiple rows
        # Look for rows with high schema matches and also check for partial keywords
        # BUT: Only consider first 4 rows as potential headers to avoid false positives
        header_rows = []
        for idx in range(min(4, df.shape[0])):  # Changed from 8 to 4
            potential_header = [normalize_whitespace(h) for h in df.iloc[idx].tolist()]
            match_count = 0
            partial_match = False
            for h in potential_header:
                field, score = _best_header_match(h, synonyms)
                if score >= 70:
                    match_count += 1
                elif any(kw in h.lower() for kw in ['current', 'value', 'cost', 'par', 'date', 'rate', 'lessor', 'issue', 'issuer', 'identity']):
                    partial_match = True
            # Only mark as header if we have strong matches OR partial matches in the first 3 rows
            if (match_count >= 2) or (idx < 3 and (match_count > 0 or partial_match)):
                header_rows.append(idx)
        
        # If we found header rows, use the first and last to determine the header span
        if header_rows:
            best_header_row = header_rows[0]
            last_header_row = header_rows[-1]
        else:
            # Fallback to row 0 if no matches found
            best_header_row = 0
            last_header_row = 0
        
        # Combine headers from all header rows
        combined_header = [""] * df.shape[1]
        for hrow in range(best_header_row, last_header_row + 1):
            row_vals = [normalize_whitespace(h) for h in df.iloc[hrow].tolist()]
            for col_idx, val in enumerate(row_vals):
                if val and not combined_header[col_idx]:
                    combined_header[col_idx] = val
                elif val and combined_header[col_idx]:
                    # Append multi-row headers with a space
                    combined_header[col_idx] = combined_header[col_idx] + " " + val
        
        header = combined_header

        column_map = {}
        for i, h in enumerate(header):
            field, score = _best_header_match(h, synonyms)
            if field and score >= 70:
                column_map[i] = field

        if use_llm and client is not None:
            llm_map = _llm_normalize_headers(client, model, header, fields)
            for k, v in llm_map.items():
                column_map[k] = v

        page_num = int(table.page)
        # Start from the row after the last header row
        data_start_row = last_header_row + 1
        for row_idx in range(data_start_row, df.shape[0]):
            row_data = {f: "" for f in fields}
            row_data["page_number"] = page_num
            row_data["row_id"] = row_idx - data_start_row + 1
            row = df.iloc[row_idx].tolist()
            for col_idx, cell in enumerate(row):
                text = normalize_whitespace(str(cell))
                if not text:
                    continue
                field = column_map.get(col_idx)
                if field:
                    if row_data[field]:
                        row_data[field] = normalize_whitespace(row_data[field] + " " + text)
                    else:
                        row_data[field] = text
            mapped_pages.setdefault(page_num, []).append(row_data)

    # FALLBACK: Check if table extraction produced mostly empty data
    # If so, try text-based extraction instead
    pages_to_retry = []
    for page_num, rows in mapped_pages.items():
        if not rows:
            pages_to_retry.append(page_num)
            continue
        
        # Count how many rows have meaningful data (non-empty issuer or description with value)
        meaningful_rows = 0
        for row in rows:
            issuer = row.get('issuer_name', '').strip()
            desc = row.get('investment_description', '').strip()
            value = row.get('current_value', '').strip()
            if (issuer or desc) and value and value not in ['', '**', '-', 'nan']:
                meaningful_rows += 1
        
        # If less than 10% of rows have data, consider it a failed extraction
        if len(rows) > 5 and meaningful_rows / len(rows) < 0.1:
            print(f"    Table extraction on page {page_num} yielded poor results ({meaningful_rows}/{len(rows)} meaningful rows)")
            pages_to_retry.append(page_num)
    
    # Remove poor quality pages from mapped_pages so we can retry with text extraction
    for page_num in pages_to_retry:
        if page_num in mapped_pages:
            del mapped_pages[page_num]
    
    # RETRY with text-based extraction for pages that had no tables or poor table extraction
    for page_num in supplemental_pages:
        if page_num not in pages_with_tables or page_num in pages_to_retry:
            print(f"    No tables found on page {page_num}, trying text-based extraction...")
            text_investments = extract_text_based_investments(pdf_path, page_num)
            if text_investments:
                print(f"      ✓ Extracted {len(text_investments)} investments from text")
                # Store separately - these are already properly formatted
                text_extracted_pages[page_num] = text_investments
            else:
                print(f"      ⚠ No investments found in text format either")

    # Build results: Process table data and text data separately
    result = []
    
    # Process TABLE-EXTRACTED pages with DataFrame operations
    for page_num, rows in mapped_pages.items():
        df = pd.DataFrame(rows)
        df = handle_split_rows(df)
        cleaned_rows = []
        for row in df.to_dict(orient='records'):
            parsed = parse_investment_row(row)
            # Merge parsed fields back
            row['issuer_name'] = parsed['issuer_name']
            row['asset_type'] = parsed['asset_type']
            row['investment_description'] = parsed['investment_description']
            cleaned_rows.append(row)
        
        result.append(
            {
                "pdf": pdf_path,
                "pdf_stem": pdf_path.split("/")[-1].rsplit(".", 1)[0],
                "page_number": page_num,
                "mapped_rows": cleaned_rows,
                "ocr_cells": [],
                "normalized_path": pdf_path,
            }
        )
    
    # Process TEXT-EXTRACTED pages WITHOUT DataFrame operations (already clean)
    for page_num, rows in text_extracted_pages.items():
        # Text-extracted data is already properly formatted, use as-is
        result.append(
            {
                "pdf": pdf_path,
                "pdf_stem": pdf_path.split("/")[-1].rsplit(".", 1)[0],
                "page_number": page_num,
                "mapped_rows": rows,  # Use directly without processing
                "ocr_cells": [],
                "normalized_path": pdf_path,
            }
        )
    
    return plan_info, result
