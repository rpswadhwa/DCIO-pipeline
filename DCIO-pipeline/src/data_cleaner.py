import pandas as pd
import re
from .asset_type_patterns import ASSET_TYPE_PATTERNS, detect_asset_type, detect_asset_type_strict

_SHARES_OF_RE = re.compile(
    r'^\s*[\d,]+(?:\.\d+)?\s+shares?\s+of\s+(.+)$',
    re.IGNORECASE,
)

_FINANCIAL_JUNK_RE = re.compile(
    r'(\d+\.\d+%)'                        # interest rate: 4.25%
    r'|(\b\d{1,2}/\d{1,2}/\d{2,4}\b)'    # date: 03/15/2027
    r'|(\b(?=[A-Z0-9]*[0-9])[A-Z0-9]{9}\b)'  # CUSIP: 912828YK0 (must contain digit)
    r'|\b(due|maturing|coupon|collateral|maturity|interest|rate|dated)\b',
    re.IGNORECASE,
)

# Strips trailing share/unit counts before asset type label check.
# Handles both "Mutual Fund, 5,770,653 shares" (comma separator) and
# "Mutual Fund-6,576,777 shares" (dash separator).
# Uses two alternatives so plain years like "2035" are NOT stripped:
#   1. any number followed by explicit shares/units keyword
#   2. comma-formatted count (has internal commas like 5,770,653) — optional keyword
_TRAILING_SHARES_RE = re.compile(
    r'[,\s\-]+\d[\d,]*\s+(?:shares?|units?)\s*$'
    r'|[,\s\-]+\d{1,3}(?:,\d{3})+\.?\d*\s*(?:shares?|units?)?\s*$',
    re.IGNORECASE,
)


def is_meaningful_description(text: str) -> bool:
    """
    Returns True if text is a real fund name worth keeping.
    Returns False if it is blank, a pure asset type label (with or without
    trailing share counts), or financial junk.
    """
    text = (text or '').strip()
    if not text:
        return False

    # Strip trailing share/unit counts then check if what remains is a pure asset type label
    stripped = _TRAILING_SHARES_RE.sub('', text).strip()
    for pattern, _ in ASSET_TYPE_PATTERNS:
        if re.fullmatch(pattern, stripped, re.IGNORECASE):
            return False

    # Financial junk — rates, dates, CUSIPs, bond terminology
    if _FINANCIAL_JUNK_RE.search(text):
        return False

    return True

def extract_fund_names_from_descriptions(rows):
    """
    For rows where investment_description matches '{number} shares of {Fund Name}',
    replace investment_description with the extracted fund name.
    """
    result = []
    for row in rows:
        row = dict(row)
        description = str(row.get('investment_description', '') or '').strip()
        if description:
            m = _SHARES_OF_RE.match(description)
            if m:
                row['investment_description'] = m.group(1).strip()
        result.append(row)
    return result


def parse_investment_row(row):
    """
    Parse investment row to properly separate issuer, asset type, and description
    """
    issuer = row.get('issuer_name', '').strip()
    description = row.get('investment_description', '').strip()
    existing_asset_type = row.get('asset_type', '').strip()
    
    # Description-level type is more specific than a propagated section type.
    # Use fullmatch so fund names containing type keywords (e.g. 'BlackRock Index Fund')
    # don't override a correct section type.
    desc_type = detect_asset_type_strict(description) if description else ''
    if desc_type:
        asset_type = desc_type
    elif not existing_asset_type:
        asset_type = detect_asset_type(issuer)
    else:
        asset_type = existing_asset_type
    
    return {
        'issuer_name': issuer,
        'investment_description': description,
        'asset_type': asset_type
    }

def handle_split_rows(df):
    """
    Handle cases where investment value is shifted to next row
    Merge rows where issuer is incomplete but has value in next row
    """
    merged_rows = []
    skip_indices = set()
    
    for idx in range(len(df)):
        if idx in skip_indices:
            continue
        current_row = df.iloc[idx].copy()
        issuer = str(current_row.get('issuer_name', '')).strip()
        description = str(current_row.get('investment_description', '')).strip()
        asset_type = str(current_row.get('asset_type', '')).strip()
        value = str(current_row.get('current_value', '')).strip()
        
        # Check if current row has issuer/description but no value
        has_content = issuer or description or asset_type
        has_value = value and value != '' and value != '0' and value != 'nan'
        
        if has_content and not has_value:
            # Look ahead for value in next row(s) - check up to 3 rows ahead
            merged_description_parts = []
            for lookahead in range(1, 4):
                if idx + lookahead < len(df):
                    next_row = df.iloc[idx + lookahead]
                    next_issuer = str(next_row.get('issuer_name', '')).strip()
                    next_description = str(next_row.get('investment_description', '')).strip()
                    next_asset_type = str(next_row.get('asset_type', '')).strip()
                    next_value = str(next_row.get('current_value', '')).strip()
                    
                    # Check if next row has value
                    next_has_value = next_value and next_value != '' and next_value != '0' and next_value != 'nan'
                    # Row should not have a different issuer (to avoid merging separate entries)
                    next_has_no_issuer = not next_issuer or next_issuer == 'nan'
                    
                    if next_has_value and next_has_no_issuer:
                        # Merge: take value from next row
                        current_row['current_value'] = next_value
                        # Combine any description parts from intermediate rows
                        if merged_description_parts:
                            combined_desc = ' '.join(merged_description_parts)
                            if description:
                                current_row['investment_description'] = description + ' ' + combined_desc
                            else:
                                current_row['investment_description'] = combined_desc
                        if next_description and next_description != 'nan':
                            curr_desc = str(current_row.get('investment_description', '')).strip()
                            if curr_desc:
                                current_row['investment_description'] = curr_desc + ' ' + next_description
                            else:
                                current_row['investment_description'] = next_description
                        # Also merge asset_type if present in next row
                        if next_asset_type and not asset_type:
                            current_row['asset_type'] = next_asset_type
                        # Mark all intermediate rows and the value row for skipping
                        for skip_offset in range(1, lookahead + 1):
                            skip_indices.add(idx + skip_offset)
                        break
                    elif not next_has_value and next_has_no_issuer and next_description and next_description != 'nan':
                        # Intermediate row with description but no value - collect it
                        merged_description_parts.append(next_description)
        
        merged_rows.append(current_row)
    return pd.DataFrame(merged_rows)


def remove_total_rows(rows, verbose=True):
    """
    Remove total/summary rows from investment data
    
    Args:
        rows: List of dict records
        verbose: Print removal details
    
    Returns:
        Tuple of (filtered_rows, removed_rows)
    """
    # Words that indicate a total/summary row
    total_indicators = {
        "total investments", "total assets", "total plan", "grand total",
        "subtotal", "sub-total", "sum of",
        "total synthetic", "synthetic investment contracts"
    }
    
    # Specific fund name patterns that indicate a legitimate investment
    # These are multi-word patterns that appear in actual fund names
    fund_name_patterns = {
        'total return', 'total bond market', 'total stock market', 
        'total international', 'total world', 'total market',
        'bond index', 'stock index', 'bond fund', 'intl', 'international bond'
    }
    
    # Fund company/manager names
    fund_companies = {
        'pimco', 'vanguard', 'blackrock', 'fidelity', 'schwab', 
        'ishares', 'metropolitan west', 'metwest', 'spdr', 'ssga'
    }
    
    filtered_rows = []
    removed_rows = []
    
    for row in rows:
        issuer = str(row.get('issuer_name', '')).strip() if row.get('issuer_name') else ''
        description = str(row.get('investment_description', '')).strip() if row.get('investment_description') else ''
        issuer_lower = issuer.lower()
        description_lower = description.lower()
        combined = (issuer + " " + description).lower()
        
        is_total = False
        
        # FIRST: Check for exact "total" or total indicator phrases (highest priority)
        if issuer_lower == "total" or any(indicator in combined for indicator in total_indicators):
            is_total = True
        # Description-only totals such as "Total mutual funds" often have blank issuer_name
        elif not issuer and description_lower.startswith('total'):
            # Preserve common legitimate fund names that begin with Total
            if not any(pattern in description_lower for pattern in fund_name_patterns) and not any(company in description_lower for company in fund_companies):
                is_total = True
        
        # SECOND: Check if issuer starts with 'Total' or contains 'TOT' abbreviation
        elif issuer_lower.startswith('total') or ' tot ' in issuer_lower:
            # Could be either a fund name or a subtotal row
            is_subtotal = True
            
            if issuer_lower.startswith('total'):
                after_total = issuer_lower[5:].strip()  # Remove "total" prefix
                
                # Check for legitimate fund name patterns
                # e.g., "Total International Bond", "Total Return Fund", "Total Bond Market"
                has_fund_pattern = any(pattern in issuer_lower for pattern in fund_name_patterns)
                
                # Check for fund company names (e.g., "PIMCO TOTAL RTN II")
                has_fund_company = any(company in issuer_lower for company in fund_companies)
                
                if has_fund_pattern or has_fund_company:
                    # This is a legitimate fund name
                    is_subtotal = False
                    if verbose:
                        print(f"  ✓ Preserving fund with 'Total' in name: {issuer[:60]}")
                # If after "Total" is empty or very short, it's a subtotal marker
                elif not after_total or len(after_total) < 3:
                    # "Total" alone or "Total XX"
                    is_subtotal = True
                # If after "Total" is a short abbreviation (2-15 chars, few spaces)
                # e.g., "Total BHMS", "Total WB"
                elif len(after_total) <= 15 and after_total.count(' ') <= 1:
                    # Likely a subtotal for a manager/company
                    is_subtotal = True
                # Check for "Total [Company Name] [Asset Type]" pattern (subtotals)
                # e.g., "Total William Blair Small-Mid Cap Growth Common Stock"
                elif 'common stock' in issuer_lower and issuer_lower.startswith('total '):
                    # This is typically a subtotal of stocks, not a fund name
                    is_subtotal = True
                elif 'separately managed account' in issuer_lower and issuer_lower.startswith('total '):
                    # This is typically a subtotal of SMAs, not a fund name
                    is_subtotal = True
                else:
                    # Default: if it starts with "Total " followed by something we can't identify,
                    # it's more likely a subtotal than a fund
                    is_subtotal = True
            else:
                # Contains ' TOT ' (abbreviation) but doesn't start with 'Total'
                # e.g., "VG IS TOT BD MKT IDX" (Vanguard Total Bond Market Index)
                # These are typically abbreviated fund codes with multiple spaces
                if issuer.count(' ') >= 3:
                    # Likely an abbreviated fund code
                    is_subtotal = False
                    if verbose:
                        print(f"  ✓ Preserving fund with 'TOT' abbreviation: {issuer[:60]}")
                else:
                    is_subtotal = True
            
            if is_subtotal:
                is_total = True
        
        # Also remove if both issuer and description are empty (blank row)
        if not issuer and not description:
            is_total = True
        
        if is_total:
            if verbose:
                print(f"  Removing Total row: {issuer[:40]} | {description[:40]}")
            removed_rows.append(row)
        else:
            filtered_rows.append(row)
    
    if verbose:
        print(f"  Removed {len(removed_rows)} total/summary rows")
    
    return filtered_rows, removed_rows


def remove_metadata_rows(rows, preserve_loans=True, verbose=True):
    """
    Remove metadata/header rows from investment data
    
    Args:
        rows: List of dict records
        preserve_loans: Keep participant loan entries even without values
        verbose: Print removal details
    
    Returns:
        List of filtered rows
    """
    excluded_keywords = {
        "form 5500", "schedule", "omb no", "department", "plan number",
        "file as", "identity of", "issue lessor", "maturity date", "rate of",
        "collateral", "cost", "current value", "par value",
        "notes receivable", "self-directed", "section", "notes:",
        "loans to", "interest rates", "maturities",
    }
    
    filtered_rows = []
    
    for row in rows:
        issuer = str(row.get('issuer_name', '')).strip() if row.get('issuer_name') else ''
        description = str(row.get('investment_description', '')).strip() if row.get('investment_description') else ''
        current_value = str(row.get('current_value', '')).strip() if row.get('current_value') else ''
        
        # Skip if both issuer and description are empty
        if not issuer and not description:
            continue
        
        combined = (issuer + " " + description).lower()
        
        # Check if this is a participant loan entry
        is_loan = ('loan' in combined and 'participant' in combined) if preserve_loans else False
        
        # Skip metadata rows (unless it's a loan)
        if any(keyword in combined for keyword in excluded_keywords):
            if is_loan:
                filtered_rows.append(row)
            continue
        
        # Keep rows with issuer or description AND a current value
        if (issuer or description) and current_value and current_value not in ["", "**", "-"]:
            if issuer and issuer not in ["", "Mutual Funds", "Collective Trusts"]:
                filtered_rows.append(row)
            elif description:
                filtered_rows.append(row)
        # Also keep participant loan entries even without current_value
        elif is_loan:
            filtered_rows.append(row)
    
    removed_count = len(rows) - len(filtered_rows)
    if verbose:
        print(f"  Removed {removed_count} metadata rows")
    
    return filtered_rows


def remove_duplicates(rows, verbose=True):
    """
    Remove duplicate investment records based on composite key
    
    Args:
        rows: List of dict records
        verbose: Print removal details
    
    Returns:
        List of deduplicated rows
    """
    seen = set()
    deduped_rows = []
    
    def normalize_value(value):
        if value is None:
            return ''
        s = str(value).strip()
        if s in ['', '**', '-', '0', '0.0', 'nan', 'None']:
            return ''

        # Remove currency formatting
        s = s.replace('$', '').replace(',', '').strip()

        # Handle negative parens
        if s.startswith('(') and s.endswith(')'):
            s = '-' + s[1:-1].strip()

        try:
            num = float(s)
            # Keep an normalized numeric string to avoid duplicate format variants
            if num.is_integer():
                return str(int(num))
            return str(round(num, 2))
        except ValueError:
            return s

    for row in rows:
        # Create unique key: pdf_stem (or pdf_name) + issuer + description + current_value
        pdf_key = row.get('pdf_stem', '') or row.get('pdf_name', '')
        issuer = str(row.get('issuer_name', '')).strip() if row.get('issuer_name') else ''
        description = str(row.get('investment_description', '')).strip() if row.get('investment_description') else ''
        value = normalize_value(row.get('current_value', ''))
        
        key = (pdf_key, issuer, description, value)
        
        if key not in seen:
            seen.add(key)
            deduped_rows.append(row)
        elif verbose:
            print(f"  Removing duplicate: {issuer[:40]} | {description[:40]}")
    
    duplicates_removed = len(rows) - len(deduped_rows)
    if verbose:
        print(f"  Removed {duplicates_removed} duplicate records")
    
    return deduped_rows



_KNOWN_MANAGERS_LOWER = frozenset({
    'the vanguard group', 'vanguard', 'fidelity', 'fidelity investments',
    't. rowe price', 'blackrock', 'american funds', 'jp morgan', 'jpmorgan',
    'pimco', 'dimensional', 'invesco', 'schwab', 'principal', 'prudential',
    'columbia', 'dodge & cox', 'metropolitan life', 'massmutual', 'john hancock',
    'state street', 'ssga', 'tiaa', 'cref', 'putnam', 'oppenheimer', 'nuveen',
    'wells fargo', 'morgan stanley', 'merrill lynch', 'lord abbett', 'loomis sayles',
    'eaton vance', 'mfs', 'franklin', 'franklin templeton', 'templeton',
    'american century', 'calvert', 'baird', 'william blair', 'parametric',
})


def _fund_specificity_score(row):
    """Higher score = more specific fund name (prefer over generic manager name)."""
    desc = str(row.get('investment_description', '') or '').strip()
    issuer = str(row.get('issuer_name', '') or '').strip()
    best = desc if desc else issuer
    if not best:
        return 0
    best_lower = best.lower()
    if best_lower in _KNOWN_MANAGERS_LOWER:
        return 0
    if re.search(r'20[2-9]\d', best):
        return 200 + len(best)
    if re.search(r'(fund|index|etf|trust|portfolio|class|shares?)', best_lower):
        return 100 + len(best)
    return len(best)


def remove_cross_page_duplicates(rows, value_threshold=10000, verbose=True):
    """
    Secondary dedup: same (pdf, value) appearing on multiple pages with different field layouts.
    Keeps the row with the most specific fund name; drops generic manager-name rows.
    """
    from collections import defaultdict

    def norm_val(v):
        if v is None:
            return None
        s = str(v).replace('$', '').replace(',', '').strip()
        if s in ('', '-', 'nan', 'None', '0', '0.0'):
            return None
        if s.startswith('(') and s.endswith(')'):
            s = '-' + s[1:-1].strip()
        try:
            return round(float(s), 2)
        except ValueError:
            return None

    groups = defaultdict(list)
    for i, row in enumerate(rows):
        pdf_key = row.get('pdf_stem', '') or row.get('pdf_name', '')
        val = norm_val(row.get('current_value'))
        if val and abs(val) >= value_threshold:
            groups[(pdf_key, val)].append(i)

    indices_to_remove = set()
    for (pdf_key, val), idxs in groups.items():
        if len(idxs) < 2:
            continue
        scored = sorted([(_fund_specificity_score(rows[i]), i) for i in idxs], reverse=True)
        best_score, best_idx = scored[0]
        if best_score > 0:
            for score, idx in scored[1:]:
                if verbose:
                    r = rows[idx]
                    print(f"  CROSS-PAGE DEDUP: removing issuer={r.get('issuer_name')!r} "
                          f"desc={r.get('investment_description')!r} val={val:,.0f}")
                indices_to_remove.add(idx)

    # Second pass: same (pdf, description) → keep smallest value (larger is a subtotal)
    desc_groups = defaultdict(list)
    for i, row in enumerate(rows):
        if i in indices_to_remove:
            continue
        pdf_key = row.get('pdf_stem', '') or row.get('pdf_name', '')
        desc = str(row.get('investment_description', '') or '').strip().lower()
        if desc:
            desc_groups[(pdf_key, desc)].append(i)

    for (pdf_key, desc), idxs in desc_groups.items():
        if len(idxs) < 2:
            continue
        # > 2 occurrences = generic category column value, not a fund name -> skip dedup
        if len(idxs) > 2:
            continue
        def get_val(i):
            v = str(rows[i].get('current_value', '') or '').replace('$','').replace(',','').strip()
            try: return float(v)
            except: return 0.0
        scored = sorted(idxs, key=get_val)
        for idx in scored[1:]:
            if verbose:
                r = rows[idx]
                print(f"  SAME-DESC DEDUP: keeping smaller, dropping val={r.get('current_value')} "
                      f"desc={r.get('investment_description','')[:50]!r}")
            indices_to_remove.add(idx)

    result = [row for i, row in enumerate(rows) if i not in indices_to_remove]
    if verbose and indices_to_remove:
        print(f"  Cross-page dedup removed {len(indices_to_remove)} rows")
    return result


def remove_outlier_value_rows(rows, verbose=True):
    """
    Remove rows whose value is > 9x the sum of all other rows for the same PDF.
    These are subtotal rows that slipped through keyword-based detection.
    """
    from collections import defaultdict

    def parse_val(v):
        if v is None:
            return None
        s = str(v).replace('$', '').replace(',', '').strip()
        if s.startswith('(') and s.endswith(')'):
            s = '-' + s[1:-1].strip()
        try:
            return abs(float(s))
        except ValueError:
            return None

    pdf_val_map = defaultdict(list)
    for i, row in enumerate(rows):
        pdf_key = row.get('pdf_stem', '') or row.get('pdf_name', '')
        val = parse_val(row.get('current_value'))
        if val and val > 0:
            pdf_val_map[pdf_key].append((val, i))

    outlier_idx = set()
    for pdf_key, pairs in pdf_val_map.items():
        if len(pairs) < 2:
            continue
        total = sum(v for v, _ in pairs)
        for val, idx in pairs:
            rest = total - val
            if rest > 0 and val > rest * 9:
                if verbose:
                    r = rows[idx]
                    print(f"  OUTLIER REMOVED (val {val:,.0f} > 9x rest {rest:,.0f}): "
                          f"{r.get('issuer_name', '')[:40]} | {r.get('investment_description', '')[:40]}")
                outlier_idx.add(idx)

    result = [row for i, row in enumerate(rows) if i not in outlier_idx]
    if verbose and outlier_idx:
        print(f"  Outlier removal removed {len(outlier_idx)} rows")
    return result

def clean_investment_data(rows, preserve_loans=True, remove_dupes=True, verbose=True):
    """
    Comprehensive cleanup: remove totals, metadata, and duplicates
    
    Args:
        rows: List of dict records
        preserve_loans: Keep participant loan entries even without values
        remove_dupes: Apply deduplication
        verbose: Print detailed progress
    
    Returns:
        Tuple of (clean_rows, removed_total_rows) for verification
    """
    if verbose:
        print(f"Starting with: {len(rows)} records")
    
    # Step 0: Extract fund names from "X shares of Fund Name" descriptions
    rows = extract_fund_names_from_descriptions(rows)

    # Step 1: Remove total/summary rows
    filtered_rows, removed_totals = remove_total_rows(rows, verbose=verbose)
    
    # Step 2: Remove metadata rows
    filtered_rows = remove_metadata_rows(filtered_rows, preserve_loans=preserve_loans, verbose=verbose)
    
    # Step 3: Remove outlier value rows (subtotals with no keyword markers)
    filtered_rows = remove_outlier_value_rows(filtered_rows, verbose=verbose)

    # Step 4: Remove duplicates (optional)
    if remove_dupes:
        filtered_rows = remove_duplicates(filtered_rows, verbose=verbose)
        filtered_rows = remove_cross_page_duplicates(filtered_rows, verbose=verbose)
    
    if verbose:
        print(f"Final: {len(filtered_rows)} clean records")
    
    return filtered_rows, removed_totals
