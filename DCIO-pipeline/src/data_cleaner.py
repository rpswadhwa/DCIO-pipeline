import pandas as pd
import re

def parse_investment_row(row):
    """
    Parse investment row to properly separate issuer, asset type, and description
    """
    issuer = row.get('issuer_name', '').strip()
    description = row.get('investment_description', '').strip()
    existing_asset_type = row.get('asset_type', '').strip()
    
    # Asset type patterns (ordered from most specific to least specific)
    asset_type_patterns = [
        r'Common/Collective Trust Fund',
        r'Common Collective Trust',
        r'Collective Trust Fund',
        r'Collective Investment Trust',
        r'Separately Managed Account',
        r'Self-Directed Brokerage Account',
        r'Commingled Fund',
        r'Stable Value Fund',
        r'Money Market Fund',
        r'Mutual Fund',
        r'Registered Investment Compan(?:y|ies)',
        r'Institutional Funds?',
        r'Publicly.traded Stock',
        r'Employer Stock',
        r'Employer Securities',
        r'Preferred Stock',
        r'Currency',
        r'Partnership Interest',
        r'ETF'
    ]
    
    # Check if asset type is embedded in issuer name — only if no explicit column value
    asset_type = existing_asset_type

    if not asset_type:
        for pattern in asset_type_patterns:
            if re.search(pattern, issuer, re.IGNORECASE):
                match_text = re.search(pattern, issuer, re.IGNORECASE).group(0)
                asset_type = match_text
                issuer = re.sub(pattern, '', issuer, flags=re.IGNORECASE).strip()
                break

    # Also check if asset type is in investment_description
    if not asset_type:
        for pattern in asset_type_patterns:
            if re.search(pattern, description, re.IGNORECASE):
                match_text = re.search(pattern, description, re.IGNORECASE).group(0)
                asset_type = match_text
                description = re.sub(pattern, '', description, flags=re.IGNORECASE).strip()
                # Clean up extra spaces, commas, dashes at start/end
                description = re.sub(r'^[,\-\s/]+|[,\-\s/]+$', '', description).strip()
                break
    
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
                    # Also merge if next issuer is a generic continuation word (e.g. "Management Company")
                    _CONTINUATION_ISSUERS = {
                        'management company', 'management', 'company', 'inc', 'llc', 'lp',
                        'fund', 'group', 'corporation', 'corp', 'associates', 'advisors',
                    }
                    next_is_continuation = next_issuer.lower() in _CONTINUATION_ISSUERS

                    if next_has_value and (next_has_no_issuer or next_is_continuation):
                        # Merge: take value from next row
                        current_row['current_value'] = next_value
                        # If next issuer is a continuation word, append it to current issuer
                        if next_is_continuation and next_issuer:
                            current_row['issuer_name'] = (issuer + ' ' + next_issuer).strip()
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
        "subtotal", "sub-total", "sum of", "aggregate",
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
        combined = (issuer + " " + description).lower()
        
        is_total = False
        
        # FIRST: Check for exact "total" or total indicator phrases (highest priority)
        if issuer_lower == "total" or any(indicator in combined for indicator in total_indicators):
            is_total = True
        
        # SECOND: Check if description starts with 'Total' when issuer is empty
        elif not issuer and description.lower().startswith('total'):
            is_total = True

        # THIRD: Check if issuer starts with 'Total' or contains 'TOT' abbreviation
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
                        print(f"  Preserving fund with 'Total' in name: {issuer[:60]}")
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
                        print(f"  Preserving fund with 'TOT' abbreviation: {issuer[:60]}")
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
    
    # Pass 2: catch rows with empty issuer whose value exceeds every legitimate
    # single-fund value in the same PDF — these are grand totals misplaced by the parser.
    # Build max value per pdf_stem from rows that have a real issuer name.
    max_val_by_pdf = {}
    for row in filtered_rows:
        issuer = str(row.get('issuer_name', '')).strip()
        if not issuer:
            continue
        pdf_key = row.get('pdf_stem') or row.get('pdf_name', '')
        raw_val = str(row.get('current_value', '')).replace(',', '').replace('$', '').strip()
        try:
            val = float(raw_val)
            if val > max_val_by_pdf.get(pdf_key, 0):
                max_val_by_pdf[pdf_key] = val
        except ValueError:
            pass

    if max_val_by_pdf:
        second_pass_kept = []
        for row in filtered_rows:
            issuer = str(row.get('issuer_name', '')).strip()
            if issuer:
                second_pass_kept.append(row)
                continue
            pdf_key = row.get('pdf_stem') or row.get('pdf_name', '')
            raw_val = str(row.get('current_value', '')).replace(',', '').replace('$', '').strip()
            try:
                val = float(raw_val)
                max_legit = max_val_by_pdf.get(pdf_key, 0)
                if max_legit > 0 and val > max_legit:
                    if verbose:
                        print(f"  Removing grand-total row (val {val:,.0f} > max fund {max_legit:,.0f}): {row.get('investment_description','')[:50]}")
                    removed_rows.append(row)
                    continue
            except ValueError:
                pass
            second_pass_kept.append(row)
        filtered_rows = second_pass_kept

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
        "collateral", "cost", "current value", "par value", "commingled",
        "notes receivable", "self-directed", "section", "notes:",
        "loans to", "interest rates", "maturities",
        "cash reserves",
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

        # Drop rows whose issuer_name IS a section heading label — these are
        # header/subtotal rows whose current_value is a section total, not a fund value.
        if issuer.lower().rstrip(':') in _SECTION_HEADING_LABELS:
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
    
    for row in rows:
        # Dedup key: pdf + issuer + description (value excluded — extraction differences cause false mismatches)
        pdf_key = row.get('pdf_stem', '') or row.get('pdf_name', '')
        issuer = str(row.get('issuer_name', '')).strip() if row.get('issuer_name') else ''
        description = str(row.get('investment_description', '')).strip() if row.get('investment_description') else ''

        key = (pdf_key, issuer, description)
        
        if key not in seen:
            seen.add(key)
            deduped_rows.append(row)
        elif verbose:
            print(f"  Removing duplicate: {issuer[:40]} | {description[:40]}")
    
    duplicates_removed = len(rows) - len(deduped_rows)
    if verbose:
        print(f"  Removed {duplicates_removed} duplicate records")
    
    return deduped_rows


_EXCLUDED_ASSET_TYPES = {
    # Common Stock / Publicly traded equity
    'common stock',
    'publicly-traded stock',
    'publicly traded stock',
    'publicly-traded stocks',
    'publicly traded stocks',
    # CIT variants
    'common/collective trust fund',
    'common collective trust fund',
    'common collective trust',
    'common collective trusts',
    'collective trust fund',
    'collective investment trust',
    'cit',
    # Currency
    'currency',
    'currencies',
    # Employer Stock
    'employer stock',
    'employer stocks',
    'employer securities',
    # Stable Value
    'stable value fund',
    'stable value funds',
    # Pooled Separate Accounts (insurance company vehicle, distinct from commingled funds)
    'pooled separate account',
    'pooled separate accounts',
    # Group Annuity / CREF / Insurance General Account
    'group annuity contract',
    'group annuity contracts',
    'insurance general account',
    'cref account',
    'cref accounts',
    # Fully/Non-benefit responsive (stable value wrappers)
    'fully-benefit responsive',
    'non-benefit responsive',
}


_ASSET_TYPE_NORMALIZE = {
    'registered investment company': 'Mutual Fund',
    'registered investment companies': 'Mutual Fund',
    'institutional fund': 'Mutual Fund',
    'institutional funds': 'Mutual Fund',
    'common stock fund': 'Mutual Fund',
    'common stock funds': 'Mutual Fund',
}

_TARGET_ASSET_TYPES = {
    'mutual fund',
    'commingled fund',
    'self-directed brokerage account',
    'money market fund',
}


# Known fund custodians whose holdings are Mutual Funds (used for cross-page inference)
_MUTUAL_FUND_CUSTODIANS = {
    'fidelity', 'vanguard', 'pimco', 'blackrock',
    'fidelity management', 'lincoln national', 'valic', 'voya retirement',
    'prudential insurance', 'tiaa', 'empower', 'principal life',
    'john hancock', 'transamerica', 'nationwide', 'massmutual',
    'great-west', 'hartford life', 'protective life',
}

# Signals in issuer or description that should override a section-heading-derived asset_type.
# Maps lowercase signal phrase → canonical asset type to assign.
_DESCRIPTION_OVERRIDE_MAP = {
    'common shares':   'Common Stock',
    'common stock':    'Common Stock',
    'participant loan': 'Participant Loan',
    'participant loans': 'Participant Loan',
}

# Issuer names (lowercase, exact or substring) that force a specific asset type.
_ISSUER_OVERRIDE_MAP = {
    'participant loans': 'Participant Loan',
    'participant loan':  'Participant Loan',
    'brokerage account': 'Self-Directed Brokerage Account',
    # Catches garbled annuity contract names (e.g. "TIAA Tra di ti ona l Annui ty Contra ct")
    'annuity contract': 'Insurance General Account',
    'annuity contra':   'Insurance General Account',
}


def override_asset_type_from_signals(rows, verbose=True):
    """
    Override asset_type (including section-heading-inherited values) when the
    issuer or description clearly signals a different type — e.g. 'Common Shares'
    or 'Participant Loans' propagated as Mutual Fund by a prior heading.
    Also assigns 'Self-Directed Brokerage Account' when issuer is 'Brokerage Account'.
    """
    count = 0
    for row in rows:
        issuer = str(row.get('issuer_name', '')).strip().lower()
        desc   = str(row.get('investment_description', '')).strip().lower()

        # Issuer-based override (checked first — most authoritative)
        for signal, asset_type in _ISSUER_OVERRIDE_MAP.items():
            if signal in issuer:
                if row.get('asset_type') != asset_type:
                    row['asset_type'] = asset_type
                    count += 1
                break
        else:
            # Description-based override
            for signal, asset_type in _DESCRIPTION_OVERRIDE_MAP.items():
                if signal in desc:
                    if row.get('asset_type') != asset_type:
                        row['asset_type'] = asset_type
                        count += 1
                    break

    if verbose and count:
        print(f"  Overrode asset_type for {count} records based on issuer/description signals")
    return rows


def normalize_asset_types(rows, verbose=True):
    """Normalize asset type aliases to canonical target types."""
    count = 0
    for row in rows:
        at = str(row.get('asset_type', '')).strip().lower()
        if at in _ASSET_TYPE_NORMALIZE:
            row['asset_type'] = _ASSET_TYPE_NORMALIZE[at]
            count += 1
    if verbose and count:
        print(f"  Normalized {count} asset type aliases")
    return rows


# Phrases in investment_description that indicate a non-mutual-fund type.
# If any of these appear, skip custodian-based inference so we don't mislabel.
_NON_MUTUAL_FUND_DESC_SIGNALS = {
    'group annuity contract',
    'group annuity',
    'annuity contract',
    'pooled separate account',
    'separate account',
    'stable value',
    'guaranteed investment contract',
    'common/collective trust',
    'common collective trust',
    'collective trust',
    'employer stock',
    'company stock',
    'self-directed brokerage',
}


_NUMERIC_RE = re.compile(r'^\d[\d,]*(\.\d+)?$')


def infer_asset_types_from_custodian(rows, verbose=True):
    """
    For records with no asset_type, infer 'Mutual Fund' when:
      - Pass 1: issuer or description contains a known custodian name
      - Pass 2 (fallback): record has a real issuer, real description (not a label/
        share count), and a numeric current_value — covers unknown fund managers
        whose fund name was extracted from a merged description+value field
    Skips inference when investment_description already signals a different type.
    """
    count = 0
    for row in rows:
        if str(row.get('asset_type', '')).strip():
            continue
        desc = str(row.get('investment_description', '')).strip()
        desc_lower = desc.lower()
        issuer = str(row.get('issuer_name', '')).strip().lower()
        combined = issuer + ' ' + desc_lower
        if any(signal in combined for signal in _NON_MUTUAL_FUND_DESC_SIGNALS):
            continue

        # Pass 1: known custodian substring match
        if any(custodian in combined for custodian in _MUTUAL_FUND_CUSTODIANS):
            row['asset_type'] = 'Mutual Fund'
            count += 1
            continue

        # Pass 2: generic fallback — real issuer + numeric value
        # Description may be empty (e.g. "CREF Core Bond Market Fund R3" with no desc)
        # or may contain a real extracted fund name; either way if issuer is a real
        # investment name (not an asset-type label) and the value is numeric, treat as fund.
        cv = str(row.get('current_value', '')).strip()
        issuer_raw = str(row.get('issuer_name', '')).strip()
        issuer_is_real = (
            issuer_raw
            and issuer_raw.lower() not in _ASSET_TYPE_LABELS
            and issuer_raw.lower() not in _EXCLUDED_ASSET_TYPES
        )
        desc_ok = (
            not desc                             # empty description is fine
            or (desc_lower not in _ASSET_TYPE_LABELS and not _SHARES_ONLY_RE.match(desc))
        )
        cv_is_numeric = bool(_NUMERIC_RE.match(cv.replace(',', ''))) if cv else False
        if issuer_is_real and desc_ok and cv_is_numeric:
            row['asset_type'] = 'Mutual Fund'
            count += 1

    if verbose and count:
        print(f"  Inferred 'Mutual Fund' for {count} records by custodian name / fallback")
    return rows


_MERGED_VALUE_RE = re.compile(r'^(.*?)\s*\$?\s*([\d,]{3,})\s*$')

# Phrases in the merged description that are asset type labels, not fund names
_DESC_ASSET_TYPE_MAP = {
    'common/collective trust fund': 'Common/Collective Trust Fund',
    'common/collective trust':      'Common/Collective Trust Fund',
    'common collective trust fund': 'Common/Collective Trust Fund',
    'common collective trust':      'Common/Collective Trust Fund',
    'collective trust fund':        'Common/Collective Trust Fund',
    'collective investment trust':  'Common/Collective Trust Fund',
    'mutual fund':                  'Mutual Fund',
    'commingled fund':              'Commingled Fund',
    'commingled pool':              'Commingled Fund',
    'pooled separate account':      'Pooled Separate Account',
    'separately managed account':   'Separately Managed Account',
    'self-directed brokerage':      'Self-Directed Brokerage Account',
    'stable value fund':            'Stable Value Fund',
    'money market fund':            'Money Market Fund',
}


def fix_merged_value_column(rows, verbose=True):
    """
    Fix records where camelot merged description + value into current_value.
    E.g., current_value = "Common/collective trust 287,461,105"
          → asset_type = "Common/Collective Trust Fund", current_value = "287461105"
    Also strips leading $ from already-numeric values.
    """
    fixed = 0
    for row in rows:
        cv = str(row.get('current_value', '')).strip()
        if not cv or cv in ('nan', '', '-', '**'):
            continue
        # Already numeric (possibly with $ prefix) — strip $ and leave alone
        try:
            float(cv.replace(',', '').replace('$', '').strip())
            row['current_value'] = cv.replace('$', '').replace(',', '').strip()
            continue
        except ValueError:
            pass
        m = _MERGED_VALUE_RE.match(cv)
        if m:
            desc_part = m.group(1).strip().rstrip('$').strip()
            num_part  = m.group(2).replace(',', '')
            if desc_part and num_part:
                # Check if desc_part is a known asset type label
                desc_lower = desc_part.lower()
                canonical = next(
                    (v for k, v in _DESC_ASSET_TYPE_MAP.items() if k in desc_lower),
                    None
                )
                if canonical:
                    if not str(row.get('asset_type', '')).strip():
                        row['asset_type'] = canonical
                    row['investment_description'] = ''
                else:
                    if not str(row.get('investment_description', '')).strip():
                        row['investment_description'] = desc_part
                row['current_value'] = num_part
                fixed += 1
    if verbose and fixed:
        print(f"  Fixed {fixed} records with merged description+value columns")
    return rows


# "Issuer Name 1,234.56 shares of Fund Name [trailing number]"
_SHARES_OF_RE = re.compile(
    r'^(.+?)\s+([\d,]+\.?\d*)\s+shares?\s+of\s+(.+?)(?:\s+\d+)?\s*$',
    re.IGNORECASE,
)
# "1,234.56 shares of Fund Name" — share count only, no issuer prefix
_SHARES_OF_DESC_RE = re.compile(
    r'^([\d,]+\.?\d*)\s+shares?\s+of\s+(.+?)(?:\s+\d+)?\s*$',
    re.IGNORECASE,
)


_TRAILING_DIGIT_RE = re.compile(r'\s+\d+$')


def fix_merged_issuer_fields(rows, verbose=True):
    """
    Split issuer_name values where the description got merged in, e.g.:
      'Brokerage Account Various Common Stocks' → issuer='Brokerage Account', desc='Various Common Stocks'
    Also strips trailing noise digits from descriptions, e.g.:
      'Various Preferred Stocks 7' → 'Various Preferred Stocks'
    """
    _ISSUER_PREFIXES = ['brokerage account']
    fixed = 0
    for row in rows:
        issuer = str(row.get('issuer_name', '')).strip()
        issuer_lower = issuer.lower()
        for prefix in _ISSUER_PREFIXES:
            if issuer_lower.startswith(prefix + ' ') and len(issuer_lower) > len(prefix):
                remainder = issuer[len(prefix):].strip()
                # Strip trailing noise digit from the remainder
                remainder = _TRAILING_DIGIT_RE.sub('', remainder).strip()
                row['issuer_name'] = issuer[:len(prefix)]
                if not str(row.get('investment_description', '')).strip():
                    row['investment_description'] = remainder
                fixed += 1
                break

        # Strip trailing noise digits from description regardless of issuer
        desc = str(row.get('investment_description', '')).strip()
        if desc:
            cleaned = _TRAILING_DIGIT_RE.sub('', desc).strip()
            if cleaned != desc:
                row['investment_description'] = cleaned

    if verbose and fixed:
        print(f"  Fixed {fixed} records with merged issuer+description fields")
    return rows


def fix_shares_of_pattern(rows, verbose=True):
    """
    Normalize 'shares of' patterns across all pages so descriptions are
    consistent before deduplication.

    Case 1: issuer_name or investment_description contains the full merged
      string "Issuer 1,234.56 shares of Fund Name" — split all three fields.
    Case 2: investment_description starts with just the share count
      "1,234.56 shares of Fund Name" (camelot put count in description column)
      — extract fund name only, leave issuer_name unchanged.
    """
    fixed = 0
    for row in rows:
        # Case 1: full merged string in issuer_name or investment_description
        for field in ('issuer_name', 'investment_description'):
            text = str(row.get(field, '')).strip()
            if not text or 'shares of' not in text.lower():
                continue
            m = _SHARES_OF_RE.match(text)
            if m:
                row['issuer_name'] = m.group(1).strip()
                if not str(row.get('units_or_shares', '')).strip():
                    row['units_or_shares'] = m.group(2).replace(',', '')
                row['investment_description'] = m.group(3).strip()
                fixed += 1
                break
        else:
            # Case 2: description starts with share count, no issuer prefix
            desc = str(row.get('investment_description', '')).strip()
            if desc and 'shares of' in desc.lower():
                m = _SHARES_OF_DESC_RE.match(desc)
                if m:
                    if not str(row.get('units_or_shares', '')).strip():
                        row['units_or_shares'] = m.group(1).replace(',', '')
                    row['investment_description'] = m.group(2).strip()
                    fixed += 1
    if verbose and fixed:
        print(f"  Fixed {fixed} records with merged issuer+shares+description field")
    return rows


def filter_excluded_asset_types(rows, verbose=True):
    kept, removed = [], []
    for row in rows:
        asset_type = str(row.get('asset_type', '')).strip().lower()
        # Also check description for CIT label rows that weren't caught upstream
        description = str(row.get('investment_description', '')).strip().lower()
        if asset_type in _EXCLUDED_ASSET_TYPES or description in _EXCLUDED_ASSET_TYPES:
            removed.append(row)
        else:
            kept.append(row)
    if verbose:
        print(f"  Removed {len(removed)} excluded asset type records (CIT, Common Stock, Currency, Employer Stock)")
    return kept


def filter_target_asset_types(rows, verbose=True):
    """Keep only records whose asset_type maps to a target type (Mutual Fund, Commingled Fund, Self-Directed Brokerage)."""
    kept, removed = [], []
    for row in rows:
        asset_type = str(row.get('asset_type', '')).strip().lower()
        if asset_type in _TARGET_ASSET_TYPES:
            kept.append(row)
        else:
            removed.append(row)
    if verbose:
        print(f"  Kept {len(kept)} target records (Mutual Fund, Commingled Fund, Self-Directed Brokerage)")
        print(f"  Ignored {len(removed)} records with other/unknown asset types")
    return kept


# Section heading labels — any issuer_name that exactly matches one of these
# is a header/subtotal row and must be dropped, not treated as a real investment.
_SECTION_HEADING_LABELS = {
    'mutual fund', 'mutual funds',
    'commingled fund', 'commingled funds',
    'common/collective trust fund', 'common/collective trust funds',
    'collective/common trust fund', 'collective/common trust funds',
    'common collective trust fund', 'common collective trust funds',
    'collective trust fund', 'collective trust funds',
    'collective investment trust', 'collective investment trusts',
    'pooled separate account', 'pooled separate accounts',
    'self-directed brokerage account', 'self-directed brokerage accounts',
    'stable value fund', 'stable value funds',
    'money market fund', 'money market funds',
    'separately managed account', 'separately managed accounts',
    'group annuity contract', 'group annuity contracts',
    'insurance general account', 'insurance company general account contracts',
    'participant loan', 'participant loans', 'participant loan fund',
    'registered investment company', 'registered investment companies',
    'index fund', 'index funds',
    'common stock', 'common stocks',
    'employer stock', 'employer stocks', 'employer securities',
    'self-directed accounts',
}

# Description values that are asset type labels, not fund names
_ASSET_TYPE_LABELS = {
    'mutual fund', 'commingled fund', 'self-directed brokerage account',
    'pooled separate account', 'stable value fund', 'money market fund',
    'money market', 'common/collective trust fund', 'separately managed account',
    'group annuity contract', 'participant loan', 'index fund',
}

# Issuer name suffixes that indicate a fund manager, not a fund name
_MANAGER_SUFFIXES = (
    ' group', ' management', ' management company', ' management co',
    ' asset management', ' investments', ' investment management',
    ' advisors', ' advisers', ' & co', ' & company',
    ' financial', ' capital management',
)

# Matches descriptions that are purely a share count, e.g. "2,242,410 shares"
_SHARES_ONLY_RE = re.compile(r'^[\d,]+\.?\d*\s+shares?\s*$', re.IGNORECASE)


def _is_bare_manager_name(issuer_lower):
    """Return True only when issuer is a bare company/manager name with no fund-specific words."""
    # Exact match against known custodians
    if issuer_lower in _MUTUAL_FUND_CUSTODIANS:
        return True
    # custodian + optional manager suffix (e.g. "fidelity management")
    for custodian in _MUTUAL_FUND_CUSTODIANS:
        if issuer_lower.startswith(custodian):
            remainder = issuer_lower[len(custodian):].strip()
            if not remainder or any(remainder == s.strip() for s in _MANAGER_SUFFIXES):
                return True
    # No custodian prefix but ends with a manager suffix AND the non-suffix part is short
    for suffix in _MANAGER_SUFFIXES:
        if issuer_lower.endswith(suffix):
            prefix = issuer_lower[: -len(suffix)].strip()
            # ≤2 words → still a bare manager name (e.g. "lincoln national management")
            if len(prefix.split()) <= 2:
                return True
    return False


def derive_fund_name(rows, verbose=True):
    """
    Derive a clean fund_name field from issuer_name and investment_description.

    Logic:
    1. If description is blank, an asset type label, or just a share count → fund_name = issuer_name
    2. If issuer is a *bare* manager/custodian name (not a full fund name) → fund_name = description
    3. If issuer is "Brokerage Account" → fund_name = issuer + " - " + description
    4. Default → fund_name = issuer_name
    """
    for row in rows:
        issuer = str(row.get('issuer_name', '')).strip()
        desc   = str(row.get('investment_description', '')).strip()
        issuer_lower = issuer.lower()
        desc_lower   = desc.lower()

        desc_is_label = (
            not desc
            or desc_lower == 'nan'
            or desc_lower in _ASSET_TYPE_LABELS
            or bool(_SHARES_ONLY_RE.match(desc))
        )

        issuer_is_manager = _is_bare_manager_name(issuer_lower)

        # A short issuer (≤2 words) with a real description means the description
        # is the specific fund name and the issuer is just the parent company.
        # e.g. issuer="Northern Trust", desc="NT S&P 500 Index Fund"
        issuer_is_short_company = (
            not desc_is_label
            and len(issuer.split()) <= 2
        )

        if not issuer:
            fund_name = desc
        elif desc_is_label:
            fund_name = issuer
        elif issuer_lower == 'brokerage account':
            fund_name = f"Brokerage Account - {desc}" if desc else issuer
        elif issuer_is_manager or issuer_is_short_company:
            fund_name = desc if desc else issuer
        else:
            fund_name = issuer

        row['fund_name'] = fund_name

    if verbose:
        print(f"  Derived fund_name for {len(rows)} records")
    return rows


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
    
    # Step 0a: Fix records where camelot merged description+value into one column
    rows = fix_merged_value_column(rows, verbose=verbose)
    # Step 0b: Fix filer entry errors where issuer+shares+fund name are in one field
    rows = fix_shares_of_pattern(rows, verbose=verbose)
    # Step 0c: Split merged issuer+description fields (e.g. "Brokerage Account Various X")
    rows = fix_merged_issuer_fields(rows, verbose=verbose)

    # Step 1: Remove total/summary rows
    filtered_rows, removed_totals = remove_total_rows(rows, verbose=verbose)

    # Step 2: Remove excluded asset types (CIT, Common Stock, Currency, Employer Stock)
    filtered_rows = filter_excluded_asset_types(filtered_rows, verbose=verbose)

    # Step 3: Remove metadata rows
    filtered_rows = remove_metadata_rows(filtered_rows, preserve_loans=preserve_loans, verbose=verbose)

    # Step 4: Normalize asset type aliases, apply signal overrides, infer from custodian, then keep only target types
    filtered_rows = normalize_asset_types(filtered_rows, verbose=verbose)
    filtered_rows = override_asset_type_from_signals(filtered_rows, verbose=verbose)
    filtered_rows = infer_asset_types_from_custodian(filtered_rows, verbose=verbose)
    filtered_rows = filter_target_asset_types(filtered_rows, verbose=verbose)

    # Step 5: Remove duplicates (optional)
    if remove_dupes:
        filtered_rows = remove_duplicates(filtered_rows, verbose=verbose)

    # Step 6: Derive consolidated fund_name field
    filtered_rows = derive_fund_name(filtered_rows, verbose=verbose)

    if verbose:
        print(f"Final: {len(filtered_rows)} clean records")

    return filtered_rows, removed_totals
