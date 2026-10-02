"""
Enhancement script to populate missing asset_type fields
"""
import sqlite3
import re
import sys
import os
from pathlib import Path

# Import shared patterns — works whether run standalone or as part of the package
try:
    from src.asset_type_patterns import detect_asset_type
    from src.post_extract_validator import pick_fund_name
except ImportError:
    sys.path.insert(0, os.path.dirname(__file__))
    from asset_type_patterns import detect_asset_type
    from post_extract_validator import pick_fund_name


def infer_asset_type(issuer, description):
    return detect_asset_type(f"{description} {issuer}")


def _lookup_known_mf_names(candidates, verbose=True):
    """Check candidate fund names against fund_intelligence_mapping_mf -- the
    mapping table this pipeline has been building up across every plan it has
    ever processed. A name's mere presence there (regardless of whether its
    asset_class is resolved or still PENDING_AI) means some earlier run's
    mf_only gate already let it into plan_mf_history_v3 as a mutual fund, so
    it's a reliable asset_type signal independent of this page's own LLM call
    or regex detector -- and it's free: no new classification work, just a
    lookup against work already done.

    Returns the subset of `candidates` (each lowercased+trimmed) found in the
    mapping table. Skips silently (empty result) when Athena isn't reachable
    or configured, since this is a best-effort enhancement, not a required step.
    """
    staging = os.environ.get("ATHENA_STAGING_S3", "")
    if not staging or not candidates:
        return set()
    try:
        import awswrangler as wr
    except ImportError:
        return set()

    mapping_db = os.environ.get("FUND_MAPPING_GLUE_DB", os.environ.get("VALIDATED_GLUE_DB", "default"))
    mapping_table = os.environ.get("FUND_MAPPING_TABLE", "fund_intelligence_mapping_mf")
    workgroup = os.environ.get("ATHENA_WORKGROUP", "primary")

    quoted = ", ".join("'" + c.replace("'", "''") + "'" for c in candidates)
    sql = f"""
        SELECT DISTINCT lower(trim(raw_entity_name)) AS k
        FROM {mapping_db}.{mapping_table}
        WHERE lower(trim(raw_entity_name)) IN ({quoted})
    """
    try:
        df = wr.athena.read_sql_query(sql=sql, database=mapping_db, workgroup=workgroup, s3_output=staging)
    except Exception as exc:
        if verbose:
            print(f"    [ENHANCEMENT] known-fund-name lookup skipped ({exc})")
        return set()
    if df.empty:
        return set()
    return set(df["k"].tolist())


def enhance_asset_types(db_path, verbose=True):
    """
    Populate missing asset_type fields in the database
    
    Args:
        db_path: Path to SQLite database
        verbose: Print progress
    
    Returns:
        Number of records updated
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get investments with missing asset_type
    cursor.execute("""
        SELECT id, issuer_name, investment_description, asset_type
        FROM investments
        WHERE asset_type IS NULL OR asset_type = ''
    """)
    
    missing_records = cursor.fetchall()
    
    if verbose:
        print(f"\n[ENHANCEMENT] Populating missing asset_type fields...")
        print(f"  Found {len(missing_records)} records with missing asset_type")
    
    updates = 0
    inferred_types = {}
    
    for record_id, issuer, description, current_type in missing_records:
        issuer = issuer or ''
        description = description or ''
        
        # Infer asset type
        inferred_type = infer_asset_type(issuer, description)
        
        if inferred_type:
            # Update the database
            cursor.execute("""
                UPDATE investments 
                SET asset_type = ?
                WHERE id = ?
            """, (inferred_type, record_id))
            
            updates += 1
            
            # Track inferred types for reporting
            inferred_types[inferred_type] = inferred_types.get(inferred_type, 0) + 1
            
            if verbose:
                print(f"    [OK] {issuer[:50]:50} -> {inferred_type}")
    
    # Second pass: regex found no keyword match, but this exact fund name may
    # already be a confirmed mutual fund from some other plan's prior run --
    # check fund_intelligence_mapping_mf before giving up on these rows.
    cursor.execute("""
        SELECT id, issuer_name, investment_description
        FROM investments
        WHERE asset_type IS NULL OR asset_type = ''
    """)
    still_missing = cursor.fetchall()
    known_updates = 0
    if still_missing:
        candidates_by_key = {}
        for record_id, issuer, description in still_missing:
            name = pick_fund_name(issuer or "", description or "")
            key = name.strip().lower()
            if key:
                candidates_by_key.setdefault(key, []).append(record_id)

        matched_keys = _lookup_known_mf_names(list(candidates_by_key.keys()), verbose=verbose)
        if matched_keys and verbose:
            print(f"\n  [ENHANCEMENT] Known-fund-name lookup: "
                  f"{len(matched_keys)}/{len(candidates_by_key)} candidate name(s) "
                  f"already confirmed as Mutual Fund in fund_intelligence_mapping_mf")
        for key in matched_keys:
            for record_id in candidates_by_key.get(key, []):
                cursor.execute("""
                    UPDATE investments
                    SET asset_type = 'Mutual Fund'
                    WHERE id = ?
                """, (record_id,))
                known_updates += 1
        if known_updates:
            conn.commit()

    if verbose:
        print(f"\n  Summary:")
        print(f"    Total updated: {updates + known_updates} records")
        if inferred_types:
            print(f"    Asset types assigned (keyword match):")
            for asset_type, count in sorted(inferred_types.items(), key=lambda x: -x[1]):
                print(f"      - {asset_type}: {count}")
        if known_updates:
            print(f"    Asset types assigned (known fund name match): {known_updates}")

        # Check remaining gaps
        cursor.execute("""
            SELECT COUNT(*)
            FROM investments
            WHERE asset_type IS NULL OR asset_type = ''
        """)
        remaining = cursor.fetchone()[0]

        if remaining > 0:
            print(f"\n    [!] {remaining} records still missing asset_type")
            print(f"      (These may need manual classification)")
        else:
            print(f"\n    [OK] All records now have asset_type populated!")

    conn.close()

    return updates + known_updates


if __name__ == '__main__':
    db_path = Path('data/outputs/pipeline.db')
    
    print("=" * 70)
    print("ASSET TYPE ENHANCEMENT")
    print("=" * 70)
    
    updated = enhance_asset_types(db_path, verbose=True)
    
    print("\n" + "=" * 70)
    print(f"[OK] Enhancement complete: {updated} records updated")
    print("=" * 70)
