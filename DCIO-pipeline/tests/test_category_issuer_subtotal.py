"""(2026-11-26) A category word in the issuer column ("Mutual Funds", "Common/Collective Trusts") is a subtotal
only when the description is not a real name too. Rite Aid / Petco print the category on every fund row."""
from src.data_cleaner import remove_total_rows


def _row(issuer, desc, value):
    return {"issuer_name": issuer, "investment_description": desc, "current_value": value}


def test_fund_rows_with_category_issuer_are_kept():
    rows = [_row("Common/Collective Trusts", "T. ROWE PRICE RETIREMENT 2005 TRUST K", "9,829,142.88"),
            _row("Mutual Funds", "Jpm Core Plus Bnd R6", "10,130,941")]
    kept, removed = remove_total_rows(rows, verbose=False)
    assert len(kept) == 2 and not removed


def test_category_subtotal_rows_still_removed():
    rows = [_row("Registered Investment Companies", "", "52,000,000"),
            _row("Mutual Funds", "Mutual Fund", "52,000,000")]
    kept, removed = remove_total_rows(rows, verbose=False)
    assert not kept and len(removed) == 2
