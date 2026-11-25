"""(2026-11-24) Bullet / tick-box glyphs "(cid:98)" around fund names must not get the whole filing junk-deleted."""
from src.post_extract_validator import strip_glyph_markers
from src.junk_detect import clean_plan


def _row(name, desc="Mutual Fund", amt=1234567.0):
    return {"issuer_name": name, "investment_description": desc, "current_value": f"{amt:,.0f}",
            "fund_name": name, "plan_investment_amt": amt, "pdf_stem": "X"}


def test_markers_stripped_leading_trailing_glued():
    rows = [_row("(cid:98) Vanguard Explorer Adm"), _row("cid98 Fidelity Contrafund"),
            _row("(cid:98)(cid:98) American Funds Growth Fund R6"),
            _row("Vanguard 500 Index Fund Mutual Fundcid98 cid98 cid98"),
            _row("Great Greycid98 Core Plus Bond R1 cid98 cid98"),
            _row("Fidelity", "Fidelity DFA US Small Cap I (cid:98) (cid:98) (cid:98)"),
            _row("cid98 Vanguard Total Stock Market Index Fund cid98Mutual Fundcid98 cid98")]
    assert strip_glyph_markers(rows) == 7
    assert rows[6]["issuer_name"] == "Vanguard Total Stock Market Index Fund Mutual Fund"
    assert [r["issuer_name"] for r in rows[:5]] == [
        "Vanguard Explorer Adm", "Fidelity Contrafund", "American Funds Growth Fund R6",
        "Vanguard 500 Index Fund Mutual Fund", "Great Grey Core Plus Bond R1"]
    assert rows[5]["investment_description"] == "Fidelity DFA US Small Cap I"


def test_glyph_only_and_inword_left_alone():
    rows = [_row("(cid:3)(cid:4)"), _row("Va(cid:72)guard Index"), _row("Vacid72guard Index"),
            _row("Vanguard 500 Index Adm")]
    assert strip_glyph_markers(rows) == 0
    assert [r["issuer_name"] for r in rows[:3]] == ["(cid:3)(cid:4)", "Va(cid:72)guard Index", "Vacid72guard Index"]


def test_stripped_rows_survive_junk_filter():
    rows = [_row("(cid:98) Vanguard Explorer Adm", amt=1234567.0),
            _row("T. Rowe Price Blue Chip Growth (cid:98) (cid:98)", amt=2345678.0),
            _row("Fidelity 500 Index Mutual Fundcid98 cid98", amt=987654.0)]
    strip_glyph_markers(rows)
    for r in rows:
        r["fund_name"] = r["issuer_name"]
    assert len(clean_plan(rows)["keep"]) == 3
