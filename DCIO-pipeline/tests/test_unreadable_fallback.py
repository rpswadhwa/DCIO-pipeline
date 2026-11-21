"""(2026-11-21) Page-kind rules added to the unreadable fallback for the group-3 filings."""
from src.unreadable_fallback import _money_lines, _header, is_image_body, useful_row_stats


def test_run_on_line_counts_amounts():
    # Entergy master trust: pdfium returns a whole schedule page as one line plus a stamp.
    row = "BOEING CO NOTES 5.930% 05/01/2060 40,536 44,297 "
    text = row * 20 + "\nEIN 11-3567859\n2025 Form 5500\nPage 14 of 135"
    assert _money_lines(text) >= 3
    assert len(_header(text)) <= 700


def test_normal_lines_unchanged():
    assert _money_lines("Vanguard 500 Index 1,234,567\nFidelity Contrafund 2,345,678\nTotal 3,580,245") == 3


def test_image_body_needs_cover_and_no_money():
    title = "Massachusetts Institute of Technology Schedule of Assets (Held at End of Year)"
    assert is_image_body(title, 0.38)
    assert not is_image_body(title, 0.05)
    assert not is_image_body(title + "\nA 1,000,000\nB 2,000,000\nC 3,000,000", 0.6)


def test_useful_row_stats():
    pages = [{"mapped_rows": [
        {"issuer_name": "Vanguard", "current_value": "1,000,000"},
        {"issuer_name": "", "investment_description": "", "current_value": "5,000"},
        {"issuer_name": "See pages 19-22", "current_value": ""},
    ]}]
    assert useful_row_stats(pages) == (1, 1000000.0)
