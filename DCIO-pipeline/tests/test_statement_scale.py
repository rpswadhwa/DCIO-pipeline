"""(2026-11-27) Thousands-scale fixes: squashed label (RWJBarnabas), label only on the statements
page (Amazon, Endeavor), and the printed row type beating the model's label (Amazon)."""
from src.llm_row_extract import _apply_printed_type
from src.statement_scale import _figures_from_text, decide_statement_scale
from src.text_extract import _page_value_scale_factor


def _rows(values, **extra):
    return [{"issuer_name": "", "investment_description": f"Fund {i}", "current_value": str(v),
             "asset_type": "Mutual Fund", **extra} for i, v in enumerate(values)]


def test_squashed_dollars_in_thousands_label():
    assert _page_value_scale_factor("(DollarsinThousands)") == 1000
    assert _page_value_scale_factor("(Dollars in Thousands)") == 1000
    assert _page_value_scale_factor("thousands of participants") == 1


def test_dollar_and_omitted_thousands_labels():  # Exeter Finance, GE HealthCare
    assert _page_value_scale_factor("SUPPLEMENTAL SCHEDULE\n($ thousands)") == 1000
    assert _page_value_scale_factor("December 31, 2025\n(000s omitted)") == 1000
    assert _page_value_scale_factor("($000s)") == 1000
    assert _page_value_scale_factor("Total 1,000 shares $ 5,000") == 1


def test_statement_figures_only_from_investment_lines():
    text = ("Statements of Net Assets Available for Benefits\n(In Thousands)\n"
            "Investments, at fair value $ 43,831,067 $ 34,167,624\nTotal receivables 734,471 426,120")
    assert _figures_from_text(text) == [43831067.0, 34167624.0]


def test_scales_when_rows_match_statement_figure():  # Amazon: rows sum = 43,831,067 exactly
    st = {"page": 7, "in_thousands": True, "figures": [43831067.0, 34167624.0]}
    assert decide_statement_scale(_rows([40000000, 3000000, 831067]), st) == 43831067.0


def test_endeavor_within_two_percent():  # rows 2,153,683 vs fair value 2,184,663 (1.4%)
    st = {"page": 6, "in_thousands": True, "figures": [2184663.0, 2194521.0]}
    assert decide_statement_scale(_rows([2000000, 100000, 53683]), st) == 2184663.0


def test_whole_dollar_schedule_is_not_scaled():  # statements in thousands, schedule in dollars
    st = {"page": 4, "in_thousands": True, "figures": [43831.0]}
    assert decide_statement_scale(_rows([40000000, 3000000, 831067]), st) is None


def test_already_scaled_rows_are_not_rescaled():
    st = {"page": 4, "in_thousands": True, "figures": [43831067.0]}
    assert decide_statement_scale(_rows([40000000000, 3000000000, 831067000]), st) is None


def test_no_thousands_label_no_scale():
    st = {"page": 4, "in_thousands": False, "figures": [43831067.0]}
    assert decide_statement_scale(_rows([40000000, 3000000, 831067]), st) is None


def test_loans_excluded_from_row_sum():
    st = {"page": 4, "in_thousands": True, "figures": [1000000.0]}
    rows = _rows([600000, 300000, 100000]) + [{"investment_description": "Notes receivable from participants",
                                               "current_value": "537997", "asset_type": "Participant Loan"}]
    assert decide_statement_scale(rows, st) == 1000000.0


def test_printed_type_beats_model_label():
    row = {"asset_type": "Mutual Fund", "asset_type_source": "inferred"}
    _apply_printed_type(row, {"printed_type": "COLLECTIVE INVESTMENT TRUST"}, 18, 3, "t")
    assert row == {"asset_type": "Common/Collective Trust Fund", "asset_type_source": "row"}


_GE_P17 = """Registered investment companies:
Dodge & Cox Income Fund 88,230
Fidelity Treasury Only Money Market Fund 12,140
Synthetic guaranteed investment contracts:
Massachusetts Mutual Life Insurance Company:
Goldman Sachs Trust Co Term Fund 2025 696
Goldman Sachs Trust Co Term Fund 2026 9,564
"""


def test_section_heading_retypes_model_mf_rows():  # GE HealthCare (2026-11-28)
    from src.llm_row_extract import _apply_page_headings
    rows = [{"investment_description": d, "asset_type": "Mutual Fund", "asset_type_source": "inferred"}
            for d in ("Dodge & Cox", "Term 2025", "Term 2026")]
    assert _apply_page_headings(rows, ["88,230", "696", "9,564"], _GE_P17, 17) == 2
    assert [r["asset_type"] for r in rows] == ["Mutual Fund", "Stable Value Fund", "Stable Value Fund"]


def test_section_heading_never_beats_printed_row_type():
    from src.llm_row_extract import _apply_page_headings
    rows = [{"investment_description": "T", "asset_type": "Mutual Fund", "asset_type_source": "row"}]
    assert _apply_page_headings(rows, ["696"], _GE_P17, 17) == 0


def test_squashed_text_reread(monkeypatch):  # RWJBarnabas (2026-11-28)
    import src.llm_row_extract as m

    class _Page:
        def extract_text(self, x_tolerance=3):
            return "AmericanFundsGrowthFundofAmerica 1,234" if x_tolerance == 3 else "American Funds Growth Fund of America 1,234"

    class _Pdf:
        pages = [_Page()]
        def __enter__(self): return self
        def __exit__(self, *a): return False

    monkeypatch.setattr(m.pdfplumber, "open", lambda p: _Pdf())
    assert m._get_page_text("x.pdf", 1).startswith("American Funds Growth")


def test_continuation_page_inherits_thousands(monkeypatch):  # CNA (2026-11-28)
    import src.llm_row_extract as m
    texts = {15: "Schedule H, Line 4i\n(In thousands)\nIGT Fund 5,000",
             16: "Identity of issue Current value\nVanguard Intl Growth 92,297",
             17: "Schedule of Assets\nOther 1,000"}
    monkeypatch.setattr(m, "_get_page_text", lambda p, n: texts[n])
    pages = [{"page_number": 15, "scale_multiplier": 1000, "mapped_rows": [{"current_value": "5000000"}]},
             {"page_number": 16, "scale_multiplier": 1, "mapped_rows": [{"current_value": "92297"}]},
             {"page_number": 17, "scale_multiplier": 1, "mapped_rows": [{"current_value": "1000"}]}]
    assert m._inherit_continuation_scale("x.pdf", pages) == 1
    assert float(pages[1]["mapped_rows"][0]["current_value"]) == 92297000
    assert pages[2]["mapped_rows"][0]["current_value"] == "1000"   # own title -> left alone


def test_model_label_kept_when_no_printed_type():
    row = {"asset_type": "Mutual Fund", "asset_type_source": "inferred"}
    _apply_printed_type(row, {"printed_type": ""}, 18, 3, "t")
    assert row["asset_type"] == "Mutual Fund"
