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


def test_model_label_kept_when_no_printed_type():
    row = {"asset_type": "Mutual Fund", "asset_type_source": "inferred"}
    _apply_printed_type(row, {"printed_type": ""}, 18, 3, "t")
    assert row["asset_type"] == "Mutual Fund"
