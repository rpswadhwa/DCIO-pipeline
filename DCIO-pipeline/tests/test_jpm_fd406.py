"""(2026-11-26) jpm_fd406: JPMorgan's FD406 "Detail Statement of ... Assets and Liabilities" attached as the
4i schedule (Vought, BMW). Pages trimmed from the real prints."""
from contextlib import contextmanager
import src.schedule_4i_text_parser as sp

HEAD = "FD406 PAGE {n}\nDETAIL STATEMENT OF ACME MASTER TRUST C 1\nASSETS AND LIABILITIES\n" \
       "BASE CURRENCY: USD AS OF DECEMBER 31,2025\n"
USD_COLS = "UNIT COST\n* * SECURITIES * * MARKET PRICE COST MARKET VALUE UNREALIZED GAIN(LOSS)\n" \
           "SHARES/PAR ACCRUED INCOME\n"
FX_COLS = "UNREALIZED GAIN(LOSS)\n* * SECURITIES * * UNIT COST LOCAL COST LOCAL MARKET VALUE LOCAL\n" \
          "MARKET PRICE BASE COST BASE MARKET VALUE TOTAL BASE\nSHARES/PAR LOCAL ACCRUAL MARKET\n" \
          "BASE ACCRUAL BASE ACCRUAL CURRENCY\n"
FOOT = "Acme Master Trust\nSchedule H Line 4i - Assets Held\nJPMorgan Investor Services\n"
CASH = HEAD.format(n=1) + "* * CASH * * LOCAL COST LOCAL MARKET VALUE\nU. S. DOLLAR\n____________\n" \
       "TOTAL CASH 100.00 100.00 0.00\n" + FOOT
EURO = HEAD.format(n=2) + FX_COLS + """EURO
____
MISCELLANEOUS
LIMITED PARTNERSHIP
CVC CAPITAL PARTNERS VIII(A) LP 2565,891.28 2,565,891.28 2,934,382.94 368,491.66
2934,382.94 2,756,117.84 3,447,899.95 691,782.11
1 USD 0.00 432,977.70
0.00 0.00 258,804.41
TOTAL LIMITED PARTNERSHIP 2,565,891.28 2,934,382.94 368,491.66
2,756,117.84 3,447,899.95 691,782.11
0.00 432,977.70
0.00 0.00 258,804.41
TOTAL MISCELLANEOUS 2,565,891.28 2,934,382.94 368,491.66
2,756,117.84 3,447,899.95 691,782.11
0.00 432,977.70
0.00 0.00 258,804.41
_____________________ _____________________ _____________________
TOTAL EURO 2,565,891.28 2,934,382.94 368,491.66
2,756,117.84 3,447,899.95 691,782.11
0.00 432,977.70
0.00 0.00 258,804.41
""" + FOOT
USD = HEAD.format(n=3) + USD_COLS + """U. S. DOLLAR
____________
INVESTMENT COMPANIES
MUTUAL FUNDS
OPEN END MUTUAL FUND
SIIT LARGE CAP INDEX FUND OPEN-END FUND 176.6332
222.91 62,050,859.33 78,307,810.63 16,256,951.30
351,297.8809 0.00
SECURIS OPPORTUNITIES FUND 15,852.6623
AAO9925W4 P 46206 20.2465 15,852,662.26 20,246.52 (15,832,415.74)
1,000 0.00
TOTAL OPEN END MUTUAL FUND 77,903,521.59 78,328,057.15 424,535.56
0.00
TOTAL MUTUAL FUNDS 77,903,521.59 78,328,057.15 424,535.56
0.00
TOTAL INVESTMENT COMPANIES 77,903,521.59 78,328,057.15 424,535.56
0.00
_____________________ _____________________ _____________________
TOTAL U. S. DOLLAR 77,903,521.59 78,328,057.15 424,535.56
0.00
TOTAL SECURITIES 80,659,639.43 81,775,957.10 1,116,317.67
0.00
""" + FOOT


class _Page:
    def __init__(self, text):
        self._t = text

    def extract_text(self):
        return self._t


def _parse(monkeypatch, texts):
    @contextmanager
    def fake_open(_):
        yield type("D", (), {"pages": [_Page(t) for t in texts]})()
    monkeypatch.setattr(sp.pdfplumber, "open", fake_open)
    return sp.parse_schedule_4i_pages("x/ACK.pdf", list(range(1, len(texts) + 1)), "jpm_fd406")


def test_rows_types_and_totals_tie(monkeypatch):
    r = _parse(monkeypatch, [CASH, EURO, USD])
    rows = [row for p in r["pages"] for row in p["mapped_rows"]]
    assert [(x["investment_description"], x["asset_type"], x["current_value"]) for x in rows] == [
        ("CVC CAPITAL PARTNERS VIII(A) LP", "Partnership Interest", "3447899.95"),  # base, not local, value
        ("SIIT LARGE CAP INDEX FUND OPEN-END FUND", "Mutual Fund", "78307810.63"),
        ("SECURIS OPPORTUNITIES FUND", "Mutual Fund", "20246.52"),
    ]
    assert rows[2]["asset_identifier"] == "AAO9925W4"
    assert all(s["reconciled"] for s in r["sections"])
    assert r["grand_reconciled"] is True and r["grand_total"] == [80659639.43, 81775957.10]
    assert sp.is_trustworthy(r) and not r["unparsed"]


def test_grand_mismatch_is_not_trusted(monkeypatch):
    r = _parse(monkeypatch, [EURO, USD.replace("TOTAL SECURITIES 80,659,639.43 81,775,957.10",
                                               "TOTAL SECURITIES 80,659,639.43 91,775,957.10")])
    assert r["grand_reconciled"] is False


def test_amount_only_lines():
    assert sp._fd406_amount_only("1 USD 0.00 432,977.70")
    assert sp._fd406_amount_only("AAN9950N3 P 43410 1,000 0.00")
    assert not sp._fd406_amount_only("3M CO 12.50")
