"""(2026-11-24) Page finder fallback: an untitled holdings table (identity-of-issue/party + current value columns)
is picked only when no titled page was found. Custodian 'Detail Statement' layouts are NOT picked (own parser later)."""
import os
from contextlib import contextmanager
import src.text_extract as te

KW = os.path.join(os.path.dirname(__file__), "..", "config", "keywords.yml")
FUNDS = "\n".join(f"Vanguard Vanguard Target Retirement 20{i}0 Fund 0 {i},215,040" for i in range(2, 9)) + \
        "\nFidelity Fidelity 500 Index 0 2,343,428\nMFS MFS Value R6 0 563,620\n"
UNTITLED = ("Attachment to 2025 Form 5500\nAssets Held at Year End\nPlan Name X PLAN\n"
            "(b) Identity of issue, (c) Description of investment\n(d) Cost (e) Current value\n" + FUNDS)
TITLED = "SCHEDULE H, LINE 4i - SCHEDULE OF ASSETS (HELD AT END OF YEAR)\nIdentity of issue Current value\n" + FUNDS
REPORTABLE = ("SCHEDULE OF REPORTABLE TRANSACTIONS\nIdentity of party involved Current value\n" + FUNDS)
CUSTODIAN = ("FD406 PAGE 11\nDETAIL STATEMENT OF ACME MASTER TRUST\nASSETS AND LIABILITIES\n" + FUNDS)
LETTER = "Dear Sir/Madam:\nThe audited financial statements are not yet complete.\n"


class _Page:
    def __init__(self, text):
        self._t, self.chars = text, []

    def extract_text(self):
        return self._t


def _classify(monkeypatch, texts):
    @contextmanager
    def fake_open(_):
        yield type("D", (), {"pages": [_Page(t) for t in texts]})()
    monkeypatch.setattr(te.pdfplumber, "open", fake_open)
    return [p["page_number"] for p in te.classify_pages_text("x/ACK.pdf", KW) if p["is_supplemental"]]


def test_untitled_tables_picked_when_nothing_else(monkeypatch):
    assert _classify(monkeypatch, [LETTER, UNTITLED]) == [2]
    assert _classify(monkeypatch, [LETTER, CUSTODIAN]) == []  # custodian layout needs its own parser


def test_fallback_off_when_titled_schedule_exists(monkeypatch):
    assert _classify(monkeypatch, [TITLED, LETTER, UNTITLED]) == [1]


def test_reportable_transactions_never_picked(monkeypatch):
    assert _classify(monkeypatch, [LETTER, REPORTABLE]) == []
    assert _classify(monkeypatch, [LETTER]) == []
