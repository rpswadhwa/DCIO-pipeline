"""Offline tests for src/dedup_alt_holdings.find_actions (no Athena)."""
import pandas as pd

from src import dedup_alt_holdings as d


def _live(rh=(), cit=(), mf=(), citx=(), mfx=()):
    def frame(rows, mgr):
        df = pd.DataFrame(list(rows), columns=["ack_id", "raw_entity_name", "plan_investment_amt", mgr] +
                          (["row_order", "sch_d_code"] if mgr == "normalized_manager" else []))
        df["_amt"] = pd.to_numeric(df.plan_investment_amt)
        df["_key"] = [d.name_key(a, n, x) for a, n, x in zip(df.ack_id, df.raw_entity_name, df.plan_investment_amt)]
        return df
    live = {"rh": frame(rh, "matched_manager_name"), "cit": frame(cit, "normalized_manager"),
            "mf": frame(mf, "normalized_sponsor_name"), "citx": frame(citx, "normalized_manager"),
            "mfx": frame(mfx, "normalized_sponsor_name")}
    live["cit"]["_cit_key"] = d.cit_keys(live["cit"])
    live["citx"]["_cit_key"] = d.cit_keys(live["citx"])
    return live


def _ops(live, cert=None):
    acts, skipped = d.find_actions(live, cert)
    return sorted((x["op"], x["table"]) for x in acts), skipped


def test_rh_vs_code_c_cit_keeps_cit():
    live = _live(rh=[("A", "JPM REAL ESTATE CIT", 5e6, "JPMorgan")],
                 cit=[("A", "JPM REAL ESTATE CIT", 5e6, "JPMorgan", 1, "C")])
    assert _ops(live)[0] == [("delete", "rh")]


def test_rh_vs_code_e_cit_keeps_rh():
    live = _live(rh=[("A", "HARBOURVEST 2016 GLOBAL FUND LP", 990449, "HarbourVest")],
                 cit=[("A", "HARBOURVEST 2016 GLOBAL LP", 990449, "HARBORVEST", 3, "E")])
    assert _ops(live)[0] == [("delete", "cit")]


def test_cit_mf_p_code_alt_name_moves_to_rh():
    live = _live(cit=[("A", "TIAA REAL ESTATE ACCOUNT", 2e7, "TIAA", 1, "P")],
                 mf=[("A", "TIAA Real Estate Account", 2e7, "TIAA")])
    assert _ops(live)[0] == [("delete", "cit"), ("delete", "mf"), ("insert", "rh")]


def test_cit_mf_p_code_mutual_fund_name_keeps_mf():
    live = _live(cit=[("A", "COHEN & STEERS REAL EST S FD Z", 1529660, None, 1, "P")],
                 mf=[("A", "Cohen&Steers Real Estate Fund", 1529660, "Cohen & Steers")])
    assert _ops(live)[0] == [("delete", "cit")]


def test_cit_mf_e_code_always_moves_to_rh():
    live = _live(cit=[("A", "WASHINGTON CAPITAL JMT REAL ESTATE", 22.7e6, None, 1, "E")],
                 mf=[("A", "Washington Capital JMT Real Estate Equity Fund", 22.7e6, None)])
    assert _ops(live)[0] == [("delete", "cit"), ("delete", "mf"), ("insert", "rh")]


def test_rh_mf_lp_name_keeps_rh_else_mf():
    live = _live(rh=[("A", "BLUE OWL CREDIT LP", 3e6, "Blue Owl"), ("A", "VANGUARD REAL ESTATE INDEX", 483997, "Vanguard")],
                 mf=[("A", "Blue Owl Credit LP", 3e6, None), ("A", "Vanguard Real Estate Index Fund", 483997, "Vanguard")])
    acts, _ = d.find_actions(live)
    kept = {live[x["table"]].loc[x["row"], "raw_entity_name"]: x["table"] for x in acts}
    assert kept == {"Blue Owl Credit LP": "mf", "VANGUARD REAL ESTATE INDEX": "rh"}


def test_placeholder_dollar_rows_never_match():
    live = _live(rh=[("A", "HARBOURVEST PTRS VII - BUYOUT HOLDING FD", 1, "HarbourVest")],
                 cit=[("A", "HARBOURVEST INTL PEP V", 1, None, 1, "E")])
    assert _ops(live)[0] == []


def test_name_only_match_is_not_acted_on():
    live = _live(rh=[("A", "ADAMS STREET 2008 US FUND LP", 1e6, "Adams Street")],
                 cit=[("A", "ADAMS STREET 2008 US FUND LP", 2e6, None, 1, "E")])
    assert _ops(live)[0] == []


def test_missing_code_is_skipped():
    live = _live(rh=[("A", "SOME POOLED FUND", 1e6, None)], cit=[("A", "SOME POOLED FUND", 1e6, None, 1, None)])
    ops, skipped = _ops(live)
    assert ops == [] and skipped and "no Schedule D code" in skipped[0][3]


def test_twin_keys_are_skipped_and_whole_move_dropped():
    # Two identical MF rows but only one CIT copy: the MF DELETE would remove both, so the move is skipped.
    live = _live(cit=[("A", "TIAA REAL ESTATE ACCOUNT", 2e7, "TIAA", 1, "P")],
                 mf=[("A", "TIAA Real Estate Account", 2e7, "TIAA"), ("A", "TIAA Real Estate Account", 2e7, "TIAA")])
    ops, skipped = _ops(live)
    assert ops == [] and any("twins" in s[3] for s in skipped)


def test_different_acks_never_match():
    live = _live(rh=[("A", "X FUND LP", 1e6, None)], cit=[("B", "X FUND LP", 1e6, None, 1, "E")])
    assert _ops(live)[0] == []


# Non-Alternatives CIT vs MF (citx_mfx).
def test_citx_code_c_drops_mf_copy():
    live = _live(citx=[("A", "WESTERN ASSET CORE PLUS BOND R1", 31.6e6, None, 1, "C")],
                 mfx=[("A", "Western Asset Core Plus Bond R1", 31.6e6, None)])
    assert _ops(live)[0] == [("delete", "mfx")]


def test_tier3_different_fund_same_brand_not_matched():
    live = _live(citx=[("A", "TRP STABLE VALUE COMMON TRUST FUND", 10.6e6, None, 1, "C")],
                 mfx=[("A", "T. Rowe Price Retirement 2025 Fund", 10.67e6, None)])
    assert _ops(live)[0] == []


def test_tier3_vintage_mismatch_not_matched():
    live = _live(citx=[("A", "TARGET RETIREMENT 2035 TRUST II", 19.4e6, None, 1, "C")],
                 mfx=[("A", "Target Retirement 2055", 19.46e6, None)])
    assert _ops(live)[0] == []


def _p_live():
    return _live(citx=[("A", "T. ROWE PRICE RETIREMENT 2040 SA", 3e8, None, 1, "P")],
                 mfx=[("A", "T. Rowe Price Retirement 2040 SA", 3e8, None), ("A", "Vanguard 500 Index Adm", 1e8, None)])


def test_citx_code_p_drops_mf_copy_when_closer_to_certified():
    assert _ops(_p_live(), {"A": 1e8})[0] == [("delete", "mfx")]


def test_citx_code_p_kept_when_filer_counts_it_as_mf():
    ops, skipped = _ops(_p_live(), {"A": 4e8})
    assert ops == [] and "counts it as MF" in skipped[-1][3]


def test_citx_code_p_kept_without_certified():
    ops, skipped = _ops(_p_live(), {})
    assert ops == [] and "no certified" in skipped[-1][3]
