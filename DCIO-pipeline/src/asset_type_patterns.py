import re

# Canonical asset type patterns — ordered most-specific first.
# Used by both text_extract.py (section heading detection) and
# data_cleaner.py (asset type inference from field text).
ASSET_TYPE_PATTERNS = [
    (r'Investments?\s+in\s+mutual\s+funds?',                    'Mutual Fund'),
    (r'Investments?\s+in\s+money\s+markets?',                   'Money Market Fund'),
    (r'Investments?\s+in\s+common\s+collective\s+trusts?',      'Common/Collective Trust Fund'),
    (r'Investments?\s+in\s+pooled\s+separate\s+(?:investment\s+)?accounts?', 'Separate Account'),
    (r'Investments?\s+in\s+investment\s+contracts?',            'Stable Value Fund'),
    (r'Insurance\s+Company[\s/]+General\s+Accounts?\s*(?:Contracts?)?', 'Insurance General Account'),
    (r'General\s+Account\s+Contracts?',                         'Insurance General Account'),
    (r'Group\s+Annuity\s+Contracts?',                           'Group Annuity Contract'),
    (r'Fixed\s+Annuity\s+Contracts?',                           'Group Annuity Contract'),
    (r'CREF\s+Accounts?',                                       'Group Annuity Contract'),
    (r'Fully[\-\s]Benefit[\-\s]Responsive\s+Contracts?',       'Stable Value Fund'),
    (r'Non[\-\s]Benefit[\-\s]Responsive\s+Contracts?',         'Stable Value Fund'),
    # COMBINED heading: one section labeled BOTH mutual funds AND collective trusts, over a
    # MIXED block with no per-row type to split them. Must NOT be typed as pure CIT -- that
    # wrongly excludes the real MFs -- so it gets its OWN label, identifiable for a later
    # MF-vs-CIT apportionment step. MUST precede the Collective Trust patterns below.
    (r'Mutual\s+Funds?\s+and\s+Common\s*/?\s*Collective\s+Trusts?', 'Mutual Fund/Collective Trust'),
    (r'Mutual\s+Funds?\s+and\s+Collective\s+Trusts?',              'Mutual Fund/Collective Trust'),
    # "Mutual and Money Market Funds" -- a combined heading (e.g. Parkview Health System)
    # over a block that is overwhelmingly individual mutual funds, with the odd literal
    # money-market-fund row mixed in by name only. Without this, no ASSET_TYPE_PATTERNS
    # entry fullmatches the heading, so the one-cell heading detector misses it, the
    # heading text gets merged onto the next row's name as a stray fragment, and the
    # embedded "Money Market Fund" substring then gets picked up by detect_asset_type()
    # -- mistyping the whole section as Money Market Fund, which is excluded from the MF
    # total (zeroing the entire section). Typed as Mutual Fund since that's the dominant
    # holding type here; the rare true-MMF-named row is immaterial by dollar value.
    (r'Mutual\s+(?:and|&)\s+Money\s+Market\s+Funds?',              'Mutual Fund'),
    (r'Short[\-\s]Term\s+Investment\s+Funds?',                     'Short-Term Investment Fund'),
    (r'Common\s*/\s*Collective\s+Trust\s+Funds?',               'Common/Collective Trust Fund'),
    (r'Common\s*/\s*Collective\s+Trusts?',                      'Common/Collective Trust Fund'),
    (r'Lifecycle\s+Investment\s+Options?',                      'Common/Collective Trust Fund'),
    (r'Collective\s*/\s*Common\s+Trust\s+Funds?',               'Common/Collective Trust Fund'),
    (r'Common\s+Collective\s+Trust\s+Funds?',                   'Common/Collective Trust Fund'),
    (r'Collective\s+Investment\s+Trusts?',                      'Common/Collective Trust Fund'),
    (r'Collective\s+Investment\s+Funds?',                       'Common/Collective Trust Fund'),
    (r'Collective\s+Trust\s+Funds?',                            'Common/Collective Trust Fund'),
    (r'Collective\s+Trusts?',                                   'Common/Collective Trust Fund'),
    (r'Common\s+Collective\s+Trusts?',                          'Common/Collective Trust Fund'),
    (r'Collecti\w{0,3}e\s+Trusts?',                             'Common/Collective Trust Fund'),  # OCR-tolerant: "Collecti11e Trusts"
    (r'Pooled\s+Separate\s+(?:Investment\s+)?Accounts?',        'Separate Account'),
    # bare "separate accounts" (no "pooled" qualifier) -- e.g. a trailing "Total separate
    # accounts" subtotal line under a block of insurance-company variable-annuity
    # sub-accounts (Navicent Health / Lincoln VIP funds). Must follow the "Pooled Separate
    # Accounts" patterns above so those more specific labels still win when present.
    (r'Separate\s+Accounts?',                                   'Separate Account'),
    (r'Pooled\s+Funds?',                                        'Commingled Fund'),
    (r'Separately\s+Managed\s+Accounts?',                       'Separately Managed Account'),
    # Reversed word order from "Separately Managed Account" above (e.g. Nouryon
    # Chemicals LLC's Schedule H, 4i page uses "MANAGED SEPARATE ACCOUNT" as its
    # section heading for a Galliard stable value holding whose dollar amount
    # matches the plan's certified amt_pooled_sep_acct bucket exactly).
    (r'Managed\s+Separate\s+Accounts?',                         'Separate Account'),
    (r'Self[\-\s]Directed\s+Brokerage\s+Accounts?',             'Self-Directed Brokerage Account'),
    (r'Commingled\s+Funds?',                                    'Commingled Fund'),
    (r'Commingled\s+Pools?',                                    'Commingled Fund'),
    (r'Commingled',                                             'Commingled Fund'),
    (r'Company\s+Stocks?',                                      'Employer Stock'),
    (r'Unallocated\s+Insurance\s+Contracts?',                   'Insurance General Account'),
    (r'Self[\-\s]?Directed\s+Brokerage',                       'Self-Directed Brokerage Account'),
    (r'Collective\s+Funds?',                                    'Commingled Fund'),
    (r'Stable\s+Value\s+Funds?',                                'Stable Value Fund'),
    (r'Money\s+Market\s+Funds?',                                'Money Market Fund'),
    # No canonical pattern here previously mapped this DOL Schedule H, 4i
    # heading at all -- it silently fell through the one-cell heading
    # detector as an unrecognized fragment (see Nouryon Chemicals LLC's
    # page, where "INTEREST BEARING CASH" preceded a Vanguard money-market
    # holding whose dollar amount is close to the plan's certified
    # amt_interest_cash bucket). 'Cash' matches the canonical name already
    # used for this same heading text in section_typing.py / text_extract.py's
    # _SCHEDULE_OF_TITLE_TYPE_MAP.
    (r'Interest[\-\s]*Bearing\s+Cash',                          'Cash'),
    (r'\bMMRK\b',                                                'Money Market Fund'),
    (r'Variable\s+Annuit(?:y|ies)(?:\s+(?:Contracts?|Accounts?))?', 'Variable Annuity Contract'),
    (r'Registered\s+Investment\s+Compan(?:y|ies)',              'Mutual Fund'),
    (r'Registered\s+Investment\s+Compan(?:y|ies)\s*\(\s*Mutual\s+Funds?\s*\)', 'Mutual Fund'),
    (r'Registered\s+Investment\s+Funds?',                       'Mutual Fund'),
    # ETFs organized/reported under this SEC structural label (e.g. iShares) -- DOL
    # certified MF totals for these filers roll them into mutual funds. See LMC DC
    # Master Trust: certified MF included these, causing a 26%-of-certified regression
    # until this was added.
    (r'Unit\s+Investment\s+Trusts?',                            'Mutual Fund'),
    (r'Institutional\s+Funds?',                                 'Mutual Fund'),
    (r'Funds?\s+of\s+Funds?',                                   'Mutual Fund'),
    (r'Mutual\s+Funds?',                                        'Mutual Fund'),
    (r'Employer\s+Stocks?',                                     'Employer Stock'),
    (r'Employer\s+Securities',                                  'Employer Stock'),
    (r'Preferred\s+Stocks?',                                    'Preferred Stock'),
    (r'Common\s+Stocks?',                                       'Employer Stock'),
    (r'Publicly[\-\s]traded\s+Stocks?',                         'Common Stock'),
    # --- additional non-MF vehicle categories (DOL Sch H taxonomy): partnerships/joint
    # ventures, real estate, hedge funds, bonds/debt, derivatives, 103-12 IEs. These match
    # SECTION HEADINGS and strict full-label descriptions only (both safe: _detect_section_heading
    # fires on value-less lines, detect_asset_type_strict on whole labels). Row-level detection
    # is kept conservative in ROW_TYPE_PATTERNS so a bond/real-estate MUTUAL FUND name is never
    # mistyped as a direct holding. Ordered specific-first; combined partnership/JV precedes both.
    (r'103[\-\s]?12\s+Investment\s+Entit(?:y|ies)',             '103-12 Investment Entity'),
    (r'103[\-\s]?12\s+IEs?',                                    '103-12 Investment Entity'),
    (r'Partnerships?\s*(?:/|&|and)\s*Joint\s+Ventures?',        'Partnership Interest'),
    (r'Joint\s+Ventures?',                                      'Joint Venture'),
    (r'Real\s+Estate\s+Investment\s+Trusts?',                   'Real Estate'),
    (r'Real\s+Propert(?:y|ies)',                                'Real Estate'),
    (r'Real\s+Estate(?!\s+(?:Funds?|Securit|Index|Mutual|Stock))', 'Real Estate'),
    (r'Funds?\s+of\s+Hedge\s+Funds?',                           'Hedge Fund'),
    (r'Hedge\s+Funds?',                                         'Hedge Fund'),
    # Large DB/Master Trust Schedule H 4i filings commonly group the PE/RE/VC/infra
    # tail under these two literal headings instead of "Partnership Interests"/"Hedge
    # Funds" -- e.g. Mayo Pension Plan: "Absolute return:" (a book of hedge-fund-style
    # LP vehicles) and "Private Investments:" (PE/RE/VC/infra LPs), each followed by a
    # subtotal line. Neither phrase was in this table before 2026-10-12, so every row
    # under them extracted with asset_type empty -- correct dollar amounts landed in
    # staging but with no vehicle-type tag, which is both why they couldn't be
    # excluded from the MF table by name and why the alternatives router had no
    # declared-type signal to fall back on for names its phrase/brand vocabulary
    # doesn't recognize. Mapped to the nearest existing canonical labels rather than
    # inventing new ones so downstream routing/exclusion logic needs no changes.
    (r'Absolute\s+Returns?',                                    'Hedge Fund'),
    (r'Private\s+Investments?',                                 'Partnership Interest'),
    (r'Corporate\s+Debt\s+Instruments?',                        'Bond'),
    (r'Corporate\s+Debt',                                       'Bond'),
    (r'Corporate\s+Bonds?',                                     'Bond'),
    (r'Government\s+Bonds?',                                     'Bond'),
    (r'Municipal\s+Bonds?',                                      'Bond'),
    (r'U\.?\s*S\.?\s+Government\s+Securities',                   'Bond'),
    (r'U\.?\s*S\.?\s+Treasury\s+(?:Securities|Obligations|Notes?|Bonds?|Bills?)', 'Bond'),
    (r'Government\s+Obligations',                               'Bond'),
    (r'Debt\s+Securities',                                      'Bond'),
    (r'Fixed\s+Income\s+Securities',                            'Bond'),
    (r'Debentures?',                                            'Bond'),
    (r'Derivatives?',                                           'Derivative'),
    (r'(?:Call|Put)\s+Options?',                                'Derivative'),
    (r'Futures\s+Contracts?',                                   'Derivative'),
    (r'(?:Interest\s+Rate\s+)?Swaps?\s+Contracts?',             'Derivative'),
    (r'Partnership\s+Interests?',                               'Partnership Interest'),
    (r'Participant\s+Loans?',                                   'Participant Loan'),
    # end-bounded: unanchored, "NETFLIX INC 5.4% 08-15-2054" read as an ETF; no leading
    # bound so glued names ("iShares EAFEETF") still match (2026-10-21)
    (r'ETFs?\b',                                                'ETF'),
    (r'Currenc(?:y|ies)',                                       'Currency'),
]


def detect_asset_type(text: str) -> str:
    """Return canonical asset type if text matches any known pattern, else empty string."""
    for pattern, asset_type in ASSET_TYPE_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return asset_type
    return ''


def detect_asset_type_strict(text: str) -> str:
    """Return canonical asset type only if text IS a type label (fullmatch).
    Unlike detect_asset_type, this will not match fund names that contain
    type keywords as substrings (e.g. 'BlackRock Index Fund' returns '')."""
    if not text:
        return ''
    text = text.strip().rstrip(':')
    for pattern, asset_type in ASSET_TYPE_PATTERNS:
        if re.fullmatch(pattern, text, re.IGNORECASE):
            return asset_type
    return ''


# Per-row asset-type DECLARATIONS carried in the row itself -- a dedicated "Type:" column
# value, or a vehicle-type phrase at the start/end of the column-(c) description
# ("Common/Collective Trust", "Insurance Company Separate Account", "mutual fund,",
# "Collective investment in ...", "Pooled Separate Accounts"). Used when there is NO
# section heading. Deliberately EXCLUDES bare name-ish fund types ("Index Fund", "Fund")
# so a fund NAME never triggers a type -- only an explicit vehicle declaration does.
# Ordered non-MF-first so an explicit CIT / separate account / stock wins over an
# incidental trailing "fund" token in the same string.
ROW_TYPE_PATTERNS = [
    (r'insurance\s+company\s+separate\s+accounts?',    'Separately Managed Account'),
    (r'pooled\s+separate\s+(?:investment\s+)?accounts?', 'Separate Account'),
    (r'separate(?:ly)?\s+managed\s+accounts?',         'Separately Managed Account'),
    # bare "separate account" fallback (no "pooled"/"insurance company"/"managed"
    # qualifier) -- e.g. "International Separate Account", a variable-annuity
    # sub-account name that carries none of the more specific phrases above.
    # Placed after them so those still win when present; "separate account" is
    # DOL/insurance vehicle terminology, never part of a real mutual fund's name,
    # so a bare match here is safe. Closes a v3-cleanup tracker gap (2026-09-11).
    (r'\bseparate\s+accounts?\b',                       'Separate Account'),
    (r'common\s*/?\s*collective\s+trusts?',            'Common/Collective Trust Fund'),
    (r'collective\s+investment\s+trusts?',             'Common/Collective Trust Fund'),
    (r'collective\s+investment',                       'Common/Collective Trust Fund'),
    (r'collecti\w{0,3}e\s+trusts?',                     'Common/Collective Trust Fund'),  # OCR-tolerant
    # Vanguard's own DC-plan terminology for its Target Retirement Trusts (a CIT
    # product line, reported in "shares" like a mutual fund would be) -- seen on
    # Trane Technologies' Schedule of Assets. Certified Schedule H CIT total for
    # that filing ($4.135B) is consistent with these being CIT, not MF; the
    # generic '40-Act mutual fund equivalent says "mutual fund," not this phrase.
    (r'lifecycle\s+investment\s+options?',             'Common/Collective Trust Fund'),
    (r'commingled',                                    'Commingled Fund'),
    (r'pooled\s+funds?',                               'Commingled Fund'),
    (r'group\s+annuity',                               'Group Annuity Contract'),
    # guaranteed annuity contracts (TIAA Traditional etc.) -- real holdings, NOT mutual funds
    (r'tiaa\s+traditional',                            'Group Annuity Contract'),
    (r'traditional\s+annuit',                          'Group Annuity Contract'),
    (r'annuity\s+contract',                            'Group Annuity Contract'),
    (r'fixed\s+annuit',                                'Group Annuity Contract'),
    (r'guaranteed\s+portfolio',                        'Group Annuity Contract'),
    (r'(?:non[\-\s]?)?benefit[\-\s]?responsive',       'Group Annuity Contract'),
    (r'ins(?:urance)?\s+(?:co(?:mpany)?[\s/]+)?general\s+accounts?', 'Insurance General Account'),
    (r'personal\s+choice\s+ret(?:ire)?ment\s+account',  'Self-Directed Brokerage Account'),
    # abbreviated/one-word forms of self-directed brokerage that the full-phrase
    # 'brokerage account' pattern below never catches: "SDBA"/"SDB" acronyms,
    # "BrokerageLink" as a single word (Fidelity's product name, no space before
    # "Link"), and "Self Directed ... Acct" using the "Acct" abbreviation instead
    # of "Account". Found sitting blank in staging and getting fuzzy-matched into
    # the mutual-fund promotion candidate pool instead of being excluded.
    (r'\bSDBA\b',                                       'Self-Directed Brokerage Account'),
    (r'\bSDB\b',                                        'Self-Directed Brokerage Account'),
    (r'Brokerage\s*Link',                               'Self-Directed Brokerage Account'),
    (r'self[\-\s]?direct(?:ed)?\s+acc?t\b',             'Self-Directed Brokerage Account'),
    (r'guaranteed\s+(?:investment\s+contract|income)', 'Stable Value Fund'),
    (r'stable\s+value',                                'Stable Value Fund'),
    # wrap/wrapper contracts -- the GIC-style principal-preservation overlay insurers
    # sell against a plan's own bond portfolio (e.g. "Metropolitan Tower Life Ins Co
    # Wrapper Contract"), non-MF. Closes a v3-cleanup tracker gap (2026-09-11).
    (r'wrap(?:per)?\s+contracts?',                     'Stable Value Fund'),
    # Variable annuities are now typed as their own non-MF vehicle (Variable Annuity Contract)
    # per direct PDF review -- these ARE their own asset-type heading in the filing, not a
    # mutual fund. A prior 2500-plan run flagged -45 PASS when this was excluded; that was
    # accepted as a known tradeoff rather than reverting the classification.
    (r'variable\s+annuit(?:y|ies)',                    'Variable Annuity Contract'),
    # insurer legal-entity name pattern for variable annuity issuers (e.g. "Prudential
    # Retirement Insurance and Annuity Company") -- "annuity" and "variable" appear in
    # the row but not adjacent, so the phrase pattern above misses it.
    (r'insurance\s+(?:and|&)\s+annuity\s+compan(?:y|ies)', 'Variable Annuity Contract'),
    # fixed / guaranteed / GIC principal-preservation vehicles -> Stable Value (non-MF)
    (r'fixed\s+account',                               'Stable Value Fund'),
    (r'fixed\s+interest',                              'Stable Value Fund'),
    (r'stable\s+return',                               'Stable Value Fund'),
    (r'\bgic\b',                                       'Stable Value Fund'),
    (r'guarante(?:ed|y)',                              'Stable Value Fund'),
    # limited partnership interests (non-MF)
    (r'limited\s+partnership|partnership\s+interest|interest\s+in\s+limited\s+part', 'Partnership Interest'),
    # additional non-MF DIRECT holdings -- ONLY phrases a mutual-fund NAME never carries.
    # detect_asset_type_row is searched against the issuer NAME too, so NO bare 'bond' /
    # 'real estate' / 'government' here (those live in fund names and would cause under-capture).
    (r'joint\s+venture',                               'Joint Venture'),
    (r'103[\-\s]?12\s+investment\s+entit',             '103-12 Investment Entity'),
    (r'hedge\s+fund',                                  'Hedge Fund'),
    (r'real\s+propert(?:y|ies)',                       'Real Estate'),
    (r'\bdebentures?\b',                               'Bond'),
    (r'corporate\s+debt\s+instrument',                 'Bond'),
    (r'\d(?:\.\d+)?\s*%[^%]{0,40}\b(?:due|matur)',      'Bond'),   # coupon% + maturity => direct bond
    (r'(?:call|put)\s+options?\b',                     'Derivative'),
    (r'\bfutures\s+contracts?\b',                      'Derivative'),
    (r'interest\s+rate\s+swaps?\b',                    'Derivative'),
    (r'registered\s+investment\s+compan(?:y|ies)',     'Mutual Fund'),
    # combined "Mutual and Money Market Funds" heading, merged onto a data row's own
    # description (e.g. "Mutual and Money Market Funds: Mid-Cap Growth Fund") -- MUST
    # precede the bare 'money market' pattern below, or that generic substring match
    # wins and mistypes the whole section (real mutual funds) as Money Market Fund.
    # See Arcadis U.S.: this zeroed out $475.4M of real MF holdings (T. Rowe Price,
    # Vanguard, PIMCO, American Funds, etc.) from the MF total.
    (r'mutual\s+(?:and|&)\s+money\s+market',            'Mutual Fund'),
    (r'money\s+market',                                'Money Market Fund'),
    (r'employer\s+securit(?:y|ies)',                   'Employer Stock'),
    (r'(?:self[\-\s]?directed\s+)?brokerage\s+account', 'Self-Directed Brokerage Account'),
    (r'money\s+mkt',                                   'Money Market Fund'),   # "MONEY MKT" abbrev
    (r'\bmmrk\b',                                      'Money Market Fund'),   # "MMRK" abbrev
    # "Common Stock" guarded against trailing "...Fund" -- some real, registered mutual
    # funds are literally named "<Manager> Common Stock Fund" (e.g. FMI Common Stock Fund,
    # ticker FMIMX). Without this guard those get mistyped as Employer Stock and excluded
    # from plan_mf_history_v3. A genuine employer-stock line never has "Fund" trailing
    # "Common Stock" -- it just ends at "... Common Stock" / "Common Stock, N shares".
    (r'common\s+stock(?!\s+fund\b)',                   'Employer Stock'),
    (r'common\s+shares',                               'Employer Stock'),
    (r'\ber\s+stock',                                  'Employer Stock'),   # "... ER Stock Fund" (employer)
    # a corporate entity (Inc/Corp/PLC/Ltd) whose name ENDS in bare "stock" -> common stock
    (r'(?:\binc\b|\bcorp\b|corporation|\bplc\b|\bltd\b)\b.*\bstock\s*$', 'Employer Stock'),
    # a directly-held company security: a corporate-entity name carrying a share COUNT and no
    # fund/trust/account vehicle word -> employer stock (a mutual fund is never named this way).
    # Checked on BOTH sides of the share count: extraction boilerplate often reads "<Manager
    # Corp> 1,161,701 shares of American Balanced Fund" -- the vehicle word trails the count,
    # not leads it, so a real mutual fund's share count was slipping through as Employer Stock.
    (r'(?:corporation|incorporated|\bcorp\b|\binc\b|\bplc\b|\bco\b|company)\b(?![^\d]*\b(?:fund|trust|account|portfolio|index)\b)[^\d]*\d[\d,]*\s*shares?\b(?!\s*(?:of\s+)?[A-Za-z0-9 &,.\-]{0,60}\b(?:fund|trust|portfolio|index)\b)', 'Employer Stock'),
    (r'mutual\s+funds?',                               'Mutual Fund'),
    # OCR-tolerant: this filer's embedded font renders with stray mid-word spaces
    # ("M ut ual fund", "Mut ual Funds") identically in the native PDF text layer
    # and in PaddleOCR output -- a true rendering artifact, not an encoding bug
    # (Toyota Motor North America, ack_id 20251008114247NAL0009333472001).
    # \s* between every letter of "mutual" tolerates any such split point.
    (r'\bm\s*u\s*t\s*u\s*a\s*l\s*fund\s*s?\b',          'Mutual Fund'),
]


# A directly held security: a bond, note, Treasury, agency pool or futures contract
# ("UNITED STATES TREAS BDS 2% 11-15-2041", "US 10YR TREAS NTS FUTURE (CBT) EXP MAR 25",
# "JPMORGAN CHASE & CO 5.35%"). Name-based typing must never make one of these a
# mutual fund or ETF: on 2026-10-21, US Steel's and Liberty Mutual's untyped bonds and
# futures reached plan_mf_history_v3 that way. A 144A tag or a futures exchange/expiry
# marks a security outright. The softer signals (a coupon rate, a maturity date, an
# abbreviated "BDS"/"NTS"/"FUT", an agency pool) yield to a fund or cash-vehicle word
# ("FIRST AM TREAS OB FD", "DREYFUS GOVT CAS MGMT INST ... 12/31/2075", "Fidelity
# Asset Manager 85%", "iShares 7-10 YR Treasry BD ETF"). A bare "bond", "treasury"
# or "pool" is no signal: "Fidelity Total Bond K6", "Vanguard Treasury MoneyMkt" and
# "FID CONTRA POOL CL F" are funds. Checked against every plan_mf_history_v3 name.
_SECURITY_HARD_RE = re.compile(
    r'\b144a\b|\b(?:cbt|xcbt|cme|cbot)\b|\bfutures?\s+(?:contract|cbt|exp)|\bexp\s+[a-z]{3}\s+\d{2}\b',
    re.IGNORECASE)
_SECURITY_SOFT_RE = re.compile(
    r'\d(?:\.\d+)?\s*%|\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b|\bdue\b|\bnts\b|\bbds\b|\bfut\b|\b\d{1,2}\s*yr\s+(?:treas|ultra)|'
    r'\bfnma\b|\bfhlmc\b|\bgnma\b|\bfhlb\b|\bdeb\b|\bmtn\b|\bcallable\b|\bsr\s+(?:unsec|nt|notes?|sec)|z-?cpn|\bstrips?\b',
    re.IGNORECASE)
_FUND_VEHICLE_RE = re.compile(
    r'\bfunds?\b|\bfnd\b|\bfds?\b|\betfs?\b|\bindex\b|\bidx\b|\bportfolio\b|money\s*(?:market|mkt)|\bmmkt\b|\bmm\b|'
    r'ishares|\bspdr\b|\badm(?:iral|in)?\b|\binstl?\b|\bcas(?:h)?\s+mgmt|\bcsh\s+mgmt|liquidity|\bliq\b|'
    r'asset\s+(?:manager|mgr)|\ballocation\b|target\s+(?:retirement|date)|lifetime|registered\s+investment',
    re.IGNORECASE)


def looks_like_security(name: str) -> bool:
    """True when a holding name reads as a directly held bond, note, Treasury, agency
    pool or futures contract rather than a pooled fund (see _SECURITY_HARD_RE)."""
    if not name:
        return False
    if _SECURITY_HARD_RE.search(name):
        return True
    return bool(_SECURITY_SOFT_RE.search(name)) and not _FUND_VEHICLE_RE.search(name)


# A holding name that names no fund at all: a container or subtotal label ("companies",
# "investment companies", "interest held in master trust", "see attached", "n/a") or a
# stray column header ("sedol"). fund_intelligence_mapping_mf holds such names because
# they once reached plan_mf_history_v3 ("sedol" alone carried $32.7B of MF-typed staging
# rows over 47 filings on 2026-10-21). A name is generic when every word is a filler
# word. A bare fund family ("vanguard", "fidelity group tr for") is NOT generic: checked
# against certified MF totals, such rows are mostly real fund money ($20.6B) rather
# than double counts ($4.3B), so they still match.
_FILLER_WORDS = frozenset("""
    fund funds fd fds fnd investment investments companies company shares share mutual trust tr account accounts
    plan see attached sedol cusip interest held in master n a na none other various total group and the of for inc
    co corp llc exhibit refer to registered""".split())


def is_generic_holding_name(name: str) -> bool:
    """True when a holding name is only filler words, so it cannot identify a fund.
    Numbers count as identifying unless they are a single digit or all zeros."""
    words = [w for w in re.findall(r'[a-z]+|\d+', (name or '').lower())
             if not (w.isdigit() and (len(w) < 2 or set(w) == {'0'}))]
    return not any(w not in _FILLER_WORDS for w in words)


# A bank collective trust named by its share-class tag alone: State Street/SSgA/BlackRock
# "NL Ser"/"NL Cl"/"Non-Lending" series and JPMorgan Chase Bank ("JPMCB") funds. No
# registered mutual fund carries these tags, yet the name-typing loop typed them MF:
# Timken Master Trust (2026-10-21) kept $343M of them in plan_mf_history_v3 because the
# filing has no certified total, so mf_reconcile never ran. 209 v3 rows / $6.9B over 89
# filings matched on 2026-10-22, every one a CIT on review.
_CIT_TAG_RE = re.compile(r'\bnl\s+(?:ser(?:ies)?|cl(?:ass)?)\b|\bnon[\-\s]?lending\b|\bjpmcb\b', re.IGNORECASE)


def looks_like_cit(name: str) -> bool:
    """True when a holding name carries a tag only a bank collective trust uses (see _CIT_TAG_RE)."""
    return bool(name) and bool(_CIT_TAG_RE.search(name))


# Names that can never be a mutual fund, whatever type the cleaner or the fund mapping gave
# them. Spot check of plan_mf_history_v3 on 2026-10-22 found ~$39B of such rows typed MF
# (collective/commingled trusts, Fidelity/Spartan pools, brokerage windows, stable value,
# bonds and loan-rate lines, single company stocks). A bare "trust" is NOT a signal:
# Templeton Developing Markets Trust, JP Morgan Trust II Core Bond, MFS Series Trust I and
# Neuberger Berman Genesis "Trust Class" are registered mutual funds. Ordered first-wins.
_CIT_NAME_RE = re.compile(r'collective|commingled|comingled|\bcit\b|\bctf\b|common\s+trust', re.IGNORECASE)
_FID_POOL_RE = re.compile(r'\bpool\b', re.IGNORECASE)
_FID_FAMILY_RE = re.compile(r'\bfid\b|fidelity|spartan|geode|contra|growth\s+co\b', re.IGNORECASE)
_BROKERAGE_RE = re.compile(r'brokerage|\bsdba\b|self[\-\s]?direct|self[\-\s]?managed', re.IGNORECASE)
_STABLE_VALUE_RE = re.compile(
    r'stable\s*val|\bgic\b|guaranteed\s+(?:option|account|income|interest)|wrapped\s+holdings|wrap(?:per)?\s+contracts?',
    re.IGNORECASE)
_LOAN_RATE_RE = re.compile(r'\brates?\s+(?:from|range)|interest\s+rates?\s+(?:from|range)', re.IGNORECASE)
# A bond needs a coupon AND a date, a STRIPS/zero-coupon tag, or a variable-rate note with a
# date. looks_like_security() alone is too loose here: its GNMA/%/FUT signals match real
# funds ("Fidelity GNMA", "Fidelity Asset Manager 70%", "AHL Managed Futures").
_DATE = r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b'
_BOND_RE = re.compile(
    r'\d(?:\.\d+)?\s*%.{0,40}' + _DATE + r'|' + _DATE + r'.{0,40}\d(?:\.\d+)?\s*%|z-?cpn|\bstrips\b|'
    r'\bvar(?:iable)?\s+r(?:ate|t)\b.{0,40}' + _DATE + r'|\bdue\s+' + _DATE,
    re.IGNORECASE)
# Money-market funds also print "VAR RT 12/31/2049" ("DREYFUS GOVT CAS MGMT INST 289 VAR RT ...").
_NOT_BOND_RE = re.compile(
    r'100\s*%|money|\bmm\b|mmkt|\bm\s+monthly|\bfu?n?d\b|\bind(?:e?x)?\b|\bidx\b|\btrusts?\b|\btr\b|\betf\b|'
    r'\binstl?\b|cas(?:h)?\s+mgmt|csh|\bliq|\bobl\b|\bcf\b|\bplus\b|12/31/20(?:49|75)', re.IGNORECASE)
# "Inc" alone is no company signal ("Dodge Cox Inc", "VANG EQUITY INC ADM" mean Income), nor
# "Company"/"Corp" after a fund word ("Fidelity Growth Company", "Vanguard High Yield Corp",
# "DFA Japanese Small Company"); insurers are contracts, not stock. Only corporate endings.
_COMPANY_END_RE = re.compile(
    r'(?:\b(?:cos|co)\.?\s+inc|&\s*co|\bcorp(?:oration)?(?:\s+inc)?|\bincorporated|\bplc|\bltd|\b(?:group|holdings?)\s+inc)\.?\s*$',
    re.IGNORECASE)
_NOT_COMPANY_RE = re.compile(
    r'insur|\blife\b|annuit|assur|invest|regist|yield|bond|growth|small|value|\bcap\b|income|partners|offshore|mutual|'
    r'\btrust\b|\btr\b|capital\s+management|asset\s+management|advis|vanguard|fidelity|\bfid\b|short[\-\s]term|'
    r'\bterm\b|\bindex\b|\bdfa\b|dimensions', re.IGNORECASE)


def non_mf_override_type(name: str) -> str:
    """Asset type for a holding NAME that can never be a mutual fund (see _CIT_NAME_RE etc.),
    else ''. Used to re-type rows the cleaner labelled Mutual Fund."""
    if not name:
        return ''
    if _STABLE_VALUE_RE.search(name):   # before CIT: "Stable Value Common Trust Fund"
        return 'Stable Value Fund'
    if looks_like_cit(name) or _CIT_NAME_RE.search(name):
        return 'Common/Collective Trust Fund'
    if _FID_POOL_RE.search(name) and _FID_FAMILY_RE.search(name):
        return 'Common/Collective Trust Fund'
    if _BROKERAGE_RE.search(name):
        return 'Self-Directed Brokerage Account'
    if _LOAN_RATE_RE.search(name) and not _FUND_VEHICLE_RE.search(name):
        return 'Participant Loan'
    if _BOND_RE.search(name) and not _NOT_BOND_RE.search(name):
        return 'Bond'
    if (_COMPANY_END_RE.search(name.strip()) and not _FUND_VEHICLE_RE.search(name)
            and not _NOT_COMPANY_RE.search(name)):
        return 'Employer Stock'
    return ''


def detect_asset_type_row(text: str) -> str:
    """Detect a per-row asset-type DECLARATION carried in the row (a Type-column value or a
    vehicle-type phrase in the description). Non-name-based: only explicit vehicle-type
    phrases match, so a fund NAME never triggers a type. Returns '' if the row carries no
    explicit type. Used when the section heading gave nothing."""
    if not text:
        return ''
    for pattern, asset_type in ROW_TYPE_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return asset_type
    return ''
