# Continue here: FedEx Vanguard duplicate-row bug (2026-10-01)

Paste this file's content into a new session to resume. Companion doc: `docs/parking_lot.md`
finding #33 (FedEx, broader context) and finding #34 (UPenn, separate bug family).

## Where things stand

FedEx's already-deployed fix (commit `df1e3ace`, on EC2) improved things but a NEW bug
surfaced on the user's own diagnostic pass of the latest run:

- Run landed: 13 rows, $1,943,638,000, all `validation_status = PASS`.
- Real/true total: **$1,707,829,000** (confirmed independently below).
- 3 Vanguard mutual funds each appear TWICE -- once under a plain name, once prefixed
  "The Vanguard Group" -- because the underlying FedEx PDF has this entire holdings
  schedule duplicated verbatim across two different pages (page 19 via text-extraction,
  page 21 via table-extraction; a known PDF-authoring pattern, same as seen on UPenn).
  Normally a dedup stage collapses these cross-page duplicates back to one row. It works
  for ~27 of 30 Vanguard funds. It fails for exactly these 3:
  - Mid-Cap Index Fund Institutional Plus Shares -- $107,809,000
  - Small-Cap Index Fund Institutional Shares -- $96,816,000
  - Inflation-Protected Securities Fund; Inst'l Shares (garbled apostrophe) -- $31,184,000

## Root cause -- CONFIRMED, not guessed (verified against real code, no mocking)

There are TWO separate dedup layers in the pipeline, run in this order inside
`run_post_extract_validation()` (`src/post_extract_validator.py:2206` then `:2229`):

1. **`dedup_plan_rows()`** (`src/post_extract_validator.py:388`) -- token-fuzzy matcher.
   Strips a stopword list (`_DEDUP_STOP`, line 301: fund/shares/institutional/admiral/
   trust/plus/etc. -- but NOT "vanguard" or "the"), tokenizes what's left, and requires
   >= 70% of the shorter name's significant tokens to match (`_dd_samefund`,
   line 346) before treating two same-value rows as duplicates. **This is the layer that
   actually runs first and is responsible for correctly collapsing 27 of the 30 pairs.**
2. `junk_detect.py`'s `fuzzy_norm()` / Tier 1a2 near-dup pass -- runs second, as a backstop.
   Not the proximate cause here (my first-pass recommendation earlier in this session
   wrongly targeted this file -- see "false start" note below).

**Why exactly these 3 and no others, verified by direct function call against the
literal strings from the user's own report:**

```
sig1=['mid', 'cap', 'index']  sig2=['vanguard','group','midcap','index']  matched=2/3=0.667 -> NOT a dupe
sig1=['small','cap','index']  sig2=['vanguard','group','smallcap','index'] matched=2/3=0.667 -> NOT a dupe
sig1=['inflation','protected','securities','l']  sig2=[...,'inflationprotected','securities','instl'] matched=2/4=0.5 -> NOT a dupe
# control, a fund with no internal hyphen -- correctly matches:
"Wellington Fund Admiral Shares" vs "The Vanguard Group Wellington Fund Admiral Shares" -> matched=1/1=1.0 -> dupe (works fine)
```

The one and only thing distinguishing these 3 funds from the other 27: **their real fund
name contains an internal hyphen or stray punctuation** (Mid-Cap, Small-Cap,
Inflation-Protected's garbled apostrophe). One copy of the duplicated PDF schedule keeps
the hyphen ("Mid-Cap" -> cleaned to 2 tokens: `mid`, `cap`). The other copy's extraction
drops the separator entirely, fusing it into one camelCase word ("MidCap" -> 1 token:
`midcap`). `_dd_clean()` (line 309) only splits on whitespace/punctuation -- it has no
concept of camelCase -- so `cap` never finds a partner token in the fused copy. That
knocks the match ratio to exactly 2/3 = 66.7%, just under the 70% cutoff. Every other
Vanguard fund in this filing (Wellington, Windsor, PRIMECAP, International Growth/Value,
LifeStrategy Moderate/Conservative) has no internal hyphen in its core name, so this
failure mode never triggers for them -- that is the entire reason it's 3 and not 30.

Reproduced end-to-end with the real pipeline (not a guess): running
`classify_pages_text` + `expand_continuation_pages` + `extract_tables_and_map` +
`dedup_plan_rows` + `junk_detect.clean_plan` against the actual `fedex.pdf`
(`C:\Users\User\AppData\Local\Temp\fedex_investigate\fedex.pdf`) and filtering to
`asset_type == 'Mutual Fund'` (the MF-only view, i.e. `plan_mf_history_v3`) reproduces
the user's $1,707,829,000 true total exactly when all 10 MF pairs are collapsed to one
row each -- confirming both the true total AND that this is an MF-view-level bug.

## Proposed fix -- verified, not yet applied

In `src/post_extract_validator.py`, `_dd_clean()` (line 309). Fuse a stray
punctuation/encoding-glitch character sitting BETWEEN two letters/digits (hyphenated
compounds, garbled apostrophes) instead of letting it split the word into two tokens:

```python
def _dd_clean(s):
    s = str(s or '').lower()
    s = re.sub(r'(?<=[a-z0-9])[^a-z0-9\s](?=[a-z0-9])', '', s)  # NEW: fuse word-internal
                                                                   # punctuation/glitches
    return re.sub(r'\s+', ' ', re.sub(r'[^a-z0-9 ]', ' ', s)).strip()
```

Verified against the real `_dd_samefund`/`_dd_tokmatch`/`_dd_years` logic (full copy,
not simplified) with the literal strings from the user's report:

- MidCap pair -> now matches (True) -- correct, same fund.
- SmallCap pair -> now matches (True) -- correct, same fund.
- Inflation-Protected garbled pair -> now matches (True) -- correct, same fund.
- Mid-Cap vs Small-Cap (must stay distinct) -> still False. Correct.
- Target Retirement 2040 vs 2045 (year-guard must still hold) -> still False. Correct.
- Wellington vs Windsor (must stay distinct) -> still False. Correct.
- LifeStrategy Moderate vs Conservative (must stay distinct) -> still False. Correct.

No false-merge regressions found in any guard case tested.

## False start this session (for context, don't repeat)

Before reproducing the bug end-to-end, I initially proposed fixing `fuzzy_norm()` in
`src/junk_detect.py` (stripping a "The Vanguard Group" sponsor prefix). That diagnosis
was plausible-sounding but WRONG once tested against the real pipeline order --
`dedup_plan_rows()` runs first and is the actual blocking stage; `junk_detect.py` never
even gets the chance to see most of these rows as un-deduped. Don't re-propose the
`junk_detect.py` fix; the `post_extract_validator.py` fix above is the right one.

## Not yet done (pending user go-ahead)

1. Apply the `_dd_clean()` edit above.
2. Re-run the full repro script against the real `fedex.pdf` end-to-end (no mocking) and
   confirm the MF total lands at exactly $1,707,829,000 / 10 rows, not 13.
3. Await explicit instruction before `git commit` and before deploying to EC2
   (`i-0eaee37f64dfe7195`, us-east-1) -- per standing policy, never deploy without
   explicit go-ahead, and never run two SSM batch jobs on that instance simultaneously.
4. `docs/parking_lot.md` is still uncommitted from earlier today (findings #33 and #34
   added, not yet committed) -- ask the user whether to commit it now or keep
   accumulating.

## Standing constraints (apply to all work, repeat every session)

- No mock: verify against real PDFs/real code execution only.
- Never `git add -A` -- stage specific files only.
- Never deploy/reload prod without explicit go-ahead.
- Toyota-related code stays parked, untouched.
- Never run two SSM batch jobs simultaneously on `i-0eaee37f64dfe7195`.
