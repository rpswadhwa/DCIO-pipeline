"""Ditto-mark handling for Schedule of Assets extraction.

Many filers write the asset type / description once and then put a ditto mark
('"', curly quotes, or '-do-') on every row below to mean "same as above".
Left unresolved, the description is unusable, asset_type can't be detected, and
the row is silently dropped from the MF table or given a junk name (just the
ditto character). This module resolves those marks.
"""
import re

_DITTO_CHARS = '"\'`“”″'
_DITTO_RE = re.compile(
    r'^[\s*]*[' + re.escape(_DITTO_CHARS) + r']+[\s*]*$'
    r'|^[\s*]*-?\s*do\s*-?[\s*]*$',
    re.IGNORECASE,
)
_STRIP_CHARS = ' "\'`*.“”″-'
_STRIP_RE = re.compile('[' + re.escape(_STRIP_CHARS) + ']')

# A description that is just a generic asset-TYPE word may be propagated across
# ditto rows. A real fund name must NOT be (it would mislabel the rows below).
_TYPE_WORD_RE = re.compile(
    r'^(mutual fund|money market fund|money mkt fund|index fund|'
    r'common[/ ]collective trust( fund)?|collective trust( fund)?|'
    r'stable value( fund)?|pooled separate account|etf|target date( fund)?|'
    r'fixed income( fund)?|bond fund|equity fund)s?$',
    re.IGNORECASE,
)


def is_ditto(value):
    """True if the cell is only a ditto mark (means 'same as the row above')."""
    return bool(_DITTO_RE.match(str(value or "")))


def is_junk_name(value):
    """True if the string carries no real fund-name content.

    Catches ditto marks, stray punctuation, and 0-1 character remnants -- the
    kind of value that should never be used as a fund name.
    """
    return len(_STRIP_RE.sub('', str(value or ""))) <= 1


def fill_ditto_marks(rows):
    """Forward-fill ditto marks in investment_description (in reading order).

    A row whose description is a ditto inherits the previous row's description,
    and -- when it has no asset_type of its own -- the previous row's asset_type.
    Rows are processed in (page_number, row_id) order so a schedule that
    continues across pages still resolves correctly.
    """
    def _key(r):
        stem = str(r.get("pdf_stem", "") or r.get("pdf_name", "") or "")
        try:
            return (stem, int(r.get("page_number", 0) or 0), int(r.get("row_id", 0) or 0))
        except (TypeError, ValueError):
            return (stem, 0, 0)

    out = []
    prev_desc = ""
    prev_type = ""
    prev_stem = None
    for row in sorted(rows, key=_key):
        row = dict(row)
        _stem = str(row.get("pdf_stem", "") or row.get("pdf_name", "") or "")
        if _stem != prev_stem:
            prev_desc = ""
            prev_type = ""
            prev_stem = _stem
        desc = str(row.get("investment_description", "") or "").strip()
        atype = str(row.get("asset_type", "") or "").strip()
        if _DITTO_RE.match(desc):
            # Always inherit the type so the row isn't dropped...
            if not atype and prev_type:
                row["asset_type"] = prev_type
            # ...but only inherit the description TEXT when it's a generic type
            # word (never a real fund name, which would mislabel rows below).
            if prev_desc and _TYPE_WORD_RE.match(prev_desc.strip()):
                row["investment_description"] = prev_desc
        # refresh carries from real (non-ditto) values
        cur_desc = str(row.get("investment_description", "") or "").strip()
        cur_type = str(row.get("asset_type", "") or "").strip()
        if cur_desc and not _DITTO_RE.match(cur_desc):
            prev_desc = cur_desc
        if cur_type:
            prev_type = cur_type
        out.append(row)
    return out
