"""
ocr_fallback.py
~~~~~~~~~~~~~~~
Decides which PDFs, after a normal text-extraction pass, are still
under-capturing badly enough to justify an OCR re-extraction pass.

Trigger: compare each PDF's own freshly-extracted, pre-promotion, ALL-asset-
types total (see post_extract_validator.compute_extracted_all_types_totals)
against the certified amt_mutual_funds for that ack_id. This is deliberately
NOT a comparison against plan_mf_history_v3 -- v3 is already asset_type-
filtered, so a plan can be under plan_mf_history_v3 purely because staging
rows are sitting there with a blank/non-MF asset_type (a separate, deferred
classification problem OCR cannot fix). Comparing against the fresh
extraction total keeps this trigger scoped to genuine extraction failures.

Only fires in the under-capture direction: a plan that is OVER-capturing
(extracted > certified) is never flagged here.

Trigger is a hard zero, not a percentage band: only a PDF whose fresh
extraction total came back at $0 is flagged. Text extraction returning
some nonzero-but-low total (e.g. classification dropped most rows) is a
different, non-OCR-fixable problem -- OCR only helps when there was no
text layer to extract from in the first place, which is exactly the
zero-extraction case (confirmed on Stanford: 0 chars on the affected
pages, OCR recovered 99.6% of certified).
"""

from typing import Dict, List

from .post_extract_validator import compute_extracted_all_types_totals


def identify_undercapture_pdfs(
    raw_rows: List[Dict],
    reference: Dict[str, Dict[str, object]],
) -> Dict[str, Dict[str, float]]:
    """Return {pdf_stem: {extracted_total, certified, gap_amt, gap_pct}} for
    every ack_id in `reference` whose freshly-extracted all-types total is
    exactly $0 -- a total extraction failure, not merely an under-capture.
    """
    extracted_totals = compute_extracted_all_types_totals(raw_rows)
    flagged: Dict[str, Dict[str, float]] = {}
    for pdf_stem, ref in reference.items():
        certified = ref.get("amt_mutual_funds")
        if not certified or certified <= 0:
            continue
        extracted = extracted_totals.get(pdf_stem, 0.0)
        if extracted <= 0:
            flagged[pdf_stem] = {
                "extracted_total": extracted,
                "certified": certified,
                "gap_amt": certified - extracted,
                "gap_pct": (certified - extracted) / certified,
            }
    return flagged
