from typing import Dict, List

from .utils import load_yaml, parse_currency


def validate_pages(pages: List[Dict], schema_yml: str) -> Dict:
    cfg = load_yaml(schema_yml)
    required = cfg["schema"]["required"]
    numeric_fields = cfg["schema"]["numeric_fields"]
    conf_th = cfg["validation"]["confidence_threshold"]
    low_conf = cfg["validation"]["low_confidence_threshold"]

    issues = []
    total_rows = 0
    low_conf_rows = 0
    validation_failures = 0

    for page in pages:
        mapped = page.get("mapped_rows", [])
        total_rows += len(mapped)
        for row in mapped:
            missing = [r for r in required if not row.get(r)]
            if missing:
                issues.append({
                    "page": page["page_number"],
                    "row_id": row["row_id"],
                    "issue": "Missing required fields",
                    "missing": missing,
                })
                validation_failures += 1

            for nf in numeric_fields:
                val = row.get(nf, "")
                if not val:
                    continue
                _, ok = parse_currency(val)
                if not ok:
                    issues.append({
                        "page": page["page_number"],
                        "row_id": row["row_id"],
                        "issue": f"Invalid numeric format: {nf}",
                        "value": val,
                    })
                    validation_failures += 1

        for cell in page.get("ocr_cells", []):
            if cell.get("confidence", 1.0) < low_conf:
                low_conf_rows += 1
                break

    return {
        "summary": {
            "total_rows": total_rows,
            "low_confidence_rows": low_conf_rows,
            "validation_failures": validation_failures,
        },
        "issues": issues,
        "confidence_threshold": conf_th,
    }
