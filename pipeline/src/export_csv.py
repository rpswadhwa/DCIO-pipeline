import csv
from typing import Dict, List

from .utils import load_yaml


def export_csv(pages: List[Dict], schema_yml: str, out_csv: str) -> None:
    cfg = load_yaml(schema_yml)
    fields = cfg["schema"]["fields"]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for page in pages:
            for row in page.get("mapped_rows", []):
                writer.writerow({f: row.get(f, "") for f in fields})
