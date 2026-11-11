"""Per-plan notes: admin-written instructions applied to every filing of a plan (2026-11-10).

A note is written on the Undercapture Desk against a plan_id (EIN-PN), confirmed there as a
small set of rules, and exported to PLAN_NOTES_URI. Every run -- manual page request or mass
chunk run -- loads the confirmed notes and, for a filing of a noted plan:

  * finds the schedule pages from start_heading/end_heading when no manual pages were given,
  * always extracts through the LLM row extractor, with the note's advice added to its prompt,
  * applies the note's rules to the extracted rows in plain code (they beat the LLM's guess),
  * writes a per-filing report of every row read, what the note did to it, and why, to
    PLAN_NOTES_REPORT_URI/<ack_id>.json for the Desk's "What got loaded" panel.

Rules are exact; advice is best effort. Notes are scoped to their own plan only -- nothing here
becomes a rule for any other plan.

File format (JSON), keyed by plan_id, confirmed notes only:
    {"<plan_id>": {"plan_name": "...", "version": 3, "note_text": "...",
                    "rules": {"start_heading": "...", "end_heading": "...",
                              "type_by_heading": [{"match": "Investment contracts", "type": "Stable Value Fund"}],
                              "type_by_name": [{"match": "Guaranteed Income", "type": "Stable Value Fund"}],
                              "skip_lines": ["Total * held"],
                              "scale": 1000,
                              "advice": "free text for the extractor"}}}

A match is a case-insensitive literal with `*` as a wildcard, or a regex when written /like this/.
"""
import json
import os
import re
from typing import Dict, List, Optional, Tuple

from .llm_row_extract import _KNOWN_ASSET_TYPES

NOTE_SOURCE = "note"

DEFAULT_NOTES_URI = "s3://retirementinsights-reference/dcio_plan_notes/plan_notes.json"
DEFAULT_REPORT_URI = "s3://retirementinsights-reference/dcio_plan_notes/reports/"


def _compile(pattern: str) -> Optional[re.Pattern]:
    pattern = str(pattern or "").strip()
    if not pattern:
        return None
    if len(pattern) > 2 and pattern.startswith("/") and pattern.endswith("/"):
        return re.compile(pattern[1:-1], re.I)
    # Literal text: any run of whitespace matches any whitespace, `*` matches anything.
    parts = [r"\s+".join(re.escape(w) for w in part.split()) for part in pattern.split("*")]
    return re.compile(".*?".join(parts), re.I | re.S)


def _matcher(pattern: str):
    rx = _compile(pattern)
    return (lambda text: bool(rx.search(text or ""))) if rx else (lambda text: False)


class PlanNote:
    """One confirmed note, with its rules compiled."""

    def __init__(self, plan_id: str, data: Dict):
        self.plan_id = plan_id
        self.plan_name = data.get("plan_name", "")
        self.version = int(data.get("version") or 1)
        rules = data.get("rules") or {}
        self.start_heading = str(rules.get("start_heading") or "").strip()
        self.end_heading = str(rules.get("end_heading") or "").strip()
        self.advice = str(rules.get("advice") or "").strip()
        scale = rules.get("scale")
        self.scale = int(scale) if scale in (1000, "1000") else None
        self.type_by_heading = self._typed(rules.get("type_by_heading"), "type_by_heading")
        self.type_by_name = self._typed(rules.get("type_by_name"), "type_by_name")
        self.skip_lines = [(f"skip_lines[{i}]", p, _matcher(p))
                           for i, p in enumerate(rules.get("skip_lines") or []) if str(p).strip()]

    @staticmethod
    def _typed(items, kind) -> List[Tuple[str, str, object, str]]:
        out = []
        for i, item in enumerate(items or []):
            t = str((item or {}).get("type") or "").strip()
            m = str((item or {}).get("match") or "").strip()
            if not m or t not in _KNOWN_ASSET_TYPES:
                print(f"    [plan_notes] ignoring {kind}[{i}] ({m!r} -> {t!r}): unknown type or empty match")
                continue
            out.append((f"{kind}[{i}]", m, _matcher(m), t))
        return out

    @property
    def tag(self) -> str:
        return f"{self.plan_id}@v{self.version}"

    def prompt_addendum(self) -> str:
        """Plain-English instructions for this plan only, appended to the extractor's prompt."""
        lines = []
        if self.advice:
            lines.append(self.advice)
        for _, m, _, t in self.type_by_heading:
            lines.append(f"Rows under a heading like '{m}' are {t}.")
        for _, m, _, t in self.type_by_name:
            lines.append(f"Rows named like '{m}' are {t}.")
        for _, p, _ in self.skip_lines:
            lines.append(f"Lines like '{p}' are not holdings; leave them out.")
        if self.scale:
            lines.append("Amounts on these pages are in thousands of dollars.")
        if not lines:
            return ""
        return ("PLAN-SPECIFIC INSTRUCTIONS from a reviewer who has read this plan's filing. "
                "Follow them where they apply; they override the general rules above: "
                + " ".join(lines))


def load_notes(uri: Optional[str] = None) -> Dict[str, PlanNote]:
    """plan_id -> PlanNote. Never raises: a missing/unreadable file means no notes this run."""
    uri = uri or os.getenv("PLAN_NOTES_FILE") or os.getenv("PLAN_NOTES_URI", DEFAULT_NOTES_URI)
    if not uri or uri == "0":
        return {}
    try:
        if uri.startswith("s3://"):
            import boto3
            bucket, key = uri[5:].split("/", 1)
            body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
            data = json.loads(body)
        else:
            if not os.path.exists(uri):
                return {}
            with open(uri, "r", encoding="utf-8") as fh:
                data = json.load(fh)
    except Exception as exc:  # noqa: BLE001 - notes are additive; a run without them is the old run
        print(f"  [plan_notes] notes not loaded from {uri}: {exc}")
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(pid).strip(): PlanNote(str(pid).strip(), d) for pid, d in data.items() if isinstance(d, dict)}


def notes_for_acks(notes: Dict[str, PlanNote], ack_ids: List[str], workgroup: str, s3_output: str) -> Dict[str, PlanNote]:
    """ack_id -> PlanNote for the filings in this run whose plan has a note.

    plan_master only holds each plan's latest filing, so older filings are looked up in
    plan_master_index_universe too."""
    if not notes or not ack_ids:
        return {}
    import awswrangler as wr

    acks = ", ".join(f"'{a}'" for a in sorted(set(ack_ids)) if re.fullmatch(r"[0-9A-Z]+", a))
    pids = ", ".join(f"'{p}'" for p in notes if re.fullmatch(r"[0-9-]+", p))
    if not acks or not pids:
        return {}
    sql = (f"SELECT ack_id, plan_id FROM plan_master WHERE ack_id IN ({acks}) AND plan_id IN ({pids}) "
           f"UNION SELECT ack_id, plan_id FROM plan_master_index_universe "
           f"WHERE ack_id IN ({acks}) AND plan_id IN ({pids})")
    df = wr.athena.read_sql_query(sql=sql, database="default", workgroup=workgroup,
                                  s3_output=s3_output, ctas_approach=False)
    return {a: notes[p] for a, p in zip(df["ack_id"], df["plan_id"]) if p in notes}


def find_note_pages(pdf_path: str, note: PlanNote) -> List[int]:
    """Pages from the first page carrying start_heading through the first later page carrying
    end_heading (inclusive). Without an end_heading, just the start page -- the caller widens it
    with the usual continuation-page check. [] when the heading isn't found."""
    if not note.start_heading:
        return []
    import pdfplumber

    start_rx, end_rx = _compile(note.start_heading), _compile(note.end_heading)
    pages: List[int] = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            start_at = 0
            if not pages:
                m = start_rx.search(text)
                if not m:
                    continue
                start_at = m.end()
            pages.append(i)
            if not end_rx or end_rx.search(text, start_at):
                break
    return pages


def _row_text(row: Dict) -> str:
    return " ".join(str(row.get(f) or "") for f in ("issuer_name", "investment_description")).strip()


def _scale_value(value: str, factor: int) -> str:
    try:
        num = float(str(value).replace(",", "").replace("$", ""))
    except (TypeError, ValueError):
        return value
    return f"{num * factor:.2f}"


def apply_rules(page_data: List[Dict], note: PlanNote) -> List[Dict]:
    """Apply the note's rules to the extracted rows in place; returns one report entry per row.

    Order: skip_lines, then type_by_name, then type_by_heading (a row's own name is more
    specific than its heading), then scale. A typed row's asset_type_source becomes 'note' so
    the report and later steps can tell a note's decision from the extractor's guess."""
    report = []
    for page in page_data:
        headings = page.get("row_headings") or {}
        page_scale = page.get("scale_multiplier") or 1
        kept = []
        for row in page.get("mapped_rows", []):
            heading = headings.get(row.get("row_id"), "")
            text = _row_text(row)
            entry = {
                "page": row.get("page_number", page.get("page_number")), "row_id": row.get("row_id"),
                "name": text, "issuer": row.get("issuer_name", ""),
                "description": row.get("investment_description", ""),
                "amount_printed": row.get("current_value", ""), "heading": heading,
                "extractor_type": row.get("asset_type", ""),
                "extractor_source": row.get("asset_type_source", ""),
                "rule": "", "action": "kept",
            }
            skip = next((rid for rid, _, match in note.skip_lines if match(text)), None)
            if skip:
                entry.update(rule=skip, action="skipped")
                report.append(entry)
                continue
            hit = next(((rid, t) for rid, _, match, t in note.type_by_name if match(text)), None) \
                or next(((rid, t) for rid, _, match, t in note.type_by_heading if heading and match(heading)), None)
            if hit:
                row["asset_type"], row["asset_type_source"] = hit[1], NOTE_SOURCE
                entry.update(rule=hit[0], action="typed")
            if note.scale and page_scale == 1:
                row["current_value"] = _scale_value(row.get("current_value", ""), note.scale)
                for f in ("cost", "par_value"):
                    if row.get(f):
                        row[f] = _scale_value(row[f], note.scale)
                entry["rule"] = (entry["rule"] + ",scale").lstrip(",")
            entry["type"] = row.get("asset_type", "")
            entry["type_source"] = row.get("asset_type_source", "")
            entry["amount"] = row.get("current_value", "")
            kept.append(row)
            report.append(entry)
        page["mapped_rows"] = kept
    return report


def write_report(ack_id: str, note: PlanNote, pages: List[int], page_source: str,
                 report: List[Dict], output_dir: str) -> None:
    """Per-filing report, locally under output_dir/plan_notes/ and to PLAN_NOTES_REPORT_URI."""
    from datetime import datetime, timezone

    body = {
        "ack_id": ack_id, "plan_id": note.plan_id, "plan_name": note.plan_name,
        "note_version": note.version, "pages": pages, "page_source": page_source,
        "advice_sent": bool(note.prompt_addendum()), "rows": report,
        "written_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    local_dir = os.path.join(output_dir, "plan_notes")
    os.makedirs(local_dir, exist_ok=True)
    with open(os.path.join(local_dir, f"{ack_id}.json"), "w", encoding="utf-8") as fh:
        json.dump(body, fh, indent=1)
    uri = os.getenv("PLAN_NOTES_REPORT_URI", DEFAULT_REPORT_URI)
    if not uri.startswith("s3://"):
        return
    try:
        import boto3
        bucket, prefix = uri[5:].split("/", 1)
        boto3.client("s3").put_object(Bucket=bucket, Key=f"{prefix.rstrip('/')}/{ack_id}.json",
                                      Body=json.dumps(body).encode("utf-8"), ContentType="application/json")
    except Exception as exc:  # noqa: BLE001 - the local copy stays; the run must not fail on this
        print(f"    [plan_notes] report upload failed for {ack_id}: {exc}")


def note_types_from_reports(report_dir: str, stems) -> Dict[str, Dict[Tuple[str, str], str]]:
    """ack_id -> {(page, row_id): type} for rows a note typed, read from this run's local reports.

    page_number and row_id are carried unchanged from extraction to investments_clean.csv,
    so they identify the same row after the cleanup steps rename or re-format it."""
    out = {}
    for stem in set(stems):
        path = os.path.join(report_dir, f"{stem}.json")
        if not os.path.exists(path):
            continue
        with open(path, "r", encoding="utf-8") as fh:
            body = json.load(fh)
        typed = {(str(e.get("page")), str(e.get("row_id"))): e.get("type")
                 for e in body.get("rows", []) if e.get("action") == "typed" and e.get("type")}
        if typed:
            out[stem] = typed
    return out


def reassert_note_types(rows: List[Dict], note_types: Dict[str, Dict[Tuple[str, str], str]]) -> int:
    """Put each note-typed row back to the note's type and mark it `_note_type` so later
    re-typing steps leave it alone. Returns rows whose type had to be put back."""
    changed = 0
    for row in rows:
        stem = str(row.get("pdf_stem", "") or "").strip()
        t = note_types.get(stem, {}).get((str(row.get("page_number")), str(row.get("row_id"))))
        if not t:
            continue
        if row.get("asset_type") != t:
            row["asset_type"] = t
            changed += 1
        row["_note_type"] = t
    return changed
