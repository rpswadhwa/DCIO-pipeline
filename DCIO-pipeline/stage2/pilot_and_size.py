"""Stage-2 measurement harness: run the section-typing correction over a list of
ACKs and report the resulting MF total per plan -- WITHOUT loading anything. Use
this to VALIDATE (compare new MF total to certified amt_mutual_funds) before
swapping any plan into the clean universe.

Usage:
    PYTHONPATH=.:stage2 python3.11 stage2/pilot_and_size.py <acks_file> <pdf_dir> <out_csv>

<acks_file>  one ACK per line
<pdf_dir>    dir containing <ACK>.pdf for each ack
<out_csv>    output: ack,status,raw_rows,retyped_non_mf,cit_caught,new_mf_rows,new_mf_ext,new_cit_ext
"""
import csv, gc, sys
from src.text_extract import classify_pages_text, extract_tables_and_map, expand_continuation_pages
from src.post_extract_validator import build_mf_rows_df, parse_currency_value
import section_typing as st

_CIT = {"common/collective trust fund", "commingled fund"}


def main(acks_file, pdf_dir, out_csv):
    acks = [l.strip() for l in open(acks_file) if l.strip()]
    with open(out_csv, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["ack", "status", "raw_rows", "retyped_non_mf", "cit_caught",
                    "new_mf_rows", "new_mf_ext", "new_cit_ext"])
        for i, ack in enumerate(acks, 1):
            pdf = "%s/%s.pdf" % (pdf_dir.rstrip("/"), ack)
            try:
                cl = classify_pages_text(pdf, "config/keywords.yml")
                supp = [p["page_number"] for p in cl if p.get("is_supplemental") == 1]
                supp = expand_continuation_pages(pdf, supp)
                _, res = extract_tables_and_map(pdf, supp, "config/schema.yml", model=None, use_llm=False)
                raw = sum(len(e.get("mapped_rows", [])) for e in res)
                stats = st.apply_section_typing_stage(res, pdf)   # the production stage
                rows = [r for e in res for r in e.get("mapped_rows", [])]
                for r in rows:
                    r["pdf_stem"] = ack
                df = build_mf_rows_df(rows)
                cit = sum(parse_currency_value(r.get("current_value")) or 0
                          for r in rows if str(r.get("asset_type", "")).strip().lower() in _CIT)
                w.writerow([ack, "OK", raw, stats.get("retyped_non_mf", 0), stats.get("cit_caught", 0),
                            len(df), int(float(df["plan_investment_amt"].sum())), int(cit)])
            except Exception as e:
                w.writerow([ack, "ERR:" + str(e)[:70], 0, 0, 0, 0, 0, 0])
            fh.flush(); gc.collect()
            if i % 25 == 0:
                sys.stderr.write("[%d/%d]\n" % (i, len(acks))); sys.stderr.flush()
    print("DONE")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
