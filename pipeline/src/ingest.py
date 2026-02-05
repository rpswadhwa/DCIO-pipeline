import os
from pathlib import Path
from typing import Dict, List

from pdf2image import convert_from_path

from .utils import ensure_dir


def pdf_to_images(pdf_path: str, out_dir: str, dpi: int = 300) -> List[str]:
    ensure_dir(out_dir)
    pages = convert_from_path(pdf_path, dpi=dpi)
    image_paths: List[str] = []
    for i, page in enumerate(pages, start=1):
        img_path = os.path.join(out_dir, f"page_{i:04d}.png")
        page.save(img_path, "PNG")
        image_paths.append(img_path)
    return image_paths


def ingest_pdfs(input_dir: str, images_root: str, dpi: int = 300) -> List[Dict[str, str]]:
    results = []
    for pdf_name in sorted(os.listdir(input_dir)):
        if not pdf_name.lower().endswith(".pdf"):
            continue
        pdf_path = os.path.join(input_dir, pdf_name)
        pdf_stem = Path(pdf_name).stem
        out_dir = os.path.join(images_root, pdf_stem)
        image_paths = pdf_to_images(pdf_path, out_dir, dpi=dpi)
        for idx, img in enumerate(image_paths, start=1):
            results.append({
                "pdf": pdf_path,
                "pdf_stem": pdf_stem,
                "page_number": idx,
                "image_path": img,
            })
    return results
