from typing import Dict, List

import cv2
import numpy as np


def deskew_image(img):
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    gray = cv2.bitwise_not(gray)
    coords = np.column_stack(np.where(gray > 0))
    if len(coords) == 0:
        return img
    angle = cv2.minAreaRect(coords)[-1]
    if angle < -45:
        angle = -(90 + angle)
    else:
        angle = -angle
    (h, w) = img.shape[:2]
    center = (w // 2, h // 2)
    M = cv2.getRotationMatrix2D(center, angle, 1.0)
    return cv2.warpAffine(img, M, (w, h), flags=cv2.INTER_CUBIC, borderMode=cv2.BORDER_REPLICATE)


def binarize(img):
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    return cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 35, 11)


def normalize_contrast(img):
    lab = cv2.cvtColor(img, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    l2 = clahe.apply(l)
    lab = cv2.merge((l2, a, b))
    return cv2.cvtColor(lab, cv2.COLOR_LAB2BGR)


def normalize_pages(pages: List[Dict[str, str]]) -> List[Dict[str, str]]:
    out = []
    for page in pages:
        img = cv2.imread(page["image_path"])
        if img is None:
            page["normalized_path"] = page["image_path"]
            out.append(page)
            continue
        img = normalize_contrast(img)
        img = deskew_image(img)
        bin_img = binarize(img)
        norm_path = page["image_path"].replace(".png", "_norm.png")
        cv2.imwrite(norm_path, bin_img)
        page["normalized_path"] = norm_path
        out.append(page)
    return out
