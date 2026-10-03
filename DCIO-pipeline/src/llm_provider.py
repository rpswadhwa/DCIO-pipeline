import json
import os

import httpx
from openai import OpenAI

GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta/models"


def call_llm_json(
    prompt: dict,
    provider: str,
    model: str,
    image_b64: str = None,
    image_mime_type: str = "image/png",
) -> str:
    """Sends `prompt` to the given provider and returns the raw JSON-text response.

    Both providers are instructed to return JSON-only, but callers should
    still guard the json.loads() on the result -- neither provider is
    contractually guaranteed to comply.

    `image_b64` (base64-encoded image bytes, no data-URI prefix) is optional --
    when set, it's attached alongside the text prompt as a vision input, so a
    rasterized page image can be sent directly to the model instead of relying
    on an extracted text layer. Used by llm_vision_extract.py for scanned
    (no-text-layer) pages. Every existing caller omits this and is unaffected.
    """
    if provider == "gemini":
        return _call_gemini(prompt, model, image_b64, image_mime_type)
    return _call_openai(prompt, model, image_b64, image_mime_type)


def _call_openai(prompt: dict, model: str, image_b64: str = None, image_mime_type: str = "image/png") -> str:
    client = OpenAI()
    user_content = json.dumps(prompt)
    if image_b64:
        user_content = [
            {"type": "text", "text": json.dumps(prompt)},
            {"type": "image_url", "image_url": {"url": f"data:{image_mime_type};base64,{image_b64}"}},
        ]
    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": "You are a data mapping assistant. Return valid JSON only, no extra text.",
            },
            {
                "role": "user",
                "content": user_content,
            },
        ],
    )
    return response.choices[0].message.content


def _call_gemini(prompt: dict, model: str, image_b64: str = None, image_mime_type: str = "image/png") -> str:
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is not set")

    url = f"{GEMINI_API_BASE}/{model}:generateContent"
    parts = [{"text": json.dumps(prompt)}]
    if image_b64:
        parts.append({"inline_data": {"mime_type": image_mime_type, "data": image_b64}})
    body = {
        "systemInstruction": {
            "parts": [
                {"text": "You are a data mapping assistant. Return valid JSON only, no extra text."}
            ]
        },
        "contents": [
            {"role": "user", "parts": parts}
        ],
        "generationConfig": {"responseMimeType": "application/json"},
    }
    resp = httpx.post(url, params={"key": api_key}, json=body, timeout=90.0)
    resp.raise_for_status()
    data = resp.json()
    return data["candidates"][0]["content"]["parts"][0]["text"]
