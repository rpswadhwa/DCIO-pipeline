import json
import os

import httpx
from openai import OpenAI

GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta/models"


def call_llm_json(prompt: dict, provider: str, model: str) -> str:
    """Sends `prompt` to the given provider and returns the raw JSON-text response.

    Both providers are instructed to return JSON-only, but callers should
    still guard the json.loads() on the result -- neither provider is
    contractually guaranteed to comply.
    """
    if provider == "gemini":
        return _call_gemini(prompt, model)
    return _call_openai(prompt, model)


def _call_openai(prompt: dict, model: str) -> str:
    client = OpenAI()
    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": "You are a data mapping assistant. Return valid JSON only, no extra text.",
            },
            {
                "role": "user",
                "content": json.dumps(prompt),
            },
        ],
    )
    return response.choices[0].message.content


def _call_gemini(prompt: dict, model: str) -> str:
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is not set")

    url = f"{GEMINI_API_BASE}/{model}:generateContent"
    body = {
        "systemInstruction": {
            "parts": [
                {"text": "You are a data mapping assistant. Return valid JSON only, no extra text."}
            ]
        },
        "contents": [
            {"role": "user", "parts": [{"text": json.dumps(prompt)}]}
        ],
        "generationConfig": {"responseMimeType": "application/json"},
    }
    resp = httpx.post(url, params={"key": api_key}, json=body, timeout=60.0)
    resp.raise_for_status()
    data = resp.json()
    return data["candidates"][0]["content"]["parts"][0]["text"]
