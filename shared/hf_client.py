import json
import logging
import os
import requests
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

logger = logging.getLogger("shared.hf_client")

LLAMA_33_70B = "meta-llama/Llama-3.3-70B-Instruct"
DEEPSEEK_R1_QWEN = "deepseek-ai/DeepSeek-R1-Distill-Qwen-32B"
MISTRAL_7B = "mistralai/Mistral-7B-Instruct-v0.3"


def call_hf_llm(
    prompt: str,
    system_prompt: str = "You are an expert multi-agent AI research intelligence system.",
    model: str = LLAMA_33_70B,
    temperature: float = 0.7,
    max_tokens: int = 1500,
) -> Optional[str]:
    """Universal LLM Client supporting Groq API, Gemini API, OpenAI API, and Hugging Face Serverless."""

    # 1. Groq API (Free & Ultra Fast)
    groq_key = os.getenv("GROQ_API_KEY")
    if groq_key and groq_key.strip():
        try:
            url = "https://api.groq.com/openai/v1/chat/completions"
            headers = {"Authorization": f"Bearer {groq_key.strip()}", "Content-Type": "application/json"}
            payload = {
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt},
                ],
                "max_tokens": max_tokens,
                "temperature": temperature,
            }
            res = requests.post(url, headers=headers, json=payload, timeout=20)
            if res.status_code == 200:
                content = res.json()["choices"][0]["message"]["content"]
                if content:
                    logger.info("Successfully generated via Groq API (Llama-3.3-70B)")
                    return content
        except Exception as e:
            logger.warning(f"Groq API exception: {e}")

    # 2. Google Gemini API (Free Tier)
    gemini_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if gemini_key and gemini_key.strip():
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={gemini_key.strip()}"
            headers = {"Content-Type": "application/json"}
            payload = {
                "contents": [{"parts": [{"text": f"{system_prompt}\n\nUser Prompt: {prompt}"}]}],
                "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens},
            }
            res = requests.post(url, headers=headers, json=payload, timeout=20)
            if res.status_code == 200:
                content = res.json()["candidates"][0]["content"]["parts"][0]["text"]
                if content:
                    logger.info("Successfully generated via Google Gemini 1.5 Flash")
                    return content
        except Exception as e:
            logger.warning(f"Gemini API exception: {e}")

    # 3. Hugging Face Serverless API
    hf_token = (
        os.getenv("HF_TOKEN")
        or os.getenv("HUGGINGFACE_TOKEN")
        or os.getenv("HUGGINGFACE_API_KEY")
    )
    if hf_token and hf_token.strip() and not hf_token.startswith("hf_your"):
        endpoints = [
            "https://router.huggingface.co/hf-inference/v1/chat/completions",
            f"https://api-inference.huggingface.co/models/{model}/v1/chat/completions",
        ]
        headers = {"Authorization": f"Bearer {hf_token.strip()}", "Content-Type": "application/json"}
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        for url in endpoints:
            try:
                res = requests.post(url, headers=headers, json=payload, timeout=20)
                if res.status_code == 200:
                    content = res.json()["choices"][0]["message"]["content"]
                    if content:
                        logger.info(f"Successfully generated via Hugging Face ({model})")
                        return content
            except Exception as e:
                logger.warning(f"HF API exception for {url}: {e}")

    return None
