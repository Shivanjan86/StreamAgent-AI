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

# Hugging Face Models
LLAMA_33_70B = "meta-llama/Llama-3.3-70B-Instruct"
DEEPSEEK_R1_QWEN = "deepseek-ai/DeepSeek-R1-Distill-Qwen-32B"
MISTRAL_7B = "mistralai/Mistral-7B-Instruct-v0.3"
QWEN_72B = "Qwen/Qwen2.5-72B-Instruct"


def call_hf_llm(
    prompt: str,
    system_prompt: str = "You are an expert multi-agent AI research intelligence system.",
    model: str = LLAMA_33_70B,
    temperature: float = 0.7,
    max_tokens: int = 1500,
) -> Optional[str]:
    """Call Hugging Face Serverless Chat Completions API with automatic fallback."""
    token = (
        os.getenv("HF_TOKEN")
        or os.getenv("HUGGINGFACE_TOKEN")
        or os.getenv("HUGGINGFACE_API_KEY")
        or os.getenv("HUGGINGFACEHUB_API_TOKEN")
    )

    if not token or not token.strip() or token.startswith("hf_your"):
        logger.warning("HF_TOKEN missing or default in .env file.")
        return None

    # Hugging Face Serverless Inference API endpoint
    url = f"https://api-inference.huggingface.co/models/{model}/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {token.strip()}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": max_tokens,
        "temperature": temperature,
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        if response.status_code == 200:
            res_data = response.json()
            choices = res_data.get("choices", [])
            if choices:
                content = choices[0].get("message", {}).get("content", "")
                if content:
                    logger.info(f"Successfully generated response using HF model '{model}'")
                    return content
        else:
            logger.warning(f"HF API returned status {response.status_code} for {model}: {response.text[:200]}")
    except Exception as e:
        logger.warning(f"HF API call exception for {model}: {e}")

    return None
