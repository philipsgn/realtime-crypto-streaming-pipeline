"""Generate resilient Vietnamese market summaries from Gold-layer metrics."""

import json
import logging
import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from dotenv import load_dotenv
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from dags.utils.gold_queries import fetch_daily_summary, insert_market_summaries

log = logging.getLogger(__name__)

GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/"
    "models/gemini-2.5-flash:generateContent"
)


class GeminiUnavailableError(RuntimeError):
    """Represent a Gemini failure that should use the template fallback."""


class RetryableGeminiError(GeminiUnavailableError):
    """Represent a Gemini 429 or 5xx response eligible for retry."""


def _extract_text(response: dict[str, Any]) -> str:
    """Extract generated text from a Gemini generateContent response."""
    try:
        candidates = response["candidates"]
        first_candidate = candidates[0]
        parts = first_candidate["content"]["parts"]
        text = parts[0]["text"].strip()
    except (KeyError, IndexError, TypeError, AttributeError) as exc:
        raise GeminiUnavailableError("Gemini returned no usable summary") from exc

    if not text:
        raise GeminiUnavailableError("Gemini returned an empty summary")
    return text


@retry(
    retry=retry_if_exception_type(RetryableGeminiError),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=2, max=8),
    reraise=True,
)
def request_gemini_summary(prompt: str, api_key: str) -> str:
    """Request one Gemini summary, retrying only rate limits and server errors."""
    payload = json.dumps({"contents": [{"parts": [{"text": prompt}]}]}).encode("utf-8")
    request = Request(
        GEMINI_URL,
        data=payload,
        headers={"Content-Type": "application/json", "x-goog-api-key": api_key},
        method="POST",
    )

    try:
        with urlopen(request, timeout=30) as response:  # noqa: S310 - fixed Google API URL
            body = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        if exc.code == 429 or 500 <= exc.code < 600:
            raise RetryableGeminiError(f"Gemini HTTP {exc.code}") from exc
        raise GeminiUnavailableError(f"Gemini non-retryable HTTP {exc.code}") from exc
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise GeminiUnavailableError("Gemini request failed") from exc

    return _extract_text(body)


def build_prompt(metric: dict[str, Any]) -> str:
    """Build a concise Vietnamese prompt for one symbol."""
    return (
        "Hãy viết một câu tóm tắt thị trường bằng tiếng Việt, rõ ràng, không markdown, "
        "không đưa ra lời khuyên đầu tư. Dữ liệu Gold mới nhất: "
        f"symbol={metric['symbol']}, VWAP={float(metric['daily_vwap']):.2f} USD, "
        f"volume={float(metric['daily_volume']):.4f}, "
        f"price_change={float(metric['daily_price_change_pct']):+.2f}%."
    )


def build_fallback_summary(metric: dict[str, Any]) -> str:
    """Build a deterministic Vietnamese summary from raw Gold metrics."""
    return (
        f"{metric['symbol']}: VWAP ${float(metric['daily_vwap']):,.2f}, "
        f"biến động {float(metric['daily_price_change_pct']):+.2f}% trong ngày, "
        f"khối lượng {float(metric['daily_volume']):,.4f}."
    )


def summarize_symbol(metric: dict[str, Any], api_key: str) -> tuple[str, str]:
    """Generate one summary or return a transparent template fallback."""
    if not api_key:
        log.warning("Gemini unavailable, used fallback template")
        return build_fallback_summary(metric), "fallback_template"

    try:
        return request_gemini_summary(build_prompt(metric), api_key), "gemini"
    except GeminiUnavailableError as exc:
        log.warning("Gemini unavailable, used fallback template")
        log.info("Gemini fallback reason: %s", exc)
        return build_fallback_summary(metric), "fallback_template"


def generate_market_summaries() -> None:
    """Read Gold metrics, generate one summary per symbol, and persist results."""
    load_dotenv()
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    configured_symbols = {
        symbol.strip() for symbol in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")
    }
    metrics = [
        metric for metric in fetch_daily_summary() if metric["symbol"] in configured_symbols
    ]

    if not metrics:
        log.warning("No Gold-layer market data available for AI summary")
        return

    rows: list[tuple[str, str, str]] = []
    for metric in metrics:
        summary_text, source = summarize_symbol(metric, api_key)
        rows.append((str(metric["symbol"]), summary_text, source))

    insert_market_summaries(rows)
    log.info("Stored %d market summaries", len(rows))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    generate_market_summaries()
