"""Unit tests for ai/gemini_summary.py -- no real API or database calls."""

from unittest.mock import MagicMock, patch
from urllib.error import HTTPError

import pytest

from ai.gemini_summary import (
    GeminiUnavailableError,
    RetryableGeminiError,
    _extract_text,
    build_fallback_summary,
    generate_market_summaries,
    request_gemini_summary,
    summarize_symbol,
)

SAMPLE_METRIC = {
    "symbol": "BTCUSDT",
    "daily_vwap": 62000.0,
    "daily_volume": 1234.5,
    "daily_price_change_pct": 1.23,
    "daily_high": 63000.0,
    "daily_low": 61000.0,
}

ALL_SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "AVAXUSDT",
)


def test_extract_text_valid() -> None:
    """Extract text from a valid Gemini response."""
    response = {"candidates": [{"content": {"parts": [{"text": "summary"}]}}]}

    assert _extract_text(response) == "summary"


def test_extract_text_malformed() -> None:
    """Normalize malformed Gemini payloads to the module fallback exception."""
    with pytest.raises(GeminiUnavailableError, match="no usable summary"):
        _extract_text({"bad": "response"})


@pytest.mark.parametrize("status_code", [429, 503])
def test_request_gemini_retryable_http_error(status_code: int) -> None:
    """Classify rate-limit and server responses as retryable."""
    error = HTTPError(url="", code=status_code, msg="", hdrs=None, fp=None)

    with patch("ai.gemini_summary.urlopen", side_effect=error):
        with pytest.raises(RetryableGeminiError):
            request_gemini_summary.__wrapped__("prompt", "fake-key")


def test_request_gemini_400_not_retried() -> None:
    """Classify client errors as unavailable without retry."""
    error = HTTPError(url="", code=400, msg="", hdrs=None, fp=None)

    with patch("ai.gemini_summary.urlopen", side_effect=error):
        with pytest.raises(GeminiUnavailableError) as exc_info:
            request_gemini_summary.__wrapped__("prompt", "fake-key")

    assert not isinstance(exc_info.value, RetryableGeminiError)


def test_request_gemini_timeout() -> None:
    """Convert network timeouts to the fallback exception."""
    with patch("ai.gemini_summary.urlopen", side_effect=TimeoutError):
        with pytest.raises(GeminiUnavailableError, match="request failed"):
            request_gemini_summary.__wrapped__("prompt", "fake-key")


def test_fallback_summary_is_non_empty() -> None:
    """Always produce a deterministic non-empty fallback string."""
    text = build_fallback_summary(SAMPLE_METRIC)

    assert isinstance(text, str)
    assert text
    assert "BTCUSDT" in text


def test_missing_api_key_uses_fallback_without_http() -> None:
    """Avoid HTTP entirely when no Gemini API key is configured."""
    with patch("ai.gemini_summary.request_gemini_summary") as mock_request:
        text, source = summarize_symbol(SAMPLE_METRIC, api_key="")

    mock_request.assert_not_called()
    assert source == "fallback_template"
    assert isinstance(text, str)
    assert text


def test_gemini_unavailable_uses_fallback_source_label() -> None:
    """Persist the transparent fallback label when Gemini is unavailable."""
    with patch(
        "ai.gemini_summary.request_gemini_summary",
        side_effect=GeminiUnavailableError("quota"),
    ):
        text, source = summarize_symbol(SAMPLE_METRIC, api_key="fake-key")

    assert source == "fallback_template"
    assert isinstance(text, str)
    assert text


def test_one_summary_per_symbol(monkeypatch: pytest.MonkeyPatch) -> None:
    """Generate and persist at most one summary for each symbol per run."""
    monkeypatch.setenv("GEMINI_API_KEY", "fake-key")
    monkeypatch.setenv("SYMBOLS", "BTCUSDT")
    duplicate_metrics = [dict(SAMPLE_METRIC) for _ in range(3)]

    with (
        patch("ai.gemini_summary.load_dotenv"),
        patch("ai.gemini_summary.fetch_daily_summary", return_value=duplicate_metrics),
        patch(
            "ai.gemini_summary.summarize_symbol",
            return_value=("summary", "gemini"),
        ) as mock_summarize,
        patch("ai.gemini_summary.insert_market_summaries") as mock_insert,
    ):
        generate_market_summaries()

    mock_summarize.assert_called_once_with(duplicate_metrics[0], "fake-key")
    mock_insert.assert_called_once_with([("BTCUSDT", "summary", "gemini")])


def test_default_configuration_includes_all_eight_symbols(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Generate summaries for all eight project symbols by default."""
    monkeypatch.setenv("GEMINI_API_KEY", "fake-key")
    monkeypatch.delenv("SYMBOLS", raising=False)
    metrics = [{**SAMPLE_METRIC, "symbol": symbol} for symbol in ALL_SYMBOLS]
    mock_summarize = MagicMock(return_value=("summary", "gemini"))

    with (
        patch("ai.gemini_summary.load_dotenv"),
        patch("ai.gemini_summary.fetch_daily_summary", return_value=metrics),
        patch("ai.gemini_summary.summarize_symbol", mock_summarize),
        patch("ai.gemini_summary.insert_market_summaries") as mock_insert,
    ):
        generate_market_summaries()

    assert mock_summarize.call_count == 8
    assert len(mock_insert.call_args.args[0]) == 8
