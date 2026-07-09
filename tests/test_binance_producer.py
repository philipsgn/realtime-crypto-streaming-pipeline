"""Unit tests for the real Binance trade event parser."""

import unittest

from ingestion.binance_producer import parse_trade_event


class ParseTradeEventTests(unittest.TestCase):
    """Validate normalization of Binance combined-stream events."""

    def test_parse_combined_stream_trade(self) -> None:
        """Parse the nested payload returned by Binance combined streams."""
        raw = {
            "stream": "btcusdt@trade",
            "data": {
                "s": "BTCUSDT",
                "p": "62000.50",
                "q": "0.125",
                "T": 1_720_000_000_000,
                "m": False,
                "t": 123456,
            },
        }

        event = parse_trade_event(raw)

        self.assertEqual(event["symbol"], "BTCUSDT")
        self.assertEqual(event["price"], 62000.5)
        self.assertEqual(event["quantity"], 0.125)
        self.assertEqual(event["trade_time"], 1_720_000_000_000)
        self.assertEqual(event["trade_id"], 123456)
        self.assertFalse(event["is_buyer_maker"])
        self.assertTrue(str(event["trade_time_iso"]).endswith("+00:00"))

    def test_parse_rejects_missing_required_field(self) -> None:
        """Reject incomplete events instead of publishing invalid records."""
        with self.assertRaises(KeyError):
            parse_trade_event({"data": {"s": "BTCUSDT"}})


if __name__ == "__main__":
    unittest.main()
