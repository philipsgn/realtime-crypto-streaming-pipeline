"""
Stage 1 - Ingestion
Binance WebSocket -> Apache Kafka producer
"""

import asyncio
import json
import logging
import os
import ssl
from typing import Any

import certifi
from datetime import datetime, timezone
from dotenv import load_dotenv

import websockets
from kafka import KafkaProducer

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# Config
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-trades")
KAFKA_RETRIES = int(os.getenv("KAFKA_PRODUCER_RETRIES", "5"))
FLUSH_INTERVAL = int(os.getenv("KAFKA_PRODUCER_FLUSH_INTERVAL", "10000"))
SYMBOLS = tuple(
    symbol.strip().upper()
    for symbol in os.getenv(
        "SYMBOLS",
        "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,ADAUSDT,DOGEUSDT,AVAXUSDT",
    ).split(",")
    if symbol.strip()
)

STREAM_NAMES = "/".join(f"{symbol.lower()}@trade" for symbol in SYMBOLS)
BINANCE_WS_URL = f"wss://stream.binance.com:9443/stream?streams={STREAM_NAMES}"


def create_producer() -> KafkaProducer:
    """Create a reliable Kafka producer for real Binance trade events."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=KAFKA_RETRIES,
        max_in_flight_requests_per_connection=1,
        linger_ms=100,
        batch_size=16384,
        compression_type="gzip",
    )


def parse_trade_event(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize one Binance combined-stream trade event."""
    data = raw.get("data", raw)
    return {
        "symbol": data["s"],
        "price": float(data["p"]),
        "quantity": float(data["q"]),
        "trade_time": data["T"],
        "trade_time_iso": datetime.fromtimestamp(data["T"] / 1000, tz=timezone.utc).isoformat(),
        "is_buyer_maker": data["m"],
        "trade_id": data["t"],
    }


def log_delivery_error(exc: BaseException) -> None:
    """Log asynchronous Kafka delivery failures."""
    log.error("Kafka delivery failed: %s", exc)


async def stream_to_kafka(producer: KafkaProducer) -> None:
    """Continuously stream real Binance trades into Kafka."""
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    log.info("Connecting to Binance WebSocket | symbols: %s", SYMBOLS)
    log.info("URL: %s", BINANCE_WS_URL)

    while True:
        try:
            async with websockets.connect(
                BINANCE_WS_URL,
                ssl=ssl_context,
                ping_interval=20,
                ping_timeout=30,
                open_timeout=30,
            ) as ws:
                log.info("WebSocket connected. Streaming to Kafka...")
                event_count = 0

                async for message in ws:
                    try:
                        raw = json.loads(message)
                        event = parse_trade_event(raw)
                    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
                        log.warning("Discarded invalid Binance event: %s", exc)
                        continue

                    producer.send(
                        KAFKA_TOPIC,
                        value=event,
                        key=event["symbol"].encode(),
                    ).add_errback(log_delivery_error)

                    event_count += 1
                    if event_count % FLUSH_INTERVAL == 0:
                        producer.flush(timeout=10)
                    if event_count % 100 == 0:
                        log.info(
                            "Published %s events | latest: %s @ %s",
                            event_count,
                            event["symbol"],
                            event["price"],
                        )

        except (websockets.exceptions.ConnectionClosed, TimeoutError, OSError) as e:
            log.warning("WebSocket disconnected: %s — reconnecting in 3s...", e)
            await asyncio.sleep(3)
        except Exception as e:
            log.exception("Unexpected producer error: %s — reconnecting in 5s...", e)
            await asyncio.sleep(5)


def main() -> None:
    """Run the Binance-to-Kafka producer until interrupted."""
    if not SYMBOLS:
        raise ValueError("SYMBOLS must contain at least one Binance symbol")

    producer = create_producer()
    log.info("Kafka producer ready. Bootstrap: %s", KAFKA_BOOTSTRAP_SERVERS)
    try:
        asyncio.run(stream_to_kafka(producer))
    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        producer.flush()
        producer.close()
        log.info("Producer closed.")


if __name__ == "__main__":
    main()
