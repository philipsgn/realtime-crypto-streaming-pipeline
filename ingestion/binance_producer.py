"""
Stage 1 - Ingestion
Binance WebSocket -> Apache Kafka producer
"""

import asyncio
import json
import os
import logging
import ssl
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
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "crypto-trades")
SYMBOLS                 = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")

STREAM_NAMES   = "/".join([f"{s.lower()}@trade" for s in SYMBOLS])
BINANCE_WS_URL = f"wss://stream.binance.com:9443/stream?streams={STREAM_NAMES}"


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=100,
        batch_size=16384,
        compression_type="gzip",
    )


def parse_trade_event(raw: dict) -> dict:
    data = raw.get("data", raw)
    return {
        "symbol":         data["s"],
        "price":          float(data["p"]),
        "quantity":       float(data["q"]),
        "trade_time":     data["T"],
        "trade_time_iso": datetime.fromtimestamp(data["T"] / 1000, tz=timezone.utc).isoformat(),
        "is_buyer_maker": data["m"],
        "trade_id":       data["t"],
    }


async def stream_to_kafka(producer: KafkaProducer) -> None:
    # SSL context dung certifi de tranh timeout/SSL error
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    log.info(f"Connecting to Binance WebSocket | symbols: {SYMBOLS}")
    log.info(f"URL: {BINANCE_WS_URL}")

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
                    raw   = json.loads(message)
                    event = parse_trade_event(raw)
                    producer.send(KAFKA_TOPIC, value=event, key=event["symbol"].encode())

                    event_count += 1
                    if event_count % 100 == 0:
                        log.info(f"Published {event_count} events | latest: {event['symbol']} @ {event['price']}")

        except (websockets.exceptions.ConnectionClosed, TimeoutError, OSError) as e:
            log.warning(f"WebSocket disconnected: {e} — reconnecting in 3s...")
            await asyncio.sleep(3)
        except Exception as e:
            log.error(f"Unexpected error: {e} — reconnecting in 5s...")
            await asyncio.sleep(5)


def main() -> None:
    producer = create_producer()
    log.info(f"Kafka producer ready. Bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")
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
