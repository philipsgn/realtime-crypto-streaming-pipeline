# Stage 1 — Ingestion

> **Mục tiêu cuối tuần 1:** Binance WebSocket kết nối thành công, events BTC/ETH/SOL chảy liên tục vào Kafka topic `crypto-trades`. Dùng `kafka-console-consumer` verify thấy JSON.

---

## Bối cảnh

Stage này giải quyết câu hỏi: *"Làm sao lấy dữ liệu real-time từ thế giới bên ngoài vào hệ thống của mình?"*

Binance cung cấp WebSocket API public — không cần API key, không cần đăng ký, hoàn toàn miễn phí cho public market data. Mỗi khi có một giao dịch BTC/USDT xảy ra trên sàn Binance, hệ thống của họ push ngay một JSON event tới tất cả client đang kết nối.

Nhiệm vụ của Stage 1 là nhận event đó và đẩy vào Kafka để Stage 2 (Spark) có thể xử lý.

---

## Luồng dữ liệu Stage 1

```
Binance Server
  wss://stream.binance.com:9443
        │  push JSON mỗi khi có trade
        ▼
[binance_producer.py]  ← code của bạn
  asyncio WebSocket client
  parse raw event → clean schema
        │  KafkaProducer.send()
        ▼
[Kafka Topic: crypto-trades]
  3 partitions (BTCUSDT | ETHUSDT | SOLUSDT)
  message key = symbol
```

---

## Schema dữ liệu

### Raw event từ Binance

```json
{
  "stream": "btcusdt@trade",
  "data": {
    "e": "trade",
    "E": 1718000000000,
    "s": "BTCUSDT",
    "t": 3521847293,
    "p": "67432.50",
    "q": "0.00234",
    "T": 1718000000000,
    "m": false
  }
}
```

### Schema sau khi parse (gửi vào Kafka)

```json
{
  "symbol":         "BTCUSDT",
  "price":          67432.50,
  "quantity":       0.00234,
  "trade_time":     1718000000000,
  "trade_time_iso": "2024-06-10T07:33:20",
  "is_buyer_maker": false,
  "trade_id":       3521847293
}
```

**Giải thích field:**
- `is_buyer_maker = false` → người mua chủ động đặt lệnh (buy pressure)
- `is_buyer_maker = true` → người bán chủ động đặt lệnh (sell pressure)
- `trade_time` → milliseconds epoch, dùng làm timestamp chính

---

## File liên quan

| File | Vai trò |
|---|---|
| `ingestion/binance_producer.py` | Code chính của Stage 1 |
| `infrastructure/docker-compose.yml` | Khởi động Kafka KRaft |
| `.env` | Config symbols, kafka bootstrap |

---

## Cách chạy Stage 1

### Bước 1 — Khởi động Kafka

```bash
# Start Kafka container (KRaft mode — không cần Zookeeper)
make up

# Verify Kafka đang chạy
docker ps | grep kafka

# Kiểm tra topic tự tạo (sẽ tạo khi producer gửi message đầu tiên)
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

### Bước 2 — Chạy producer

```bash
# Copy env file
cp .env.example .env

# Cài dependencies
pip install -r requirements.txt

# Chạy producer
make producer
# hoặc: python ingestion/binance_producer.py
```

**Output mong đợi:**
```
2024-06-10 07:33:20 [INFO] Kafka producer ready.
2024-06-10 07:33:21 [INFO] Connecting to Binance WebSocket | symbols: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']
2024-06-10 07:33:21 [INFO] WebSocket connected. Streaming to Kafka...
2024-06-10 07:33:22 [INFO] Published 100 events | latest: BTCUSDT @ 67432.5
2024-06-10 07:33:24 [INFO] Published 200 events | latest: ETHUSDT @ 3521.2
```

### Bước 3 — Verify events trong Kafka

```bash
# Mở consumer để xem messages thật
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic crypto-trades \
  --from-beginning \
  --max-messages 5
```

---

## Memory footprint Stage 1

| Service | RAM dùng |
|---|---|
| Kafka (KRaft, Xmx256m) | ~350 MB |
| Python producer | ~50 MB |
| **Tổng Stage 1** | **~400 MB** |

---

## Các lỗi thường gặp

| Lỗi | Nguyên nhân | Cách fix |
|---|---|---|
| `NoBrokersAvailable` | Kafka chưa sẵn sàng | Chờ 15–20s sau `docker compose up` |
| `Connection refused 9092` | Docker network issue | Kiểm tra `KAFKA_ADVERTISED_LISTENERS` trong docker-compose |
| `websockets.exceptions.ConnectionClosedError` | Binance timeout | Producer tự reconnect sau 3s — bình thường |
| `KeyError: 'data'` | Binance gửi ping frame | Đã handle trong `parse_trade_event()` bằng `.get("data", raw)` |

---

## Definition of Done — Stage 1 hoàn thành khi

- [ ] `docker ps` thấy kafka container `healthy`
- [ ] Producer chạy không crash trong 5 phút liên tục
- [ ] `kafka-console-consumer` thấy JSON events chảy vào
- [ ] Log hiện `Published 1000 events` mà không có ERROR
- [ ] `kafka-topics.sh --describe` thấy topic `crypto-trades` có 3 partitions

---

## Skills học được ở Stage này

- Apache Kafka: topic, partition, producer API, KRaft mode
- Python `asyncio` + `websockets`: async I/O cho streaming
- JSON schema design: raw → clean transformation
- Docker: healthcheck, memory limits, container networking
