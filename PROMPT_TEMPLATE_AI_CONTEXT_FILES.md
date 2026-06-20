# PROMPT TEMPLATE — Tạo AI Context Files cho Project

> Lưu file này lại. Mỗi khi có project mới, copy prompt bên dưới,
> điền thông tin vào các ô [IN HOA], gửi cho Claude là xong.

---

## PROMPT ĐỂ GỬI CHO CLAUDE

---

Tôi muốn bạn tạo cho tôi bộ AI Context Files hoàn chỉnh cho project của tôi,
bao gồm đúng các file sau (giống như bạn đã làm cho Realtime Crypto Streaming Pipeline v2):

```
AGENTS.md
GEMINI.md
.agents/skills/[TÊN-SKILL]/
    SKILL.md
    references/
        PROJECT_CONTEXT.md
        STAGE_1_[TÊN].md
        STAGE_2_[TÊN].md
        ... (bao nhiêu stage thì tạo bấy nhiêu)
docs/
    PROJECT_CONTEXT.md
    stages/
        STAGE_1_[TÊN].md
        STAGE_2_[TÊN].md
        ...
```

Sau khi tạo xong, đóng gói tất cả thành file .zip và gửi cho tôi.

---

### THÔNG TIN PROJECT

**Tên project (repo name):** [vd: realtime-crypto-streaming-pipeline]

**Mô tả một câu:** [vd: End-to-end streaming pipeline từ Binance WebSocket đến Grafana dashboard]

**Mục đích:** [vd: Portfolio project để apply Data Engineer Intern tại HCM]

---

### THÔNG TIN NGƯỜI DÙNG

**Vị trí đang apply:** [vd: Data Engineer Intern / Junior Data Engineer / Analytics Engineer]

**Kinh nghiệm hiện tại:** [vd: Sinh viên năm cuối, chưa có kinh nghiệm DE]

**Địa điểm:** [vd: TP. Hồ Chí Minh, Việt Nam]

---

### MACHINE CONSTRAINTS

**CPU:** [vd: Intel i3-1115G4 @ 3.00GHz, 2 cores]

**RAM:** [vd: 7.7GB total, ~2GB free khi chạy Docker]

**OS:** [vd: Windows với Docker Desktop]

**Disk:** [vd: SSD NVMe]

---

### TECH STACK CHÍNH THỨC

> Liệt kê từng layer và tool được chọn. Ghi rõ lý do nếu có.

| Layer | Tool | Lý do chọn |
|---|---|---|
| [vd: Ingestion] | [vd: Python websockets] | [vd: Nhẹ, không cần framework] |
| [vd: Queue] | [vd: Apache Kafka KRaft] | [vd: JD yêu cầu Kafka] |
| [vd: Processing] | [vd: PySpark Structured Streaming] | [vd: JD yêu cầu Spark] |
| [vd: Storage] | [vd: TimescaleDB + Parquet] | [vd: Time-series optimized] |
| [vd: Dashboard] | [vd: Grafana] | [vd: Production standard] |
| [vd: Infra] | [vd: Docker Compose] | [vd: Local dev] |
| [vd: CI/CD] | [vd: GitHub Actions] | [vd: Free, phổ biến] |

---

### STACK KHÔNG ĐƯỢC DÙNG (Hard Rules)

> Liệt kê những tool BỊ CẤM — AI sẽ không bao giờ gợi ý những thứ này.

- ❌ [vd: Zookeeper — đã thay bằng KRaft]
- ❌ [vd: Superset — đã thay bằng Grafana]
- ❌ [vd: Mock/simulated data — chỉ dùng real data]
- ❌ [vd: Flink — user muốn Spark]
- ❌ [thêm vào nếu có]

---

### DATA SOURCE

**Loại dữ liệu:** [vd: Real-time / Batch / Hybrid]

**Nguồn dữ liệu:** [vd: Binance WebSocket API — public, không cần API key]

**URL / Endpoint:** [vd: wss://stream.binance.com:9443/stream]

**Không dùng:** [vd: Mock data, CSV file, simulated events]

---

### CÁC STAGE CỦA PROJECT

> Mỗi stage = 1 tuần. Mô tả ngắn gọn mục tiêu và output.

**Stage 1 — [TÊN STAGE]**
- Mục tiêu: [vd: Kết nối Binance WebSocket, push events vào Kafka]
- Input: [vd: Binance WebSocket stream]
- Output: [vd: Kafka topic crypto-trades có messages]
- File chính: [vd: ingestion/binance_producer.py]
- Done khi: [vd: kafka-console-consumer thấy JSON chảy vào]

**Stage 2 — [TÊN STAGE]**
- Mục tiêu: [vd: Spark đọc Kafka, tính VWAP theo window 1 phút]
- Input: [vd: Kafka topic crypto-trades]
- Output: [vd: Aggregated metrics in console]
- File chính: [vd: processing/spark_streaming.py]
- Done khi: [vd: Console output hiện VWAP mỗi 30 giây]

**Stage 3 — [TÊN STAGE]**
- Mục tiêu: [điền vào]
- Input: [điền vào]
- Output: [điền vào]
- File chính: [điền vào]
- Done khi: [điền vào]

**Stage 4 — [TÊN STAGE]**
- Mục tiêu: [điền vào]
- Input: [điền vào]
- Output: [điền vào]
- File chính: [điền vào]
- Done khi: [điền vào]

---

### METRICS / KPIs PROJECT TÍNH TOÁN (nếu có)

| Metric | Công thức | Ý nghĩa |
|---|---|---|
| [vd: VWAP] | [vd: Σ(price×qty)/Σ(qty)] | [vd: Giá trung bình có tính khối lượng] |
| [vd: trade_count] | [vd: COUNT(*) per window] | [vd: Số lượng giao dịch trong 1 phút] |
| [thêm vào] | | |

---

### YÊU CẦU THÊM CHO AI AGENT (tuỳ chọn)

**Ngôn ngữ giải thích:** [vd: Tiếng Việt cho giải thích, Tiếng Anh cho code]

**Style giải thích:** [vd: Giải thích như dạy sinh viên, dùng ví dụ thực tế]

**Turbo mode:** [vd: OFF — cần review từng bước trước khi execute]

**Ưu tiên khi gợi ý giải pháp:** [vd: Luôn ưu tiên giải pháp nhẹ RAM nhất]

---

## GHI CHÚ SỬ DỤNG TEMPLATE

### Những trường BẮT BUỘC phải điền:
- Tên project
- Tech stack (ít nhất 4 layers)
- Stack không được dùng
- Mô tả ít nhất 2 stages

### Những trường có thể bỏ qua:
- Metrics (nếu project không tính metrics cụ thể)
- Yêu cầu thêm cho AI Agent

### Tips để ra kết quả tốt nhất:
1. **Stack không được dùng** — càng chi tiết càng tốt, giúp AI không gợi ý sai
2. **Done khi** — viết cụ thể, measurable (tránh viết "khi xong")
3. **Machine constraints** — bắt buộc nếu máy yếu, AI sẽ tự tối ưu RAM
4. **Data source** — ghi rõ real hay mock để AI không suggest ngược lại

---

## VÍ DỤ ĐÃ ĐIỀN SẴN (project crypto vừa làm)

Tôi muốn bạn tạo cho tôi bộ AI Context Files hoàn chỉnh cho project của tôi...

**Tên project:** realtime-crypto-streaming-pipeline

**Mô tả:** End-to-end streaming pipeline từ Binance WebSocket API đến Grafana dashboard

**Mục đích:** Portfolio project để apply Data Engineer Intern tại HCM

**Vị trí apply:** Data Engineer Intern

**Kinh nghiệm:** Sinh viên năm cuối, chưa có kinh nghiệm DE

**CPU:** Intel i3-1115G4, 2 cores | **RAM:** 7.7GB, ~2GB free | **OS:** Windows + Docker

| Layer | Tool |
|---|---|
| Ingestion | Python websockets + kafka-python |
| Queue | Apache Kafka 3.7 KRaft |
| Processing | PySpark 3.5 Structured Streaming |
| Storage | TimescaleDB + Parquet |
| Dashboard | Grafana 10.4 |
| Infra | Docker Compose |

❌ Zookeeper ❌ Superset ❌ Mock data ❌ Flink ❌ Redpanda

**Stage 1 — Ingestion:** Binance WebSocket → Kafka | Done khi: console-consumer thấy JSON

**Stage 2 — Processing:** Kafka → Spark → VWAP window 1min | Done khi: console output mỗi 30s

**Stage 3 — Storage:** Spark → TimescaleDB + Parquet | Done khi: Grafana query thấy data

**Stage 4 — Dashboard:** Grafana live + Docker Compose + AWS deploy | Done khi: public URL live
