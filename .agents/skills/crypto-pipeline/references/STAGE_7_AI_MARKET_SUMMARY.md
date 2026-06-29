# Stage 7 — AI Market Summary

> **Mục tiêu cuối ngày 4:** Tự động tạo bản tóm tắt thị trường bằng Gemini API miễn phí và hiển thị kết quả trong Grafana.

---

## Bối cảnh

Stage này giải quyết câu hỏi: *"Làm sao biến số liệu kỹ thuật thành insight bằng ngôn ngữ tự nhiên?"*

Sau khi Gold layer có dữ liệu VWAP, volume, price change, project có thể tạo summary mỗi vài phút để người xem dễ hiểu trạng thái thị trường. Đây là bước nâng cấp portfolio bằng AI layer.

Điểm quan trọng: stage này dùng **Google Gemini API** với model `gemini-2.5-flash` và đọc key từ `.env` bằng `python-dotenv`. Mục tiêu là giữ chi phí ở mức **$0**.

---

## Luồng dữ liệu Stage 7

```
[Gold tables / analytics query]
        │  mỗi 30 phút
        ▼
[AI summary script]
  query VWAP, volume, price change
  call Gemini API
        │
        ▼
[market_summaries table]
  lưu latest summary + timestamp
        │
        ▼
[Grafana summary panel]
  auto-refresh mỗi 30 phút
```

---

## File liên quan

| File | Vai trò |
|---|---|
| `ai/` | Chứa script tạo market summary |
| `docs/stages/STAGE_5_DBT_TRANSFORMATION.md` | Cung cấp Gold models để query |
| `.env` | Chứa `GEMINI_API_KEY` |
| `dashboard/grafana/` | Panel hiển thị summary |

---

## Cách chạy Stage 7

### Bước 1 — Cài dependency

```bash
pip install python-dotenv tenacity
```

### Bước 2 — Cấu hình API key

Trong `.env`:

```env
GEMINI_API_KEY=your_free_api_key_here
```

> Không hardcode API key trong code. Luôn đọc từ `.env`.

### Bước 3 — Chạy script summary

```bash
python -m ai.gemini_summary
```

### Bước 4 — Verify kết quả

- Kiểm tra bảng `market_summaries`
- Mở Grafana và xem Text panel

**Output mong đợi:**
```
[INFO] Latest market summary saved successfully.
[INFO] Summary timestamp: 2026-06-17 14:35:00
```

---

## Memory footprint Stage 7

| Service | RAM dùng |
|---|---|
| Python summary script | ~50–80 MB |
| Grafana text panel | ~20 MB |
| **Tổng Stage 7** | **~100 MB** |

---

## Các lỗi thường gặp

| Lỗi | Nguyên nhân | Cách fix |
|---|---|---|
| `API key not found` | Chưa set `GEMINI_API_KEY` | Check `.env` và `python-dotenv` |
| `429 Too Many Requests` | Gọi quá nhanh hoặc quota bị giới hạn | Retry 3 lần với backoff 2s, 4s, 8s rồi dùng template fallback |
| `Table market_summaries does not exist` | Chưa chạy migration schema | Tạo table trước khi chạy script |
| `Grafana không refresh` | Panel chưa set auto-refresh | Cấu hình 30 phút refresh |

---

## Definition of Done — Stage 7 hoàn thành khi

- [ ] Script chạy được mà không lỗi trong 15 phút liên tục
- [ ] Summary được lưu vào bảng `market_summaries`
- [ ] Grafana summary panel hiển thị summary mới nhất
- [ ] Gọi API dùng Gemini free tier và không cần thẻ tín dụng
- [ ] Mô tả kết quả bằng ngôn ngữ tự nhiên, dễ đọc

---

## Skills học được ở Stage này

- API integration: REST/HTTP request handling
- Environment management: `.env` và secret safety
- Data storytelling: chuyển số liệu thành insight
- Scheduling jobs: chạy định kỳ mỗi 30 phút để bảo toàn free-tier quota
- Monitoring UI: dashboard text panel và auto-refresh
