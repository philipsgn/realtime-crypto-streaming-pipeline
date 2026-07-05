# Stage 4 — Dashboard & Trực quan hoá

Giai đoạn này chuyển dữ liệu đã được xử lý thành dashboard phục vụ quan sát thị trường và
trình diễn pipeline.

## 1. Công nghệ sử dụng

- **Grafana 10.4**: dashboard và visualization.
- **PostgreSQL datasource**: kết nối TimescaleDB trong Docker network qua `postgres:5432`.
- **dbt Gold models**: serving layer chuẩn cho panel phân tích mới.

## 2. Data source

Datasource được provision từ `dashboard/grafana/provisioning/datasources/`. Host, database,
user và password phải lấy từ environment hoặc secret provisioning; không ghi credential thật
trong tài liệu hoặc dashboard JSON.

Grafana phải truy vấn Gold layer cho các panel analytics mới. `gold_minute_volume`,
`gold_hourly_vwap`, `gold_daily_summary` và `market_summaries` là các nguồn phục vụ chính.
Các panel giá cũ còn đọc trực tiếp `trade_metrics_1min` là technical debt và phải được migrate
sang Gold model trước khi tuyên bố dashboard tuân thủ hoàn toàn serving-layer boundary.

## 3. Panel chính

1. Volume theo phút bằng `gold_minute_volume`.
2. VWAP theo giờ bằng `gold_hourly_vwap`.
3. Tổng quan ngày bằng `gold_daily_summary`.
4. AI market summary mới nhất bằng `market_summaries`.

Ví dụ query Gold volume:

```sql
SELECT
  window_start AS "time",
  symbol AS metric,
  quote_volume_usdt AS value
FROM gold_minute_volume
WHERE $__timeFilter(window_start)
ORDER BY window_start ASC
```

## 4. Definition of Done

- [ ] Datasource PostgreSQL được provision và health check thành công.
- [ ] Dashboard hiển thị dữ liệu Binance thật cho BTCUSDT, ETHUSDT và SOLUSDT.
- [ ] Panel analytics truy vấn Gold models, không bypass Spark sink hoặc dbt serving layer.
- [ ] AI summary panel hiển thị `source=gemini` hoặc `fallback_template` minh bạch.
- [ ] Không có password hoặc token trong dashboard JSON và screenshot CV.
