# Stage 4 — Dashboard & Trực quan hoá

Giai đoạn cuối cùng của pipeline là biến những con số khô khan trong Database thành các biểu đồ trực quan, giúp người dùng theo dõi biến động thị trường theo thời gian thực.

## 1. Công nghệ sử dụng
- **Grafana**: Nền tảng phân tích và giám sát mã nguồn mở hàng đầu.

## 2. Data Source (Nguồn dữ liệu)
Để Grafana vẽ được biểu đồ, nó cần biết lấy dữ liệu từ đâu. Chúng ta cấu hình PostgreSQL làm Data Source cho Grafana:
- **Host**: `postgres:5432` (Vì cả Grafana và Postgres đều nằm chung mạng Docker, chúng có thể gọi nhau bằng tên container).
- **Database**: `crypto_pipeline`
- **User**: `pipeline`
- **Password**: `changeme`

*(Cấu hình này đã được khai báo sẵn trong file `dashboard/grafana/provisioning/datasources/postgres.yml` để Grafana tự nhận diện khi khởi động).*

## 3. Vẽ biểu đồ (Panels)
Trong Grafana, mỗi biểu đồ được gọi là một **Panel**. Bạn có thể viết câu lệnh SQL để truy vấn dữ liệu từ bảng `trade_metrics_1min` và Grafana sẽ tự động render thành biểu đồ:

### Ví dụ Câu lệnh SQL cho Biểu đồ giá (VWAP):
```sql
SELECT
  window_start AS "time",
  symbol AS metric,
  vwap
FROM trade_metrics_1min
WHERE
  $__timeFilter(window_start)
ORDER BY 1
```
Hàm `$__timeFilter` là một biến đặc biệt của Grafana để tự động lọc dữ liệu theo khoảng thời gian người dùng đang chọn trên giao diện (ví dụ: 6 tiếng qua).

## 4. Các biểu đồ nên có trong Dashboard
1. **Biểu đồ nến hoặc biểu đồ đường (Line chart)**: Hiển thị giá trung bình VWAP của BTC, ETH, SOL.
2. **Biểu đồ cột (Bar chart)**: Hiển thị Tổng Volume giao dịch (Total Volume) trong mỗi phút.
3. **Gauge hoặc Stat**: Hiển thị phần trăm thay đổi giá (`price_change_pct`).
