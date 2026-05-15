# Warehouse Management System Simulation - WMSS (AGV Control Service / Golang)

Dự án này là tầng **AGV Control Service** - "Hệ Cơ Bắp & Thực Thi Realtime" trong hệ thống mô phỏng tự động hóa nhà kho đa ngôn ngữ (Polyglot Microservices).

## 🧩 Vai trò trong hệ thống

Trong kiến trúc 3 tầng (Business → Decision → Execution), dịch vụ Go đảm nhận lớp **Execution Layer** — trả lời cho câu hỏi *"Thực thi ra sao?"*:

- **ERP / WMS (Node.js):** Lớp Business — *"Cần làm gì?"* (Ra yêu cầu nghiệp vụ: nhập/xuất hàng).
- **MES (Python):** Lớp Decision — *"Làm như thế nào?"* (Tính toán logic, định tuyến, phân bổ slot, tạo kế hoạch thực thi).
- **AGV Control Service (Golang):** Lớp Execution — *"Thực thi ra sao?"* (Điều khiển AGV realtime dựa trên kế hoạch từ MES).

> **Lưu ý quan trọng:** AGV Service là một **Machine Runtime Layer**. Nó KHÔNG chứa logic nghiệp vụ kho (Business), KHÔNG chứa quy tắc Inventory. Khi MES nói *"Đi từ A → B"*, AGV Service mới là nơi tính toán *"Mỗi frame di chuyển thế nào"*, tốc độ ra sao, khi nào thì dừng.

## 🚀 Các Chức Năng Cốt Lõi (Execution Engine)

- **AGV State Management:** Quản lý trạng thái realtime của từng AGV (Tọa độ X/Y, mức Pin, Vận tốc, Trạng thái hoạt động).
- **Task Consumer:** Tiếp nhận Execution Plan (Kế hoạch thực thi bao gồm danh sách Task và Waypoints) từ tầng MES.
- **Runtime Execution:** Sử dụng Goroutines để giả lập và điều khiển sự di chuyển của hàng trăm AGV cùng lúc trên từng frame hình (Mô phỏng máy trạng thái - State Machine).
- **Sensor Processing:** Mô phỏng việc xử lý tín hiệu cảm biến (Phát hiện vật cản đột xuất, Pin yếu cần tìm trạm sạc).
- **Realtime Broadcasting:** Liên tục đẩy (Publish) vị trí X/Y mới nhất của các AGV lên **Redis Pub/Sub** để WMS Socket broadcast xuống Frontend (FE).

## 🛠 Công nghệ sử dụng

- **Golang**: Ngôn ngữ biên dịch siêu tốc, sinh ra để xử lý đồng thời (Concurrency) với hàng nghìn Goroutines vô cùng nhẹ nhàng, phù hợp cho điều khiển hệ thống đa thiết bị.
- **gRPC**: Khởi tạo Server để nhận lệnh (Execution Plan) từ MES.
- **Kafka**: Giao tiếp bất đồng bộ, báo cáo (Produce) trạng thái Task đã hoàn thành để WMS cập nhật tồn kho.
- **Redis**: Sử dụng tính năng Pub/Sub để phát luồng (stream) vị trí của AGV theo thời gian thực với độ trễ cực thấp.

## 🔄 Luồng xử lý chính (Ví dụ: Di chuyển thực thi)

1. **MES (Python)** gửi một "Execution Plan" xuống cho **AGV Control (Go)** qua gRPC.
2. Go tìm một AGV đang rảnh rỗi và kích hoạt một **Goroutine** riêng biệt để quản lý tác vụ của AGV đó.
3. Trong Goroutine, Go tính toán vòng lặp mô phỏng di chuyển (vận tốc, nội suy tọa độ giữa các Waypoints do MES cấp).
4. Cứ mỗi khung hình (ví dụ: 10 lần/giây), Go **Publish** tọa độ mới của AGV vào **Redis**.
5. Frontend nhận dữ liệu từ Redis (thông qua kết nối Socket với Node.js) và diễn hoạt mượt mà sự di chuyển của AGV trên màn hình.
6. Khi AGV đi đến đích và hoàn tất việc (VD: Nâng hàng), Go gửi tín hiệu `Task Done` qua Kafka để WMS cập nhật lại tồn kho.

## 💻 Cấu trúc thư mục định hướng

Dự án áp dụng Standard Go Project Layout:
- `cmd/`: Chứa các entrypoint (hàm main) để chạy ứng dụng.
- `internal/`: Chứa các logic thực thi vật lý, quản lý bộ nhớ của đội xe AGV, hoàn toàn tách biệt.
- `transport/`: Nơi tiếp nhận và xử lý các giao thức mạng (gRPC, Kafka).
