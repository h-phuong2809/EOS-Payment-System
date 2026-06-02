# Hệ thống thanh toán EOS - Exactly-Once Semantics

Project demo **Topic 117: Exactly-Once Semantics (EOS) cho xử lý thanh toán idempotent trên luồng `Payment_Requests`**.

Hệ thống mô phỏng một nền tảng thanh toán phân tán gồm gateway, nhiều processing node, cơ chế chống trùng lặp toàn cục, Write-Ahead Log và benchmark so sánh giữa:

- **EOS - Exactly-Once Semantics**: cùng một `idempotencyKey` chỉ được commit một lần, kể cả khi retry, gửi lệch node hoặc xử lý đồng thời.
- **ALO - At-Least-Once**: request retry có thể bị xử lý nhiều lần, dùng làm baseline để thấy rủi ro duplicate charge.

## Công nghệ sử dụng

| Thành phần | Công nghệ |
| --- | --- |
| Backend | Java 21, Spring Boot 3.3.5 |
| REST API | Spring Web |
| Persistence | Spring Data JPA, H2 file database |
| Validation | Jakarta Validation |
| Frontend | React 18, Vite 5 |
| Chart | Chart.js, react-chartjs-2 |
| Test | JUnit 5, Spring Boot Test, AssertJ |

## Mục tiêu của project

Project tập trung chứng minh các yêu cầu chính của xử lý stream thanh toán có idempotency:

1. **Chặn thanh toán trùng** bằng `idempotencyKey`.
2. **Đảm bảo chỉ commit một transaction thành công** cho mỗi payment logic trong chế độ EOS.
3. **So sánh EOS với ALO** để thấy ALO có thể tạo duplicate transaction.
4. **Mô phỏng xử lý phân tán** qua gateway và 3 node độc lập.
5. **Lưu trạng thái bền vững** bằng H2, WAL, checkpoint và bảng de-dup.
6. **Benchmark stream theo event-time** với tumbling window, watermark, late event, out-of-order event, backpressure và latency percentile.

## Kiến trúc tổng thể

```text
React Dashboard
   |
   v
Gateway API :8080
   |
   +-- NODE_A service :8081 -> H2 ./data/node_a
   +-- NODE_B service :8082 -> H2 ./data/node_b
   +-- NODE_C service :8083 -> H2 ./data/node_c
   |
   +-- Global de-dup H2 ./data/global_dedup
```

Có 2 chế độ chạy:

- **Single-process mode**: một Spring Boot app ở port `8080`, tự xử lý node logic trong cùng process.
- **Distributed demo mode**: gateway chạy ở `8080`, ba node chạy riêng ở `8081`, `8082`, `8083`.

Trong distributed mode, mỗi node có database local riêng, còn de-duplication dùng chung database `global_dedup` để chống duplicate xuyên node.

## Cấu trúc thư mục

```text
eos_payment_project/
+-- backend/
|   +-- pom.xml
|   +-- run-distributed.ps1
|   +-- stop-distributed.ps1
|   +-- src/
|       +-- main/java/com/eos/payment/
|       |   +-- controller/
|       |   +-- dto/
|       |   +-- model/
|       |   +-- repository/
|       |   +-- service/
|       +-- main/resources/
|       |   +-- application.yml
|       |   +-- application-gateway.yml
|       |   +-- application-node-a.yml
|       |   +-- application-node-b.yml
|       |   +-- application-node-c.yml
|       +-- test/java/com/eos/payment/
+-- frontend/
|   +-- package.json
|   +-- vite.config.js
|   +-- src/
|       +-- App.jsx
|       +-- services/api.js
|       +-- components/
+-- docs/
    +-- EOS_Payment_Improved.pptx
    +-- EOS_Idempotent_Payments_Report.docx
    +-- Design_Document_EOS_Idempotent_Payments.docx
    +-- demo-screenshots/
```

## Các thành phần backend chính

| File | Vai trò |
| --- | --- |
| `PaymentController.java` | REST controller cho toàn bộ API `/api/*`. |
| `PaymentOperations.java` | Interface chung cho node service và gateway service. |
| `PaymentStreamService.java` | Logic xử lý thanh toán, EOS, ALO, benchmark, WAL, checkpoint. |
| `GatewayPaymentService.java` | Gateway định tuyến request tới các node và tổng hợp dữ liệu. |
| `GlobalDeduplicationStore.java` | Kho de-dup toàn cục dùng JDBC trực tiếp trên H2. |
| `model/*` | Entity: account, payment request, transaction, WAL, benchmark run, window, checkpoint, node. |
| `repository/*` | Spring Data JPA repository cho các bảng local. |

## Luồng xử lý thanh toán EOS

Khi gửi một payment ở mode `EOS`, backend xử lý theo thứ tự:

1. Nhận request từ API `/api/payment`.
2. Xác định node xử lý (`NODE_A`, `NODE_B`, `NODE_C`).
3. Kiểm tra trạng thái node, nếu node `FAILED` thì reject.
4. Tạo bản ghi `PaymentRequest` trạng thái `PENDING`.
5. Kiểm tra event có quá cũ so với `app.max-event-age-ms` hay không.
6. Xóa de-dup record hết hạn theo `app.dedup-window-ms`.
7. Tra bảng `global_deduplication_record` bằng `idempotencyKey`.
8. Nếu key đã tồn tại, request bị đánh dấu `DUPLICATE_REJECTED`.
9. Nếu chưa tồn tại, node claim key toàn cục bằng insert có primary key.
10. Ghi WAL trạng thái `PENDING` trước khi thay đổi số dư.
11. Trừ tiền sender, cộng tiền receiver.
12. Tạo `PaymentTransaction`.
13. Cập nhật de-dup record thành `SUCCESS`.
14. Cập nhật WAL thành `COMMITTED`.
15. Trả kết quả `SUCCESS`.

Điểm quan trọng: bảng de-dup toàn cục có khóa chính là `idempotency_key`, nên cùng một payment retry trên node khác vẫn bị chặn.

## Luồng xử lý ALO

Mode `ALO` bỏ qua de-dup và WAL:

1. Nhận request.
2. Kiểm tra số dư.
3. Cập nhật số dư.
4. Tạo transaction.
5. Trả `SUCCESS`.

Vì không kiểm tra `idempotencyKey`, cùng một request retry nhiều lần có thể tạo nhiều transaction. Dashboard dùng ALO để so sánh trực tiếp với EOS.

## Benchmark stream

Endpoint `/api/benchmark/compare` tạo một luồng payment giả lập và chạy cả 2 mode `ALO` và `EOS`.
Dashboard dùng thêm endpoint SSE `/api/benchmark/stream` để nhận `started`, `progress`, `run` và `complete` trong lúc benchmark đang xử lý, nên chart TPS/latency và bảng tumbling window cập nhật theo thời gian thực thay vì chờ kết quả cuối.

Các tham số benchmark:

| Tham số | Ý nghĩa | Giới hạn trong code |
| --- | --- | --- |
| `eventCount` | Số event thanh toán | 10 đến 1000 |
| `duplicateRate` | Tỷ lệ event trùng | 0 đến 0.8 |
| `windowSizeSec` | Kích thước tumbling window | 2 đến 60 giây |
| `seed` | Seed để tái lập dữ liệu | Mặc định 117 |
| `distributed` | Chạy qua nhiều node | Gateway tự set `true` |
| `streamDelayMs` | Độ trễ mô phỏng giữa các event SSE | 0 đến 100 ms |

Metric được ghi nhận:

- TPS tổng và TPS theo tumbling window.
- Average latency, p50, p95, p99.
- WAL write latency.
- Số duplicate bị chặn.
- Số duplicate charge trong ALO.
- Event lag giữa `processingTimeMs` và `eventTimeMs`.
- Out-of-order event.
- Late event theo watermark.
- Backpressure delayed event.
- WAL entries và WAL committed.
- Processor checkpoint.

## Cấu hình backend

File mặc định: `backend/src/main/resources/application.yml`

```yaml
server:
  port: 8080

app:
  role: node
  dedup-window-ms: 86400000
  max-event-age-ms: 2592000000
  allowed-lateness-ms: 2000
  max-in-flight-events: 24
```

Các profile distributed:

| Profile | Port | Node ID | Database |
| --- | --- | --- | --- |
| `gateway` | 8080 | Gateway | `./data/gateway` |
| `node-a` | 8081 | `NODE_A` | `./data/node_a` |
| `node-b` | 8082 | `NODE_B` | `./data/node_b` |
| `node-c` | 8083 | `NODE_C` | `./data/node_c` |

Global de-dup database:

```text
./data/global_dedup
```

## Yêu cầu môi trường

Cần cài sẵn:

- Java 21
- Maven
- Node.js và npm
- PowerShell nếu chạy distributed script trên Windows

Kiểm tra nhanh:

```powershell
java -version
mvn -version
node -v
npm -v
```

## Chạy backend single-process

Chế độ này đơn giản nhất để chạy API và kiểm thử nhanh.

```powershell
cd backend
mvn spring-boot:run
```

Sau khi chạy, backend mở tại:

```text
http://localhost:8080
```

API base:

```text
http://localhost:8080/api
```

## Chạy frontend ở chế độ dev

Mở terminal khác:

```powershell
cd frontend
npm install
npm run dev
```

Frontend dev URL:

```text
http://localhost:5173
```

Khi chạy ở port `5173`, frontend tự gọi API:

```text
http://localhost:8080/api
```

## Build frontend vào Spring Boot static

Nếu muốn backend phục vụ luôn dashboard React:

```powershell
cd frontend
npm install
npm run build:backend
cd ..\backend
mvn spring-boot:run
```

Sau đó mở:

```text
http://localhost:8080
```

## Chạy distributed demo

Distributed mode khởi động 4 process:

- Gateway ở `8080`
- Node A ở `8081`
- Node B ở `8082`
- Node C ở `8083`

Chạy:

```powershell
cd backend
.\run-distributed.ps1
```

Script sẽ tạo log trong thư mục `backend`:

```text
node-a.out.log
node-b.out.log
node-c.out.log
gateway.out.log
```

Dừng distributed demo:

```powershell
cd backend
.\stop-distributed.ps1
```

Các URL chính:

```text
Gateway: http://localhost:8080
NODE_A:  http://localhost:8081
NODE_B:  http://localhost:8082
NODE_C:  http://localhost:8083
```

## H2 Console

H2 console được bật ở các service Spring Boot.

Ví dụ mở node A:

```text
http://localhost:8081/h2-console
```

Thông tin đăng nhập:

```text
User: sa
Password: để trống
```

JDBC URL thường dùng:

```text
jdbc:h2:file:./data/node_a
jdbc:h2:file:./data/node_b
jdbc:h2:file:./data/node_c
jdbc:h2:file:./data/gateway
jdbc:h2:file:./data/global_dedup
```

## Dashboard

Dashboard React gồm các phần chính:

| Khu vực | Chức năng |
| --- | --- |
| Stats Grid | Tổng request, processed, duplicate blocked, transactions, WAL, de-dup size, latency mới nhất. |
| Node Panel | Xem trạng thái node và toggle `ACTIVE` / `FAILED`. |
| Payment Console | Gửi thanh toán thủ công theo account, amount, node, mode và idempotency key. |
| Retry Simulation | Gửi nhiều retry cùng một idempotency key để so sánh EOS và ALO. |
| Live Stream Benchmark | Chạy benchmark EOS vs ALO theo event count, duplicate rate và window size, nhận tiến độ realtime bằng SSE. |
| Charts | TPS và average latency theo tumbling window, cập nhật khi từng event/window mới được xử lý. |
| De-dup Table | Xem các idempotency key đã claim / success. |
| WAL Log | Xem WAL sequence, operation, node, trạng thái commit. |
| Transactions | Xem các transaction đã commit. |

## REST API chính

Base path:

```text
/api
```

| Method | Endpoint | Mô tả |
| --- | --- | --- |
| `GET` | `/accounts` | Danh sách tài khoản demo. |
| `GET` | `/nodes` | Danh sách node và trạng thái. |
| `POST` | `/nodes/{nodeId}/toggle` | Toggle node `ACTIVE` / `FAILED`. |
| `POST` | `/payment` | Gửi một payment. |
| `POST` | `/simulate-retries` | Mô phỏng retry cùng idempotency key. |
| `POST` | `/benchmark/compare` | Chạy benchmark ALO và EOS. |
| `GET` | `/benchmark/stream` | Stream benchmark realtime bằng Server-Sent Events. |
| `GET` | `/benchmark/latest` | Lấy benchmark run, window, checkpoint mới nhất. |
| `GET` | `/deduplication-table` | Xem bảng de-dup toàn cục. |
| `GET` | `/wal-log` | Xem 50 WAL entry mới nhất. |
| `GET` | `/transactions` | Xem 50 transaction mới nhất. |
| `GET` | `/stats` | Tổng hợp thống kê. |
| `POST` | `/recovery/replay-wal` | Replay các WAL còn `PENDING`. |
| `POST` | `/reset` | Reset operational state. |

## Ví dụ gọi API

Gửi payment EOS:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri http://localhost:8080/api/payment `
  -ContentType "application/json" `
  -Body '{
    "senderAccount": "ACC001",
    "receiverAccount": "ACC002",
    "amount": 100000,
    "nodeId": "NODE_A",
    "mode": "EOS",
    "idempotencyKey": "PAY-DEMO-001",
    "retryCount": 0
  }'
```

Gửi lại cùng `idempotencyKey` ở mode EOS:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri http://localhost:8080/api/payment `
  -ContentType "application/json" `
  -Body '{
    "senderAccount": "ACC001",
    "receiverAccount": "ACC002",
    "amount": 100000,
    "nodeId": "NODE_B",
    "mode": "EOS",
    "idempotencyKey": "PAY-DEMO-001",
    "retryCount": 1
  }'
```

Kết quả mong đợi: request thứ hai trả về `DUPLICATE_REJECTED`.

Chạy benchmark:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri http://localhost:8080/api/benchmark/compare `
  -ContentType "application/json" `
  -Body '{
    "eventCount": 160,
    "duplicateRate": 0.3,
    "windowSizeSec": 10,
    "seed": 117
  }'
```

Stream benchmark realtime bằng SSE:

```powershell
curl.exe "http://localhost:8080/api/benchmark/stream?eventCount=160&duplicateRate=0.3&windowSizeSec=10&seed=117&streamDelayMs=25"
```

## Tài khoản demo

Khi database trống hoặc reset, backend seed các tài khoản:

| Account | Chủ tài khoản | Số dư ban đầu |
| --- | --- | --- |
| `ACC001` | Nguyen Van An | 5,000,000 VND |
| `ACC002` | Tran Thi Bich | 3,000,000 VND |
| `ACC003` | Le Hoang Cuong | 8,000,000 VND |
| `ACC004` | Pham Thi Dung | 2,000,000 VND |
| `ACC005` | Hoang Van Enh | 6,500,000 VND |

## Kịch bản demo đề xuất

1. Chạy distributed mode bằng `.\run-distributed.ps1`.
2. Mở dashboard tại `http://localhost:8080`.
3. Gửi một payment mode `EOS` với idempotency key cố định, ví dụ `PAY-DEMO-001`.
4. Gửi lại cùng key sang node khác, ví dụ từ `NODE_A` sang `NODE_B`.
5. Kiểm tra request retry bị `DUPLICATE_REJECTED`.
6. Chuyển mode sang `ALO` và gửi retry cùng key.
7. Kiểm tra ALO tạo nhiều transaction.
8. Chạy `Live Stream Benchmark` với:
   - Payment Events: `160`
   - Duplicate Rate: `30`
   - Window: `10`
9. Quan sát:
   - EOS có `Duplicates Blocked` > 0.
   - ALO có duplicate charge.
   - TPS và latency hiển thị theo tumbling window và cập nhật realtime trong lúc benchmark đang chạy.
   - Late event, backpressure, WAL metrics xuất hiện.
10. Toggle một node sang `FAILED`, rồi gửi payment vào node đó để thấy request bị reject.
11. Bấm `Replay WAL` để mô phỏng recovery các WAL entry còn pending.

## Test

Chạy backend test:

```powershell
cd backend
mvn test
```

Các test hiện có kiểm tra:

- EOS reject duplicate và chỉ commit một transaction.
- ALO xử lý duplicate thành nhiều transaction.
- Retry out-of-order vẫn bị chặn theo event-time.
- Duplicate đồng thời chỉ có một request thành công.
- Node failed thì payment bị reject.
- Benchmark thể hiện EOS chặn duplicate còn ALO không chặn.
- Profile `gateway` dùng `GatewayPaymentService`.
- Profile node chỉ xử lý đúng node-id của chính nó.

Build frontend:

```powershell
cd frontend
npm run build
```

Build frontend vào backend:

```powershell
cd frontend
npm run build:backend
```

## Mapping tiêu chí rubric

| Tiêu chí | Cách project đáp ứng |
| --- | --- |
| Windowing logic | Benchmark dùng event-time, tumbling window, watermark, late event và out-of-order metric. |
| State management | Mỗi node có state local; de-dup table dùng chung toàn cục; có account, request, transaction, WAL, checkpoint, window. |
| Latency analysis | Ghi avg, p50, p95, p99, WAL write latency, event lag, TPS tổng và TPS theo window. |
| Robustness | EOS transaction, unique idempotency key, concurrent duplicate rejection, node failure rejection, WAL replay, backpressure simulation. |
| Distributed behavior | Gateway định tuyến request tới 3 node; duplicate key vẫn bị chặn xuyên node bằng global de-dup. |

## Ghi chú dữ liệu

- Database H2 được tạo trong thư mục `backend/data` khi chạy từ `backend`.
- File `eos_payment.db` ở root là database cũ / local artifact, không bắt buộc cho distributed mode.
- Endpoint `/api/reset` xóa dữ liệu vận hành và seed lại account/node mặc định.
- Trong distributed mode, gateway tổng hợp dữ liệu từ các node; nếu node offline, dashboard có thể hiển thị node đó là `FAILED`.

## Tài liệu đi kèm

Thư mục `docs/` chứa báo cáo, thiết kế và slide:

- `Distributed Database Project Proposal.docx`
- `Design_Document_EOS_Idempotent_Payments.docx`
- `EOS_Idempotent_Payments_Report.docx`
- `EOS_Payment_Improved.pptx`
- `demo-screenshots/`
