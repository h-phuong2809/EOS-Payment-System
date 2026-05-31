# Distributed Database Project Proposal: EOS Payment System

Due Date: Week 3 - 30/05/2026

Project ID & Category: #117 - Distributed Stream Processing & Transaction Reliability

## 1. Project Identity

Team Name: EOS Payment Lab

Team Members: [Điền tên thành viên nhóm]

Project Title: Exactly-Once Semantics for Idempotent Payments in a Distributed Stream Processing System

## 2. Objective & Problem Statement

The "Why": Trong hệ thống thanh toán phân tán, client retry, lỗi mạng, request đến muộn hoặc request được định tuyến qua nhiều node có thể làm cùng một giao dịch bị xử lý nhiều lần. Với nghiệp vụ chuyển tiền, duplicate processing gây lỗi nghiêm trọng vì số dư có thể bị trừ nhiều lần. Project này giải quyết bài toán bảo đảm một payment request chỉ được commit đúng một lần trên toàn hệ thống dù có retry, duplicate event, out-of-order event hoặc node failure.

Core Logic: Thuật toán chính là Exactly-Once Semantics (EOS) dựa trên idempotency key toàn cục. Gateway nhận request và định tuyến đến một trong ba node xử lý. Mỗi node trước khi thay đổi số dư phải atomic claim `idempotency_key` trong global deduplication store. Nếu key đã tồn tại, node trả `DUPLICATE_REJECTED`; nếu key chưa tồn tại, node ghi WAL, cập nhật balance, tạo transaction và đánh dấu kết quả thành công. Project cũng cài đặt chế độ At-Least-Once (ALO) để làm baseline so sánh với EOS.

## 3. Dataset Specification

Source: Synthetic dataset generated inside the application from seeded account data and stream benchmark generator. Không dùng dataset bên ngoài vì mục tiêu là kiểm soát duplicate rate, event-time disorder và retry pattern.

Size: Demo seed gồm 5 tài khoản ban đầu; benchmark mặc định sinh 160 payment events với duplicate rate 30%, window size 10 giây, seed 117. Hệ thống cho phép tăng event count đến 1000 events cho thử nghiệm lớn hơn.

Schema: Các bảng chính gồm `account(account_id, owner_name, balance, created_at, updated_at)`, `payment_request(request_id, idempotency_key, sender_account, receiver_account, amount, status, retry_count, node_id, event_time_ms, processing_time_ms)`, `payment_transaction(transaction_id, payment_request_id, idempotency_key, sender_account, receiver_account, amount, executed_at, node_id)`, `global_deduplication_record(idempotency_key, original_request, result_status, result_data, processed_at, node_id, event_time_ms)`, `wal_log_entry(log_id, sequence_number, operation_type, payment_id, idempotency_key, data_before, data_after, node_id, status, timestamp, committed_at)`, `benchmark_run`, `stream_window`, `processor_checkpoint`, và `processing_node`.

Fragmentation Strategy: Dữ liệu vận hành được phân mảnh ngang theo processing node. `NODE_A`, `NODE_B`, `NODE_C` lưu local H2 database riêng cho request, WAL, transaction, checkpoint và stream window của node đó. Riêng global deduplication table là state dùng chung để điều phối EOS cross-node. Gateway định tuyến request theo `nodeId`; nếu request không chỉ định node thì gateway hash `idempotencyKey` hoặc round-robin.

## 4. System Architecture

Nodes: 4 tiến trình localhost trong distributed mode: 1 Gateway và 3 processing nodes. Gateway chạy port 8080; `NODE_A`, `NODE_B`, `NODE_C` lần lượt chạy port 8081, 8082, 8083.

Communication Layer: HTTP/REST qua Spring Boot controller. Client hoặc React dashboard gọi Gateway bằng REST API. Gateway dùng REST call đến node services. Các endpoint chính gồm `POST /api/payment`, `POST /api/simulate-retries`, `POST /api/benchmark/compare`, `GET /api/benchmark/latest`, `GET /api/deduplication-table`, `GET /api/wal-log`, `GET /api/transactions`, `GET /api/stats`, `POST /api/recovery/replay-wal`, và `POST /api/reset`.

Storage: H2 file database. Mỗi node có local database riêng: `node_a.mv.db`, `node_b.mv.db`, `node_c.mv.db`. Gateway có `gateway.mv.db`. Global deduplication store dùng `global_dedup.mv.db`, được chia sẻ để chặn duplicate trên toàn hệ thống. WAL được lưu trong local node database để hỗ trợ recovery.

System Components:

| Component | File / Module | Responsibility |
| --- | --- | --- |
| REST API | `PaymentController.java` | Expose API cho payment, benchmark, WAL, dedup, transactions, stats, reset |
| Gateway | `GatewayPaymentService.java` | Route request đến node, aggregate data từ ba node |
| Node processor | `PaymentStreamService.java` | Thực thi EOS/ALO, WAL, balance update, benchmark, checkpoint |
| Global dedup store | `GlobalDeduplicationStore.java` | Atomic claim/check idempotency key bằng JDBC |
| Frontend dashboard | `frontend/src/components/*.jsx` | UI gửi payment, chạy benchmark, xem node/WAL/dedup/transaction |

## 5. Tech Stack & Implementation Plan

Programming Language: Java 21 cho backend và JavaScript/React cho frontend.

Deployment: Localhost distributed processes. Script `backend/run-distributed.ps1` khởi động Gateway + 3 node; `backend/stop-distributed.ps1` dừng các tiến trình demo. Hệ thống cũng hỗ trợ single-process mode để chạy nhanh khi phát triển.

Libraries/Frameworks: Spring Boot, Spring Web, Spring Data JPA, JDBC, H2 Database, Maven, React, Vite, Chart.js, JUnit 5, AssertJ.

Implementation Plan:

1. Khởi tạo backend Spring Boot, model entity, repository và API payment.
2. Cài đặt logic ALO làm baseline, sau đó thêm EOS bằng global deduplication store.
3. Thêm WAL, transaction boundary, node status và endpoint replay WAL để demo recovery.
4. Thêm benchmark generator với event-time, duplicate rate, tumbling window, watermark và latency percentiles.
5. Xây dashboard React để gửi payment, chạy benchmark, xem charts/tables và điều khiển node failure.
6. Viết test tự động cho duplicate rejection, ALO duplicate processing, concurrent duplicate, out-of-order retry, node failure và benchmark result.

## 6. Success Metrics & Analysis

Quantitative Metric: Các chỉ số đo lường gồm average latency, p50/p95/p99 latency, TPS, duplicate events, duplicates blocked, WAL write latency, WAL overhead, event lag, out-of-order count, late events và backpressure delayed. Kết quả chính cần chứng minh là trong EOS, số transaction commit bằng số unique payment; trong ALO, duplicate event có thể tạo thêm transaction.

The "Failure" Scenario: Project mô phỏng ba loại lỗi phân tán. Thứ nhất, cùng `idempotencyKey` được gửi nhiều lần hoặc gửi qua node khác nhau; global dedup store phải chặn duplicate. Thứ hai, nhiều request duplicate chạy đồng thời; chỉ một request được commit thành công. Thứ ba, một node bị toggle sang `FAILED`; payment đến node đó bị từ chối với HTTP 503 và không tạo transaction. Ngoài ra WAL replay endpoint được dùng để kiểm tra recovery của pending log.

Expected Analysis Output:

| Scenario | Expected Result | Evidence |
| --- | --- | --- |
| EOS duplicate retry | 1 successful transaction, remaining retries rejected | `transactions.count() = 1`, dedup table has key |
| ALO duplicate retry | Multiple successful transactions possible | transaction count increases with retries |
| Cross-node duplicate | Duplicate rejected even if routed to another node | global dedup table stores original node/key |
| Node failure | Request rejected, no partial commit | API returns 503, no transaction written |
| Benchmark compare | EOS blocks duplicates, ALO processes duplicates | dashboard metrics and `benchmark_run` rows |

## 7. Project Milestones

Milestone 1 (Week 5): Environment setup complete; Spring Boot backend, React frontend, H2 schema, account seed data and basic payment API operational.

Milestone 2 (Week 8): Core distributed logic operational; Gateway routes to three node services, EOS idempotency key handling works, ALO baseline implemented, global dedup table shared across nodes.

Milestone 3 (Week 12): Failure handling and benchmarking complete; WAL/replay endpoint, node failure simulation, concurrent duplicate test, event-time benchmark, latency analysis dashboard and automated tests completed.
