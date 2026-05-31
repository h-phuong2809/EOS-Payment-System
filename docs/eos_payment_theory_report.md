# EOS Payment System

## Báo Cáo Biện Minh Thiết Kế Dựa Trên Lý Thuyết Özsu & Valduriez

| Thuộc tính | Nội dung |
| --- | --- |
| Đề tài | Exactly-Once Semantics for Idempotent Payments in a Distributed Stream Processing System |
| Project ID | #117 |
| Công nghệ | Java 21, Spring Boot, H2, React, Vite |
| Tài liệu lý thuyết | M. Tamer Özsu, Patrick Valduriez, Principles of Distributed Database Systems, 4th Edition |
| Mục tiêu báo cáo | Giải thích và bảo vệ các quyết định thiết kế của project bằng lý thuyết CSDL phân tán |

## 1. Giới Thiệu

Project EOS Payment System mô phỏng một hệ thống thanh toán phân tán trong đó các yêu cầu chuyển tiền có thể bị gửi lặp lại do retry từ client, lỗi mạng, xử lý song song hoặc định tuyến sang nhiều node khác nhau. Nếu hệ thống chỉ bảo đảm At-Least-Once (ALO), cùng một payment request có thể tạo nhiều transaction và làm sai số dư tài khoản. Vì vậy project chọn thiết kế **Exactly-Once Semantics (EOS)** dựa trên idempotency key toàn cục, local transaction, Write-Ahead Log (WAL), event-time window và dashboard benchmark.

Theo Özsu & Valduriez, hệ quản trị CSDL phân tán không chỉ là nhiều file dữ liệu đặt ở nhiều máy, mà là một hệ thống phải quản lý dữ liệu phân tán một cách tích hợp, nhất quán, tin cậy và tối ưu theo chi phí truyền thông. Báo cáo này dùng các khái niệm trong sách để trả lời câu hỏi: vì sao project chọn Gateway + ba processing nodes, vì sao dùng global deduplication store, vì sao phân mảnh theo node, vì sao cần WAL/recovery, và vì sao benchmark phải đo latency, duplicate, throughput, event lag và tail latency.

## 2. Cơ Sở Lý Thuyết Từ Özsu & Valduriez

### 2.1 Transparency Trong Hệ CSDL Phân Tán

Özsu & Valduriez trình bày transparency như khả năng che giấu chi tiết triển khai ở tầng thấp khỏi người dùng hoặc ứng dụng. Trong CSDL phân tán, người dùng không nên phải biết dữ liệu đang ở site nào, request đi qua network ra sao, hoặc có bao nhiêu bản sao/phân mảnh bên dưới. Các dạng transparency quan trọng gồm distribution transparency, network transparency, fragmentation transparency và replication transparency.

Áp dụng vào project, client chỉ gọi Gateway API tại `localhost:8080` hoặc thao tác trên React dashboard. Client không cần biết request cuối cùng được xử lý bởi `NODE_A`, `NODE_B` hay `NODE_C`, cũng không cần biết local H2 database nào đang ghi WAL/transaction. Gateway đóng vai trò lớp che giấu phân tán, giúp hệ thống có interface logic thống nhất dù bên dưới có nhiều node.

### 2.2 Distributed Database Design, Fragmentation Và Allocation

Trong chương thiết kế CSDL phân tán, Özsu & Valduriez mô tả bài toán đặt dữ liệu trên nhiều site. Horizontal fragmentation chia một quan hệ theo tuple; allocation quyết định fragment nằm ở site nào. Sách nhấn mạnh ba tiêu chí đúng đắn của fragmentation: completeness, reconstruction và disjointness. Nghĩa là không được mất dữ liệu khi phân mảnh, phải có cách tái dựng quan hệ logic từ các fragment, và các fragment ngang nên không chồng lặp trừ khi có chủ ý replication.

Project áp dụng phân mảnh ngang theo processing node: `NODE_A`, `NODE_B`, `NODE_C` ghi request, WAL, transaction, stream window và checkpoint riêng. Về mặt logic, tập request/transaction toàn cục có thể tái dựng bằng union dữ liệu từ ba node. Cách này phù hợp với lý thuyết horizontal fragmentation: mỗi node xử lý một tập sự kiện riêng, tăng khả năng mở rộng và cho phép benchmark phân tán.

### 2.3 Distributed Cost Model

Özsu & Valduriez đưa ra mô hình chi phí phân tán trong đó tổng thời gian gồm CPU, I/O và communication cost:

```text
Total_Time = T_CPU * #instructions
           + T_IO  * #disk_IOs
           + T_MSG * #messages
           + T_TR  * #bytes_transferred
```

Trong môi trường phân tán, `T_MSG` và `T_TR` làm cho thiết kế khác biệt so với hệ thống tập trung. Một thiết kế tốt không chỉ tối ưu local CPU/I/O mà còn phải giảm số lần gọi mạng, giảm số byte truyền, và tránh coordination không cần thiết.

Trong project, mỗi payment qua Gateway có communication cost do HTTP call từ dashboard/client đến Gateway và từ Gateway đến target node. EOS thêm chi phí coordination với global dedup store, nhưng đổi lại loại bỏ chi phí nghiệp vụ lớn hơn: duplicate transaction và sai số dư. Benchmark đo `elapsedMs`, `dedupLookupMs`, `walWriteMs`, TPS và percentile latency để tách tác động local processing, dedup lookup và WAL overhead.

### 2.4 Transaction, Atomicity, Reliability Và Recovery

Trong chương distributed transaction processing, Özsu & Valduriez xem transaction là đơn vị cơ bản để giữ consistency và reliability. ACID yêu cầu atomicity: một transaction hoặc phản ánh đầy đủ các hành động vào database hoặc không phản ánh gì. Sách cũng chỉ ra commit/recovery cần log để khi crash xảy ra, hệ thống có thể đưa database về trạng thái nhất quán.

Project chuyển các nguyên tắc này vào xử lý thanh toán. Với EOS, node phải thực hiện một chuỗi hành động: claim idempotency key, ghi WAL, cập nhật balance, tạo transaction, cập nhật request và hoàn tất dedup result. WAL được ghi trước khi thay đổi số dư để sau lỗi có thể replay pending log. Endpoint `POST /api/recovery/replay-wal` hiện thực hóa ý tưởng recovery: log pending được kiểm tra lại, sau đó đánh dấu `COMMITTED` nếu transaction đã tồn tại hoặc `ROLLED_BACK` nếu không có commit tương ứng.

## 3. Kiến Trúc Project

### 3.1 Tổng Quan Thành Phần

```text
React Dashboard / API Client
          |
          v
Gateway API :8080
          |
   +------+-------+----------------+
   v              v                v
NODE_A :8081   NODE_B :8082     NODE_C :8083
node_a H2      node_b H2        node_c H2
   |              |                |
   +--------------+----------------+
          |
          v
Global Dedup H2: idempotency_key -> result
```

| Thành phần | File / Module | Vai trò |
| --- | --- | --- |
| Gateway API | `GatewayPaymentService.java` | Chọn node, gọi REST sang node, aggregate data cho dashboard |
| Node Processor | `PaymentStreamService.java` | Xử lý EOS/ALO, WAL, account balance, transaction, benchmark |
| Global Dedup Store | `GlobalDeduplicationStore.java` | Lưu khóa idempotency toàn cục bằng JDBC/H2 |
| REST Controller | `PaymentController.java` | API payment, benchmark, WAL, dedup table, stats, reset |
| Model Layer | `model/*.java` | Account, PaymentRequest, Transaction, WAL, BenchmarkRun, StreamWindow |
| Frontend | `frontend/src/components/*.jsx` | Dashboard gửi payment, chạy benchmark, xem node/WAL/dedup/transaction |

### 3.2 Dataset Và State

Project không dùng dataset bên ngoài vì cần kiểm soát duplicate rate và event-time disorder. Dữ liệu được sinh tổng hợp: 5 account seed ban đầu và stream benchmark mặc định 160 events, duplicate rate 30%, window 10 giây, seed 117. Các bảng chính gồm `account`, `payment_request`, `payment_transaction`, `global_deduplication_record`, `wal_log_entry`, `benchmark_run`, `stream_window`, `processor_checkpoint` và `processing_node`.

| State | Vị trí lưu | Lý do thiết kế |
| --- | --- | --- |
| Account, request, WAL, transaction | Local H2 của từng node | Giữ local autonomy và giảm phụ thuộc giữa node |
| Global dedup table | H2 dùng chung `global_dedup` | Bảo đảm idempotency toàn hệ thống |
| Benchmark run/window/checkpoint | Local node DB | Đo hiệu năng theo luồng xử lý của node |
| Dashboard state | API aggregate từ Gateway | Tạo distribution transparency cho người dùng |

## 4. Biện Minh Các Quyết Định Thiết Kế

### 4.1 Gateway Là Lớp Transparency

**Design choice:** Dùng Gateway trên port 8080 làm entry point duy nhất cho dashboard/client.

**Theory:** Distribution transparency và network transparency trong Özsu & Valduriez yêu cầu tầng ứng dụng không cần biết dữ liệu nằm ở site nào hoặc giao tiếp mạng nội bộ diễn ra thế nào.

**Justification:** Gateway giúp client gửi cùng một API dù hệ thống có ba node xử lý. Điều này làm demo rõ hơn: request có thể được route sang `NODE_A`, `NODE_B` hoặc `NODE_C`, nhưng người dùng vẫn nhìn thấy một hệ thống thống nhất. Nếu bỏ Gateway và cho client gọi node trực tiếp, client phải tự biết topology, làm giảm transparency và tăng coupling.

**Evidence in project:** `GatewayPaymentService` chọn node theo `nodeId`, hash `idempotencyKey` hoặc round-robin; các API như `/api/stats`, `/api/wal-log`, `/api/transactions` aggregate dữ liệu từ nhiều node.

### 4.2 Phân Mảnh Theo Processing Node

**Design choice:** Mỗi node có H2 database riêng, lưu request/WAL/transaction/window/checkpoint của node đó.

**Theory:** Horizontal fragmentation chia tuple của quan hệ logic thành nhiều fragment. Correctness cần completeness, reconstruction và disjointness.

**Justification:** Payment request có thuộc tính `nodeId`, vì vậy node-based fragmentation là tự nhiên. Tập transaction toàn cục có thể được reconstruction bằng union transaction của `NODE_A`, `NODE_B`, `NODE_C`. Disjointness đạt được vì mỗi request được xử lý bởi một target node. Completeness đạt được vì Gateway luôn route request đến một node hợp lệ hoặc trả lỗi nếu node failed.

**Trade-off:** Bảng account được seed giống nhau ở mỗi node để đơn giản hóa demo, không phải mô hình account replication đầy đủ. Nếu mở rộng production, cần protocol replica consistency hoặc phân vùng account rõ hơn.

### 4.3 Global Deduplication Store Thay Cho Local Dedup Riêng Lẻ

**Design choice:** Dùng một bảng `global_deduplication_record` chung cho tất cả node.

**Theory:** Trong môi trường phân tán, data control phải duy trì consistency khi dữ liệu và transaction nằm ở nhiều site. Nếu control rule nằm cục bộ, một node không thể biết node khác đã xử lý request cùng key hay chưa.

**Justification:** Local dedup chỉ chặn duplicate trên cùng node; nó thất bại với cross-node retry. Global dedup store cung cấp một coordination point nhỏ, tập trung vào khóa idempotency, thay vì phân tán toàn bộ dữ liệu thanh toán. Đây là đánh đổi hợp lý: tăng một bước lookup/claim nhưng đạt EOS trên toàn hệ thống.

**Evidence in project:** `GlobalDeduplicationStore.claimOrExisting()` dùng primary key `idempotency_key`. Nếu insert trùng key, request mới nhận record cũ và bị trả `DUPLICATE_REJECTED`.

### 4.4 EOS So Với ALO

**Design choice:** Cài đặt cả EOS và ALO, nhưng chọn EOS làm mode đúng cho payment.

**Theory:** Transaction processing yêu cầu consistency và atomicity. Với nghiệp vụ tài chính, “xử lý ít nhất một lần” không đủ vì duplicate update phá vỡ consistency.

**Justification:** ALO phù hợp cho log hoặc event analytics nơi duplicate có thể được xử lý sau. Với chuyển tiền, duplicate là lỗi nghiệp vụ nghiêm trọng. EOS dùng idempotency key để biến retry thành thao tác an toàn: request đầu tiên commit, các request sau bị từ chối hoặc có thể trả lại kết quả cũ. Baseline ALO vẫn được giữ để benchmark cho thấy chi phí và rủi ro của việc không dedup.

| Mode | Ưu điểm | Rủi ro | Kết luận |
| --- | --- | --- | --- |
| ALO | Đơn giản, ít overhead | Có thể trừ tiền nhiều lần khi retry | Chỉ dùng làm baseline |
| EOS | Chặn duplicate, đúng nghiệp vụ payment | Thêm dedup lookup và WAL overhead | Phù hợp cho payment |

### 4.5 WAL Và Recovery

**Design choice:** EOS ghi WAL trước khi cập nhật balance, sau đó đánh dấu `COMMITTED`.

**Theory:** Özsu & Valduriez nhấn mạnh log là thành phần cần thiết để commit/recovery đưa database về trạng thái nhất quán sau crash. Atomicity và durability không thể chỉ dựa vào thao tác ghi dữ liệu trực tiếp.

**Justification:** Nếu node lỗi sau khi claim key nhưng trước khi hoàn tất transaction, WAL cho phép hệ thống kiểm tra lại trạng thái pending. WAL cũng làm demo robustness rõ hơn: người dùng có thể xem `wal_log_entry` và gọi replay endpoint để xử lý pending records.

**Evidence in project:** `writeWal()` lưu `dataBefore`, `dataAfter`, `nodeId`, `idempotencyKey`, `status=PENDING`; sau commit chuyển sang `COMMITTED`. `replayPendingWal()` kiểm tra transaction tương ứng để commit hoặc rollback log.

### 4.6 Event-Time Window Và Benchmark

**Design choice:** Benchmark dùng `eventTimeMs`, `processingTimeMs`, tumbling window, watermark, out-of-order và late-event metrics.

**Theory:** Mô hình chi phí phân tán phân biệt local CPU/I/O và communication cost; response time có thể khác total cost khi có parallelism. Với stream processing, thứ tự xử lý không luôn trùng thứ tự phát sinh sự kiện, nên event-time là cơ sở đúng hơn processing-time khi đánh giá duplicate và late events.

**Justification:** Payment retry có thể đến muộn hoặc out-of-order. Nếu dedup chỉ dựa vào processing order, hệ thống dễ xử lý sai retry cũ đến sau. Dùng event-time giúp benchmark phản ánh luồng sự kiện thực tế. Các metrics p50/p95/p99 cũng cần thiết vì average latency không thể hiện tail latency.

### 4.7 H2 File Database Cho Demo Phân Tán

**Design choice:** Dùng H2 file database thay vì PostgreSQL/MySQL cluster.

**Theory:** Allocation trong CSDL phân tán liên quan đến việc đặt fragment tại các site. Với mục tiêu học thuật/demo, mỗi local database file đại diện cho một site độc lập là đủ để chứng minh phân mảnh, local state, WAL và recovery.

**Justification:** H2 giúp project chạy được trên localhost, không cần Docker hoặc server ngoài. Điều này giảm chi phí triển khai nhưng vẫn giữ được đặc tính quan trọng: mỗi node có durable local state riêng và global dedup state tách biệt. Đây là lựa chọn phù hợp với scope môn học.

## 5. Phương Pháp Đánh Giá

### 5.1 Kịch Bản Thực Nghiệm

| Kịch bản | Mục tiêu | Kỳ vọng |
| --- | --- | --- |
| EOS duplicate cùng node | Chứng minh idempotency cục bộ | 1 success, các retry bị reject |
| EOS duplicate cross-node | Chứng minh global dedup | Node khác vẫn reject cùng key |
| ALO duplicate | Làm baseline | Duplicate có thể tạo thêm transaction |
| Concurrent duplicate | Kiểm tra race condition | Chỉ một request commit |
| Node failure | Kiểm tra failure handling | Node failed trả 503, không ghi transaction |
| Benchmark compare | So sánh ALO/EOS | EOS có duplicatesBlocked, ALO không chặn duplicate |
| WAL replay | Kiểm tra recovery | Pending WAL được commit/rollback hợp lệ |

### 5.2 Chỉ Số Đo Lường

| Metric | Ý nghĩa | Liên hệ lý thuyết |
| --- | --- | --- |
| `elapsedMs` | Thời gian xử lý payment end-to-end | Response time |
| `dedupLookupMs` | Chi phí coordination với global dedup | Communication/control cost |
| `walWriteMs` | Chi phí durability | I/O cost |
| `avg/p50/p95/p99LatencyMs` | Độ trễ trung bình và tail latency | Cost model và response-time analysis |
| `tps` | Throughput theo run/window | Hiệu năng hệ phân tán |
| `duplicatesBlocked` | Số duplicate bị EOS chặn | Correctness của idempotency |
| `outOfOrderCount`, `lateEvents` | Sự kiện lệch thứ tự/thời gian | Event-time correctness |
| `walEntries`, `walCommitted` | Trạng thái log | Reliability/recovery evidence |

## 6. Kết Quả Và Bằng Chứng Trong Project

### 6.1 Automated Tests

Bộ test `PaymentStreamServiceTests` có 6 test case và hiện chạy thành công: 6 tests, 0 failures, 0 errors. Các test bao phủ trực tiếp các yêu cầu chính:

| Test | Ý nghĩa |
| --- | --- |
| `eosRejectsDuplicateAndCommitsOnce` | EOS reject duplicate và chỉ tạo 1 transaction |
| `aloProcessesDuplicates` | ALO xử lý duplicate, dùng làm baseline |
| `outOfOrderRetryUsesEventTimeWindow` | Retry out-of-order vẫn bị dedup |
| `concurrentEosDuplicateStillCommitsOnce` | Duplicate đồng thời chỉ commit một lần |
| `nodeFailureRejectsPayment` | Node failed trả lỗi và không commit |
| `benchmarkShowsEosBlocksDuplicatesAndAloDoesNot` | Benchmark chứng minh EOS chặn duplicate còn ALO thì không |

### 6.2 Phân Tích Theo Mô Hình Chi Phí

Áp dụng mô hình của Özsu & Valduriez vào project:

```text
EOS_Total_Time =
  local_validation_CPU
  + global_dedup_lookup_or_claim
  + WAL_write_IO
  + account_update_IO
  + transaction_insert_IO
  + gateway_node_HTTP_cost
```

Thiết kế EOS có thêm `global_dedup_lookup_or_claim` và `WAL_write_IO` so với ALO. Đây là overhead có chủ ý. Nếu bỏ overhead này, hệ thống nhanh hơn trong trường hợp không lỗi nhưng sai khi có retry. Với payment system, correctness quan trọng hơn việc giảm vài millisecond latency. Benchmark ALO/EOS cho phép chứng minh trade-off này bằng số liệu: ALO có ít bước hơn nhưng không chặn duplicate; EOS thêm coordination/logging nhưng giữ invariant “một idempotency key chỉ commit một lần”.

### 6.3 Invariant Cần Bảo Toàn

| Invariant | Cơ chế bảo vệ | Bằng chứng |
| --- | --- | --- |
| Một `idempotency_key` chỉ commit một lần | Global dedup primary key + atomic claim | Duplicate tests |
| Không cập nhật account khi node failed | Node status check trước xử lý | `nodeFailureRejectsPayment` |
| Có thể kiểm tra recovery sau lỗi | WAL `PENDING/COMMITTED/ROLLED_BACK` | WAL table + replay endpoint |
| Benchmark tái lập được | Seed, event count, duplicate rate, window size | `BenchmarkCommand` |
| Dashboard không phụ thuộc topology node | Gateway aggregate API | React dashboard gọi Gateway |

## 7. Hạn Chế Và Hướng Mở Rộng

Thiết kế hiện tại phù hợp cho demo môn học nhưng còn một số giới hạn. Thứ nhất, global dedup store là coordination point tập trung; nếu production cần high availability thì phải replication hoặc consensus cho dedup table. Thứ hai, account state được seed giống nhau giữa node để đơn giản hóa demo, chưa phải mô hình account sharding/replication đầy đủ. Thứ ba, hệ thống chưa cài đặt distributed commit protocol kiểu 2PC cho transfer chạm nhiều shard account; project tập trung vào EOS/idempotency cho payment request. Thứ tư, benchmark chạy local nên network latency thật chưa được mô phỏng sâu như WAN; có thể mở rộng bằng latency injection hoặc Docker network emulation.

Hướng mở rộng hợp lý là: phân vùng account theo account id, thêm replicated dedup store, mô phỏng WAN latency bằng delay profile, thêm biểu đồ cost breakdown theo công thức `T_CPU/T_IO/T_MSG/T_TR`, và triển khai recovery scenario có crash thật giữa WAL và commit.

## 8. Kết Luận

Các quyết định thiết kế của EOS Payment System phù hợp với lý thuyết CSDL phân tán của Özsu & Valduriez. Gateway cung cấp transparency; node-based H2 state thể hiện horizontal fragmentation/allocation; global deduplication store giải quyết distributed data control cho idempotency; WAL và replay endpoint hỗ trợ reliability/recovery; benchmark event-time/windowing giúp đánh giá đúng duplicate, late event, latency và throughput.

Trade-off chính của project là chấp nhận thêm coordination và WAL overhead để đạt correctness. Với hệ thống thanh toán, đây là lựa chọn hợp lý: một giao dịch nhanh nhưng có thể trừ tiền hai lần là không chấp nhận được, trong khi EOS làm chậm hơn nhưng bảo toàn invariant nghiệp vụ quan trọng nhất. Do đó, project không chỉ mô phỏng một kiến trúc phân tán mà còn thể hiện rõ cách lý thuyết CSDL phân tán được áp dụng vào thiết kế hệ thống thanh toán tin cậy.

## Tài Liệu Tham Khảo

1. M. Tamer Özsu, Patrick Valduriez. Principles of Distributed Database Systems, 4th Edition. Springer.
2. EOS Payment Project Source Code: `backend/src/main/java/com/eos/payment`.
3. EOS Payment Automated Tests: `backend/src/test/java/com/eos/payment/PaymentStreamServiceTests.java`.
4. Project README: `README.md`.
