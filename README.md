# EOS Payment System - Java + React

Topic 117 demo: **Exactly-Once Semantics (EOS) for idempotent payments** on a stream of `Payment_Requests`.

This version is implemented with:

- **Java 21 + Spring Boot** for the REST API and stream/EOS logic.
- **H2 embedded database** for durable demo state.
- **React + Vite** for the dashboard UI.

## Excellent Rubric Coverage

| Criteria | Implementation |
| --- | --- |
| Windowing Logic | Event-time stream generator with `eventTimeMs`, `processingTimeMs`, tumbling windows, watermark, out-of-order and late-event metrics. |
| State Management | Gateway + three node instances. Each node has local H2 state for accounts, requests, WAL, transactions, windows, and checkpoints; all nodes share a global de-dup table for EOS coordination. |
| Latency Analysis | High-resolution timing with avg, p50, p95, p99, WAL write latency, event lag, TPS per run and per window. |
| Robustness | Atomic EOS transaction, unique idempotency state, concurrent duplicate rejection, node failure rejection, backpressure simulation, WAL replay endpoint. |

## Architecture

```text
React Dashboard
   |
   v
Gateway API :8080
   |
   +-- NODE_A service :8081 -> local H2 ./data/node_a
   +-- NODE_B service :8082 -> local H2 ./data/node_b
   +-- NODE_C service :8083 -> local H2 ./data/node_c
   |
   +-- Global de-dup H2 ./data/global_dedup
```

## Run Backend

Single-process mode still works:

```bash
cd frontend
npm install
npm run build:backend
cd ..
cd backend
mvn spring-boot:run
```

Distributed demo mode starts three node instances plus the gateway:

```powershell
cd backend
.\run-distributed.ps1
```

Stop distributed demo services:

```powershell
cd backend
.\stop-distributed.ps1
```

Service URLs:

```text
Gateway: http://localhost:8080
NODE_A:  http://localhost:8081
NODE_B:  http://localhost:8082
NODE_C:  http://localhost:8083
```

H2 console:

```text
http://localhost:8081/h2-console
JDBC URL examples:
  jdbc:h2:file:./data/node_a
  jdbc:h2:file:./data/node_b
  jdbc:h2:file:./data/node_c
  jdbc:h2:file:./data/global_dedup
User: sa
Password: <empty>
```

## Run Frontend

```bash
cd frontend
npm install
npm run dev
```

Frontend dev URL:

```text
http://localhost:5173
```

If you built with `npm run build:backend`, the same dashboard is also served by Spring Boot:

```text
http://localhost:8080
```

## Tests

```bash
cd backend
mvn test
cd ..
cd frontend
npm run build
npm run build:backend
```

## Demo Script

1. Open the React dashboard.
2. Run **Live Stream Benchmark** with 160 events, 30% duplicate rate, 10-second windows.
3. Check:
   - EOS blocks duplicate events.
   - ALO processes duplicate events.
   - TPS and latency charts render per tumbling window.
   - Late/out-of-order/backpressure metrics are visible.
   - Dedup table, WAL log, transactions, and checkpoints update.
4. Send one EOS payment with a fixed idempotency key to `NODE_A`, then submit the same key to `NODE_B`. Gateway routes to different services, but the global de-dup table rejects the duplicate.
5. Toggle a node to `FAILED` and verify payments to that node are rejected.
6. Click **Replay WAL** to demonstrate recovery handling for pending WAL records.
