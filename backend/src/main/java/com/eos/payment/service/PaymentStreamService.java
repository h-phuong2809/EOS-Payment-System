package com.eos.payment.service;

import com.eos.payment.dto.BenchmarkCommand;
import com.eos.payment.dto.PaymentCommand;
import com.eos.payment.dto.RetrySimulationCommand;
import com.eos.payment.model.*;
import com.eos.payment.repository.*;
import jakarta.annotation.PostConstruct;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Service
@ConditionalOnProperty(name = "app.role", havingValue = "node", matchIfMissing = true)
public class PaymentStreamService implements PaymentOperations {
    private static final int DEFAULT_EVENT_COUNT = 160;
    private static final double DEFAULT_DUPLICATE_RATE = 0.3;
    private static final int DEFAULT_WINDOW_SIZE_SEC = 10;
    private static final int DEFAULT_SEED = 117;
    private static final int DEFAULT_STREAM_DELAY_MS = 25;
    private static final int STREAM_INTERVAL_MS = 250;

    private final AccountRepository accounts;
    private final PaymentRequestRepository requests;
    private final DeduplicationRepository dedup;
    private final WalLogRepository wal;
    private final PaymentTransactionRepository transactions;
    private final ProcessingNodeRepository nodes;
    private final BenchmarkRunRepository runs;
    private final StreamWindowRepository windows;
    private final ProcessorCheckpointRepository checkpoints;
    private final TransactionTemplate tx;
    private final GlobalDeduplicationStore globalDedup;
    private final AtomicLong walSequence = new AtomicLong();
    private final Object paymentLock = new Object();
    private volatile long walDelayMillis;
    private final RestTemplate http = new RestTemplate();

    @Value("${app.dedup-window-ms}")
    private long dedupWindowMs;
    @Value("${app.max-event-age-ms}")
    private long maxEventAgeMs;
    @Value("${app.allowed-lateness-ms}")
    private long allowedLatenessMs;
    @Value("${app.max-in-flight-events}")
    private int maxInFlightEvents;
    @Value("${app.node-id:}")
    private String configuredNodeId;

    public PaymentStreamService(
            AccountRepository accounts,
            PaymentRequestRepository requests,
            DeduplicationRepository dedup,
            WalLogRepository wal,
            PaymentTransactionRepository transactions,
            ProcessingNodeRepository nodes,
            BenchmarkRunRepository runs,
            StreamWindowRepository windows,
            ProcessorCheckpointRepository checkpoints,
            TransactionTemplate tx,
            GlobalDeduplicationStore globalDedup) {
        this.accounts = accounts;
        this.requests = requests;
        this.dedup = dedup;
        this.wal = wal;
        this.transactions = transactions;
        this.nodes = nodes;
        this.runs = runs;
        this.windows = windows;
        this.checkpoints = checkpoints;
        this.tx = tx;
        this.globalDedup = globalDedup;
    }

    @PostConstruct
    @Transactional
    public void seed() {
        walSequence.set(wal.findAll().stream().mapToLong(w -> w.sequenceNumber).max().orElse(0));
        if (accounts.count() == 0) {
            accounts.saveAll(List.of(
                    new Account("ACC001", "Nguyen Van An", new BigDecimal("5000000")),
                    new Account("ACC002", "Tran Thi Bich", new BigDecimal("3000000")),
                    new Account("ACC003", "Le Hoang Cuong", new BigDecimal("8000000")),
                    new Account("ACC004", "Pham Thi Dung", new BigDecimal("2000000")),
                    new Account("ACC005", "Hoang Van Enh", new BigDecimal("6500000"))));
        }
        if (nodes.count() == 0) {
            nodes.saveAll(nodeDefinitions());
        }
    }

    @Transactional
    public void resetOperationalState() {
        windows.deleteAll();
        runs.deleteAll();
        checkpoints.deleteAll();
        wal.deleteAll();
        dedup.deleteAll();
        globalDedup.deleteAll();
        transactions.deleteAll();
        requests.deleteAll();
        accounts.saveAll(List.of(
                new Account("ACC001", "Nguyen Van An", new BigDecimal("5000000")),
                new Account("ACC002", "Tran Thi Bich", new BigDecimal("3000000")),
                new Account("ACC003", "Le Hoang Cuong", new BigDecimal("8000000")),
                new Account("ACC004", "Pham Thi Dung", new BigDecimal("2000000")),
                new Account("ACC005", "Hoang Van Enh", new BigDecimal("6500000"))));
        nodes.saveAll(nodeDefinitions());
        walSequence.set(0);
    }

    @Transactional
    public ProcessingNode toggleNode(String nodeId) {
        ProcessingNode node = nodes.findById(nodeId).orElseThrow();
        node.status = "ACTIVE".equals(node.status) ? "FAILED" : "ACTIVE";
        node.lastHeartbeat = Instant.now();
        return nodes.save(node);
    }

    public Map<String, Object> submitPayment(PaymentCommand command) {
        synchronized (paymentLock) {
            return tx.execute(status -> submitPaymentInTransaction(command));
        }
    }

    private Map<String, Object> submitPaymentInTransaction(PaymentCommand command) {
        String mode = normalizeMode(command.mode());
        String nodeId = effectiveNodeId(command.nodeId());
        ProcessingNode node = nodes.findById(nodeId).orElseThrow();
        if ("FAILED".equals(node.status)) {
            return Map.of("status", "ERROR", "message", nodeId + " is FAILED. Payment rejected.", "httpStatus", 503);
        }

        String key = defaultText(command.idempotencyKey(), UUID.randomUUID().toString());
        long eventTimeMs = command.eventTimeMs() == null ? System.currentTimeMillis() : command.eventTimeMs();
        PaymentRequest request = requests.save(newRequest(command.senderAccount(), command.receiverAccount(), BigDecimal.valueOf(command.amount()),
                key, command.retryCount() == null ? 0 : command.retryCount(), nodeId, eventTimeMs));

        Map<String, Object> result = "EOS".equals(mode)
                ? processEos(request)
                : processAlo(request);
        node.processedCount += 1;
        node.lastHeartbeat = Instant.now();
        result.put("requestId", request.requestId);
        return result;
    }

    private Map<String, Object> processEos(PaymentRequest request) {
        long start = System.nanoTime();
        validateFreshEvent(request.eventTimeMs);
        // Uses EVENT-TIME (request.eventTimeMs), not processing-time,
        // so out-of-order retries arriving late are still correctly suppressed.
        globalDedup.deleteExpired(request.eventTimeMs - dedupWindowMs);

        long lookupStart = System.nanoTime();
        Optional<DeduplicationRecord> existing = globalDedup.findById(request.idempotencyKey);
        double dedupLookupMs = elapsedMs(lookupStart);
        if (existing.isPresent()) {
            request.status = "DUPLICATE_REJECTED";
            request.processedAt = Instant.now();
            requests.save(request);
            return mutableMap(
                    "status", "DUPLICATE_REJECTED",
                    "message", "Payment already processed (idempotency key: " + request.idempotencyKey + ")",
                    "eventTimeMs", request.eventTimeMs,
                    "duplicateReason", "IDEMPOTENCY_KEY",
                    "dedupLookupMs", round(dedupLookupMs),
                    "elapsedMs", round(elapsedMs(start)),
                    "eosApplied", true);
        }

        Account sender = accounts.findById(request.senderAccount).orElseThrow();
        Account receiver = accounts.findById(request.receiverAccount).orElseThrow();
        if (sender.balance.compareTo(request.amount) < 0) {
            request.status = "ERROR";
            request.processedAt = Instant.now();
            requests.save(request);
            return mutableMap("status", "ERROR", "message", "Insufficient balance", "elapsedMs", round(elapsedMs(start)));
        }

        DeduplicationRecord claimed = globalDedup.claimOrExisting(request.idempotencyKey, request.requestId, request.nodeId, request.eventTimeMs);
        if (!request.requestId.equals(claimed.originalRequest)) {
            request.status = "DUPLICATE_REJECTED";
            request.processedAt = Instant.now();
            requests.save(request);
            return mutableMap(
                    "status", "DUPLICATE_REJECTED",
                    "message", "Payment already processed globally (idempotency key: " + request.idempotencyKey + ")",
                    "eventTimeMs", request.eventTimeMs,
                    "duplicateReason", "GLOBAL_IDEMPOTENCY_KEY",
                    "dedupLookupMs", round(dedupLookupMs),
                    "elapsedMs", round(elapsedMs(start)),
                    "eosApplied", true);
        }

        String before = "{\"sender\":" + sender.balance + ",\"receiver\":" + receiver.balance + "}";
        String after = "{\"sender\":" + sender.balance.subtract(request.amount) + ",\"receiver\":" + receiver.balance.add(request.amount) + "}";
        long walStart = System.nanoTime();
        WalLogEntry entry = writeWal(request, before, after);
        double walWriteMs = elapsedMs(walStart);

        sender.balance = sender.balance.subtract(request.amount);
        receiver.balance = receiver.balance.add(request.amount);
        sender.updatedAt = Instant.now();
        receiver.updatedAt = Instant.now();
        accounts.save(sender);
        accounts.save(receiver);

        PaymentTransaction txn = newTransaction(request, request.idempotencyKey);
        transactions.save(txn);

        globalDedup.completeSuccess(request.idempotencyKey,
                "{\"transactionId\":\"" + txn.transactionId + "\",\"amount\":" + request.amount + "}",
                request.nodeId,
                request.eventTimeMs);

        request.status = "PROCESSED";
        request.processedAt = Instant.now();
        requests.save(request);
        entry.status = "COMMITTED";
        entry.committedAt = Instant.now();
        wal.save(entry);

        return mutableMap(
                "status", "SUCCESS",
                "transactionId", txn.transactionId,
                "message", "Payment processed successfully (EOS)",
                "eventTimeMs", request.eventTimeMs,
                "elapsedMs", round(elapsedMs(start)),
                "dedupLookupMs", round(dedupLookupMs),
                "walWriteMs", round(walWriteMs),
                "eosApplied", true,
                "walSeq", entry.sequenceNumber);
    }

    private Map<String, Object> processAlo(PaymentRequest request) {
        long start = System.nanoTime();
        Account sender = accounts.findById(request.senderAccount).orElseThrow();
        Account receiver = accounts.findById(request.receiverAccount).orElseThrow();
        if (sender.balance.compareTo(request.amount) < 0) {
            request.status = "ERROR";
            request.processedAt = Instant.now();
            requests.save(request);
            return mutableMap("status", "ERROR", "message", "Insufficient balance", "elapsedMs", round(elapsedMs(start)));
        }
        sender.balance = sender.balance.subtract(request.amount);
        receiver.balance = receiver.balance.add(request.amount);
        sender.updatedAt = Instant.now();
        receiver.updatedAt = Instant.now();
        accounts.save(sender);
        accounts.save(receiver);
        PaymentTransaction txn = newTransaction(request, request.idempotencyKey + "_alo");
        transactions.save(txn);
        request.status = "PROCESSED";
        request.processedAt = Instant.now();
        requests.save(request);
        return mutableMap(
                "status", "SUCCESS",
                "transactionId", txn.transactionId,
                "message", "Payment processed (At-Least-Once - NO dedup check)",
                "eventTimeMs", request.eventTimeMs,
                "elapsedMs", round(elapsedMs(start)),
                "eosApplied", false);
    }

    public Map<String, Object> simulateRetries(RetrySimulationCommand command) {
        int retries = Math.max(1, Math.min(command.retries() == null ? 3 : command.retries(), 20));
        String mode = normalizeMode(command.mode());
        String sender = defaultText(command.sender(), "ACC001");
        String receiver = defaultText(command.receiver(), "ACC002");
        double amount = command.amount() == null ? 100000 : command.amount();
        String key = UUID.randomUUID().toString();
        List<Map<String, Object>> results = new ArrayList<>();
        for (int i = 0; i < retries; i++) {
            String nodeId = currentNodeConfigured()
                    ? configuredNodeId
                    : List.of("NODE_A", "NODE_B", "NODE_C").get(ThreadLocalRandom.current().nextInt(3));
            PaymentCommand payment = new PaymentCommand(sender, receiver, amount, nodeId, mode, key, i,
                    System.currentTimeMillis() + ThreadLocalRandom.current().nextInt(-1500, 1501));
            Map<String, Object> result = submitPayment(payment);
            result.put("attempt", i + 1);
            result.put("nodeId", nodeId);
            results.add(result);
        }
        return Map.of("idempotencyKey", key, "results", results);
    }

    public Map<String, Object> runBenchmark(BenchmarkCommand command) {
        return runBenchmarkInternal(command, null);
    }

    public SseEmitter streamBenchmark(BenchmarkCommand command) {
        SseEmitter emitter = new SseEmitter(0L);
        Thread worker = new Thread(() -> {
            try {
                runBenchmarkInternal(command, (eventName, payload) -> sendToEmitter(emitter, eventName, payload));
                emitter.complete();
            } catch (Exception e) {
                try {
                    sendToEmitter(emitter, "error", mutableMap("message", e.getMessage() == null ? "Benchmark stream failed" : e.getMessage()));
                } catch (Exception ignored) {
                    // Client may already be disconnected.
                }
                emitter.complete();
            }
        }, "benchmark-sse-" + UUID.randomUUID());
        worker.setDaemon(true);
        worker.start();
        return emitter;
    }

    private Map<String, Object> runBenchmarkInternal(BenchmarkCommand command, BenchmarkSink sink) {
        BenchmarkSettings settings = BenchmarkSettings.from(command);
        List<StreamEvent> events = makePaymentStream(
                settings.eventCount(),
                settings.duplicateRate(),
                settings.seed(),
                STREAM_INTERVAL_MS,
                settings.distributed());
        Map<String, Object> dataset = benchmarkDataset(events, settings);
        sendIfPresent(sink, "started", mutableMap(
                "dataset", dataset,
                "runs", List.of(),
                "windows", List.of(),
                "checkpoints", List.of()));

        windows.deleteAll();
        runs.deleteAll();
        checkpoints.deleteAll();

        if (settings.distributed()) {
            resetAllNodesOperationalState();
        } else {
            resetOperationalTablesOnly();
        }
        runStreamBenchmark("ALO", events, settings, sink);

        if (settings.distributed()) {
            resetAllNodesOperationalState();
        } else {
            resetOperationalTablesOnly();
        }
        runStreamBenchmark("EOS", events, settings, sink);

        Map<String, Object> payload = latestBenchmarkPayload();
        payload.put("dataset", dataset);
        sendIfPresent(sink, "complete", payload);
        return payload;
    }

    private void runStreamBenchmark(String mode, List<StreamEvent> events, BenchmarkSettings settings, BenchmarkSink sink) {
        String runId = UUID.randomUUID().toString();
        long start = System.nanoTime();
        List<Double> latencies = new ArrayList<>();
        List<Double> successLatencies = new ArrayList<>();
        List<Double> duplicateLatencies = new ArrayList<>();
        List<Double> walLatencies = new ArrayList<>();
        List<Long> eventLags = new ArrayList<>();
        Map<Long, WindowAccumulator> windowState = new TreeMap<>();
        long previousEventTime = Long.MIN_VALUE;
        long maxEventTime = Long.MIN_VALUE;
        int processedSuccess = 0;
        int duplicatesBlocked = 0;
        int errors = 0;
        int outOfOrder = 0;
        int lateEvents = 0;
        int backpressureDelayed = 0;
        int walEntriesObserved = 0;

        for (int i = 0; i < events.size(); i++) {
            StreamEvent event = events.get(i);
            if (event.eventTimeMs < previousEventTime) {
                outOfOrder++;
            }
            previousEventTime = event.eventTimeMs;
            maxEventTime = Math.max(maxEventTime, event.eventTimeMs);
            long watermark = maxEventTime - allowedLatenessMs;
            boolean late = event.eventTimeMs < watermark;
            if (late) {
                lateEvents++;
            }
            if ((i % (maxInFlightEvents + 1)) == maxInFlightEvents) {
                backpressureDelayed++;
            }
            if (i > 0 && i % 100 == 0) {
                saveCheckpoint(mode, maxEventTime, maxEventTime - allowedLatenessMs, windowState.size());
            }
            eventLags.add(Math.max(0, event.processingTimeMs - event.eventTimeMs));

            long wStart = windowKey(event.eventTimeMs, settings.windowSizeSec());
            WindowAccumulator acc = windowState.computeIfAbsent(wStart, ignored -> new WindowAccumulator());
            acc.totalEvents++;
            acc.uniqueKeys.add(event.idempotencyKey);
            if (late) {
                acc.lateEvents++;
            }

            PaymentCommand payment = new PaymentCommand(event.senderAccount, event.receiverAccount, event.amount.doubleValue(),
                    event.nodeId, mode, event.idempotencyKey, event.retryCount, event.eventTimeMs);

            Map<String, Object> result;
            if (settings.distributed()) {
                if (currentNodeConfigured() && configuredNodeId.equals(event.nodeId)) {
                    result = submitPayment(payment);
                } else {
                    result = postToNode(event.nodeId, "/payment", payment);
                }
            } else {
                result = submitPayment(payment);
            }

            double latency = ((Number) result.getOrDefault("elapsedMs", 0)).doubleValue();
            latencies.add(latency);
            acc.latencies.add(latency);
            if (result.containsKey("walWriteMs")) {
                walLatencies.add(((Number) result.get("walWriteMs")).doubleValue());
            }
            String status = String.valueOf(result.get("status"));
            if ("SUCCESS".equals(status)) {
                processedSuccess++;
                successLatencies.add(latency);
                acc.processedSuccess++;
                if (result.containsKey("walSeq")) {
                    walEntriesObserved++;
                    acc.walEntries++;
                }
            } else if ("DUPLICATE_REJECTED".equals(status)) {
                duplicatesBlocked++;
                duplicateLatencies.add(latency);
                acc.duplicatesBlocked++;
            } else {
                errors++;
                acc.errors++;
            }

            sendIfPresent(sink, "progress", benchmarkProgressPayload(
                    runId,
                    mode,
                    i + 1,
                    events.size(),
                    status,
                    late,
                    watermark,
                    processedSuccess,
                    duplicatesBlocked,
                    errors,
                    outOfOrder,
                    lateEvents,
                    backpressureDelayed,
                    wStart,
                    settings.windowSizeSec(),
                    acc));
            if (sink != null && settings.streamDelayMs() > 0) {
                sleepQuietly(settings.streamDelayMs());
            }
        }

        double durationMs = Math.max(elapsedMs(start), 1.0);
        int uniquePayments = events.stream().map(e -> e.idempotencyKey).collect(Collectors.toSet()).size();
        long walEntries = settings.distributed() ? walEntriesObserved : wal.count();
        long walCommitted = settings.distributed() ? walEntriesObserved : wal.countByStatus("COMMITTED");
        BenchmarkRun run = new BenchmarkRun();
        run.runId = runId;
        run.mode = mode;
        run.eventCount = events.size();
        run.uniquePayments = uniquePayments;
        run.duplicateEvents = events.size() - uniquePayments;
        run.processedSuccess = processedSuccess;
        run.duplicatesBlocked = duplicatesBlocked;
        run.errors = errors;
        run.durationMs = round(durationMs);
        run.tps = round(events.size() / (durationMs / 1000.0));
        run.avgLatencyMs = round(avg(latencies));
        run.p50LatencyMs = round(percentile(latencies, 50));
        run.p95LatencyMs = round(percentile(latencies, 95));
        run.p99LatencyMs = round(percentile(latencies, 99));
        run.duplicateLatencyMs = round(avg(duplicateLatencies));
        run.newLatencyMs = round(avg(successLatencies));
        run.avgEventLagMs = round(eventLags.stream().mapToLong(Long::longValue).average().orElse(0));
        run.outOfOrderCount = outOfOrder;
        run.lateEvents = lateEvents;
        run.backpressureDelayed = backpressureDelayed;
        run.walWriteLatencyMs = round(avg(walLatencies));
        run.walEntries = (int) walEntries;
        run.walCommitted = (int) walCommitted;
        run.walOverheadPct = round((walEntries / Math.max(processedSuccess, 1.0)) * 100.0);
        run.windowSizeSec = settings.windowSizeSec();
        run.watermarkMs = maxEventTime - allowedLatenessMs;
        run.createdAt = Instant.now();
        runs.save(run);

        List<StreamWindow> modeWindows = new ArrayList<>();
        for (Map.Entry<Long, WindowAccumulator> entry : windowState.entrySet()) {
            WindowAccumulator acc = entry.getValue();
            StreamWindow window = new StreamWindow();
            window.runId = runId;
            window.mode = mode;
            window.windowStartMs = entry.getKey();
            window.windowEndMs = entry.getKey() + settings.windowSizeSec() * 1000L;
            window.totalEvents = acc.totalEvents;
            window.uniquePayments = acc.uniqueKeys.size();
            window.processedSuccess = acc.processedSuccess;
            window.duplicatesBlocked = acc.duplicatesBlocked;
            window.errors = acc.errors;
            window.lateEvents = acc.lateEvents;
            window.avgLatencyMs = round(avg(acc.latencies));
            window.avgEventLagMs = round(events.stream()
                    .filter(e -> windowKey(e.eventTimeMs, settings.windowSizeSec()) == entry.getKey())
                    .mapToLong(e -> Math.max(0, e.processingTimeMs - e.eventTimeMs))
                    .average().orElse(0));
            window.p50LatencyMs = round(percentile(acc.latencies, 50));
            window.p95LatencyMs = round(percentile(acc.latencies, 95));
            window.p99LatencyMs = round(percentile(acc.latencies, 99));
            window.tps = round(acc.totalEvents / (double) settings.windowSizeSec());
            window.walEntries = acc.walEntries;
            window.createdAt = Instant.now();
            windows.save(window);
            modeWindows.add(window);
        }

        saveCheckpoint(mode, maxEventTime, maxEventTime - allowedLatenessMs, windowState.size());
        sendIfPresent(sink, "run", mutableMap("mode", mode, "run", run, "windows", modeWindows));
    }

    private void saveCheckpoint(String mode, long lastEventTimeMs, long watermarkMs, int windowsPersisted) {
        ProcessorCheckpoint checkpoint = new ProcessorCheckpoint();
        checkpoint.mode = mode;
        checkpoint.lastEventTimeMs = lastEventTimeMs;
        checkpoint.watermarkMs = watermarkMs;
        checkpoint.windowsPersisted = windowsPersisted;
        checkpoint.updatedAt = Instant.now();
        checkpoints.save(checkpoint);
    }

    public void setWalDelayMillis(long walDelayMillis) {
        this.walDelayMillis = Math.max(0, walDelayMillis);
    }

    @Transactional
    public Map<String, Object> replayPendingWal() {
        List<WalLogEntry> pending = wal.findByStatusOrderBySequenceNumberAsc("PENDING");
        int rolledBack = 0;
        for (WalLogEntry entry : pending) {
            boolean committed = transactions.findAll().stream()
                    .anyMatch(t -> t.paymentRequestId.equals(entry.paymentId) || t.idempotencyKey.equals(entry.idempotencyKey));
            entry.status = committed ? "COMMITTED" : "ROLLED_BACK";
            entry.committedAt = Instant.now();
            wal.save(entry);
            if (!committed) {
                rolledBack++;
            }
        }
        return Map.of("pendingFound", pending.size(), "rolledBack", rolledBack, "replayedAt", Instant.now().toString());
    }

    public Map<String, Object> latestBenchmarkPayload() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("runs", runs.findAllByOrderByCreatedAtDescModeAsc());
        payload.put("windows", windows.findAllByOrderByWindowStartMsAscModeAsc());
        payload.put("checkpoints", checkpoints.findAll());
        return payload;
    }

    public Map<String, Object> stats() {
        BenchmarkRun eos = runs.findFirstByModeOrderByCreatedAtDesc("EOS");
        BenchmarkRun alo = runs.findFirstByModeOrderByCreatedAtDesc("ALO");
        return mutableMap(
                "totalRequests", requests.count(),
                "processed", requests.countByStatus("PROCESSED"),
                "duplicatesBlocked", requests.countByStatus("DUPLICATE_REJECTED"),
                "transactions", transactions.count(),
                "walEntries", wal.count(),
                "walCommitted", wal.countByStatus("COMMITTED"),
                "dedupTableSize", globalDedup.count(),
                "walOverheadPct", round((wal.count() / Math.max(requests.countByStatus("PROCESSED"), 1.0)) * 100.0),
                "latestEos", runMap(eos),
                "latestAlo", runMap(alo));
    }

    public List<Account> accounts() {
        return accounts.findAll();
    }

    public List<ProcessingNode> nodes() {
        return nodes.findAll();
    }

    public List<DeduplicationRecord> deduplicationTable() {
        return globalDedup.findAll();
    }

    public List<WalLogEntry> walLog() {
        return wal.findTop50ByOrderBySequenceNumberDesc();
    }

    public List<PaymentTransaction> transactions() {
        return transactions.findTop50ByOrderByExecutedAtDesc();
    }

    private PaymentRequest newRequest(String sender, String receiver, BigDecimal amount, String key, int retryCount, String nodeId, long eventTimeMs) {
        PaymentRequest request = new PaymentRequest();
        request.requestId = UUID.randomUUID().toString();
        request.idempotencyKey = key;
        request.senderAccount = sender;
        request.receiverAccount = receiver;
        request.amount = amount;
        request.status = "PENDING";
        request.retryCount = retryCount;
        request.createdAt = Instant.now();
        request.nodeId = nodeId;
        request.eventTimeMs = eventTimeMs;
        request.processingTimeMs = System.currentTimeMillis();
        return request;
    }

    private PaymentTransaction newTransaction(PaymentRequest request, String transactionKey) {
        PaymentTransaction txn = new PaymentTransaction();
        txn.transactionId = UUID.randomUUID().toString();
        txn.paymentRequestId = request.requestId;
        txn.idempotencyKey = transactionKey;
        txn.senderAccount = request.senderAccount;
        txn.receiverAccount = request.receiverAccount;
        txn.amount = request.amount;
        txn.executedAt = Instant.now();
        txn.nodeId = request.nodeId;
        return txn;
    }

    // Write-Ahead Log: persisted BEFORE state change to enable crash recovery.
    // Overhead: ~2 writes per EOS success in this implementation; ALO skips WAL.
    private WalLogEntry writeWal(PaymentRequest request, String before, String after) {
        if (walDelayMillis > 0) {
            try {
                Thread.sleep(walDelayMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        WalLogEntry entry = new WalLogEntry();
        entry.sequenceNumber = walSequence.incrementAndGet();
        entry.operationType = "TRANSFER";
        entry.paymentId = request.requestId;
        entry.idempotencyKey = request.idempotencyKey;
        entry.dataBefore = before;
        entry.dataAfter = after;
        entry.nodeId = request.nodeId;
        entry.status = "PENDING";
        entry.timestamp = Instant.now();
        return wal.save(entry);
    }

    private List<StreamEvent> makePaymentStream(int eventCount, double duplicateRate, int seed, int intervalMs, boolean distributed) {
        Random rng = new Random(seed);
        List<StreamEvent> originals = new ArrayList<>();
        List<StreamEvent> events = new ArrayList<>();
        List<String> accountIds = List.of("ACC001", "ACC002", "ACC003", "ACC004", "ACC005");
        long baseMs = System.currentTimeMillis();
        for (int i = 0; i < eventCount; i++) {
            StreamEvent event;
            if (!originals.isEmpty() && rng.nextDouble() < duplicateRate) {
                StreamEvent original = originals.get(rng.nextInt(originals.size()));
                event = original.copy();
                event.eventId = UUID.randomUUID().toString();
                event.retryCount = original.retryCount + 1;
                event.duplicate = true;
            } else {
                String sender = accountIds.get(rng.nextInt(accountIds.size()));
                List<String> receivers = accountIds.stream().filter(a -> !a.equals(sender)).toList();
                event = new StreamEvent();
                event.eventId = UUID.randomUUID().toString();
                event.idempotencyKey = "PAY-" + seed + "-" + String.format("%05d", originals.size() + 1);
                event.senderAccount = sender;
                event.receiverAccount = receivers.get(rng.nextInt(receivers.size()));
                event.amount = BigDecimal.valueOf((rng.nextInt(21) + 5) * 1000L);
                originals.add(event.copy());
            }
            event.eventTimeMs = baseMs + (long) i * intervalMs + rng.nextInt(3001) - 1500;
            event.processingTimeMs = baseMs + (long) i * intervalMs;
            event.nodeId = distributed
                    ? List.of("NODE_A", "NODE_B", "NODE_C").get(rng.nextInt(3))
                    : (currentNodeConfigured() ? configuredNodeId : List.of("NODE_A", "NODE_B", "NODE_C").get(rng.nextInt(3)));
            events.add(event);
        }
        return events;
    }

    private void resetOperationalTablesOnly() {
        wal.deleteAll();
        dedup.deleteAll();
        globalDedup.deleteAll();
        transactions.deleteAll();
        requests.deleteAll();
        accounts.saveAll(List.of(
                new Account("ACC001", "Nguyen Van An", new BigDecimal("5000000")),
                new Account("ACC002", "Tran Thi Bich", new BigDecimal("3000000")),
                new Account("ACC003", "Le Hoang Cuong", new BigDecimal("8000000")),
                new Account("ACC004", "Pham Thi Dung", new BigDecimal("2000000")),
                new Account("ACC005", "Hoang Van Enh", new BigDecimal("6500000"))));
        nodes.saveAll(nodeDefinitions());
        walSequence.set(0);
    }

    private void validateFreshEvent(long eventTimeMs) {
        if (System.currentTimeMillis() - eventTimeMs > maxEventAgeMs) {
            throw new IllegalArgumentException("Event is older than the configured stale-event window");
        }
    }

    private static long windowKey(long eventTimeMs, int windowSizeSec) {
        long windowMs = windowSizeSec * 1000L;
        return (eventTimeMs / windowMs) * windowMs;
    }

    private static String normalizeMode(String mode) {
        return "ALO".equalsIgnoreCase(mode) ? "ALO" : "EOS";
    }

    private String effectiveNodeId(String requestedNodeId) {
        if (currentNodeConfigured()) {
            if (requestedNodeId != null && !requestedNodeId.isBlank() && !configuredNodeId.equals(requestedNodeId)) {
                throw new IllegalArgumentException("This service is " + configuredNodeId + " and cannot process " + requestedNodeId);
            }
            return configuredNodeId;
        }
        return defaultText(requestedNodeId, "NODE_A");
    }

    private String getNodeUrl(String nodeId) {
        if ("NODE_A".equals(nodeId)) return "http://localhost:8081/api";
        if ("NODE_B".equals(nodeId)) return "http://localhost:8082/api";
        if ("NODE_C".equals(nodeId)) return "http://localhost:8083/api";
        throw new IllegalArgumentException("Unknown node: " + nodeId);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> postToNode(String nodeId, String path, Object body) {
        try {
            return http.postForObject(getNodeUrl(nodeId) + path, body, Map.class);
        } catch (Exception e) {
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("status", "ERROR");
            err.put("message", nodeId + " is offline/unreachable.");
            err.put("httpStatus", 503);
            err.put("eosApplied", false);
            return err;
        }
    }

    private void resetAllNodesOperationalState() {
        globalDedup.deleteAll();
        List<String> nodeIds = List.of("NODE_A", "NODE_B", "NODE_C");
        for (String nodeId : nodeIds) {
            if (currentNodeConfigured() && configuredNodeId.equals(nodeId)) {
                resetOperationalTablesOnly();
            } else {
                try {
                    http.postForObject(getNodeUrl(nodeId) + "/reset", null, Map.class);
                } catch (Exception e) {
                    // Ignore offline node
                }
            }
        }
    }

    private boolean currentNodeConfigured() {
        return configuredNodeId != null && !configuredNodeId.isBlank();
    }

    private List<ProcessingNode> nodeDefinitions() {
        if (currentNodeConfigured()) {
            return List.of(new ProcessingNode(configuredNodeId, configuredNodeId + " Service", "ACTIVE"));
        }
        return List.of(
                new ProcessingNode("NODE_A", "Primary Node A", "ACTIVE"),
                new ProcessingNode("NODE_B", "Secondary Node B", "ACTIVE"),
                new ProcessingNode("NODE_C", "Backup Node C", "ACTIVE"));
    }

    private static String defaultText(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }

    private static double elapsedMs(long startNs) {
        return (System.nanoTime() - startNs) / 1_000_000.0;
    }

    private static double avg(Collection<Double> values) {
        return values.stream().mapToDouble(Double::doubleValue).average().orElse(0);
    }

    private static double percentile(List<Double> values, double q) {
        if (values.isEmpty()) {
            return 0;
        }
        List<Double> sorted = values.stream().sorted().toList();
        double rank = (sorted.size() - 1) * (q / 100.0);
        int lower = (int) Math.floor(rank);
        int upper = Math.min(lower + 1, sorted.size() - 1);
        double weight = rank - lower;
        return sorted.get(lower) + (sorted.get(upper) - sorted.get(lower)) * weight;
    }

    private static double round(double value) {
        return BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP).doubleValue();
    }

    private static Map<String, Object> runMap(BenchmarkRun run) {
        if (run == null) {
            return Map.of("tps", 0, "avgLatencyMs", 0, "p95LatencyMs", 0, "p99LatencyMs", 0, "avgEventLagMs", 0, "outOfOrderCount", 0);
        }
        return mutableMap(
                "tps", run.tps,
                "avgLatencyMs", run.avgLatencyMs,
                "p95LatencyMs", run.p95LatencyMs,
                "p99LatencyMs", run.p99LatencyMs,
                "avgEventLagMs", run.avgEventLagMs,
                "outOfOrderCount", run.outOfOrderCount,
                "lateEvents", run.lateEvents,
                "backpressureDelayed", run.backpressureDelayed);
    }

    private static Map<String, Object> benchmarkDataset(List<StreamEvent> events, BenchmarkSettings settings) {
        int uniquePayments = events.stream().map(e -> e.idempotencyKey).collect(Collectors.toSet()).size();
        return mutableMap(
                "eventCount", events.size(),
                "uniquePayments", uniquePayments,
                "duplicateEvents", events.size() - uniquePayments,
                "duplicateRate", settings.duplicateRate(),
                "windowSizeSec", settings.windowSizeSec(),
                "seed", settings.seed());
    }

    private static Map<String, Object> benchmarkProgressPayload(
            String runId,
            String mode,
            int eventIndex,
            int totalEvents,
            String status,
            boolean late,
            long watermark,
            int processedSuccess,
            int duplicatesBlocked,
            int errors,
            int outOfOrder,
            int lateEvents,
            int backpressureDelayed,
            long windowStartMs,
            int windowSizeSec,
            WindowAccumulator acc) {
        return mutableMap(
                "mode", mode,
                "eventIndex", eventIndex,
                "totalEvents", totalEvents,
                "status", status,
                "late", late,
                "watermarkMs", watermark,
                "processedSuccess", processedSuccess,
                "duplicatesBlocked", duplicatesBlocked,
                "errors", errors,
                "outOfOrderCount", outOfOrder,
                "lateEvents", lateEvents,
                "backpressureDelayed", backpressureDelayed,
                "window", windowPayload(runId, mode, windowStartMs, windowSizeSec, acc));
    }

    private static Map<String, Object> windowPayload(String runId, String mode, long windowStartMs, int windowSizeSec, WindowAccumulator acc) {
        return mutableMap(
                "runId", runId,
                "mode", mode,
                "windowStartMs", windowStartMs,
                "windowEndMs", windowStartMs + windowSizeSec * 1000L,
                "totalEvents", acc.totalEvents,
                "uniquePayments", acc.uniqueKeys.size(),
                "processedSuccess", acc.processedSuccess,
                "duplicatesBlocked", acc.duplicatesBlocked,
                "errors", acc.errors,
                "lateEvents", acc.lateEvents,
                "avgLatencyMs", round(avg(acc.latencies)),
                "p50LatencyMs", round(percentile(acc.latencies, 50)),
                "p95LatencyMs", round(percentile(acc.latencies, 95)),
                "p99LatencyMs", round(percentile(acc.latencies, 99)),
                "tps", round(acc.totalEvents / (double) windowSizeSec),
                "walEntries", acc.walEntries);
    }

    private static void sendIfPresent(BenchmarkSink sink, String eventName, Map<String, Object> payload) {
        if (sink != null) {
            sink.send(eventName, payload);
        }
    }

    private static void sendToEmitter(SseEmitter emitter, String eventName, Object payload) {
        try {
            emitter.send(SseEmitter.event().name(eventName).data(payload));
        } catch (Exception e) {
            throw new IllegalStateException("Benchmark stream client disconnected", e);
        }
    }

    private static void sleepQuietly(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Benchmark stream interrupted", e);
        }
    }

    private static int clamp(Integer value, int fallback, int min, int max) {
        return Math.max(min, Math.min(value == null ? fallback : value, max));
    }

    private static double clamp(Double value, double fallback, double min, double max) {
        return Math.max(min, Math.min(value == null ? fallback : value, max));
    }

    private static Map<String, Object> mutableMap(Object... values) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            map.put(String.valueOf(values[i]), values[i + 1]);
        }
        return map;
    }

    @FunctionalInterface
    private interface BenchmarkSink {
        void send(String eventName, Map<String, Object> payload);
    }

    private record BenchmarkSettings(
            int eventCount,
            double duplicateRate,
            int windowSizeSec,
            int seed,
            boolean distributed,
            int streamDelayMs) {
        static BenchmarkSettings from(BenchmarkCommand command) {
            return new BenchmarkSettings(
                    clamp(command.eventCount(), DEFAULT_EVENT_COUNT, 10, 1000),
                    clamp(command.duplicateRate(), DEFAULT_DUPLICATE_RATE, 0, 0.8),
                    clamp(command.windowSizeSec(), DEFAULT_WINDOW_SIZE_SEC, 2, 60),
                    command.seed() == null ? DEFAULT_SEED : command.seed(),
                    Boolean.TRUE.equals(command.distributed()),
                    clamp(command.streamDelayMs(), DEFAULT_STREAM_DELAY_MS, 0, 100));
        }
    }

    private static class WindowAccumulator {
        int totalEvents;
        int processedSuccess;
        int duplicatesBlocked;
        int errors;
        int lateEvents;
        int walEntries;
        Set<String> uniqueKeys = new HashSet<>();
        List<Double> latencies = new ArrayList<>();
    }

    private static class StreamEvent {
        String eventId;
        String idempotencyKey;
        String senderAccount;
        String receiverAccount;
        BigDecimal amount;
        int retryCount;
        boolean duplicate;
        long eventTimeMs;
        long processingTimeMs;
        String nodeId;

        StreamEvent copy() {
            StreamEvent copy = new StreamEvent();
            copy.eventId = eventId;
            copy.idempotencyKey = idempotencyKey;
            copy.senderAccount = senderAccount;
            copy.receiverAccount = receiverAccount;
            copy.amount = amount;
            copy.retryCount = retryCount;
            copy.duplicate = duplicate;
            copy.eventTimeMs = eventTimeMs;
            copy.processingTimeMs = processingTimeMs;
            copy.nodeId = nodeId;
            return copy;
        }
    }
}
