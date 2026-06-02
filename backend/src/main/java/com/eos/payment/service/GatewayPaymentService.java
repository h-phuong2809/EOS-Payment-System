package com.eos.payment.service;

import com.eos.payment.dto.BenchmarkCommand;
import com.eos.payment.dto.PaymentCommand;
import com.eos.payment.dto.RetrySimulationCommand;
import com.eos.payment.model.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.util.UriComponentsBuilder;

@Service
@ConditionalOnProperty(name = "app.role", havingValue = "gateway")
public class GatewayPaymentService implements PaymentOperations {
    private static final int DEFAULT_EVENT_COUNT = 160;
    private static final double DEFAULT_DUPLICATE_RATE = 0.3;
    private static final int DEFAULT_WINDOW_SIZE_SEC = 10;
    private static final int DEFAULT_SEED = 117;
    private static final int DEFAULT_STREAM_DELAY_MS = 25;

    private final RestTemplate http = new RestTemplate();
    private final GlobalDeduplicationStore globalDedup;
    private final Map<String, String> nodeUrls;
    private final AtomicInteger nextNode = new AtomicInteger();

    public GatewayPaymentService(
            GlobalDeduplicationStore globalDedup,
            @Value("${app.gateway.node-a-url:http://localhost:8081/api}") String nodeAUrl,
            @Value("${app.gateway.node-b-url:http://localhost:8082/api}") String nodeBUrl,
            @Value("${app.gateway.node-c-url:http://localhost:8083/api}") String nodeCUrl) {
        this.globalDedup = globalDedup;
        this.nodeUrls = new LinkedHashMap<>();
        this.nodeUrls.put("NODE_A", nodeAUrl);
        this.nodeUrls.put("NODE_B", nodeBUrl);
        this.nodeUrls.put("NODE_C", nodeCUrl);
    }

    @Override
    public Map<String, Object> submitPayment(PaymentCommand command) {
        String nodeId = chooseNode(command.nodeId(), command.idempotencyKey());
        PaymentCommand routed = new PaymentCommand(command.senderAccount(), command.receiverAccount(), command.amount(),
                nodeId, command.mode(), command.idempotencyKey(), command.retryCount(), command.eventTimeMs());
        Map<String, Object> result = post(nodeId, "/payment", routed);
        result.put("routedByGateway", true);
        result.put("targetNode", nodeId);
        return result;
    }

    @Override
    public Map<String, Object> simulateRetries(RetrySimulationCommand command) {
        int retries = Math.max(1, Math.min(command.retries() == null ? 3 : command.retries(), 20));
        String key = UUID.randomUUID().toString();
        List<String> ids = new ArrayList<>(nodeUrls.keySet());
        List<Map<String, Object>> results = new ArrayList<>();
        for (int i = 0; i < retries; i++) {
            String nodeId = ids.get(i % ids.size());
            PaymentCommand payment = new PaymentCommand(
                    defaultText(command.sender(), "ACC001"),
                    defaultText(command.receiver(), "ACC002"),
                    command.amount() == null ? 100000 : command.amount(),
                    nodeId,
                    command.mode(),
                    key,
                    i,
                    System.currentTimeMillis() + (i * 250L));
            Map<String, Object> result = submitPayment(payment);
            result.put("attempt", i + 1);
            result.put("nodeId", nodeId);
            results.add(result);
        }
        return Map.of("idempotencyKey", key, "results", results);
    }

    @Override
    public Map<String, Object> runBenchmark(BenchmarkCommand command) {
        Map<String, Object> result = post("NODE_A", "/benchmark/compare", distributedBenchmarkCommand(command));
        result.put("gatewayNote", "Benchmark executed across all nodes; statistics recorded on NODE_A.");
        return result;
    }

    @Override
    public SseEmitter streamBenchmark(BenchmarkCommand command) {
        SseEmitter emitter = new SseEmitter(0L);
        Thread worker = new Thread(() -> proxyBenchmarkStream(command, emitter), "gateway-benchmark-sse-" + UUID.randomUUID());
        worker.setDaemon(true);
        worker.start();
        return emitter;
    }

    @Override
    public Map<String, Object> latestBenchmarkPayload() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("runs", aggregateLists("/benchmark/latest", "runs"));
        payload.put("windows", aggregateLists("/benchmark/latest", "windows"));
        payload.put("checkpoints", aggregateLists("/benchmark/latest", "checkpoints"));
        return payload;
    }

    @Override
    public Map<String, Object> stats() {
        List<Map<String, Object>> stats = nodeUrls.keySet().stream().map(id -> getMap(id, "/stats")).toList();
        return mutableMap(
                "totalRequests", sum(stats, "totalRequests"),
                "processed", sum(stats, "processed"),
                "duplicatesBlocked", sum(stats, "duplicatesBlocked"),
                "transactions", sum(stats, "transactions"),
                "walEntries", sum(stats, "walEntries"),
                "walCommitted", sum(stats, "walCommitted"),
                "dedupTableSize", globalDedup.count(),
                "walOverheadPct", 0,
                "latestEos", firstNested(stats, "latestEos"),
                "latestAlo", firstNested(stats, "latestAlo"));
    }

    @Override
    public List<Account> accounts() {
        try {
            Account[] rows = http.getForObject(nodeUrls.get("NODE_A") + "/accounts", Account[].class);
            return rows == null ? List.of() : Arrays.asList(rows);
        } catch (org.springframework.web.client.RestClientException e) {
            return List.of();
        }
    }

    @Override
    public List<ProcessingNode> nodes() {
        List<ProcessingNode> result = new ArrayList<>();
        for (String nodeId : nodeUrls.keySet()) {
            try {
                ProcessingNode[] rows = http.getForObject(nodeUrls.get(nodeId) + "/nodes", ProcessingNode[].class);
                if (rows != null) {
                    result.addAll(Arrays.asList(rows));
                }
            } catch (Exception e) {
                result.add(new ProcessingNode(nodeId, nodeId + " Service (Offline)", "FAILED"));
            }
        }
        return result;
    }

    @Override
    public ProcessingNode toggleNode(String nodeId) {
        try {
            return http.postForObject(nodeUrls.get(nodeId) + "/nodes/" + nodeId + "/toggle", null, ProcessingNode.class);
        } catch (Exception e) {
            return new ProcessingNode(nodeId, nodeId + " Service", "FAILED");
        }
    }

    @Override
    public List<DeduplicationRecord> deduplicationTable() {
        return globalDedup.findAll();
    }

    @Override
    public List<WalLogEntry> walLog() {
        return aggregateArrays("/wal-log", WalLogEntry[].class);
    }

    @Override
    public List<PaymentTransaction> transactions() {
        return aggregateArrays("/transactions", PaymentTransaction[].class);
    }

    @Override
    public Map<String, Object> replayPendingWal() {
        int pending = 0;
        int rolledBack = 0;
        for (String nodeId : nodeUrls.keySet()) {
            Map<String, Object> result = post(nodeId, "/recovery/replay-wal", null);
            pending += number(result, "pendingFound");
            rolledBack += number(result, "rolledBack");
        }
        return Map.of("pendingFound", pending, "rolledBack", rolledBack);
    }

    @Override
    public void resetOperationalState() {
        globalDedup.deleteAll();
        for (String nodeId : nodeUrls.keySet()) {
            post(nodeId, "/reset", null);
        }
    }

    private String chooseNode(String requestedNodeId, String idempotencyKey) {
        if (requestedNodeId != null && !requestedNodeId.isBlank()) {
            return requestedNodeId;
        }
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            List<String> ids = new ArrayList<>(nodeUrls.keySet());
            return ids.get(Math.floorMod(idempotencyKey.hashCode(), ids.size()));
        }
        List<String> ids = new ArrayList<>(nodeUrls.keySet());
        return ids.get(Math.floorMod(nextNode.getAndIncrement(), ids.size()));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> post(String nodeId, String path, Object body) {
        try {
            return http.postForObject(nodeUrls.get(nodeId) + path, body, Map.class);
        } catch (org.springframework.web.client.RestClientException e) {
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("status", "ERROR");
            err.put("message", nodeId + " is offline/unreachable.");
            err.put("httpStatus", 503);
            err.put("eosApplied", false);
            return err;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getMap(String nodeId, String path) {
        try {
            return http.getForObject(nodeUrls.get(nodeId) + path, Map.class);
        } catch (org.springframework.web.client.RestClientException e) {
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("status", "FAILED");
            err.put("totalRequests", 0);
            err.put("processed", 0);
            err.put("duplicatesBlocked", 0);
            err.put("transactions", 0);
            err.put("walEntries", 0);
            err.put("walCommitted", 0);
            return err;
        }
    }

    private void proxyBenchmarkStream(BenchmarkCommand command, SseEmitter emitter) {
        String url = benchmarkStreamUrl(command);
        try {
            http.execute(url, HttpMethod.GET, null, response -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getBody(), StandardCharsets.UTF_8))) {
                    String eventName = "message";
                    StringBuilder data = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.isBlank()) {
                            if (data.length() > 0) {
                                sendSse(emitter, eventName, data.toString());
                                data.setLength(0);
                            }
                            eventName = "message";
                        } else if (line.startsWith("event:")) {
                            eventName = line.substring("event:".length()).trim();
                        } else if (line.startsWith("data:")) {
                            if (data.length() > 0) {
                                data.append('\n');
                            }
                            data.append(line.substring("data:".length()).trim());
                        }
                    }
                    if (data.length() > 0) {
                        sendSse(emitter, eventName, data.toString());
                    }
                }
                return null;
            });
            emitter.complete();
        } catch (Exception e) {
            try {
                sendSse(emitter, "error", "{\"message\":\"NODE_A is offline/unreachable.\"}");
            } catch (Exception ignored) {
                // Client may already be disconnected.
            }
            emitter.complete();
        }
    }

    private BenchmarkCommand distributedBenchmarkCommand(BenchmarkCommand command) {
        return new BenchmarkCommand(
                command.eventCount(),
                command.duplicateRate(),
                command.windowSizeSec(),
                command.seed(),
                true,
                command.streamDelayMs());
    }

    private String benchmarkStreamUrl(BenchmarkCommand command) {
        BenchmarkCommand distributed = distributedBenchmarkCommand(command);
        return UriComponentsBuilder
                .fromHttpUrl(nodeUrls.get("NODE_A") + "/benchmark/stream")
                .queryParam("eventCount", valueOrDefault(distributed.eventCount(), DEFAULT_EVENT_COUNT))
                .queryParam("duplicateRate", valueOrDefault(distributed.duplicateRate(), DEFAULT_DUPLICATE_RATE))
                .queryParam("windowSizeSec", valueOrDefault(distributed.windowSizeSec(), DEFAULT_WINDOW_SIZE_SEC))
                .queryParam("seed", valueOrDefault(distributed.seed(), DEFAULT_SEED))
                .queryParam("distributed", true)
                .queryParam("streamDelayMs", valueOrDefault(distributed.streamDelayMs(), DEFAULT_STREAM_DELAY_MS))
                .toUriString();
    }

    private static void sendSse(SseEmitter emitter, String eventName, Object data) {
        try {
            emitter.send(SseEmitter.event().name(eventName).data(data));
        } catch (Exception e) {
            throw new IllegalStateException("Benchmark stream client disconnected", e);
        }
    }

    private static <T> T valueOrDefault(T value, T fallback) {
        return value == null ? fallback : value;
    }

    @SuppressWarnings("unchecked")
    private List<Object> aggregateLists(String path, String key) {
        List<Object> result = new ArrayList<>();
        for (String nodeId : nodeUrls.keySet()) {
            try {
                Map<String, Object> payload = getMap(nodeId, path);
                Object value = payload.get(key);
                if (value instanceof List<?> rows) {
                    result.addAll((List<Object>) rows);
                }
            } catch (Exception e) {
                // ignore
            }
        }
        return result;
    }

    private <T> List<T> aggregateArrays(String path, Class<T[]> type) {
        List<T> result = new ArrayList<>();
        for (String url : nodeUrls.values()) {
            try {
                T[] rows = http.getForObject(url + path, type);
                if (rows != null) {
                    result.addAll(Arrays.asList(rows));
                }
            } catch (Exception e) {
                // ignore
            }
        }
        return result;
    }

    private static int sum(List<Map<String, Object>> stats, String key) {
        return stats.stream().mapToInt(row -> number(row, key)).sum();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> firstNested(List<Map<String, Object>> stats, String key) {
        return stats.stream()
                .map(row -> row.get(key))
                .filter(Map.class::isInstance)
                .map(row -> (Map<String, Object>) row)
                .findFirst()
                .orElse(Map.of("tps", 0, "avgLatencyMs", 0, "p95LatencyMs", 0, "p99LatencyMs", 0, "avgEventLagMs", 0, "outOfOrderCount", 0));
    }

    private static int number(Map<String, Object> row, String key) {
        Object value = row.get(key);
        return value instanceof Number n ? n.intValue() : 0;
    }

    private static String defaultText(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }

    private static Map<String, Object> mutableMap(Object... values) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            map.put(String.valueOf(values[i]), values[i + 1]);
        }
        return map;
    }
}
