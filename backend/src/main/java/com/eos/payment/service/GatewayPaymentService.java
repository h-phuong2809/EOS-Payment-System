package com.eos.payment.service;

import com.eos.payment.dto.BenchmarkCommand;
import com.eos.payment.dto.PaymentCommand;
import com.eos.payment.dto.RetrySimulationCommand;
import com.eos.payment.model.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
@ConditionalOnProperty(name = "app.role", havingValue = "gateway")
public class GatewayPaymentService implements PaymentOperations {
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
        Map<String, Object> result = post("NODE_A", "/benchmark/compare", command);
        result.put("gatewayNote", "Benchmark executed on NODE_A; payment/retry demo routes across all nodes.");
        return result;
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
        Account[] rows = http.getForObject(nodeUrls.get("NODE_A") + "/accounts", Account[].class);
        return rows == null ? List.of() : Arrays.asList(rows);
    }

    @Override
    public List<ProcessingNode> nodes() {
        List<ProcessingNode> result = new ArrayList<>();
        for (String nodeId : nodeUrls.keySet()) {
            ProcessingNode[] rows = http.getForObject(nodeUrls.get(nodeId) + "/nodes", ProcessingNode[].class);
            if (rows != null) {
                result.addAll(Arrays.asList(rows));
            }
        }
        return result;
    }

    @Override
    public ProcessingNode toggleNode(String nodeId) {
        return http.postForObject(nodeUrls.get(nodeId) + "/nodes/" + nodeId + "/toggle", null, ProcessingNode.class);
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
        return http.postForObject(nodeUrls.get(nodeId) + path, body, Map.class);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getMap(String nodeId, String path) {
        return http.getForObject(nodeUrls.get(nodeId) + path, Map.class);
    }

    @SuppressWarnings("unchecked")
    private List<Object> aggregateLists(String path, String key) {
        List<Object> result = new ArrayList<>();
        for (String nodeId : nodeUrls.keySet()) {
            Map<String, Object> payload = getMap(nodeId, path);
            Object value = payload.get(key);
            if (value instanceof List<?> rows) {
                result.addAll((List<Object>) rows);
            }
        }
        return result;
    }

    private <T> List<T> aggregateArrays(String path, Class<T[]> type) {
        List<T> result = new ArrayList<>();
        for (String url : nodeUrls.values()) {
            T[] rows = http.getForObject(url + path, type);
            if (rows != null) {
                result.addAll(Arrays.asList(rows));
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
