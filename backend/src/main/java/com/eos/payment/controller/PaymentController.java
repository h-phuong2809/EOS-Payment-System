package com.eos.payment.controller;

import com.eos.payment.dto.BenchmarkCommand;
import com.eos.payment.dto.PaymentCommand;
import com.eos.payment.dto.RetrySimulationCommand;
import com.eos.payment.service.PaymentOperations;
import jakarta.validation.Valid;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = {"http://localhost:5173", "http://127.0.0.1:5173"})
public class PaymentController {
    private final PaymentOperations service;

    public PaymentController(PaymentOperations service) {
        this.service = service;
    }

    @GetMapping("/accounts")
    public Object accounts() {
        return service.accounts();
    }

    @GetMapping("/nodes")
    public Object nodes() {
        return service.nodes();
    }

    @PostMapping("/nodes/{nodeId}/toggle")
    public Object toggleNode(@PathVariable String nodeId) {
        return service.toggleNode(nodeId);
    }

    @PostMapping("/payment")
    public ResponseEntity<Map<String, Object>> submitPayment(@Valid @RequestBody PaymentCommand command) {
        Map<String, Object> result = service.submitPayment(command);
        int status = ((Number) result.getOrDefault("httpStatus", 200)).intValue();
        result.remove("httpStatus");
        return ResponseEntity.status(status).body(result);
    }

    @PostMapping("/simulate-retries")
    public Object simulateRetries(@RequestBody RetrySimulationCommand command) {
        return service.simulateRetries(command);
    }

    @PostMapping("/benchmark/compare")
    public Object benchmarkCompare(@RequestBody BenchmarkCommand command) {
        return service.runBenchmark(command);
    }

    @GetMapping(value = "/benchmark/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter benchmarkStream(
            @RequestParam(required = false) Integer eventCount,
            @RequestParam(required = false) Double duplicateRate,
            @RequestParam(required = false) Integer windowSizeSec,
            @RequestParam(required = false) Integer seed,
            @RequestParam(required = false) Boolean distributed,
            @RequestParam(required = false) Integer streamDelayMs) {
        return service.streamBenchmark(new BenchmarkCommand(eventCount, duplicateRate, windowSizeSec, seed, distributed, streamDelayMs));
    }

    @GetMapping("/benchmark/latest")
    public Object benchmarkLatest() {
        return service.latestBenchmarkPayload();
    }

    @GetMapping("/deduplication-table")
    public Object deduplicationTable() {
        return service.deduplicationTable();
    }

    @GetMapping("/wal-log")
    public Object walLog() {
        return service.walLog();
    }

    @GetMapping("/transactions")
    public Object transactions() {
        return service.transactions();
    }

    @GetMapping("/stats")
    public Object stats() {
        return service.stats();
    }

    @PostMapping("/recovery/replay-wal")
    public Object replayWal() {
        return service.replayPendingWal();
    }

    @PostMapping("/reset")
    public Object reset() {
        service.resetOperationalState();
        return Map.of("message", "Database reset successfully");
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> badRequest(IllegalArgumentException ex) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("status", "ERROR", "message", ex.getMessage()));
    }
}
