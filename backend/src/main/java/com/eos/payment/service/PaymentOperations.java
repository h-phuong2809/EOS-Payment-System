package com.eos.payment.service;

import com.eos.payment.dto.BenchmarkCommand;
import com.eos.payment.dto.PaymentCommand;
import com.eos.payment.dto.RetrySimulationCommand;
import com.eos.payment.model.*;
import java.util.List;
import java.util.Map;

public interface PaymentOperations {
    Map<String, Object> submitPayment(PaymentCommand command);
    Map<String, Object> simulateRetries(RetrySimulationCommand command);
    Map<String, Object> runBenchmark(BenchmarkCommand command);
    Map<String, Object> latestBenchmarkPayload();
    Map<String, Object> stats();
    List<Account> accounts();
    List<ProcessingNode> nodes();
    ProcessingNode toggleNode(String nodeId);
    List<DeduplicationRecord> deduplicationTable();
    List<WalLogEntry> walLog();
    List<PaymentTransaction> transactions();
    Map<String, Object> replayPendingWal();
    void resetOperationalState();
}
