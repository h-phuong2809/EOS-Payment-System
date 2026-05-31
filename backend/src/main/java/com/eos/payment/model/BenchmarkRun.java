package com.eos.payment.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import java.time.Instant;

@Entity
public class BenchmarkRun {
    @Id
    public String runId;
    public String mode;
    public int eventCount;
    public int uniquePayments;
    public int duplicateEvents;
    public int processedSuccess;
    public int duplicatesBlocked;
    public int errors;
    public double durationMs;
    public double tps;
    public double avgLatencyMs;
    public double p50LatencyMs;
    public double p95LatencyMs;
    public double p99LatencyMs;
    public double duplicateLatencyMs;
    public double newLatencyMs;
    public double avgEventLagMs;
    public int outOfOrderCount;
    public int lateEvents;
    public int backpressureDelayed;
    public double walWriteLatencyMs;
    public int walEntries;
    public int walCommitted;
    public double walOverheadPct;
    public int windowSizeSec;
    public long watermarkMs;
    public Instant createdAt;

    public BenchmarkRun() {}
}
