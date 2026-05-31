package com.eos.payment.model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import java.time.Instant;

@Entity
public class StreamWindow {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;
    public String runId;
    public String mode;
    public long windowStartMs;
    public long windowEndMs;
    public int totalEvents;
    public int uniquePayments;
    public int processedSuccess;
    public int duplicatesBlocked;
    public int errors;
    public int lateEvents;
    public double avgLatencyMs;
    public double avgEventLagMs;
    public double p50LatencyMs;
    public double p95LatencyMs;
    public double p99LatencyMs;
    public double tps;
    public int walEntries;
    public Instant createdAt;

    public StreamWindow() {}
}
