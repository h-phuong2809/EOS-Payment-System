package com.eos.payment.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import java.time.Instant;

@Entity
public class DeduplicationRecord {
    @Id
    public String idempotencyKey;
    public String originalRequest;
    public String resultStatus;
    @Column(length = 4000)
    public String resultData;
    public Instant processedAt;
    public String nodeId;
    public long eventTimeMs;

    public DeduplicationRecord() {}
}
