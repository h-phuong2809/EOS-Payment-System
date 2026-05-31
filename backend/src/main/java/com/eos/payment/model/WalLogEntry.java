package com.eos.payment.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import java.time.Instant;

@Entity
public class WalLogEntry {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long logId;
    public long sequenceNumber;
    public String operationType;
    public String paymentId;
    public String idempotencyKey;
    @Column(length = 4000)
    public String dataBefore;
    @Column(length = 4000)
    public String dataAfter;
    public String nodeId;
    public String status;
    public Instant timestamp;
    public Instant committedAt;

    public WalLogEntry() {}
}
