package com.eos.payment.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import java.math.BigDecimal;
import java.time.Instant;

@Entity
public class PaymentRequest {
    @Id
    public String requestId;
    public String idempotencyKey;
    public String senderAccount;
    public String receiverAccount;
    public BigDecimal amount;
    public String status;
    public int retryCount;
    public Instant createdAt;
    public Instant processedAt;
    public String nodeId;
    public long eventTimeMs;
    public long processingTimeMs;

    public PaymentRequest() {}
}
