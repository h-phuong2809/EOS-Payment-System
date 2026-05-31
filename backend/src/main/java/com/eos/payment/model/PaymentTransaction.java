package com.eos.payment.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import java.math.BigDecimal;
import java.time.Instant;

@Entity
public class PaymentTransaction {
    @Id
    public String transactionId;
    public String paymentRequestId;
    public String idempotencyKey;
    public String senderAccount;
    public String receiverAccount;
    public BigDecimal amount;
    public Instant executedAt;
    public String nodeId;

    public PaymentTransaction() {}
}
