package com.eos.payment.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import java.math.BigDecimal;
import java.time.Instant;

@Entity
public class Account {
    @Id
    public String accountId;
    public String ownerName;
    public BigDecimal balance;
    public Instant createdAt;
    public Instant updatedAt;

    public Account() {}

    public Account(String accountId, String ownerName, BigDecimal balance) {
        this.accountId = accountId;
        this.ownerName = ownerName;
        this.balance = balance;
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }
}
