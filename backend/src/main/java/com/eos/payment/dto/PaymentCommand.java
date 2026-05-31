package com.eos.payment.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

public record PaymentCommand(
        @JsonAlias("sender_account")
        @NotBlank String senderAccount,
        @JsonAlias("receiver_account")
        @NotBlank String receiverAccount,
        @Positive double amount,
        @JsonAlias("node_id")
        String nodeId,
        String mode,
        @JsonAlias("idempotency_key")
        String idempotencyKey,
        @JsonAlias("retry_count")
        Integer retryCount,
        @JsonAlias("event_time_ms")
        Long eventTimeMs
) {}
