package com.eos.payment.dto;

public record RetrySimulationCommand(
        String mode,
        Integer retries,
        String sender,
        String receiver,
        Double amount
) {}
