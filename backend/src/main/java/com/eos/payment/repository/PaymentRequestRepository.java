package com.eos.payment.repository;

import com.eos.payment.model.PaymentRequest;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentRequestRepository extends JpaRepository<PaymentRequest, String> {
    long countByStatus(String status);
}
