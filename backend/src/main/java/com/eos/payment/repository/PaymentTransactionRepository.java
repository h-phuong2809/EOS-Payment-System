package com.eos.payment.repository;

import com.eos.payment.model.PaymentTransaction;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentTransactionRepository extends JpaRepository<PaymentTransaction, String> {
    List<PaymentTransaction> findTop50ByOrderByExecutedAtDesc();
}
