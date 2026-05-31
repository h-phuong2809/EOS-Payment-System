package com.eos.payment.repository;

import com.eos.payment.model.WalLogEntry;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WalLogRepository extends JpaRepository<WalLogEntry, Long> {
    long countByStatus(String status);
    List<WalLogEntry> findTop50ByOrderBySequenceNumberDesc();
    List<WalLogEntry> findByStatusOrderBySequenceNumberAsc(String status);
}
