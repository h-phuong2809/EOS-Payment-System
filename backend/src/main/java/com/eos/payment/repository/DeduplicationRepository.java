package com.eos.payment.repository;

import com.eos.payment.model.DeduplicationRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface DeduplicationRepository extends JpaRepository<DeduplicationRecord, String> {
    @Modifying
    @Query("delete from DeduplicationRecord d where d.eventTimeMs < :threshold")
    int deleteExpired(@Param("threshold") long threshold);
}
