package com.eos.payment.repository;

import com.eos.payment.model.BenchmarkRun;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface BenchmarkRunRepository extends JpaRepository<BenchmarkRun, String> {
    List<BenchmarkRun> findAllByOrderByCreatedAtDescModeAsc();
    BenchmarkRun findFirstByModeOrderByCreatedAtDesc(String mode);
}
