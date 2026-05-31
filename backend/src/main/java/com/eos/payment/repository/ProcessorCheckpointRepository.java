package com.eos.payment.repository;

import com.eos.payment.model.ProcessorCheckpoint;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessorCheckpointRepository extends JpaRepository<ProcessorCheckpoint, String> {}
