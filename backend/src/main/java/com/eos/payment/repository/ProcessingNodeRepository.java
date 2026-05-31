package com.eos.payment.repository;

import com.eos.payment.model.ProcessingNode;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessingNodeRepository extends JpaRepository<ProcessingNode, String> {}
