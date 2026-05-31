package com.eos.payment.repository;

import com.eos.payment.model.StreamWindow;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface StreamWindowRepository extends JpaRepository<StreamWindow, Long> {
    List<StreamWindow> findAllByOrderByWindowStartMsAscModeAsc();
}
