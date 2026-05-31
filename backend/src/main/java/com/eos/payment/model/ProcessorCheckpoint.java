package com.eos.payment.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import java.time.Instant;

@Entity
public class ProcessorCheckpoint {
    @Id
    public String mode;
    public long lastEventTimeMs;
    public long watermarkMs;
    public int windowsPersisted;
    public Instant updatedAt;

    public ProcessorCheckpoint() {}
}
