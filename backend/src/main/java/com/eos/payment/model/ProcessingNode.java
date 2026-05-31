package com.eos.payment.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import java.time.Instant;

@Entity
public class ProcessingNode {
    @Id
    public String nodeId;
    public String nodeName;
    public String status;
    public Instant lastHeartbeat;
    public long processedCount;

    public ProcessingNode() {}

    public ProcessingNode(String nodeId, String nodeName, String status) {
        this.nodeId = nodeId;
        this.nodeName = nodeName;
        this.status = status;
        this.lastHeartbeat = Instant.now();
        this.processedCount = 0;
    }
}
