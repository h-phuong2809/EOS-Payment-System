package com.eos.payment;

import com.eos.payment.dto.PaymentCommand;
import com.eos.payment.service.PaymentStreamService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = {
        "app.role=node",
        "app.node-id=NODE_A",
        "spring.datasource.url=jdbc:h2:mem:eos_node_a_test;DB_CLOSE_DELAY=-1",
        "spring.jpa.hibernate.ddl-auto=create-drop"
})
class NodeProfileContextTests {
    @Autowired
    PaymentStreamService service;

    @BeforeEach
    void reset() {
        service.resetOperationalState();
    }

    @Test
    void configuredNodeOnlyProcessesItsOwnNodeId() {
        assertThat(service.nodes()).extracting("nodeId").containsExactly("NODE_A");

        assertThat(service.submitPayment(new PaymentCommand("ACC001", "ACC002", 100000, "NODE_A", "EOS", "NODE-A-ONLY", 0, System.currentTimeMillis())).get("status"))
                .isEqualTo("SUCCESS");
        assertThatThrownBy(() -> service.submitPayment(new PaymentCommand("ACC001", "ACC002", 100000, "NODE_B", "EOS", "WRONG-NODE", 0, System.currentTimeMillis())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot process NODE_B");
    }
}
