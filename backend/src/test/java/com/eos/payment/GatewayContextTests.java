package com.eos.payment;

import com.eos.payment.service.GatewayPaymentService;
import com.eos.payment.service.PaymentOperations;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "app.role=gateway",
        "spring.datasource.url=jdbc:h2:mem:eos_gateway_test;DB_CLOSE_DELAY=-1",
        "spring.jpa.hibernate.ddl-auto=create-drop"
})
class GatewayContextTests {
    @Autowired
    PaymentOperations operations;

    @Test
    void gatewayRoleUsesGatewayPaymentService() {
        assertThat(operations).isInstanceOf(GatewayPaymentService.class);
    }
}
