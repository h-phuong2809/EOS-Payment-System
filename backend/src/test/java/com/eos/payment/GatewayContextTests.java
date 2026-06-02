package com.eos.payment;

import com.eos.payment.service.GatewayPaymentService;
import com.eos.payment.service.PaymentOperations;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "app.role=gateway",
        "app.gateway.node-a-url=http://localhost:65531/api",
        "app.gateway.node-b-url=http://localhost:65532/api",
        "app.gateway.node-c-url=http://localhost:65533/api",
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

    @Test
    void gatewayAccountsReturnsEmptyListWhenPrimaryNodeIsOffline() {
        assertThat(operations.accounts()).isEmpty();
    }
}
