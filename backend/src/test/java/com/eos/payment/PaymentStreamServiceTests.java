package com.eos.payment;

import com.eos.payment.dto.BenchmarkCommand;
import com.eos.payment.dto.PaymentCommand;
import com.eos.payment.repository.PaymentTransactionRepository;
import com.eos.payment.service.PaymentStreamService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:eos_payment_test;DB_CLOSE_DELAY=-1",
        "spring.jpa.hibernate.ddl-auto=create-drop"
})
@AutoConfigureMockMvc
class PaymentStreamServiceTests {
    @Autowired
    PaymentStreamService service;
    @Autowired
    PaymentTransactionRepository transactions;
    @Autowired
    MockMvc mockMvc;

    @BeforeEach
    void reset() {
        service.resetOperationalState();
    }

    @Test
    void eosRejectsDuplicateAndCommitsOnce() {
        PaymentCommand command = new PaymentCommand("ACC001", "ACC002", 100000, "NODE_A", "EOS", "TEST-KEY", 0, System.currentTimeMillis());

        Map<String, Object> first = service.submitPayment(command);
        Map<String, Object> second = service.submitPayment(command);

        assertThat(first.get("status")).isEqualTo("SUCCESS");
        assertThat(second.get("status")).isEqualTo("DUPLICATE_REJECTED");
        assertThat(transactions.count()).isEqualTo(1);
    }

    @Test
    void aloProcessesDuplicates() {
        PaymentCommand command = new PaymentCommand("ACC001", "ACC002", 100000, "NODE_A", "ALO", "TEST-KEY", 0, System.currentTimeMillis());

        assertThat(service.submitPayment(command).get("status")).isEqualTo("SUCCESS");
        assertThat(service.submitPayment(command).get("status")).isEqualTo("SUCCESS");

        assertThat(transactions.count()).isEqualTo(2);
    }

    @Test
    void outOfOrderRetryUsesEventTimeWindow() {
        long eventTime = System.currentTimeMillis();

        assertThat(service.submitPayment(new PaymentCommand("ACC001", "ACC002", 100000, "NODE_A", "EOS", "ORDER-KEY-1", 0, eventTime)).get("status"))
                .isEqualTo("SUCCESS");
        assertThat(service.submitPayment(new PaymentCommand("ACC003", "ACC004", 100000, "NODE_A", "EOS", "ORDER-KEY-2", 0, eventTime + 30000)).get("status"))
                .isEqualTo("SUCCESS");
        assertThat(service.submitPayment(new PaymentCommand("ACC001", "ACC002", 100000, "NODE_B", "EOS", "ORDER-KEY-1", 1, eventTime)).get("status"))
                .isEqualTo("DUPLICATE_REJECTED");

        assertThat(transactions.count()).isEqualTo(2);
    }

    @Test
    void concurrentEosDuplicateStillCommitsOnce() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(8);
        List<Callable<Object>> calls = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            calls.add(() -> service.submitPayment(new PaymentCommand("ACC001", "ACC002", 50000, "NODE_A", "EOS", "CONCURRENT-KEY", 0, System.currentTimeMillis())).get("status"));
        }

        List<Object> statuses = new ArrayList<>();
        for (Future<Object> future : pool.invokeAll(calls)) {
            statuses.add(future.get(5, TimeUnit.SECONDS));
        }
        pool.shutdownNow();

        assertThat(statuses).contains("SUCCESS");
        assertThat(statuses.stream().filter("SUCCESS"::equals).count()).isEqualTo(1);
        assertThat(transactions.count()).isEqualTo(1);
    }

    @Test
    void nodeFailureRejectsPayment() {
        service.toggleNode("NODE_A");

        Map<String, Object> result = service.submitPayment(new PaymentCommand("ACC001", "ACC002", 100000, "NODE_A", "EOS", "NODE-FAIL", 0, System.currentTimeMillis()));

        assertThat(result.get("status")).isEqualTo("ERROR");
        assertThat(result.get("httpStatus")).isEqualTo(503);
    }

    @Test
    void benchmarkShowsEosBlocksDuplicatesAndAloDoesNot() {
        Map<String, Object> payload = service.runBenchmark(new BenchmarkCommand(80, 0.35, 10, 117));
        List<?> runs = (List<?>) payload.get("runs");

        Object eosRun = runs.stream().filter(r -> "EOS".equals(read(r, "mode"))).findFirst().orElseThrow();
        Object aloRun = runs.stream().filter(r -> "ALO".equals(read(r, "mode"))).findFirst().orElseThrow();

        assertThat(readNumber(eosRun, "processedSuccess")).isEqualTo(readNumber(eosRun, "uniquePayments"));
        assertThat(readNumber(eosRun, "duplicatesBlocked")).isGreaterThan(0);
        assertThat(readNumber(aloRun, "processedSuccess")).isEqualTo(80);
        assertThat(readNumber(aloRun, "duplicatesBlocked")).isEqualTo(0);
        assertThat(((List<?>) payload.get("windows"))).isNotEmpty();

        Map<String, Object> stats = service.stats();
        assertThat(((Number) stats.get("processed")).intValue()).isEqualTo(readNumber(eosRun, "processedSuccess"));
        assertThat(((Number) stats.get("transactions")).intValue()).isEqualTo(readNumber(eosRun, "processedSuccess"));
    }

    @Test
    void benchmarkStreamEmitsLiveSseEvents() throws Exception {
        MvcResult stream = mockMvc.perform(get("/api/benchmark/stream")
                        .param("eventCount", "10")
                        .param("duplicateRate", "0.3")
                        .param("windowSizeSec", "2")
                        .param("seed", "117")
                        .param("streamDelayMs", "0"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM))
                .andExpect(request().asyncStarted())
                .andReturn();

        String body = waitForStreamEvent(stream, "event:complete");

        assertThat(body).contains("event:started");
        assertThat(body).contains("event:progress");
        assertThat(body).contains("event:complete");
    }

    private static String waitForStreamEvent(MvcResult stream, String marker) throws Exception {
        long deadline = System.currentTimeMillis() + 5000;
        String body = "";
        while (System.currentTimeMillis() < deadline) {
            body = stream.getResponse().getContentAsString();
            if (body.contains(marker)) {
                return body;
            }
            Thread.sleep(50);
        }
        return body;
    }

    private static Object read(Object bean, String field) {
        try {
            var f = bean.getClass().getField(field);
            return f.get(bean);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static int readNumber(Object bean, String field) {
        return ((Number) read(bean, field)).intValue();
    }
}
