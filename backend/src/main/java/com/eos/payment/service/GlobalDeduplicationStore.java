package com.eos.payment.service;

import com.eos.payment.model.DeduplicationRecord;
import jakarta.annotation.PostConstruct;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Service;

@Service
public class GlobalDeduplicationStore {
    private final JdbcTemplate jdbc;

    public GlobalDeduplicationStore(
            @Value("${app.global-dedup.datasource.url:jdbc:h2:file:./data/global_dedup;DB_CLOSE_DELAY=-1;AUTO_SERVER=TRUE}") String url,
            @Value("${app.global-dedup.datasource.username:sa}") String username,
            @Value("${app.global-dedup.datasource.password:}") String password) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.h2.Driver");
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        this.jdbc = new JdbcTemplate(dataSource);
    }

    @PostConstruct
    public void init() {
        jdbc.execute("""
                create table if not exists global_deduplication_record (
                    idempotency_key varchar(255) primary key,
                    original_request varchar(255),
                    result_status varchar(64),
                    result_data varchar(4000),
                    processed_at timestamp,
                    node_id varchar(64),
                    event_time_ms bigint
                )
                """);
    }

    public Optional<DeduplicationRecord> findById(String key) {
        List<DeduplicationRecord> rows = jdbc.query(
                "select * from global_deduplication_record where idempotency_key = ?",
                (rs, rowNum) -> map(rs),
                key);
        return rows.stream().findFirst();
    }

    public DeduplicationRecord claimOrExisting(String key, String requestId, String nodeId, long eventTimeMs) {
        try {
            jdbc.update("""
                    insert into global_deduplication_record
                    (idempotency_key, original_request, result_status, result_data, processed_at, node_id, event_time_ms)
                    values (?, ?, ?, ?, ?, ?, ?)
                    """,
                    key, requestId, "PROCESSING", "{}", Timestamp.from(Instant.now()), nodeId, eventTimeMs);
            DeduplicationRecord record = new DeduplicationRecord();
            record.idempotencyKey = key;
            record.originalRequest = requestId;
            record.resultStatus = "PROCESSING";
            record.resultData = "{}";
            record.processedAt = Instant.now();
            record.nodeId = nodeId;
            record.eventTimeMs = eventTimeMs;
            return record;
        } catch (DuplicateKeyException ignored) {
            return findById(key).orElseThrow();
        }
    }

    public void completeSuccess(String key, String resultData, String nodeId, long eventTimeMs) {
        jdbc.update("""
                update global_deduplication_record
                set result_status = ?, result_data = ?, processed_at = ?, node_id = ?, event_time_ms = ?
                where idempotency_key = ?
                """,
                "SUCCESS", resultData, Timestamp.from(Instant.now()), nodeId, eventTimeMs, key);
    }

    public int deleteExpired(long threshold) {
        return jdbc.update("delete from global_deduplication_record where event_time_ms < ?", threshold);
    }

    public List<DeduplicationRecord> findAll() {
        return jdbc.query("select * from global_deduplication_record order by processed_at desc", (rs, rowNum) -> map(rs));
    }

    public long count() {
        Long count = jdbc.queryForObject("select count(*) from global_deduplication_record", Long.class);
        return count == null ? 0 : count;
    }

    public void deleteAll() {
        jdbc.update("delete from global_deduplication_record");
    }

    private static DeduplicationRecord map(ResultSet rs) throws java.sql.SQLException {
        DeduplicationRecord record = new DeduplicationRecord();
        record.idempotencyKey = rs.getString("idempotency_key");
        record.originalRequest = rs.getString("original_request");
        record.resultStatus = rs.getString("result_status");
        record.resultData = rs.getString("result_data");
        Timestamp processedAt = rs.getTimestamp("processed_at");
        record.processedAt = processedAt == null ? null : processedAt.toInstant();
        record.nodeId = rs.getString("node_id");
        record.eventTimeMs = rs.getLong("event_time_ms");
        return record;
    }
}
