package com.eos.payment.dto;

import com.fasterxml.jackson.annotation.JsonAlias;

public record BenchmarkCommand(
        @JsonAlias("event_count")
        Integer eventCount,
        @JsonAlias("duplicate_rate")
        Double duplicateRate,
        @JsonAlias("window_size_sec")
        Integer windowSizeSec,
        Integer seed,
        Boolean distributed,
        @JsonAlias("stream_delay_ms")
        Integer streamDelayMs
) {
    public BenchmarkCommand(Integer eventCount, Double duplicateRate, Integer windowSizeSec, Integer seed) {
        this(eventCount, duplicateRate, windowSizeSec, seed, false, null);
    }

    public BenchmarkCommand(Integer eventCount, Double duplicateRate, Integer windowSizeSec, Integer seed, Boolean distributed) {
        this(eventCount, duplicateRate, windowSizeSec, seed, distributed, null);
    }
}
