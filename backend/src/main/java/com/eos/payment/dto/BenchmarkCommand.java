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
        Boolean distributed
) {
    public BenchmarkCommand(Integer eventCount, Double duplicateRate, Integer windowSizeSec, Integer seed) {
        this(eventCount, duplicateRate, windowSizeSec, seed, false);
    }
}
