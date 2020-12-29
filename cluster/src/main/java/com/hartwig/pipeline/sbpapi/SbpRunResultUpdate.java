package com.hartwig.pipeline.sbpapi;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@JsonSerialize(as = ImmutableSbpRunResultUpdate.class)
@Value.Immutable
public interface SbpRunResultUpdate {

    String status();

    Optional<SbpRunResult> result();

    String bucket();

    @Value.Default
    default String endTime() {
        return nowUtc().format(DateTimeFormatter.ISO_DATE_TIME);
    }

    @Value.Default
    default String cluster() {
        return "gcp";
    }

    static LocalDateTime nowUtc() {
        return LocalDateTime.now(ZoneId.of("UTC"));
    }

    static SbpRunResultUpdate of(final String status, final String bucket) {
        return ImmutableSbpRunResultUpdate.builder().status(status).bucket(bucket).build();
    }

    static SbpRunResultUpdate of(final String status, final SbpRunResult result, final String bucket) {
        return ImmutableSbpRunResultUpdate.builder().status(status).bucket(bucket).result(result).build();
    }
}