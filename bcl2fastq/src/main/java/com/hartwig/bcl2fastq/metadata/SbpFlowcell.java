package com.hartwig.bcl2fastq.metadata;

import java.util.Optional;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableSbpFlowcell.class)
public interface SbpFlowcell {

    String STATUS_CONVERTED = "Converted";

    Optional<String> convertTime();

    Optional<String> updateTime();

    String name();

    String status();

    Optional<Long> yld();

    Optional<Long> under_rds();

    Optional<Double> q30();

    Optional<Double> undet_rds_p();

    int id();

    String flowcell_id();

    boolean undet_rds_p_pass();

    static ImmutableSbpFlowcell.Builder builder() {
        return ImmutableSbpFlowcell.builder();
    }

    static ImmutableSbpFlowcell.Builder builderFrom(SbpFlowcell other) {
        return ImmutableSbpFlowcell.builder().from(other);
    }
}