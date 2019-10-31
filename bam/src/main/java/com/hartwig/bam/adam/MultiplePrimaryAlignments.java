package com.hartwig.bam.adam;

import org.immutables.value.Value;

@Value.Immutable
public interface MultiplePrimaryAlignments {

    enum ReadOrdinal {
        FIRST,
        SECOND
    }

    @Value.Parameter
    String readName();

    @Value.Parameter
    ReadOrdinal ordinal();

    @Value.Parameter
    long numPrimary();

    @Value.Parameter
    long numAlignmentsForOrdinal();

    static MultiplePrimaryAlignments of(String readName, ReadOrdinal ordinal, long numPrimary, long numAlignments) {
        return ImmutableMultiplePrimaryAlignments.of(readName, ordinal, numPrimary, numAlignments);
    }
}