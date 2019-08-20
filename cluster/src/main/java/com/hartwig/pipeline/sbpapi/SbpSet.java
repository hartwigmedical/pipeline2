package com.hartwig.pipeline.sbpapi;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableSbpSet.class)
@Value.Immutable
public interface SbpSet {

    String name();

    String id();
}