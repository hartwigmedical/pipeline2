package com.hartwig.pipeline.tertiary.virusbreakend;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface VirusBreakendOutputLocations {

    GoogleStorageLocation summaryFile();

    static ImmutableVirusBreakendOutputLocations.Builder builder() {
        return ImmutableVirusBreakendOutputLocations.builder();
    }
}