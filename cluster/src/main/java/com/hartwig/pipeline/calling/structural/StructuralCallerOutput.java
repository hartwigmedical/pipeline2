package com.hartwig.pipeline.calling.structural;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface StructuralCallerOutput extends StageOutput {

    @Override
    default String name() {
        return StructuralCaller.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeUnfilteredVcf();

    Optional<GoogleStorageLocation> maybeUnfilteredVcfIndex();

    default GoogleStorageLocation unfilteredVcf() {
        return maybeUnfilteredVcf().orElseThrow(() -> new IllegalStateException("No unfiltered VCF available"));
    }

    default GoogleStorageLocation unfilteredVcfIndex() {
        return maybeUnfilteredVcfIndex().orElseThrow(() -> new IllegalStateException("No unfiltered VCF index available"));
    }

    Optional<GoogleStorageLocation> maybeFilteredVcf();

    Optional<GoogleStorageLocation> maybeFilteredVcfIndex();

    Optional<GoogleStorageLocation> maybeFullVcf();

    Optional<GoogleStorageLocation> maybeFullVcfIndex();

    default GoogleStorageLocation filteredVcf() {
        return maybeFilteredVcf().orElseThrow(() -> new IllegalStateException("No filtered VCF available"));
    }

    default GoogleStorageLocation fullVcf() {
        return maybeFullVcf().orElseThrow(() -> new IllegalStateException("No full VCF available"));
    }

    default GoogleStorageLocation filteredVcfIndex() {
        return maybeFilteredVcfIndex().orElseThrow(() -> new IllegalStateException("No filtered VCF index available"));
    }

    default GoogleStorageLocation fullVcfIndex() {
        return maybeFullVcfIndex().orElseThrow(() -> new IllegalStateException("No full VCF index available"));
    }

    static ImmutableStructuralCallerOutput.Builder builder() {
        return ImmutableStructuralCallerOutput.builder();
    }
}
