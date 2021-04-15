package com.hartwig.pipeline.tertiary.lilac;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface LilacOutput extends StageOutput {
    @Override
    default String name() {
        return Lilac.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeOutputDirectory();

    default GoogleStorageLocation outputDirectory() {
        return maybeOutputDirectory().orElseThrow(() -> new IllegalStateException("No output directory available"));
    }

    static ImmutableLilacOutput.Builder builder() {
        return LilacOutput.builder();
    }

}
