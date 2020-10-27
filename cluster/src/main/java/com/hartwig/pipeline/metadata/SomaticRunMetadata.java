package com.hartwig.pipeline.metadata;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@JsonSerialize(as = ImmutableSomaticRunMetadata.class)
@Value.Immutable
public interface SomaticRunMetadata extends RunMetadata {

    int MAX_SAMPLE_LENGTH = 13;

    SingleSampleRunMetadata reference();

    @JsonProperty("tumor")
    Optional<SingleSampleRunMetadata> maybeTumor();

    @Override
    default String name() {
        return String.format("%s-%s", truncate(reference().barcode()), truncate(tumor().barcode()));
    }

    @Override
    default String barcode() {
        return maybeTumor().map(SingleSampleRunMetadata::barcode).orElse(reference().barcode());
    }

    static String truncate(final String sample) {
        return sample.length() > MAX_SAMPLE_LENGTH ? sample.substring(0, MAX_SAMPLE_LENGTH) : sample;
    }

    @JsonIgnore
    @Value.Derived
    default boolean isSingleSample() {
        return maybeTumor().map(s -> Boolean.FALSE).orElse(Boolean.TRUE);
    }

    default SingleSampleRunMetadata tumor() {
        return maybeTumor().orElseThrow(() -> new IllegalStateException(
                "No tumor is present in this run/set. Somatic algorithms should not be called."));
    }

    static ImmutableSomaticRunMetadata.Builder builder() {
        return ImmutableSomaticRunMetadata.builder();
    }
}