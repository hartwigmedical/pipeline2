package com.hartwig.pipeline.io;

import org.immutables.value.Value;

@Value.Immutable
public interface GoogleStorageLocation {

    @Value.Parameter
    String bucket();

    @Value.Parameter
    String path();

    static GoogleStorageLocation of(String bucket, String path) {
        return ImmutableGoogleStorageLocation.of(bucket, path);
    }
}
