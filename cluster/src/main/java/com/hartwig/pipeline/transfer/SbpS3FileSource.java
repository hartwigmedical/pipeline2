package com.hartwig.pipeline.transfer;

import java.util.function.Function;

public class SbpS3FileSource implements Function<String, String> {

    @Override
    public String apply(final String file) {
        return String.format("s3://%s", file);
    }
}