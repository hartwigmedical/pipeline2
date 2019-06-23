package com.hartwig.pipeline.metadata;

import com.hartwig.pipeline.execution.PipelineStatus;

public interface SomaticMetadataApi {

    SomaticRunMetadata get();

    void complete(PipelineStatus status);
}