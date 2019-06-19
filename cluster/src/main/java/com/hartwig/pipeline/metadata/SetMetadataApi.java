package com.hartwig.pipeline.metadata;

import com.hartwig.pipeline.execution.PipelineStatus;

public interface SetMetadataApi {

    SetMetadata get();

    void complete(PipelineStatus status);
}
