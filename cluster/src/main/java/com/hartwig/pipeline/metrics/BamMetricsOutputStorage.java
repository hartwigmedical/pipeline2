package com.hartwig.pipeline.metrics;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class BamMetricsOutputStorage {

    private final Storage storage;
    private final Arguments arguments;
    private final ResultsDirectory resultsDirectory;

    public BamMetricsOutputStorage(final Storage storage, final Arguments arguments, final ResultsDirectory resultsDirectory) {
        this.storage = storage;
        this.arguments = arguments;
        this.resultsDirectory = resultsDirectory;
    }

    public BamMetricsOutput get(SingleSampleRunMetadata sample) {
        RuntimeBucket metricsBucket = RuntimeBucket.from(storage, BamMetrics.NAMESPACE, sample, arguments);
        return BamMetricsOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .sample(sample.sampleName())
                .maybeMetricsOutputFile(GoogleStorageLocation.of(metricsBucket.name(),
                        resultsDirectory.path(BamMetricsOutput.outputFile(sample.sampleName()))))
                .build();
    }
}