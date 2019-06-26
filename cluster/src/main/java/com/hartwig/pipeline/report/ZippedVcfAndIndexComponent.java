package com.hartwig.pipeline.report;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;

public class ZippedVcfAndIndexComponent implements ReportComponent {

    private final RuntimeBucket runtimeBucket;
    private final String namespace;
    private final String sampleName;
    private final String sourceVcfFileName;
    private final String targetFileName;
    private final ResultsDirectory resultsDirectory;

    public ZippedVcfAndIndexComponent(final RuntimeBucket runtimeBucket, final String namespace, final String sampleName,
            final String sourceFileName, final String targetFileName, final ResultsDirectory resultsDirectory) {
        this.runtimeBucket = runtimeBucket;
        this.namespace = namespace;
        this.sampleName = sampleName;
        this.sourceVcfFileName = sourceFileName;
        this.targetFileName = targetFileName;
        this.resultsDirectory = resultsDirectory;
    }

    @Override
    public void addToReport(final Storage storage, final Bucket reportBucket, final String setName) {
        runtimeBucket.copyOutOf(resultsDirectory.path(sourceVcfFileName),
                reportBucket.getName(),
                String.format("%s/%s/%s/%s", setName, sampleName, namespace, targetFileName));
        runtimeBucket.copyOutOf(resultsDirectory.path(sourceVcfFileName),
                reportBucket.getName(),
                String.format("%s/%s/%s/%s", setName, sampleName, namespace, targetFileName + ".tbi"));
    }
}
