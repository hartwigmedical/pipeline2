package com.hartwig.pipeline.report;

import java.time.Duration;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Iterables;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.RunTag;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

public class FullSomaticResults {

    private static final Logger LOGGER = LoggerFactory.getLogger(FullSomaticResults.class);
    private static final int FIVE_MINUTES = 60 * 5;
    private static final int INFINITE = -1;

    private final Storage storage;
    private final Arguments arguments;
    private final int retryDelayInSeconds;

    FullSomaticResults(final Storage storage, final Arguments arguments, final int retryDelayInSeconds) {
        this.storage = storage;
        this.arguments = arguments;
        this.retryDelayInSeconds = retryDelayInSeconds;
    }

    public FullSomaticResults(final Storage storage, final Arguments arguments) {
        this(storage, arguments, FIVE_MINUTES);
    }

    public void compose(SomaticRunMetadata metadata) {
        Bucket bucket = storage.get(arguments.patientReportBucket());
        copySingleSampleRun(metadata, bucket, directory(metadata.reference()));
        metadata.maybeTumor().ifPresent(tumor -> copySingleSampleRun(metadata, bucket, directory(tumor)));
    }

    public String directory(final SingleSampleRunMetadata metadata) {
        return RunTag.apply(arguments, metadata.sampleId());
    }

    private void copySingleSampleRun(final SomaticRunMetadata metadata, final Bucket bucket, final String directory) {
        Iterable<Blob> blobs = Failsafe.with(new RetryPolicy<Iterable<Blob>>().handleResultIf(Iterables::isEmpty)
                .onFailedAttempt(event -> LOGGER.info("No results available in [{}]. Will try again in [{}] seconds.",
                        directory,
                        retryDelayInSeconds))
                .withDelay(Duration.ofSeconds(retryDelayInSeconds))
                .withMaxRetries(INFINITE)).get(() -> bucket.list(Storage.BlobListOption.prefix(directory)).iterateAll());
        for (Blob blob : blobs) {
            String pathSplit = blob.getName().substring(blob.getName().indexOf("/") + 1, blob.getName().length());
            storage.copy(Storage.CopyRequest.of(arguments.patientReportBucket(),
                    blob.getName(),
                    BlobId.of(arguments.patientReportBucket(), metadata.runName() + "/" + pathSplit))).getResult();
        }
    }
}
