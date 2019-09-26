package com.hartwig.pipeline.cleanup;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

import com.google.api.services.dataproc.v1beta2.Dataproc;
import com.google.api.services.dataproc.v1beta2.model.Job;
import com.google.api.services.dataproc.v1beta2.model.ListJobsResponse;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.RunTag;
import com.hartwig.pipeline.alignment.Run;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SomaticMetadataApi;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.function.CheckedSupplier;

public class Cleanup {

    private final static Logger LOGGER = LoggerFactory.getLogger(Cleanup.class);
    private static final int MAX_RETRIES = 5;
    private static final int RETRY_DELAY = 3;
    private final Storage storage;
    private final Arguments arguments;
    private final Dataproc dataproc;
    private final SomaticMetadataApi somaticMetadataApi;

    Cleanup(final Storage storage, final Arguments arguments, final Dataproc dataproc, final SomaticMetadataApi somaticMetadataApi) {
        this.storage = storage;
        this.arguments = arguments;
        this.dataproc = dataproc;
        this.somaticMetadataApi = somaticMetadataApi;
    }

    public void run(SomaticRunMetadata metadata) {
        if (!arguments.cleanup()) {
            return;
        }
        LOGGER.info("Cleaning up all transient resources on complete somatic pipeline run (runtime buckets and dataproc jobs)");

        metadata.maybeTumor().ifPresent(tumor -> deleteBucket(Run.from(metadata, arguments).id()));
        cleanupSample(metadata.reference());
        metadata.maybeTumor().ifPresent(this::cleanupSample);
    }

    private void cleanupSample(final SingleSampleRunMetadata metadata) {
        Run run = Run.from(metadata, arguments);
        deleteBucket(run.id());
        deleteStagingDirectory(metadata);

        try {
            Dataproc.Projects.Regions.Jobs jobs = dataproc.projects().regions().jobs();
            ListJobsResponse execute = jobs.list(arguments.project(), arguments.region()).execute();
            for (Job job : execute.getJobs()) {
                if (job.getReference().getJobId().startsWith(run.id())) {
                    LOGGER.debug("Deleting complete job [{}]", job.getReference().getJobId());
                    deleteJob(jobs, job);
                    ensureDeleted(jobs, job);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void deleteStagingDirectory(final SingleSampleRunMetadata metadata) {
        Bucket stagingBucket = storage.get(arguments.patientReportBucket());
        for (Blob blob : stagingBucket.list(Storage.BlobListOption.prefix(RunTag.apply(arguments, metadata.sampleId()))).iterateAll()) {
            blob.delete();
        }
    }

    private void ensureDeleted(final Dataproc.Projects.Regions.Jobs jobs, final Job job) {
        Job existingJob = Failsafe.with(new RetryPolicy<>().handleResultIf(Objects::nonNull)
                .onFailedAttempt(objectExecutionCompletedEvent -> deleteJob(jobs, job))
                .withDelay(Duration.ofSeconds(RETRY_DELAY))
                .withMaxRetries(MAX_RETRIES)).get(getJobAndReturnNullOn404(jobs, job));
        if (existingJob != null) {
            LOGGER.warn("Job [{}] still exists after [{}] attempts to delete it. Aborting, it will need to be deleted by hand",
                    job.getReference().getJobId(),
                    MAX_RETRIES);
        }
    }

    @NotNull
    private CheckedSupplier<Job> getJobAndReturnNullOn404(final Dataproc.Projects.Regions.Jobs jobs, final Job job) {
        return () -> {
            try {
                return jobs.get(arguments.project(), arguments.region(), job.getReference().getJobId()).execute();
            } catch (IOException e) {
                return null;
            }
        };
    }

    private void deleteJob(final Dataproc.Projects.Regions.Jobs jobs, final Job job) throws IOException {
        jobs.delete(arguments.project(), arguments.region(), job.getReference().getJobId()).execute();
    }

    private void deleteBucket(final String runId) {
        Bucket bucket = storage.get(runId);
        if (bucket != null) {
            LOGGER.debug("Cleaning up runtime bucket [{}]", runId);
            for (Blob blob : bucket.list().iterateAll()) {
                blob.delete();
            }
            bucket.delete();
        }
    }
}
