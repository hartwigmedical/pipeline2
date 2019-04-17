package com.hartwig.pipeline.execution.dataproc;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataproc.v1beta2.Dataproc;
import com.google.api.services.dataproc.v1beta2.model.Cluster;
import com.google.api.services.dataproc.v1beta2.model.ClusterConfig;
import com.google.api.services.dataproc.v1beta2.model.Job;
import com.google.api.services.dataproc.v1beta2.model.JobPlacement;
import com.google.api.services.dataproc.v1beta2.model.JobReference;
import com.google.api.services.dataproc.v1beta2.model.Operation;
import com.google.api.services.dataproc.v1beta2.model.SparkJob;
import com.google.api.services.dataproc.v1beta2.model.SubmitJobRequest;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.RuntimeBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleDataproc implements SparkExecutor {

    private static final String APPLICATION_NAME = "sample-dataproc-cluster";
    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleDataproc.class);

    private final Dataproc dataproc;
    private final NodeInitialization nodeInitialization;
    private final Arguments arguments;

    GoogleDataproc(final Dataproc dataproc, final NodeInitialization nodeInitialization, final Arguments arguments) {
        this.dataproc = dataproc;
        this.nodeInitialization = nodeInitialization;
        this.arguments = arguments;
    }

    public static GoogleDataproc from(final GoogleCredentials credential, final NodeInitialization nodeInitialization,
            final Arguments arguments) {
        return new GoogleDataproc(new Dataproc.Builder(new NetHttpTransport(),
                JacksonFactory.getDefaultInstance(),
                new HttpCredentialsAdapter(credential)).setApplicationName(APPLICATION_NAME).build(), nodeInitialization, arguments);
    }

    @Override
    public JobStatus submit(RuntimeBucket runtimeBucket, SparkJobDefinition jobDefinition) {
        try {
            String clusterName = runtimeBucket.name();
            String naturalJobId = clusterName + "-" + jobDefinition.name().toLowerCase();
            final Job job = findExistingJob(arguments, naturalJobId).orElseGet(() -> submittedJob(jobDefinition,
                    runtimeBucket,
                    naturalJobId,
                    clusterName));
            if (!isDone(job)) {
                Job completed = waitForComplete(job,
                        j -> j.getStatus() != null && (j.getStatus().getState().equals("ERROR") || isDone(j)
                                || j.getStatus().getState().equals("CANCELLED")),
                        () -> dataproc.projects()
                                .regions()
                                .jobs()
                                .get(arguments.project(), arguments.region(), job.getReference().getJobId())
                                .execute(),
                        GoogleDataproc::jobStatus);
                LOGGER.info("Spark job is complete with status [{}] details [{}]", completed.getStatus().getState(), completed.getStatus().getDetails());
                stop(clusterName);
                if (completed.getStatus().getState().equals("ERROR")) {
                    return JobStatus.FAILED;
                }
            }
            return JobStatus.SUCCESS;
        } catch (IOException e) {
            LOGGER.error("Exception while interacting with Google Dataproc APIs", e);
            return JobStatus.FAILED;
        }
    }

    private boolean isDone(final Job job) {
        return job.getStatus().getState().equals("DONE");
    }

    private void start(final DataprocPerformanceProfile performanceProfile, final RuntimeBucket runtimeBucket, final Arguments arguments,
            final String clusterName) throws IOException {
        Dataproc.Projects.Regions.Clusters clusters = dataproc.projects().regions().clusters();
        Cluster existing = findExistingCluster(arguments, clusterName);
        if (existing == null) {
            ClusterConfig clusterConfig = GoogleClusterConfig.from(runtimeBucket, nodeInitialization, performanceProfile).config();
            Operation createCluster =
                    clusters.create(arguments.project(), arguments.region(), cluster(clusterConfig, clusterName)).execute();
            LOGGER.info("Starting Google Dataproc cluster with name [{}]. This may take a minute or two...", clusterName);
            waitForOperationComplete(createCluster);
            LOGGER.info("Cluster started.");
        } else {
            LOGGER.info("Cluster [{}] already exists, using this cluster to run pipeline", clusterName);
        }
    }

    private Job submittedJob(final SparkJobDefinition jobDefinition, final RuntimeBucket runtimeBucket, final String naturalJobId,
            final String clusterName) {
        try {
            start(jobDefinition.performanceProfile(), runtimeBucket, arguments, clusterName);
            LOGGER.info("Submitting spark job [{}] to cluster [{}]", jobDefinition.name(), clusterName);
            return dataproc.projects()
                    .regions()
                    .jobs()
                    .submit(arguments.project(),
                            arguments.region(),
                            new SubmitJobRequest().setJob(new Job().setPlacement(new JobPlacement().setClusterName(clusterName))
                                    .setReference(new JobReference().setJobId(naturalJobId))
                                    .setSparkJob(new SparkJob().setProperties(jobDefinition.sparkProperties())
                                            .setMainClass(jobDefinition.mainClass())
                                            .setArgs(jobDefinition.arguments())
                                            .setJarFileUris(Collections.singletonList(jobDefinition.jarLocation())))))
                    .execute();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void stop(String clusterName) throws IOException {
        Operation deleteCluster =
                dataproc.projects().regions().clusters().delete(arguments.project(), arguments.region(), clusterName).execute();
        LOGGER.info("Deleting cluster [{}]. This may take a minute or two...", clusterName);
        waitForOperationComplete(deleteCluster);
        LOGGER.info("Cluster deleted");
    }

    private static String jobStatus(final Job job) {
        String template = "Status [%s] Substatus [%s] Details [%s]";
        com.google.api.services.dataproc.v1beta2.model.JobStatus status = job.getStatus();
        return String.format(template, status.getState(), stringOrNone(status.getSubstate()), stringOrNone(status.getDetails()));
    }

    private static String stringOrNone(final String string) {
        return string != null ? string : "None";
    }

    private Cluster findExistingCluster(final Arguments arguments, final String clusterName) throws IOException {
        try {
            return dataproc.projects().regions().clusters().get(arguments.project(), arguments.region(), clusterName).execute();
        } catch (GoogleJsonResponseException e) {
            return null;
        }
    }

    private Optional<Job> findExistingJob(Arguments arguments, String jobId) throws IOException {
        try {
            Job job = dataproc.projects().regions().jobs().get(arguments.project(), arguments.region(), jobId).execute();
            if (job != null) {
                switch (job.getStatus().getState()) {
                    case "RUNNING":
                        LOGGER.info("Job [{}] already existed and is running. Re-attaching boostrap to running job.", jobId);
                        return Optional.of(job);
                    case "DONE":
                        LOGGER.info("Job [{}] already existed and completed successfully. Skipping job", jobId);
                        return Optional.of(job);
                    default:
                        LOGGER.info("Job [{}] already existed and but is [{}]. Deleting and re-submitting",
                                jobId,
                                job.getStatus().getState());
                        dataproc.projects().regions().jobs().delete(arguments.project(), arguments.region(), jobId).execute();
                }
            }
            return Optional.empty();
        } catch (GoogleJsonResponseException e) {
            return Optional.empty();
        }
    }

    private void waitForOperationComplete(Operation operation) throws IOException {
        waitForComplete(operation,
                op1 -> op1.getDone() != null && op1.getDone(),
                () -> dataproc.projects().regions().operations().get(operation.getName()).execute(),
                op -> op.getMetadata().get("description").toString());
    }

    private <T> T waitForComplete(T operation, Predicate<T> isDone, Poll<T> poll, Function<T, String> description) throws IOException {
        boolean operationComplete = isDone.test(operation);
        while (!operationComplete) {
            sleep();
            LOGGER.debug("Operation [{}] not complete, waiting...", description.apply(operation));
            operation = poll.poll();
            operationComplete = isDone.test(operation);
        }
        return operation;
    }

    private void sleep() {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
    }

    private Cluster cluster(final ClusterConfig clusterConfig, final String clusterName) {
        return new Cluster().setClusterName(clusterName).setConfig(clusterConfig);
    }

    private interface Poll<T> {
        T poll() throws IOException;
    }
}