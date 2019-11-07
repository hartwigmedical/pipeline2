package com.hartwig.batch;

import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.credentials.CredentialProvider;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.storage.StorageProvider;

import org.apache.commons.io.FileUtils;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchDispatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchDispatcher.class);
    private final BatchArguments arguments;
    private final InstanceFactory instanceFactory;

    @Value.Immutable
    public interface StateTuple {
        String id();

        String url();

        Future<PipelineStatus> future();
    }

    private BatchDispatcher(BatchArguments arguments) {
        this.arguments = arguments;
        this.instanceFactory = InstanceFactory.from(arguments);
    }

    private void runBatch() throws Exception {
        GoogleCredentials credentials = CredentialProvider.from(arguments).get();
        Storage storage = StorageProvider.from(arguments, credentials).get();

        ExecutorService executorService = Executors.newFixedThreadPool(arguments.concurrency());
        Set<StateTuple> state = new HashSet<>();
        Set<String> urls = FileUtils.readLines(new File(arguments.inputFile()), "UTF-8")
                .stream()
                .filter(s -> !s.trim().isEmpty())
                .collect(Collectors.toSet());
        LOGGER.info("Running {} distinct input files with up to {} concurrent VMs", urls.size(), arguments.concurrency());
        int i = 0;
        String paddingFormat = format("%%0%dd", (urls.size() + "").length());
        for (String url : urls) {
            final String label = format(paddingFormat, i + 1);
            RuntimeBucket bucket = RuntimeBucket.from(storage, arguments.runName(), "batch", arguments);
            ComputeEngine compute = ComputeEngine.from(arguments, credentials, false);
            Future<PipelineStatus> future =
                    executorService.submit(() -> compute.submit(bucket, instanceFactory.get().convert(url, bucket, label), label));
            state.add(ImmutableStateTuple.builder().id(label).url(url).future(future).build());
            i++;
        }
        spawnProgessLogger(state);
        for (StateTuple job : state) {
            job.future().get();
        }
        StringBuilder report = new StringBuilder("EXECUTION REPORT\n\n");

        boolean jobsFailed = false;
        List<StateTuple> tuples = state.stream().sorted(comparing(stateTuple -> Integer.valueOf(stateTuple.id()))).collect(toList());
        for (StateTuple stateTuple : tuples) {
            report.append(String.format("  %s %s %s\n", stateTuple.id(), stateTuple.future().get(), stateTuple.url()));
            if (stateTuple.future().get() != PipelineStatus.SUCCESS) {
                jobsFailed = true;
            }
        }
        LOGGER.info("Batch completed");
        LOGGER.info(report.toString());
        System.exit(jobsFailed ? 1 : 0);
    }

    private void spawnProgessLogger(Set<StateTuple> state) {
        Thread progressLogger = new Thread(() -> {
            while (true) {
                int done = 0;
                int cancelled = 0;
                for (StateTuple stateTuple : state) {
                    if (stateTuple.future().isCancelled()) {
                        cancelled++;
                    } else if (stateTuple.future().isDone()) {
                        done++;
                    }
                }
                LOGGER.info("Job stats: {} running, {} finished, {} cancelled", state.size() - done - cancelled, done, cancelled);
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(30));
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
        });
        progressLogger.setDaemon(true);
        progressLogger.start();
    }

    public static void main(String[] args) throws Exception {
        new BatchDispatcher(BatchArguments.from(args)).runBatch();
    }
}