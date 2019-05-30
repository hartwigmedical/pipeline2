package com.hartwig.pipeline.tertiary.healthcheck;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.alignment.after.metrics.BamMetricsOutput;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.testsupport.TestInputs;
import com.hartwig.pipeline.tools.Versions;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class HealthCheckerTest {

    private static final String RUNTIME_BUCKET = "run-reference-tumor";
    private static final Arguments ARGUMENTS = Arguments.testDefaults();

    private ComputeEngine computeEngine;
    private HealthChecker victim;
    private Storage storage;

    @Before
    public void setUp() throws Exception {
        computeEngine = mock(ComputeEngine.class);
        storage = mock(Storage.class);
        final Bucket bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn(RUNTIME_BUCKET);
        when(storage.get(RUNTIME_BUCKET)).thenReturn(bucket);
        victim = new HealthChecker(ARGUMENTS, computeEngine, storage, ResultsDirectory.defaultDirectory());
    }

    @Test
    public void returnsSkippedWhenTertiaryDisabledInArguments() {
        victim = new HealthChecker(Arguments.testDefaultsBuilder().runTertiary(false).build(),
                computeEngine,
                storage,
                ResultsDirectory.defaultDirectory());
        HealthCheckOutput output = runVictim();
        assertThat(output.status()).isEqualTo(JobStatus.SKIPPED);
    }

    @Test
    public void returnsHealthCheckerOutputDirectory() {
        when(computeEngine.submit(any(), any())).thenReturn(JobStatus.SUCCESS);
        HealthCheckOutput output = runVictim();
        assertThat(output.outputDirectory()).isEqualTo(GoogleStorageLocation.of(RUNTIME_BUCKET + "/health_checker", "results"));
    }

    @Test
    public void returnsStatusFailedWhenJobFailsOnComputeEngine() {
        when(computeEngine.submit(any(), any())).thenReturn(JobStatus.FAILED);
        assertThat(runVictim().status()).isEqualTo(JobStatus.FAILED);
    }

    @Test
    public void runsHealthCheckApplicationOnComputeEngine() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        runVictim();
        System.out.println(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString());
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "java -Xmx10G -jar /data/tools/health-checker/" + Versions.HEALTH_CHECKER + "/health-checker.jar -reference reference "
                        + "-tumor tumor -metrics_dir /data/input/metrics -amber_dir /data/input/amber -purple_dir /data/input/purple "
                        + "-output_dir /data/output");
    }

    @Test
    public void downloadsInputMetricsPurpleAndAmberOutput() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        runVictim();
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "gsutil -qm cp gs://run-reference/reference.wgsmetrics /data/input/metrics/reference.wgsmetrics",
                "gsutil -qm cp gs://run-tumor/tumor.wgsmetrics /data/input/metrics/tumor.wgsmetrics",
                "gsutil -qm cp gs://run-reference-tumor/purple/* /data/input/purple/",
                "gsutil -qm cp gs://run-reference-tumor/amber/* /data/input/amber");
    }

    @Test
    public void uploadsOutputDirectory() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        runVictim();
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "gsutil -qm cp -r /data/output/* gs://run-reference-tumor/health_checker/results");
    }

    private ArgumentCaptor<VirtualMachineJobDefinition> captureAndReturnSuccess() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor =
                ArgumentCaptor.forClass(VirtualMachineJobDefinition.class);
        when(computeEngine.submit(any(), jobDefinitionArgumentCaptor.capture())).thenReturn(JobStatus.SUCCESS);
        return jobDefinitionArgumentCaptor;
    }

    private HealthCheckOutput runVictim() {
        AlignmentPair pair = TestInputs.defaultPair();
        return victim.run(pair,
                BamMetricsOutput.builder()
                        .status(JobStatus.SUCCESS)
                        .maybeMetricsOutputFile(GoogleStorageLocation.of("run-reference", "reference.wgsmetrics"))
                        .sample(pair.reference().sample())
                        .build(),
                BamMetricsOutput.builder()
                        .status(JobStatus.SUCCESS)
                        .maybeMetricsOutputFile(GoogleStorageLocation.of("run-tumor", "tumor.wgsmetrics"))
                        .sample(pair.tumor().sample())
                        .build(),
                AmberOutput.builder()
                        .status(JobStatus.SUCCESS)
                        .maybeOutputDirectory(GoogleStorageLocation.of(RUNTIME_BUCKET, "amber", true))
                        .build(),
                PurpleOutput.builder()
                        .status(JobStatus.SUCCESS)
                        .maybeOutputDirectory(GoogleStorageLocation.of(RUNTIME_BUCKET, "purple", true))
                        .build());
    }
}