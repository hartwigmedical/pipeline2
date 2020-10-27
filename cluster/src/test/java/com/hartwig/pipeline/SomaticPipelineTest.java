package com.hartwig.pipeline;

import static com.hartwig.pipeline.testsupport.TestInputs.amberOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.bachelorOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.chordOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.cobaltOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.defaultSomaticRunMetadata;
import static com.hartwig.pipeline.testsupport.TestInputs.germlineCallerOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.healthCheckerOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.linxOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.purpleOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.referenceMetricsOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.sageOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.structuralCallerOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.structuralCallerPostProcessOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.tumorMetricsOutput;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.calling.somatic.SageCaller;
import com.hartwig.pipeline.calling.somatic.SomaticCallerOutput;
import com.hartwig.pipeline.calling.structural.StructuralCaller;
import com.hartwig.pipeline.calling.structural.StructuralCallerPostProcessOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.SomaticMetadataApi;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.report.PipelineResults;
import com.hartwig.pipeline.report.PipelineResultsProvider;
import com.hartwig.pipeline.reruns.NoopPersistedDataset;
import com.hartwig.pipeline.stages.StageRunner;
import com.hartwig.pipeline.tertiary.healthcheck.HealthCheckOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;

public class SomaticPipelineTest {

    private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private SomaticPipeline victim;
    private StructuralCaller structuralCaller;
    private SomaticMetadataApi setMetadataApi;
    private StageRunner<SomaticRunMetadata> stageRunner;
    private BlockingQueue<BamMetricsOutput> referenceMetricsOutputQueue = new ArrayBlockingQueue<>(1);
    private BlockingQueue<BamMetricsOutput> tumorMetricsOutputQueue = new ArrayBlockingQueue<>(1);
    private java.util.concurrent.BlockingQueue<GermlineCallerOutput> germlineCallerOutputQueue = new ArrayBlockingQueue<>(1);

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        structuralCaller = mock(StructuralCaller.class);
        setMetadataApi = mock(SomaticMetadataApi.class);
        when(setMetadataApi.get()).thenReturn(defaultSomaticRunMetadata());
        Storage storage = mock(Storage.class);
        Bucket reportBucket = mock(Bucket.class);
        when(storage.get(ARGUMENTS.outputBucket())).thenReturn(reportBucket);
        final PipelineResults pipelineResults = PipelineResultsProvider.from(storage, ARGUMENTS, "test").get();
        stageRunner = mock(StageRunner.class);
        victim = new SomaticPipeline(ARGUMENTS,
                stageRunner,
                referenceMetricsOutputQueue,
                tumorMetricsOutputQueue,
                germlineCallerOutputQueue,
                setMetadataApi,
                pipelineResults,
                Executors.newSingleThreadExecutor(),
                new NoopPersistedDataset());
    }

    @Test
    public void runsAllSomaticStagesWhenAlignmentAndMetricsExist() {
        successfulRun();
        PipelineState state = victim.run(TestInputs.defaultPair());
        assertThat(state.stageOutputs()).containsExactlyInAnyOrder(cobaltOutput(),
                amberOutput(),
                sageOutput(),
                structuralCallerOutput(),
                structuralCallerPostProcessOutput(),
                purpleOutput(),
                healthCheckerOutput(),
                linxOutput(),
                bachelorOutput(),
                chordOutput());
    }

    @Test
    public void doesNotRunPurpleIfAnyCallersFail() {
        bothMetricsAvailable();
        SomaticCallerOutput failSomatic = SomaticCallerOutput.builder(SageCaller.NAMESPACE).status(PipelineStatus.FAILED).build();
        when(stageRunner.run(eq(defaultSomaticRunMetadata()), any())).thenReturn(amberOutput())
                .thenReturn(cobaltOutput())
                .thenReturn(failSomatic)
                .thenReturn(structuralCallerOutput());
        PipelineState state = victim.run(TestInputs.defaultPair());
        assertThat(state.stageOutputs()).containsExactlyInAnyOrder(cobaltOutput(), amberOutput(), failSomatic, structuralCallerOutput());
        assertThat(state.status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void doesNotRunPurpleWhenGripssFails() {
        bothMetricsAvailable();
        StructuralCallerPostProcessOutput failGripss = StructuralCallerPostProcessOutput.builder().status(PipelineStatus.FAILED).build();
        when(stageRunner.run(eq(defaultSomaticRunMetadata()), any())).thenReturn(amberOutput())
                .thenReturn(cobaltOutput())
                .thenReturn(sageOutput())
                .thenReturn(structuralCallerOutput())
                .thenReturn(failGripss);
        PipelineState state = victim.run(TestInputs.defaultPair());
        assertThat(state.stageOutputs()).containsExactlyInAnyOrder(cobaltOutput(),
                amberOutput(),
                sageOutput(),
                structuralCallerOutput(),
                failGripss);
        assertThat(state.status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void doesNotRunHealthCheckWhenPurpleFails() {
        bothMetricsAvailable();
        PurpleOutput failPurple = PurpleOutput.builder().status(PipelineStatus.FAILED).build();
        when(stageRunner.run(eq(defaultSomaticRunMetadata()), any())).thenReturn(amberOutput())
                .thenReturn(cobaltOutput())
                .thenReturn(sageOutput())
                .thenReturn(structuralCallerOutput())
                .thenReturn(structuralCallerPostProcessOutput())
                .thenReturn(failPurple);
        PipelineState state = victim.run(TestInputs.defaultPair());
        assertThat(state.stageOutputs()).containsExactlyInAnyOrder(cobaltOutput(),
                amberOutput(),
                sageOutput(),
                structuralCallerOutput(),
                structuralCallerPostProcessOutput(),
                failPurple);
        assertThat(state.status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void failsRunOnQcFailure() {
        bothMetricsAvailable();
        germlineCallingAvailable();
        when(stageRunner.run(eq(defaultSomaticRunMetadata()), any())).thenReturn(amberOutput())
                .thenReturn(cobaltOutput())
                .thenReturn(sageOutput())
                .thenReturn(structuralCallerOutput())
                .thenReturn(structuralCallerPostProcessOutput())
                .thenReturn(purpleOutput())
                .thenReturn(HealthCheckOutput.builder().from(healthCheckerOutput()).status(PipelineStatus.QC_FAILED).build())
                .thenReturn(linxOutput())
                .thenReturn(bachelorOutput())
                .thenReturn(chordOutput());
        PipelineState state = victim.run(TestInputs.defaultPair());
        assertThat(state.status()).isEqualTo(PipelineStatus.QC_FAILED);
    }

    @Test
    public void skipsStructuralCallerIfSingleSampleRun() {
        when(setMetadataApi.get()).thenReturn(SomaticRunMetadata.builder()
                .from(defaultSomaticRunMetadata())
                .maybeTumor(Optional.empty())
                .build());
        victim.run(TestInputs.defaultPair());
        verifyZeroInteractions(stageRunner, structuralCaller);
    }

    private void successfulRun() {
        bothMetricsAvailable();
        germlineCallingAvailable();
        when(stageRunner.run(eq(defaultSomaticRunMetadata()), any())).thenReturn(amberOutput())
                .thenReturn(cobaltOutput())
                .thenReturn(sageOutput())
                .thenReturn(structuralCallerOutput())
                .thenReturn(structuralCallerPostProcessOutput())
                .thenReturn(purpleOutput())
                .thenReturn(healthCheckerOutput())
                .thenReturn(linxOutput())
                .thenReturn(bachelorOutput())
                .thenReturn(chordOutput());
    }

    private void bothMetricsAvailable() {
        try {
            tumorMetricsOutputQueue.put(tumorMetricsOutput());
            referenceMetricsOutputQueue.put(referenceMetricsOutput());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    private void germlineCallingAvailable() {
        try {
            germlineCallerOutputQueue.put(germlineCallerOutput());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}