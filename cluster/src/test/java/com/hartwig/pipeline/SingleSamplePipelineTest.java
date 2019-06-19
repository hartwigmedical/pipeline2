package com.hartwig.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executors;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.ImmutableAlignmentOutput;
import com.hartwig.pipeline.alignment.after.metrics.BamMetrics;
import com.hartwig.pipeline.alignment.after.metrics.BamMetricsOutput;
import com.hartwig.pipeline.alignment.after.metrics.ImmutableBamMetricsOutput;
import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.calling.germline.ImmutableGermlineCallerOutput;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.flagstat.Flagstat;
import com.hartwig.pipeline.flagstat.FlagstatOutput;
import com.hartwig.pipeline.flagstat.ImmutableFlagstatOutput;
import com.hartwig.pipeline.metadata.PatientMetadata;
import com.hartwig.pipeline.metadata.PatientMetadataApi;
import com.hartwig.pipeline.report.PatientReport;
import com.hartwig.pipeline.report.PatientReportProvider;
import com.hartwig.pipeline.report.ReportComponent;
import com.hartwig.pipeline.snpgenotype.ImmutableSnpGenotypeOutput;
import com.hartwig.pipeline.snpgenotype.SnpGenotype;
import com.hartwig.pipeline.snpgenotype.SnpGenotypeOutput;
import com.hartwig.pipeline.testsupport.TestSamples;

import org.junit.Before;
import org.junit.Test;

public class SingleSamplePipelineTest {

    private static final Sample REFERENCE = Sample.builder("", "TESTR").type(Sample.Type.REFERENCE).build();
    private static final SnpGenotypeOutput SUCCESSFUL_SNPGENOTYPE_OUTPUT = SnpGenotypeOutput.builder().status(JobStatus.SUCCESS).build();
    private static final GermlineCallerOutput SUCCESSFUL_GERMLINE_OUTPUT = GermlineCallerOutput.builder().status(JobStatus.SUCCESS).build();
    private static final ImmutableFlagstatOutput SUCCESSFUL_FLAGSTAT_OUTPUT = FlagstatOutput.builder().status(JobStatus.SUCCESS).build();
    private static final BamMetricsOutput SUCCESSFUL_BAM_METRICS =
            BamMetricsOutput.builder().status(JobStatus.SUCCESS).sample(REFERENCE).build();
    private static final ImmutableAlignmentOutput SUCCESSFUL_ALIGNMENT_OUTPUT =
            AlignmentOutput.builder().status(JobStatus.SUCCESS).sample(REFERENCE).build();
    private static final String SET_NAME = "set_name";
    public static final Arguments ARGUMENTS = Arguments.testDefaults();
    private SingleSamplePipeline victim;
    private Aligner aligner;
    private BamMetrics bamMetrics;
    private GermlineCaller germlineCaller;
    private SnpGenotype snpGenotype;
    private Flagstat flagstat;

    @Before
    public void setUp() throws Exception {
        aligner = mock(Aligner.class);
        bamMetrics = mock(BamMetrics.class);
        germlineCaller = mock(GermlineCaller.class);
        snpGenotype = mock(SnpGenotype.class);
        flagstat = mock(Flagstat.class);
        PatientMetadataApi patientMetadataApi = mock(PatientMetadataApi.class);
        when(patientMetadataApi.getMetadata()).thenReturn(PatientMetadata.of("TESTR", SET_NAME));
        Storage storage = mock(Storage.class);
        Bucket reportBucket = mock(Bucket.class);
        when(storage.get(ARGUMENTS.patientReportBucket())).thenReturn(reportBucket);
        final PatientReport patientReport = PatientReportProvider.from(storage, ARGUMENTS).get();
        victim = new SingleSamplePipeline(patientMetadataApi,
                aligner,
                bamMetrics,
                germlineCaller,
                snpGenotype,
                flagstat,
                patientReport,
                Executors.newSingleThreadExecutor(),
                ARGUMENTS);
    }

    @Test
    public void returnsFailedPipelineRunWhenAlignerStageFail() throws Exception {
        ImmutableAlignmentOutput alignmentOutput =
                AlignmentOutput.builder().status(JobStatus.FAILED).sample(TestSamples.simpleReferenceSample()).build();
        when(aligner.run()).thenReturn(alignmentOutput);
        PipelineState runOutput = victim.run();
        assertFailed(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(alignmentOutput);
    }

    @Test
    public void returnsFailedPipelineRunWhenFlagstatStageFail() throws Exception {
        when(aligner.run()).thenReturn(SUCCESSFUL_ALIGNMENT_OUTPUT);
        FlagstatOutput flagstatOutput = FlagstatOutput.builder().status(JobStatus.FAILED).build();
        when(bamMetrics.run(any())).thenReturn(SUCCESSFUL_BAM_METRICS);
        when(germlineCaller.run(any())).thenReturn(SUCCESSFUL_GERMLINE_OUTPUT);
        when(snpGenotype.run(any())).thenReturn(SUCCESSFUL_SNPGENOTYPE_OUTPUT);
        when(flagstat.run(any())).thenReturn(flagstatOutput);
        PipelineState runOutput = victim.run();
        assertFailed(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(SUCCESSFUL_ALIGNMENT_OUTPUT,
                SUCCESSFUL_BAM_METRICS,
                SUCCESSFUL_GERMLINE_OUTPUT,
                SUCCESSFUL_SNPGENOTYPE_OUTPUT,
                flagstatOutput);
    }

    @Test
    public void returnsFailedPipelineRunWhenSnpGenotypeStageFail() throws Exception {
        when(aligner.run()).thenReturn(SUCCESSFUL_ALIGNMENT_OUTPUT);
        ImmutableSnpGenotypeOutput snpGenotypeOutput = SnpGenotypeOutput.builder().status(JobStatus.FAILED).build();
        when(bamMetrics.run(any())).thenReturn(SUCCESSFUL_BAM_METRICS);
        when(germlineCaller.run(any())).thenReturn(SUCCESSFUL_GERMLINE_OUTPUT);
        when(snpGenotype.run(any())).thenReturn(snpGenotypeOutput);
        when(flagstat.run(any())).thenReturn(SUCCESSFUL_FLAGSTAT_OUTPUT);
        PipelineState runOutput = victim.run();
        assertFailed(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(SUCCESSFUL_ALIGNMENT_OUTPUT,
                SUCCESSFUL_BAM_METRICS,
                SUCCESSFUL_GERMLINE_OUTPUT,
                snpGenotypeOutput,
                SUCCESSFUL_FLAGSTAT_OUTPUT);
    }

    @Test
    public void returnsFailedPipelineRunWhenMetricsStageFail() throws Exception {
        when(aligner.run()).thenReturn(SUCCESSFUL_ALIGNMENT_OUTPUT);
        ImmutableBamMetricsOutput bamMetricsOutput = BamMetricsOutput.builder().status(JobStatus.FAILED).sample(REFERENCE).build();
        when(bamMetrics.run(any())).thenReturn(bamMetricsOutput);
        when(germlineCaller.run(any())).thenReturn(SUCCESSFUL_GERMLINE_OUTPUT);
        when(snpGenotype.run(any())).thenReturn(SUCCESSFUL_SNPGENOTYPE_OUTPUT);
        when(flagstat.run(any())).thenReturn(SUCCESSFUL_FLAGSTAT_OUTPUT);
        PipelineState runOutput = victim.run();
        assertFailed(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(SUCCESSFUL_ALIGNMENT_OUTPUT,
                bamMetricsOutput,
                SUCCESSFUL_GERMLINE_OUTPUT,
                SUCCESSFUL_SNPGENOTYPE_OUTPUT,
                SUCCESSFUL_FLAGSTAT_OUTPUT);
    }

    @Test
    public void returnsFailedPipelineRunWhenGermlineStageFail() throws Exception {
        when(aligner.run()).thenReturn(SUCCESSFUL_ALIGNMENT_OUTPUT);
        when(bamMetrics.run(any())).thenReturn(SUCCESSFUL_BAM_METRICS);
        ImmutableGermlineCallerOutput germlineCallerOutput = GermlineCallerOutput.builder().status(JobStatus.FAILED).build();
        when(germlineCaller.run(SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(germlineCallerOutput);
        when(snpGenotype.run(any())).thenReturn(SUCCESSFUL_SNPGENOTYPE_OUTPUT);
        when(flagstat.run(any())).thenReturn(SUCCESSFUL_FLAGSTAT_OUTPUT);
        PipelineState state = victim.run();
        assertFailed(state);
        assertThat(state.stageOutputs()).containsExactly(SUCCESSFUL_ALIGNMENT_OUTPUT,
                SUCCESSFUL_BAM_METRICS,
                germlineCallerOutput,
                SUCCESSFUL_SNPGENOTYPE_OUTPUT,
                SUCCESSFUL_FLAGSTAT_OUTPUT);
    }


    @Test
    public void returnsSuccessfulPipelineRunAllStagesSucceed() throws Exception {
        when(aligner.run()).thenReturn(SUCCESSFUL_ALIGNMENT_OUTPUT);
        when(bamMetrics.run(any())).thenReturn(SUCCESSFUL_BAM_METRICS);
        when(germlineCaller.run(SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_GERMLINE_OUTPUT);
        when(snpGenotype.run(SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_SNPGENOTYPE_OUTPUT);
        when(flagstat.run(any())).thenReturn(SUCCESSFUL_FLAGSTAT_OUTPUT);
        PipelineState state = victim.run();
        assertSucceeded(state);
        assertThat(state.stageOutputs()).containsExactlyInAnyOrder(SUCCESSFUL_ALIGNMENT_OUTPUT,
                SUCCESSFUL_BAM_METRICS,
                SUCCESSFUL_GERMLINE_OUTPUT,
                SUCCESSFUL_SNPGENOTYPE_OUTPUT,
                SUCCESSFUL_FLAGSTAT_OUTPUT);
    }

    @Test
    public void addsCompleteStagesToFinalPatientReport() throws Exception {

        TestReportComponent alignerComponent = new TestReportComponent();
        TestReportComponent metricsComponent = new TestReportComponent();
        TestReportComponent germlineComponent = new TestReportComponent();
        TestReportComponent snpgenotypeComponent = new TestReportComponent();
        TestReportComponent flagstatComponent = new TestReportComponent();

        AlignmentOutput alignmentWithReportComponents =
                AlignmentOutput.builder().from(SUCCESSFUL_ALIGNMENT_OUTPUT).addReportComponents(alignerComponent).build();
        when(aligner.run()).thenReturn(alignmentWithReportComponents);
        when(bamMetrics.run(alignmentWithReportComponents)).thenReturn(BamMetricsOutput.builder()
                .from(alignmentWithReportComponents)
                .addReportComponents(metricsComponent)
                .sample(REFERENCE)
                .build());
        when(germlineCaller.run(alignmentWithReportComponents)).thenReturn(GermlineCallerOutput.builder()
                .from(SUCCESSFUL_GERMLINE_OUTPUT)
                .addReportComponents(germlineComponent)
                .build());
        when(snpGenotype.run(alignmentWithReportComponents)).thenReturn(SnpGenotypeOutput.builder()
                .from(SUCCESSFUL_SNPGENOTYPE_OUTPUT)
                .addReportComponents(snpgenotypeComponent)
                .build());
        when(flagstat.run(alignmentWithReportComponents)).thenReturn(FlagstatOutput.builder()
                .from(SUCCESSFUL_FLAGSTAT_OUTPUT)
                .addReportComponents(flagstatComponent)
                .build());

        victim.run();
        assertThat(alignerComponent.isAdded()).isTrue();
        assertThat(metricsComponent.isAdded()).isTrue();
        assertThat(germlineComponent.isAdded()).isTrue();
        assertThat(snpgenotypeComponent.isAdded()).isTrue();
        assertThat(flagstatComponent.isAdded()).isTrue();
    }

    private void assertFailed(final PipelineState runOutput) {
        assertThat(runOutput.status()).isEqualTo(JobStatus.FAILED);
    }

    private void assertSucceeded(final PipelineState runOutput) {
        assertThat(runOutput.status()).isEqualTo(JobStatus.SUCCESS);
    }

    private class TestReportComponent implements ReportComponent {

        private boolean isAdded;

        @Override
        public void addToReport(final Storage storage, final Bucket reportBucket, final String setName) {
            isAdded = true;
        }

        boolean isAdded() {
            return isAdded;
        }
    }
}