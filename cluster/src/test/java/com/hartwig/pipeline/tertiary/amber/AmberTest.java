package com.hartwig.pipeline.tertiary.amber;

import static com.hartwig.pipeline.testsupport.TestConstants.OUT_DIR;
import static com.hartwig.pipeline.testsupport.TestConstants.TOOLS_AMBER_JAR;
import static com.hartwig.pipeline.testsupport.TestConstants.inFile;
import static com.hartwig.pipeline.testsupport.TestConstants.resource;
import static com.hartwig.pipeline.testsupport.TestInputs.defaultPair;
import static com.hartwig.pipeline.testsupport.TestInputs.defaultSomaticRunMetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.testsupport.BucketInputOutput;
import com.hartwig.pipeline.testsupport.MockResource;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class AmberTest {

    private static final String RUNTIME_BUCKET = "run-reference-tumor-test";
    private ComputeEngine computeEngine;
    private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private Amber victim;
    private Storage storage;
    private BucketInputOutput gs;

    @Before
    public void setUp() throws Exception {
        computeEngine = mock(ComputeEngine.class);
        storage = mock(Storage.class);
        final Bucket bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn(RUNTIME_BUCKET);
        when(storage.get(RUNTIME_BUCKET)).thenReturn(bucket);
        CopyWriter copyWriter = mock(CopyWriter.class);
        when(storage.copy(any())).thenReturn(copyWriter);
        MockResource.addToStorage(storage, ResourceNames.REFERENCE_GENOME, "reference.fasta");
        MockResource.addToStorage(storage, ResourceNames.AMBER_PON, "GermlineHetPon.hg19.bed", "GermlineSnp.hg19.bed");
        gs = new BucketInputOutput(RUNTIME_BUCKET);
        victim = new Amber(ARGUMENTS, computeEngine, storage, ResultsDirectory.defaultDirectory());
    }

    @Test
    public void returnsSkippedWhenTertiaryDisabledInArguments() {
        victim = new Amber(Arguments.testDefaultsBuilder().runTertiary(false).build(),
                computeEngine,
                storage,
                ResultsDirectory.defaultDirectory());
        AmberOutput output = victim.run(defaultSomaticRunMetadata(), defaultPair());
        assertThat(output.status()).isEqualTo(PipelineStatus.SKIPPED);
    }

    @Test
    public void returnsAmberBafFileGoogleStorageLocation() {
        when(computeEngine.submit(any(), any())).thenReturn(PipelineStatus.SUCCESS);
        AmberOutput output = victim.run(defaultSomaticRunMetadata(),defaultPair());
        assertThat(output.outputDirectory()).isEqualTo(GoogleStorageLocation.of(RUNTIME_BUCKET + "/amber", "results", true));
    }

    @Test
    public void returnsStatusFailedWhenJobFailsOnComputeEngine() {
        when(computeEngine.submit(any(), any())).thenReturn(PipelineStatus.FAILED);
        assertThat(victim.run(defaultSomaticRunMetadata(),defaultPair()).status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void runsAmberOnComputeEngine() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        victim.run(defaultSomaticRunMetadata(),defaultPair());
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains("java -Xmx32G -cp "
                + TOOLS_AMBER_JAR + " com.hartwig.hmftools.amber.AmberApplication -reference reference -reference_bam "
                + inFile("reference.bam") + " -tumor tumor -tumor_bam " + inFile("tumor.bam")
                + " -output_dir " + OUT_DIR + " -threads 16 -ref_genome "
                + resource("reference.fasta") + " -bed " + resource("GermlineHetPon.hg19.bed")
                + " -snp_bed " + resource("GermlineSnp.hg19.bed"));
    }

    @Test
    public void downloadsInputBamsAndBais() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        victim.run(defaultSomaticRunMetadata(),defaultPair());
        BucketInputOutput tumorBucket = new BucketInputOutput("run-tumor");
        BucketInputOutput referenceBucket = new BucketInputOutput("run-reference");
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(tumorBucket.input(
                "aligner/results/tumor.bam"),
                referenceBucket.input("aligner/results/reference.bam"),
                tumorBucket.input("aligner/results/tumor.bam.bai"),
                referenceBucket.input("aligner/results/reference.bam.bai"));
    }

    @Test
    public void uploadsOutputDirectory() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        victim.run(defaultSomaticRunMetadata(),defaultPair());
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(gs.output("amber/results"));
    }

    private ArgumentCaptor<VirtualMachineJobDefinition> captureAndReturnSuccess() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor =
                ArgumentCaptor.forClass(VirtualMachineJobDefinition.class);
        when(computeEngine.submit(any(), jobDefinitionArgumentCaptor.capture())).thenReturn(PipelineStatus.SUCCESS);
        return jobDefinitionArgumentCaptor;
    }
}