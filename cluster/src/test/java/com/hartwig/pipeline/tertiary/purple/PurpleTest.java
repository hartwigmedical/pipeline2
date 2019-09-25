package com.hartwig.pipeline.tertiary.purple;

import static com.hartwig.pipeline.testsupport.TestConstants.IN_DIR;
import static com.hartwig.pipeline.testsupport.TestConstants.OUT_DIR;
import static com.hartwig.pipeline.testsupport.TestConstants.PROC_COUNT;
import static com.hartwig.pipeline.testsupport.TestConstants.TOOLS_DIR;
import static com.hartwig.pipeline.testsupport.TestConstants.TOOLS_PURPLE_JAR;
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
import com.hartwig.pipeline.calling.somatic.SomaticCallerOutput;
import com.hartwig.pipeline.calling.structural.StructuralCallerOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;
import com.hartwig.pipeline.testsupport.BucketInputOutput;
import com.hartwig.pipeline.testsupport.MockResource;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class PurpleTest {

    private static final String RUNTIME_BUCKET = "run-reference-tumor-test";
    private ComputeEngine computeEngine;
    private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private Purple victim;
    private Storage storage;
    private Bucket bucket;
    private BucketInputOutput gs;

    @Before
    public void setUp() throws Exception {
        computeEngine = mock(ComputeEngine.class);
        storage = mock(Storage.class);
        bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn(RUNTIME_BUCKET);
        when(storage.get(RUNTIME_BUCKET)).thenReturn(bucket);
        CopyWriter copyWriter = mock(CopyWriter.class);
        when(storage.copy(any())).thenReturn(copyWriter);
        MockResource.addToStorage(storage, ResourceNames.REFERENCE_GENOME, "reference.fasta");
        MockResource.addToStorage(storage, ResourceNames.GC_PROFILE, "gc_profile.cnp");
        gs = new BucketInputOutput(RUNTIME_BUCKET);
        victim = new Purple(ARGUMENTS, computeEngine, storage, ResultsDirectory.defaultDirectory());
    }

    @Test
    public void returnsSkippedWhenTertiaryDisabledInArguments() {
        victim = new Purple(Arguments.testDefaultsBuilder().runTertiary(false).build(),
                computeEngine,
                storage,
                ResultsDirectory.defaultDirectory());
        PurpleOutput output = runVictim();
        assertThat(output.status()).isEqualTo(PipelineStatus.SKIPPED);
    }

    @Test
    public void returnsPurpleOutputDirectory() {
        when(computeEngine.submit(any(), any())).thenReturn(PipelineStatus.SUCCESS);
        PurpleOutput output = runVictim();
        assertThat(output.outputDirectory()).isEqualTo(GoogleStorageLocation.of(RUNTIME_BUCKET + "/purple", "results", true));
    }

    @Test
    public void returnsStatusFailedWhenJobFailsOnComputeEngine() {
        when(computeEngine.submit(any(), any())).thenReturn(PipelineStatus.FAILED);
        assertThat(runVictim().status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void runsPurpleOnComputeEngine() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        runVictim();
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "java -Xmx8G -jar " + TOOLS_PURPLE_JAR + " -reference reference -tumor tumor -output_dir " + OUT_DIR
                        + " -amber " + IN_DIR + " -cobalt " + IN_DIR + " -gc_profile " + resource("gc_profile.cnp")
                        + " -somatic_vcf " + inFile("somatic.vcf")
                        + " -structural_vcf " + inFile("structural.vcf")
                        + " -sv_recovery_vcf " + inFile("sv_recovery.vcf")
                        + " -circos " + TOOLS_DIR + "/circos/0.69.6/bin/circos -ref_genome " + resource("reference.fasta")
                        + " -threads " + PROC_COUNT);
    }

    @Test
    public void runsPurpleWithLowCoverageArgsWhenShallow() {
        when(storage.get(RUNTIME_BUCKET + "-shallow")).thenReturn(bucket);
        victim = new Purple(Arguments.builder().from(ARGUMENTS).shallow(true).build(),
                computeEngine,
                storage,
                ResultsDirectory.defaultDirectory());
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        runVictim();
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                " -highly_diploid_percentage 0.88 -somatic_min_total 100 -somatic_min_purity_spread 0.1");
    }

    @Test
    public void downloadsInputVcfsCobaltAndAmberOutput() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        runVictim();
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(gs.input("somatic.vcf", "somatic.vcf"),
                gs.input("structural.vcf", "structural.vcf"),
                gs.input("sv_recovery.vcf", "sv_recovery.vcf"),
                gs.input("amber/*"),
                gs.input("cobalt/*"));
    }

    @Test
    public void uploadsOutputDirectory() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        runVictim();
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(gs.output("purple/results"));
    }

    private PurpleOutput runVictim() {
        return victim.run(defaultSomaticRunMetadata(),
                defaultPair(),
                SomaticCallerOutput.builder()
                        .status(PipelineStatus.SUCCESS)
                        .maybeFinalSomaticVcf(GoogleStorageLocation.of(RUNTIME_BUCKET, "somatic.vcf"))
                        .build(),
                StructuralCallerOutput.builder()
                        .status(PipelineStatus.SUCCESS)
                        .maybeFilteredVcf(GoogleStorageLocation.of(RUNTIME_BUCKET, "structural.vcf"))
                        .maybeFilteredVcfIndex(GoogleStorageLocation.of(RUNTIME_BUCKET, "structural.vcf.tbi"))
                        .maybeFullVcf(GoogleStorageLocation.of(RUNTIME_BUCKET, "sv_recovery.vcf"))
                        .maybeFullVcfIndex(GoogleStorageLocation.of(RUNTIME_BUCKET, "sv_recovery.vcf.tbi"))
                        .build(),
                CobaltOutput.builder()
                        .status(PipelineStatus.SUCCESS)
                        .maybeOutputDirectory(GoogleStorageLocation.of(RUNTIME_BUCKET, "cobalt", true))
                        .build(),
                AmberOutput.builder()
                        .status(PipelineStatus.SUCCESS)
                        .maybeOutputDirectory(GoogleStorageLocation.of(RUNTIME_BUCKET, "amber", true))
                        .build());
    }

    private ArgumentCaptor<VirtualMachineJobDefinition> captureAndReturnSuccess() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor =
                ArgumentCaptor.forClass(VirtualMachineJobDefinition.class);
        when(computeEngine.submit(any(), jobDefinitionArgumentCaptor.capture())).thenReturn(PipelineStatus.SUCCESS);
        return jobDefinitionArgumentCaptor;
    }
}