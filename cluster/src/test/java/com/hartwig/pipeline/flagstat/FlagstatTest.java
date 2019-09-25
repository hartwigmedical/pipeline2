package com.hartwig.pipeline.flagstat;

import static com.hartwig.pipeline.testsupport.TestConstants.PROC_COUNT;
import static com.hartwig.pipeline.testsupport.TestConstants.TOOLS_SAMBAMBA;
import static com.hartwig.pipeline.testsupport.TestConstants.inFile;
import static com.hartwig.pipeline.testsupport.TestConstants.outFile;
import static com.hartwig.pipeline.testsupport.TestInputs.referenceAlignmentOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.referenceRunMetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.testsupport.BucketInputOutput;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class FlagstatTest {

    private static final String RUNTIME_BUCKET = "run-reference-test";
    private ComputeEngine computeEngine;
    private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private Flagstat victim;

    @Before
    public void setUp() throws Exception {
        computeEngine = mock(ComputeEngine.class);
        final Storage storage = mock(Storage.class);
        final Bucket bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn(RUNTIME_BUCKET);
        when(storage.get(RUNTIME_BUCKET)).thenReturn(bucket);
        victim = new Flagstat(ARGUMENTS, computeEngine, storage, ResultsDirectory.defaultDirectory());
    }

    @Test
    public void returnsStatusFailedWhenJobFailsOnComputeEngine() {
        when(computeEngine.submit(any(), any())).thenReturn(PipelineStatus.FAILED);
        assertThat(victim.run(referenceRunMetadata(), referenceAlignmentOutput()).status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void runsSamtoolsFlagstatOnComputeEngine() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        victim.run(referenceRunMetadata(), referenceAlignmentOutput());
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "(" + TOOLS_SAMBAMBA + " flagstat -t " + PROC_COUNT + " " + inFile("reference.bam")
                        + " > " + outFile("reference.flagstat)"));
    }

    @Test
    public void downloadsInputBam() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        victim.run(referenceRunMetadata(), referenceAlignmentOutput());
        assertThat(jobDefinitionArgumentCaptor.getValue()
                .startupCommand()
                .asUnixString()).contains(new BucketInputOutput("run-reference").input("aligner/results/reference.bam"));
    }

    @Test
    public void uploadsOutputDirectory() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        victim.run(referenceRunMetadata(), referenceAlignmentOutput());
        assertThat(jobDefinitionArgumentCaptor.getValue()
                .startupCommand()
                .asUnixString()).contains(new BucketInputOutput(RUNTIME_BUCKET).output("flagstat/results"));
    }

    private ArgumentCaptor<VirtualMachineJobDefinition> captureAndReturnSuccess() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor =
                ArgumentCaptor.forClass(VirtualMachineJobDefinition.class);
        when(computeEngine.submit(any(), jobDefinitionArgumentCaptor.capture())).thenReturn(PipelineStatus.SUCCESS);
        return jobDefinitionArgumentCaptor;
    }

}