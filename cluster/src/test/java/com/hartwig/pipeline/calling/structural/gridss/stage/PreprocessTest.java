package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;

import static com.hartwig.pipeline.calling.structural.gridss.stage.BashAssertions.assertBashContains;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.calling.structural.gridss.command.CollectGridssMetrics;
import com.hartwig.pipeline.calling.structural.gridss.command.ComputeSamTags;
import com.hartwig.pipeline.calling.structural.gridss.command.ExtractSvReads;
import com.hartwig.pipeline.calling.structural.gridss.command.SambambaGridssSortCommand;
import com.hartwig.pipeline.calling.structural.gridss.command.SoftClipsToSplitReads;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.unix.MkDirCommand;
import com.hartwig.pipeline.execution.vm.unix.PipeCommands;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class PreprocessTest extends SubStageTest {
    private String workingDirectory = "/some/directory";
    private String inputBamFilename = REFERENCE_SAMPLE + ".bam";
    private String inputBamFullPath = format("%s/%s", IN_DIR, inputBamFilename);
    private BashStartupScript initialScript;
    private ExtractSvReads expectedExtractSvReads;
    private ComputeSamTags expectedComputeSamTags;
    private ArgumentCaptor<BashCommand> captor;

    @Override
    public SubStage createVictim() {
        return new Preprocess(inputBamFullPath, workingDirectory, REFERENCE_SAMPLE, REFERENCE_GENOME);
    }

    @Override
    public String expectedPath() {
        return format("%s/%s.preprocess.bam", OUT_DIR, REFERENCE_SAMPLE);
    }

    @Override
    protected String sampleName() {
        return REFERENCE_SAMPLE;
    }

    @Before
    public void setup() {
        String insertSizeMetrics = format("%s/%s.insert_size_metrics", workingDirectory, inputBamFilename);
        expectedExtractSvReads = new ExtractSvReads(inputBamFullPath, REFERENCE_SAMPLE, insertSizeMetrics, workingDirectory);
        expectedComputeSamTags = new ComputeSamTags(expectedExtractSvReads.resultantBam(), REFERENCE_GENOME, REFERENCE_SAMPLE);

        initialScript = mock(BashStartupScript.class);
        OutputFile inputFile = mock(OutputFile.class);

        captor = ArgumentCaptor.forClass(BashCommand.class);
        BashStartupScript finishedScript = createVictim().bash(inputFile, mock(OutputFile.class), initialScript);
        verify(finishedScript, times(5)).addCommand(captor.capture());
    }

    @Test
    public void shouldAddCommandsInCorrectOrder() {
        InOrder inOrder = Mockito.inOrder(initialScript);
        inOrder.verify(initialScript).addCommand(any(MkDirCommand.class));
        inOrder.verify(initialScript).addCommand(any(CollectGridssMetrics.class));
        inOrder.verify(initialScript, times(2)).addCommand(any(PipeCommands.class));
        inOrder.verify(initialScript).addCommand(any(SoftClipsToSplitReads.ForPreprocess.class));
    }

    @Test
    public void shouldMakeWorkingDirectoryFirst() {
        assertThat(captor.getAllValues().get(0).asBash()).isEqualTo(new MkDirCommand(workingDirectory).asBash());
    }

    @Test
    public void shouldAddCorrectCollectGridssMetrics() {
        assertBashContains(new CollectGridssMetrics(inputBamFullPath, format("%s/%s", workingDirectory, inputBamFilename)), captor);
    }

    @Test
    public void shouldAddCorrectExtractSvReadsPipeline() {
        SambambaGridssSortCommand sortCommand = SambambaGridssSortCommand.sortByName(expectedExtractSvReads.resultantBam());
        assertBashContains(new PipeCommands(expectedExtractSvReads, sortCommand), captor);
    }

    @Test
    public void shouldAddCorrectComputeSamTagsPipeline() {
        SambambaGridssSortCommand sortCommand = SambambaGridssSortCommand.sortByDefault(expectedComputeSamTags.resultantBam());
        assertBashContains(new PipeCommands(expectedComputeSamTags, sortCommand), captor);
    }

    @Test
    public void shouldAddCorrectSoftClipsToSplitReads() {
        String outputSvBam = format("%s/%s.sv.bam", workingDirectory, inputBamFilename);
        assertBashContains(new SoftClipsToSplitReads.ForPreprocess(expectedComputeSamTags.resultantBam(), REFERENCE_GENOME, outputSvBam),
                captor);
    }
}