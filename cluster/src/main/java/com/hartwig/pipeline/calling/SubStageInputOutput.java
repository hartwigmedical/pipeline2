package com.hartwig.pipeline.calling;

import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;

import org.immutables.value.Value;

@Value.Immutable
public interface SubStageInputOutput {

    @Value.Parameter
    String sampleName();

    @Value.Parameter
    OutputFile outputFile();

    @Value.Parameter
    BashStartupScript currentBash();

    static SubStageInputOutput of(final String tumorSampleName, final OutputFile outputFile, final BashStartupScript currentBash) {
        return ImmutableSubStageInputOutput.of(tumorSampleName, outputFile, currentBash);
    }
}