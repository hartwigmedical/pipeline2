package com.hartwig.pipeline.calling.structural.gridss.command;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.structural.gridss.GridssTestEntities;

import org.junit.Before;
import org.junit.Test;

public class AssembleSoftClipsToSplitReadsTest implements GridssTestEntities {
    private String className;
    private SoftClipsToSplitReads.ForAssemble command;

    @Before
    public void setup() {
        className = "gridss.SoftClipsToSplitReads";
        command = new SoftClipsToSplitReads.ForAssemble(REFERENCE_BAM, REFERENCE_GENOME, OUTPUT_BAM);
    }

    @Test
    public void shouldGenerateCorrectJavaArguments() {
        GridssCommonArgumentsAssert.assertThat(command).generatesJavaInvocationUpToAndIncludingClassname(className);
    }

    @Test
    public void shouldReturnClassName() {
        assertThat(command.className()).isEqualTo(className);
    }

    @Test
    public void shouldUseStandardAmountOfMemory() {
        GridssCommonArgumentsAssert.assertThat(command).usesStandardAmountOfMemory();
    }

    @Test
    public void shouldConstructGridssArguments() {
        GridssCommonArgumentsAssert.assertThat(command).hasGridssArguments(ARGS_TMP_DIR)
                .and(ARGS_WORKING_DIR)
                .and(ARGS_REFERENCE_SEQUENCE)
                .and(ARG_KEY_INPUT_SHORT, REFERENCE_BAM)
                .and(ARG_KEY_OUTPUT_SHORT, OUTPUT_BAM)
                .and("realign_entire_read", "true")
                .andNoMore();
    }
}