package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;

import org.junit.Before;
import org.junit.Test;

public class AssembleBreakendsTest implements CommonEntities {
    private AssembleBreakends command;

    @Before
    public void setup() {
        command = new AssembleBreakends(REFERENCE_BAM, TUMOR_BAM, REFERENCE_GENOME, REFERENCE_SAMPLE + "_" + TUMOR_SAMPLE);
    }

    @Test
    public void shouldReturnClassName() {
        assertThat(command.className()).isEqualTo("gridss.AssembleBreakends");
    }

    @Test
    public void shouldUseSpecificAmountOfHeap() {
        assertThat(command.memoryGb()).isEqualTo(80);
    }

    @Test
    public void shouldConstructGridssArguments() {
        GridssCommonArgumentsAssert.assertThat(command).hasGridssArguments(ARGS_TMP_DIR)
                .and(ARG_KEY_WORKING_DIR, OUT_DIR)
                .and(ARGS_REFERENCE_SEQUENCE)
                .and(ARG_KEY_INPUT, REFERENCE_BAM)
                .and(ARG_KEY_INPUT, TUMOR_BAM)
                .and(ARG_KEY_OUTPUT, command.assemblyBam())
                .and(ARGS_BLACKLIST)
                .and(ARGS_GRIDSS_CONFIG)
                .andNoMore();
    }

    @Test
    public void shouldReturnAssemblyBamPath() {
        assertThat(command.assemblyBam()).isEqualTo(format("%s/sample12345678R_sample12345678T.assembly.bam", OUT_DIR));
    }
}
