package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AnnotateUntemplatedSequenceTest implements CommonEntities {
    private String className;
    private static final String OUTFILE = String.format("%s/annotated.vcf", OUT_DIR);
    private AnnotateUntemplatedSequence command;
    private String inputVcf;

    @Before
    public void setup() {
        className = "gridss.AnnotateUntemplatedSequence";
        inputVcf = "/intermediate.vcf";
        command = new AnnotateUntemplatedSequence(inputVcf, REFERENCE_GENOME);
    }

    @Test
    public void shouldReturnClassName() {
        assertThat(command.className()).isEqualTo(className);
    }

    @Test
    public void shouldUseStandardGridssHeapSize() {
        GridssCommonArgumentsAssert.assertThat(command).usesStandardAmountOfMemory();
    }

    @Test
    public void shouldConstructGridssOptions() {
        GridssCommonArgumentsAssert.assertThat(command)
                .hasGridssArguments(ARGS_REFERENCE_SEQUENCE)
                .and(ARG_KEY_INPUT, inputVcf)
                .and(ARG_KEY_OUTPUT, command.resultantVcf())
                .andNoMore();
    }

    @Test
    public void shouldReturnResultantVcf() {
        assertThat(command.resultantVcf()).isNotNull();
        assertThat(command.resultantVcf()).isEqualTo(OUTFILE);
    }
}