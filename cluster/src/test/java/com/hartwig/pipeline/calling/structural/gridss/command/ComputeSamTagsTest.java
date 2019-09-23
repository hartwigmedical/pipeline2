package com.hartwig.pipeline.calling.structural.gridss.command;

import org.junit.Before;
import org.junit.Test;

import static com.hartwig.pipeline.calling.structural.gridss.GridssTestConstants.*;
import static com.hartwig.pipeline.testsupport.TestConstants.OUT_DIR;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class ComputeSamTagsTest {
    private ComputeSamTags command;
    private String className;
    private String expectedOutputFile;

    @Before
    public void setup() {
        command = new ComputeSamTags(REFERENCE_BAM, REFERENCE_GENOME, REFERENCE_SAMPLE);
        className = "gridss.ComputeSamTags";
        expectedOutputFile = format("%s/gridss.tmp.withtags.%s.sv.bam", OUT_DIR, REFERENCE_SAMPLE);
    }

    @Test
    public void shouldGenerateCorrectJavaArguments() {
        GridssCommonArgumentsAssert.assertThat(command).generatesJavaInvocationUpToAndIncludingClassname(className);
    }

    @Test
    public void shouldReturnClassname() {
        assertThat(command.className()).isEqualTo(className);
    }

    @Test
    public void shouldUseStandardAmountOfMemory() {
        GridssCommonArgumentsAssert.assertThat(command).usesStandardAmountOfMemory();
    }

    @Test
    public void shouldCompleteCommandLineWithGridssArguments() {
        GridssCommonArgumentsAssert.assertThat(command)
                .hasGridssArguments(ARGS_TMP_DIR)
                .and("working_dir", OUT_DIR)
                .and(ARGS_REFERENCE_SEQUENCE)
                .and(ARGS_NO_COMPRESSION)
                .and(ARG_KEY_INPUT_SHORT, REFERENCE_BAM)
                .and(ARGS_OUTPUT_TO_STDOUT)
                .and("recalculate_sa_supplementary", "true")
                .and("soften_hard_clips", "true")
                .and("fix_mate_information", "true")
                .and("fix_duplicate_flag", "true")
                .and("tags", "null")
                .and("tags", "NM")
                .and("tags", "SA")
                .and("tags", "R2")
                .and("tags", "Q2")
                .and("tags", "MC")
                .and("tags", "MQ")
                .and("assume_sorted", "true")
                .andNoMore();
    }

    @Test
    public void shouldReturnResultantBam() {
        assertThat(command.resultantBam()).isNotNull();
        assertThat(command.resultantBam()).isEqualTo(expectedOutputFile);
    }
}
