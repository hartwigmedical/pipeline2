package com.hartwig.bcl2fastq.qc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public class UndeterminedReadPercentageTest {

    private UnderminedReadPercentage victim;

    @Before
    public void setUp() {
        victim = new UnderminedReadPercentage(1);
    }

    @Test
    public void passesWhenLessThanThresholdUndetermined() {
        QualityControlResult result = victim.apply(stats(1000, 1), "");
        assertThat(result.pass()).isTrue();
    }

    @Test
    public void failsWhenMoreThanThresholdUndetermined() {
        QualityControlResult result = victim.apply(stats(50, 2), "");
        assertThat(result.pass()).isFalse();
    }

    private ImmutableStats stats(final int sampleYield, final int undeterminedYield) {
        return ImmutableStats.builder()
                .flowcell("test")
                .addConversionResults(ImmutableLaneStats.builder()
                        .laneNumber(1)
                        .addDemuxResults(ImmutableSampleStats.builder().yield(sampleYield).build())
                        .undetermined(ImmutableSampleStats.builder().yield(undeterminedYield).build())
                        .build())
                .build();
    }

}