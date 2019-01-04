package com.hartwig.pipeline.performance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.hartwig.patient.Lane;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.io.sources.SampleData;

import org.junit.Before;
import org.junit.Test;

public class ClusterOptimizerTest {

    private static final Sample SAMPLE_WITH_TWO_LANES = Sample.builder("test", "test").addLanes(mock(Lane.class), mock(Lane.class)).build();
    private static final long FORTY_GIGS = (long) (40 * 1e9);
    private ClusterOptimizer victim;

    @Before
    public void setUp() throws Exception {
        victim = new ClusterOptimizer(CpuFastQSizeRatio.of(5), false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyFilesThrowsIllegalArgumentException() {
        victim = new ClusterOptimizer(CpuFastQSizeRatio.of(5), false);
        victim.optimize(sampleData(SAMPLE_WITH_TWO_LANES, 0));
    }

    @Test
    public void providesEnoughCpusForRatioAndSplitsBetweenPrimaryAndPreemptibleWorkers() {
        PerformanceProfile profile = victim.optimize(sampleData(SAMPLE_WITH_TWO_LANES, FORTY_GIGS));
        assertThat(profile.numPrimaryWorkers()).isEqualTo(3);
        assertThat(profile.numPreemtibleWorkers()).isEqualTo(3);
    }

    @Test
    public void usesDefaultWorkerAndMasterTypes() {
        PerformanceProfile profile = victim.optimize(sampleData(SAMPLE_WITH_TWO_LANES, FORTY_GIGS));
        assertThat(profile.primaryWorkers()).isEqualTo(MachineType.defaultWorker());
        assertThat(profile.master()).isEqualTo(MachineType.defaultMaster());
    }

    @Test
    public void cpusFlooredForVerySmallFiles() {
        victim = new ClusterOptimizer(CpuFastQSizeRatio.of(5), false);
        PerformanceProfile profile = victim.optimize(sampleData(SAMPLE_WITH_TWO_LANES, 1));
        assertThat(profile.numPrimaryWorkers()).isEqualTo(2);
        assertThat(profile.numPreemtibleWorkers()).isEqualTo(0);
    }

    @Test
    public void usesOnlyPrimaryVmsWhenSpecified() {
        victim = new ClusterOptimizer(CpuFastQSizeRatio.of(5), true);
        PerformanceProfile profile = victim.optimize(sampleData(SAMPLE_WITH_TWO_LANES, FORTY_GIGS));
        assertThat(profile.numPreemtibleWorkers()).isZero();
        assertThat(profile.numPrimaryWorkers()).isEqualTo(6);
    }

    private SampleData sampleData(final Sample sample, final long size) {
        return SampleData.of(sample, size);
    }
}