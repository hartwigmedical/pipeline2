package com.hartwig.pipeline.execution.dataproc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.MachineType;
import com.hartwig.pipeline.storage.RuntimeBucket;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

public class GoogleClusterConfigTest {

    private RuntimeBucket runtimeBucket;
    private NodeInitialization nodeInitialization;
    private GoogleClusterConfig victim;

    @Before
    public void setUp() throws Exception {
        runtimeBucket = mock(RuntimeBucket.class);
        when(runtimeBucket.name()).thenReturn("runtime-bucket");
        nodeInitialization = mock(NodeInitialization.class);
        victim = GoogleClusterConfig.from(runtimeBucket, nodeInitialization, profileBuilder().build(), Arguments.testDefaults());
    }

    @Test
    public void oneMasterTwoPrimaryWorkersAndRemainingNodesSecondary() throws Exception {
        GoogleClusterConfig victim = GoogleClusterConfig.from(runtimeBucket,
                nodeInitialization,
                profileBuilder().numPreemtibleWorkers(3).build(),
                Arguments.testDefaults());
        assertThat(victim.config().getMasterConfig().getNumInstances()).isEqualTo(1);
        assertThat(victim.config().getWorkerConfig().getNumInstances()).isEqualTo(2);
        assertThat(victim.config().getSecondaryWorkerConfig().getNumInstances()).isEqualTo(3);
    }

    @Test
    public void allNodesUseResolvedMachineType() {
        assertThat(victim.config().getMasterConfig().getMachineTypeUri()).isEqualTo(MachineType.GOOGLE_STANDARD_16);
        assertThat(victim.config().getWorkerConfig().getMachineTypeUri()).isEqualTo(MachineType.GOOGLE_HIGHMEM_32);
        assertThat(victim.config().getSecondaryWorkerConfig().getMachineTypeUri()).isEqualTo(MachineType.GOOGLE_HIGHMEM_32);
    }

    @Test
    public void idleTtlSetOnLifecycleConfig() {
        assertThat(victim.config().getLifecycleConfig().getIdleDeleteTtl()).isEqualTo("600s");
    }

    @Test
    public void privateNetworkUsedWhenSpecified() throws Exception {
        victim = GoogleClusterConfig.from(runtimeBucket,
                nodeInitialization,
                profileBuilder().build(),
                Arguments.testDefaultsBuilder().privateNetwork("private").build());
        assertThat(victim.config().getGceClusterConfig().getSubnetworkUri()).isEqualTo(
                "projects/hmf-pipeline-development/regions/europe-west4/subnetworks/private");
    }

    @NotNull
    private static ImmutableDataprocPerformanceProfile.Builder profileBuilder() {
        return DataprocPerformanceProfile.builder().numPreemtibleWorkers(5).numPrimaryWorkers(2);
    }
}