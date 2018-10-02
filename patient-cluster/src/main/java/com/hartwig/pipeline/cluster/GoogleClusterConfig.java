package com.hartwig.pipeline.cluster;

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.List;

import com.google.api.services.dataproc.model.ClusterConfig;
import com.google.api.services.dataproc.model.DiskConfig;
import com.google.api.services.dataproc.model.GceClusterConfig;
import com.google.api.services.dataproc.model.InstanceGroupConfig;
import com.google.api.services.dataproc.model.NodeInitializationAction;
import com.google.api.services.dataproc.model.SoftwareConfig;
import com.google.common.collect.ImmutableMap;
import com.hartwig.pipeline.bootstrap.NodeInitialization;
import com.hartwig.pipeline.bootstrap.RuntimeBucket;
import com.hartwig.pipeline.performance.MachineType;
import com.hartwig.pipeline.performance.PerformanceProfile;

import org.jetbrains.annotations.NotNull;

class GoogleClusterConfig {

    private final ClusterConfig config;

    private GoogleClusterConfig(final ClusterConfig config) {
        this.config = config;
    }

    ClusterConfig config() {
        return config;
    }

    static GoogleClusterConfig from(final String project, RuntimeBucket runtimeBucket, NodeInitialization nodeInitialization,
            PerformanceProfile profile) throws FileNotFoundException {
        DiskConfig diskConfig = diskConfig(profile.primaryWorkers());
        ClusterConfig config = clusterConfig(masterConfig(profile.master()),
                primaryWorkerConfig(diskConfig, profile.primaryWorkers(), profile.numPrimaryWorkers()),
                secondaryWorkerConfig(profile, diskConfig, profile.preemtibleWorkers()),
                runtimeBucket.getName(),
                softwareConfig(profile),
                initializationActions(runtimeBucket, nodeInitialization),
                gceClusterConfig(project));
        return new GoogleClusterConfig(config);
    }

    @NotNull
    private static GceClusterConfig gceClusterConfig(final String project) {
        return new GceClusterConfig().setServiceAccount(String.format("dataproc-monitor@%s.iam.gserviceaccount.com", project))
                .setServiceAccountScopes(Collections.singletonList("https://www.googleapis.com/auth/monitoring"));
    }

    @NotNull
    private static SoftwareConfig softwareConfig(final PerformanceProfile profile) {
        return new SoftwareConfig().setProperties(ImmutableMap.<String, String>builder().put("yarn:yarn.scheduler.minimum-allocation-vcores",
                String.valueOf(profile.primaryWorkers().cpus()))
                .put("yarn:yarn.nodemanager.vmem-check-enabled", "false")
                .put("yarn:yarn.nodemanager.pmem-check-enabled", "false")
                .put("capacity-scheduler:yarn.scheduler.capacity.resource-calculator",
                        "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator")
                .build());
    }

    @NotNull
    private static List<NodeInitializationAction> initializationActions(final RuntimeBucket runtimeBucket,
            final NodeInitialization nodeInitialization) throws FileNotFoundException {
        return Collections.singletonList(new NodeInitializationAction().setExecutableFile(nodeInitialization.run(runtimeBucket)));
    }

    private static InstanceGroupConfig masterConfig(final MachineType machineType) {
        return new InstanceGroupConfig().setMachineTypeUri(machineType.uri()).setNumInstances(1);
    }

    private static ClusterConfig clusterConfig(final InstanceGroupConfig masterConfig, final InstanceGroupConfig primaryWorkerConfig,
            final InstanceGroupConfig secondaryWorkerConfig, final String bucket, final SoftwareConfig softwareConfig,
            final List<NodeInitializationAction> initializationActions, final GceClusterConfig gceClusterConfig) {
        return new ClusterConfig().setMasterConfig(masterConfig)
                .setWorkerConfig(primaryWorkerConfig)
                .setSecondaryWorkerConfig(secondaryWorkerConfig)
                .setConfigBucket(bucket)
                .setSoftwareConfig(softwareConfig)
                .setInitializationActions(initializationActions)
                .setGceClusterConfig(gceClusterConfig);
    }

    private static InstanceGroupConfig primaryWorkerConfig(final DiskConfig diskConfig, final MachineType machineType,
            final int numInstances) {
        return new InstanceGroupConfig().setMachineTypeUri(machineType.uri()).setNumInstances(numInstances).setDiskConfig(diskConfig);
    }

    @NotNull
    private static DiskConfig diskConfig(MachineType machineType) {
        return new DiskConfig().setBootDiskSizeGb(machineType.diskGB());
    }

    private static InstanceGroupConfig secondaryWorkerConfig(final PerformanceProfile profile, final DiskConfig diskConfig,
            final MachineType machineType) {
        return new InstanceGroupConfig().setMachineTypeUri(machineType.uri()).setNumInstances(profile.numPreemtibleWorkers())
                .setIsPreemptible(true)
                .setDiskConfig(diskConfig);
    }
}
