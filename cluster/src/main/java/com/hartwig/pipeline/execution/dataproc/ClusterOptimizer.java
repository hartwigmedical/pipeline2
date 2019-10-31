package com.hartwig.pipeline.execution.dataproc;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.alignment.sample.SampleData;
import com.hartwig.pipeline.execution.MachineType;

public class ClusterOptimizer {

    private static final long BYTES_PER_GB = (long) Math.pow(1024, 3);

    private final CpuFastQSizeRatio cpuToFastQSizeRatio;
    private final boolean usePreemtibleVms;

    public ClusterOptimizer(final CpuFastQSizeRatio cpuToFastQSizeRatio, final boolean usePreemtibleVms) {
        this.cpuToFastQSizeRatio = cpuToFastQSizeRatio;
        this.usePreemtibleVms = usePreemtibleVms;
    }

    public DataprocPerformanceProfile optimize(SampleData sampleData) {
        Sample sample = sampleData.sample();
        if (sampleData.sizeInBytesGZipped() <= 0) {
            throw new IllegalArgumentException(String.format("Sample [%s] lanes had no data. Cannot calculate data size or cpu requirements",
                    sample.name()));
        }
        long totalFileSizeGB = sampleData.sizeInBytesGZipped() / BYTES_PER_GB;
        double totalCpusRequired = totalFileSizeGB * cpuToFastQSizeRatio.cpusPerGB();
        MachineType defaultWorker = MachineType.defaultWorker();
        int numWorkers = new Double(totalCpusRequired / defaultWorker.cpus()).intValue();
        int numPreemptible = usePreemtibleVms ? numWorkers / 2 : 0;
        return DataprocPerformanceProfile.builder()
                .master(MachineType.defaultMaster())
                .primaryWorkers(defaultWorker)
                .preemtibleWorkers(MachineType.defaultPreemtibleWorker())
                .numPrimaryWorkers(Math.max(2, numWorkers - numPreemptible))
                .numPreemtibleWorkers(numPreemptible)
                .fastQSizeGB(totalFileSizeGB)
                .build();
    }
}