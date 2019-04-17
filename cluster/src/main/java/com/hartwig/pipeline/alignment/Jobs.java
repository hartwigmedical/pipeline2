package com.hartwig.pipeline.alignment;

import com.hartwig.pipeline.execution.dataproc.SparkExecutor;
import com.hartwig.pipeline.cost.CostCalculator;
import com.hartwig.pipeline.io.GoogleStorageStatusCheck;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.StatusCheck;
import com.hartwig.pipeline.metrics.Monitor;

class Jobs {

    static Job noStatusCheck(final SparkExecutor cluster, final CostCalculator costCalculator, final Monitor monitor) {
        return new Job(cluster,
                costCalculator,
                monitor,
                StatusCheck.alwaysSuccess());
    }

    static Job statusCheckGoogleStorage(final SparkExecutor cluster, final CostCalculator costCalculator, final Monitor monitor) {
        return new Job(cluster,
                costCalculator,
                monitor,
                new GoogleStorageStatusCheck(ResultsDirectory.defaultDirectory()));
    }
}