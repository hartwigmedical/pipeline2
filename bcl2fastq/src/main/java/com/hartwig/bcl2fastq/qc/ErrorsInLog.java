package com.hartwig.bcl2fastq.qc;

public class ErrorsInLog implements FlowcellQualityCheck {
    @Override
    public QualityControlResult apply(final Stats stats, final String log) {
        return QualityControlResult.of("Errors in log", log.contains("with 0 errors and"));
    }
}
