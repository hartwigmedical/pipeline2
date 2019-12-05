package com.hartwig.bcl2fastq.qc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.hartwig.bcl2fastq.FastqId;
import com.hartwig.pipeline.jackson.ObjectMappers;

public class QualityControl {

    private final List<FlowcellQualityCheck> flowcellChecks;
    private final List<SampleQualityCheck> sampleChecks;
    private final List<FastqQualityCheck> fastqQualityChecks;

    QualityControl(final List<FlowcellQualityCheck> flowcellChecks, final List<SampleQualityCheck> sampleChecks,
            final List<FastqQualityCheck> fastqQualityChecks) {
        this.sampleChecks = sampleChecks;
        this.flowcellChecks = flowcellChecks;
        this.fastqQualityChecks = fastqQualityChecks;
    }

    public QualityControlResults evaluate(String statsJson, String conversionLog) {
        try {
            Stats stats = ObjectMappers.get().readValue(statsJson, Stats.class);
            List<QualityControlResult> flowcellLevel = new ArrayList<>();
            Multimap<String, QualityControlResult> sampleLevel = ArrayListMultimap.create();
            Multimap<FastqId, QualityControlResult> fastqLevel = ArrayListMultimap.create();
            for (FlowcellQualityCheck check : flowcellChecks) {
                flowcellLevel.add(check.apply(stats, conversionLog));
            }
            for (SampleQualityCheck sampleCheck : sampleChecks) {
                sampleCheck.apply(stats).forEach(sampleLevel::put);
            }
            for (FastqQualityCheck fastqQualityCheck : fastqQualityChecks) {
                fastqQualityCheck.apply(stats).forEach(fastqLevel::put);
            }
            return QualityControlResults.builder().addAllFlowcellLevel(flowcellLevel).putAllSampleLevel(sampleLevel.asMap()).putAllFastqLevel(fastqLevel.asMap()).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static QualityControl defaultQC() {
        return new QualityControl(ImmutableList.of(new UndeterminedReadPercentage(6), new ErrorsInLog()),
                ImmutableList.of(new SampleMinimumYield(1_000_000_000)),
                ImmutableList.of(new FastqMinimumQ30(75)));
    }
}
