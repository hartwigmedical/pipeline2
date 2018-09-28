package com.hartwig.pipeline.adam;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;

import com.hartwig.patient.Lane;
import com.hartwig.patient.Sample;

class BwaCommand {

    static List<String> tokens(Sample sample, Lane lane, int bwaThreads) {
        List<String> cmd = new ArrayList<>();
        cmd.add("bwa");
        cmd.add("mem");
        cmd.add("-p");
        cmd.add("-R");
        cmd.add(format("@RG\\tID:%s\\tLB:%s\\tPL:ILLUMINA\\tPU:%s\\tSM:%s",
                lane.recordGroupId(),
                sample.name(),
                lane.flowCellId(),
                sample.name()));
        cmd.add("-c");
        cmd.add("100");
        cmd.add("-t");
        cmd.add(String.valueOf(bwaThreads));
        cmd.add("$0");
        cmd.add("-");
        return cmd;
    }
}
