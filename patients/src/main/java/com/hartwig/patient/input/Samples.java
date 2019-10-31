package com.hartwig.patient.input;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.hartwig.patient.ImmutableLane;
import com.hartwig.patient.Lane;
import com.hartwig.patient.Sample;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;

class Samples {

    static Sample createPairedEndSample(final FileSystem fileSystem, final Path sampleDirectory, final String sampleName,
            final String postfix) throws IOException {
        Map<String, ImmutableLane.Builder> builders = new HashMap<>();
        String sampleNameWithPostfix = sampleName + postfix;
        for (FileStatus fileStatus : fileSystem.listStatus(sampleDirectory,
                new GlobFilter(sampleNameWithPostfix + "_*_S*_L*_R?_*.fastq*"))) {
            String fileName = fileStatus.getPath().getName();
            String[] tokens = fileName.split("_");
            String laneNumber = tokens[3];
            String flowCellId = tokens[1];
            ImmutableLane.Builder builder = builders.computeIfAbsent(laneNumber + flowCellId,
                    s -> Lane.builder()
                            .directory(sampleDirectory.toString())
                            .laneNumber(laneNumber)
                            .name(sampleNameWithPostfix + "_" + laneNumber)
                            .flowCellId(flowCellId)
                            .index(tokens[2])
                            .suffix(tokens[5].substring(0, tokens[5].indexOf('.'))));
            if (tokens[4].equals("R1")) {
                builder.firstOfPairPath(fileStatus.getPath().toString());
            } else if (tokens[4].equals("R2")) {
                builder.secondOfPairPath(fileStatus.getPath().toString());
            }
            builder.flowCellId(flowCellId);
        }
        return Sample.builder(sampleDirectory.toString(), sampleNameWithPostfix)
                .addAllLanes(builders.values().stream().map(ImmutableLane.Builder::build).collect(Collectors.toList()))
                .type(sampleName.toLowerCase().endsWith("r") ? Sample.Type.REFERENCE : Sample.Type.TUMOR)
                .build();
    }
}