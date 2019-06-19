package com.hartwig.pipeline;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.PipelineStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineState {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineState.class);
    private final List<StageOutput> stageOutputs = Lists.newArrayList();

    <T extends StageOutput> T add(final T stageOutput) {
        stageOutputs.add(stageOutput);
        return stageOutput;
    }

    List<StageOutput> stageOutputs() {
        return stageOutputs;
    }

    boolean shouldProceed() {
        boolean shouldProceed = status() == PipelineStatus.SUCCESS;
        if (!shouldProceed) {
            LOGGER.warn("Halting pipeline due to required stage failure [{}]", this);
        }
        return shouldProceed;
    }

    PipelineStatus status() {
        return stageOutputs().stream()
                .filter(Objects::nonNull)
                .map(StageOutput::status)
                .filter(status -> status == PipelineStatus.FAILED)
                .findFirst()
                .orElse(PipelineStatus.SUCCESS);
    }

    @Override
    public String toString() {
        return "PipelineState{stageOutputs=[" + stageOutputs.stream()
                .map(stageOutput -> stageOutput.name() + ":" + stageOutput.status() + "\n")
                .collect(Collectors.joining()) + "]}";
    }
}
