package com.hartwig.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.hartwig.pipeline.execution.PipelineStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineState {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineState.class);
    private final List<StageOutput> stageOutputs;

    public PipelineState() {
        this.stageOutputs = new ArrayList<>();
    }

    private PipelineState(final List<StageOutput> stageOutputs) {
        this.stageOutputs = stageOutputs;
    }

    public <T extends StageOutput> T add(final T stageOutput) {
        stageOutputs.add(stageOutput);
        return stageOutput;
    }

    PipelineState combineWith(PipelineState state) {
        if (state != null) {
            state.stageOutputs().forEach(this::add);
        } else {
            throw new IllegalArgumentException("State from one of the two pipelines was null. Did that pipeline run successfully?");
        }
        return this;
    }

    PipelineState copy() {
        return new PipelineState(new ArrayList<>(this.stageOutputs()));
    }

    public List<StageOutput> stageOutputs() {
        return stageOutputs;
    }

    public boolean shouldProceed() {
        boolean shouldProceed = status() != PipelineStatus.FAILED;
        if (!shouldProceed) {
            LOGGER.warn("Halting pipeline due to required stage failure [{}]", this);
        }
        return shouldProceed;
    }

    public PipelineStatus status() {
        return stageOutputs().stream()
                .filter(Objects::nonNull)
                .map(StageOutput::status)
                .filter(status -> !status.equals(PipelineStatus.SUCCESS) && !status.equals(PipelineStatus.SKIPPED) && !status.equals(
                        PipelineStatus.PERSISTED) && !status.equals(PipelineStatus.PROVIDED))
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
