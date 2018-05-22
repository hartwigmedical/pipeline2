package hmf.pipeline;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import hmf.sample.FlowCell;
import hmf.sample.Lane;
import hmf.sample.RawSequencingOutput;

public class Pipeline {

    private final Map<PipelineOutput, Stage<Lane>> stages;
    private final Stage<FlowCell> merge;
    private static final String RESULTS_DIRECTORY = System.getProperty("user.dir") + "/results";

    private Pipeline(final Map<PipelineOutput, Stage<Lane>> stages, final Stage<FlowCell> merge) {
        this.stages = stages;
        this.merge = merge;
    }

    public void execute(RawSequencingOutput sequencing) throws IOException {
        createResultsOutputDirectory();
        forEachLaneIn(sequencing.sampled().lanes());
        if (merge != null) {
            merge.execute(sequencing.sampled());
        }
    }

    private void forEachLaneIn(final List<Lane> lanes) throws IOException {
        for (Lane lane : lanes) {
            executeForEachLane(lane, PipelineOutput.UNMAPPED);
            executeForEachLane(lane, PipelineOutput.ALIGNED);
            executeForEachLane(lane, PipelineOutput.SORTED);
        }
    }

    private void executeForEachLane(final Lane lane, final PipelineOutput pipelineOutput) throws IOException {
        Stage<Lane> stage = stages.get(pipelineOutput);
        if (stage != null) {
            stage.execute(lane);
        }
    }

    private static void createResultsOutputDirectory() throws IOException {
        FileUtils.deleteDirectory(new File(RESULTS_DIRECTORY));
        Files.createDirectory(Paths.get(RESULTS_DIRECTORY));
    }

    public static Pipeline.Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final Map<PipelineOutput, Stage<Lane>> stages = new HashMap<>();
        private Stage<FlowCell> merge;

        public Builder addLaneStage(Stage<Lane> stage) {
            stages.put(stage.output(), stage);
            return this;
        }

        public Builder setMergeStage(Stage<FlowCell> merge) {
            this.merge = merge;
            return this;
        }

        public Pipeline build() {
            return new Pipeline(stages, merge);
        }
    }
}
