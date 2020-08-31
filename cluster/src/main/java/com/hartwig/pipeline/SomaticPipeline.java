package com.hartwig.pipeline;

import static com.hartwig.pipeline.resource.ResourceFilesFactory.buildResourceFiles;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.AlignmentOutputStorage;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.calling.somatic.SageCaller;
import com.hartwig.pipeline.calling.somatic.SomaticCallerOutput;
import com.hartwig.pipeline.calling.structural.StructuralCaller;
import com.hartwig.pipeline.calling.structural.StructuralCallerOutput;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SomaticMetadataApi;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetrics;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.report.FullSomaticResults;
import com.hartwig.pipeline.report.PipelineResults;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.StageRunner;
import com.hartwig.pipeline.tertiary.amber.Amber;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.bachelor.Bachelor;
import com.hartwig.pipeline.tertiary.bachelor.BachelorOutput;
import com.hartwig.pipeline.tertiary.chord.Chord;
import com.hartwig.pipeline.tertiary.chord.ChordOutput;
import com.hartwig.pipeline.tertiary.cobalt.Cobalt;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;
import com.hartwig.pipeline.tertiary.healthcheck.HealthCheckOutput;
import com.hartwig.pipeline.tertiary.healthcheck.HealthChecker;
import com.hartwig.pipeline.tertiary.linx.Linx;
import com.hartwig.pipeline.tertiary.linx.LinxOutput;
import com.hartwig.pipeline.tertiary.purple.Purple;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SomaticPipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(SomaticPipeline.class);

    private final Arguments arguments;
    private final StageRunner<SomaticRunMetadata> stageRunner;
    private final AlignmentOutputStorage alignmentOutputStorage;
    private final OutputStorage<BamMetricsOutput, SingleSampleRunMetadata> bamMetricsOutputStorage;
    private final OutputStorage<GermlineCallerOutput, SingleSampleRunMetadata> germlineCallerOutputStorage;
    private final SomaticMetadataApi setMetadataApi;
    private final PipelineResults pipelineResults;
    private final FullSomaticResults fullSomaticResults;
    private final ExecutorService executorService;

    SomaticPipeline(final Arguments arguments, final StageRunner<SomaticRunMetadata> stageRunner,
            final AlignmentOutputStorage alignmentOutputStorage,
            final OutputStorage<BamMetricsOutput, SingleSampleRunMetadata> bamMetricsOutputStorage,
            final OutputStorage<GermlineCallerOutput, SingleSampleRunMetadata> germlineCallerOutputStorage,
            final SomaticMetadataApi setMetadataApi, final PipelineResults pipelineResults, final FullSomaticResults fullSomaticResults,
            final ExecutorService executorService) {
        this.arguments = arguments;
        this.stageRunner = stageRunner;
        this.alignmentOutputStorage = alignmentOutputStorage;
        this.bamMetricsOutputStorage = bamMetricsOutputStorage;
        this.germlineCallerOutputStorage = germlineCallerOutputStorage;
        this.setMetadataApi = setMetadataApi;
        this.pipelineResults = pipelineResults;
        this.fullSomaticResults = fullSomaticResults;
        this.executorService = executorService;
    }

    public PipelineState run() {

        PipelineState state = new PipelineState();

        SomaticRunMetadata metadata = setMetadataApi.get();
        LOGGER.info("Pipeline5 somatic pipeline starting for set [{}]", metadata.runName());

        final ResourceFiles resourceFiles = buildResourceFiles(arguments);

        if (metadata.maybeTumor().isPresent()) {
            AlignmentOutput referenceAlignmentOutput =
                    alignmentOutputStorage.get(metadata.reference()).orElseThrow(throwIllegalState(metadata.reference().sampleId()));
            AlignmentOutput tumorAlignmentOutput =
                    alignmentOutputStorage.get(metadata.tumor()).orElseThrow(throwIllegalState(metadata.tumor().sampleName()));
            AlignmentPair pair = AlignmentPair.of(referenceAlignmentOutput, tumorAlignmentOutput);

            try {
                Future<AmberOutput> amberOutputFuture = executorService.submit(() -> stageRunner.run(metadata, new Amber(pair, resourceFiles)));
                Future<CobaltOutput> cobaltOutputFuture = executorService.submit(() -> stageRunner.run(metadata, new Cobalt(pair, resourceFiles)));
                Future<SomaticCallerOutput> sageCallerOutputFuture =
                        executorService.submit(() -> stageRunner.run(metadata, new SageCaller(pair, resourceFiles)));
                Future<StructuralCallerOutput> structuralCallerOutputFuture =
                        executorService.submit(() -> stageRunner.run(metadata, new StructuralCaller(pair, resourceFiles)));
                AmberOutput amberOutput = pipelineResults.add(state.add(amberOutputFuture.get()));
                CobaltOutput cobaltOutput = pipelineResults.add(state.add(cobaltOutputFuture.get()));
                SomaticCallerOutput sageOutput = pipelineResults.add(state.add(sageCallerOutputFuture.get()));
                StructuralCallerOutput structuralCallerOutput = pipelineResults.add(state.add(structuralCallerOutputFuture.get()));

                if (state.shouldProceed()) {
                    Future<PurpleOutput> purpleOutputFuture = executorService.submit(() -> pipelineResults.add(state.add(stageRunner.run(
                            metadata,
                            new Purple(resourceFiles, sageOutput, structuralCallerOutput, amberOutput, cobaltOutput, arguments.shallow())))));
                    PurpleOutput purpleOutput = purpleOutputFuture.get();
                    if (state.shouldProceed()) {
                        BamMetricsOutput tumorMetrics = bamMetricsOutputStorage.get(metadata.tumor(), new BamMetrics(resourceFiles, pair.tumor()));
                        BamMetricsOutput referenceMetrics =
                                bamMetricsOutputStorage.get(metadata.reference(), new BamMetrics(resourceFiles, pair.reference()));

                        Future<HealthCheckOutput> healthCheckOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                                new HealthChecker(referenceMetrics, tumorMetrics, amberOutput, purpleOutput)));
                        Future<LinxOutput> linxOutputFuture =
                                executorService.submit(() -> stageRunner.run(metadata, new Linx(purpleOutput, resourceFiles)));
                        Future<BachelorOutput> bachelorOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                                new Bachelor(resourceFiles, purpleOutput,
                                        pair.tumor(),
                                        germlineCallerOutputStorage.get(metadata.reference(), new GermlineCaller(pair.reference(), resourceFiles)))));
                        Future<ChordOutput> chordOutputFuture =
                                executorService.submit(() -> stageRunner.run(metadata, new Chord(purpleOutput)));
                        pipelineResults.add(state.add(healthCheckOutputFuture.get()));
                        pipelineResults.add(state.add(linxOutputFuture.get()));
                        pipelineResults.add(state.add(bachelorOutputFuture.get()));
                        pipelineResults.add(state.add(chordOutputFuture.get()));
                        pipelineResults.compose(metadata);
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        fullSomaticResults.compose(metadata);
        return state;
    }

    @NotNull
    private static Supplier<RuntimeException> throwIllegalState(String sample) {
        return () -> new IllegalStateException(String.format(
                "No alignment output found for sample [%s]. Has the single sample pipeline been run?",
                sample));
    }
}
