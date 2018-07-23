package com.hartwig.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputStore;
import com.hartwig.io.OutputType;
import com.hartwig.patient.ImmutableSample;
import com.hartwig.patient.Patient;
import com.hartwig.patient.Sample;

import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class BamCreationPipelineTest {

    private static final ImmutableSample SAMPLE = Sample.builder("", "TEST").build();
    private static final InputOutput<AlignmentRecordRDD> ALIGNED_BAM =
            InputOutput.of(OutputType.ALIGNED, SAMPLE, mock(AlignmentRecordRDD.class));
    private static final Patient PATIENT = Patient.of("", "TEST", SAMPLE);
    private InputOutput<AlignmentRecordRDD> lastStored;
    private static final InputOutput<AlignmentRecordRDD> ENRICHED =
            InputOutput.of(OutputType.DUPLICATE_MARKED, SAMPLE, mock(AlignmentRecordRDD.class));

    @Test
    public void alignsAndStoresBAM() throws Exception {
        BamCreationPipeline.builder()
                .alignment(alignmentStage())
                .bamStore(bamStore(false))
                .qcFactory(qcFactory(true))
                .build()
                .execute(PATIENT);
        assertThat(lastStored).isEqualTo(ALIGNED_BAM);
    }

    @Test
    public void enrichesAlignedBAM() throws Exception {
        BamCreationPipeline.builder()
                .alignment(alignmentStage())
                .addBamEnrichment(enrichmentStage(ENRICHED))
                .bamStore(bamStore(false))
                .qcFactory(qcFactory(true))
                .build()
                .execute(PATIENT);
        assertThat(lastStored).isEqualTo(ENRICHED);
    }

    @Test
    public void skipsStagesWhenOutputAlreadyExists() throws Exception {
        BamCreationPipeline.builder()
                .alignment(alignmentStage())
                .addBamEnrichment(enrichmentStage(ENRICHED))
                .bamStore(bamStore(true))
                .qcFactory(qcFactory(true))
                .build()
                .execute(PATIENT);
        assertThat(lastStored).isEqualTo(ALIGNED_BAM);
    }

    @Test(expected = IllegalStateException.class)
    public void illegalStateExceptionWhenQCFails() throws Exception {
        BamCreationPipeline.builder()
                .alignment(alignmentStage())
                .addBamEnrichment(enrichmentStage(ENRICHED))
                .bamStore(bamStore(false))
                .qcFactory(qcFactory(false))
                .build()
                .execute(PATIENT);
    }

    @NotNull
    private OutputStore<AlignmentRecordRDD> bamStore(final boolean exists) {
        return new OutputStore<AlignmentRecordRDD>() {
            @Override
            public void store(final InputOutput<AlignmentRecordRDD> inputOutput) {
                lastStored = inputOutput;
            }

            @Override
            public boolean exists(final Sample sample, final OutputType type) {
                return exists;
            }
        };
    }

    @NotNull
    private static Stage<AlignmentRecordRDD, AlignmentRecordRDD> enrichmentStage(final InputOutput<AlignmentRecordRDD> enriched) {
        return new Stage<AlignmentRecordRDD, AlignmentRecordRDD>() {
            @Override
            public DataSource<AlignmentRecordRDD> datasource() {
                return sample -> ALIGNED_BAM;
            }

            @Override
            public OutputType outputType() {
                return OutputType.DUPLICATE_MARKED;
            }

            @Override
            public InputOutput<AlignmentRecordRDD> execute(final InputOutput<AlignmentRecordRDD> input) throws IOException {
                return enriched;
            }
        };
    }

    @NotNull
    private QualityControlFactory qcFactory(final boolean passesQC) {
        return input -> toQC -> passesQC ? QCResult.ok() : QCResult.failure("failed");
    }

    @NotNull
    private static AlignmentStage alignmentStage() {
        return input -> ALIGNED_BAM;
    }
}