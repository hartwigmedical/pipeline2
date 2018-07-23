package com.hartwig.pipeline.adam;

import java.io.IOException;
import java.util.Collections;

import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.patient.ReferenceGenome;
import com.hartwig.pipeline.Stage;

import org.bdgenomics.adam.api.java.AlignmentRecordsToAlignmentRecordsConverter;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.bdgenomics.adam.rdd.read.AnySAMOutFormatter;
import org.bdgenomics.adam.rdd.read.BAMInFormatter;

class ADAMAddMDTags implements Stage<AlignmentRecordRDD, AlignmentRecordRDD> {

    private final JavaADAMContext javaADAMContext;
    private final ReferenceGenome referenceGenome;

    ADAMAddMDTags(final JavaADAMContext javaADAMContext, final ReferenceGenome referenceGenome) {
        this.javaADAMContext = javaADAMContext;
        this.referenceGenome = referenceGenome;
    }

    @Override
    public DataSource<AlignmentRecordRDD> datasource() {
        return new AlignmentRDDSource(OutputType.INDEL_REALIGNED, javaADAMContext);
    }

    @Override
    public OutputType outputType() {
        return OutputType.MD_TAGGED;
    }

    @Override
    public InputOutput<AlignmentRecordRDD> execute(final InputOutput<AlignmentRecordRDD> input) throws IOException {
        AlignmentRecordRDD pipe = input.payload()
                .pipe(SamToolsCallMDCommand.tokens(referenceGenome),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        0,
                        BAMInFormatter.class,
                        new AnySAMOutFormatter(),
                        new AlignmentRecordsToAlignmentRecordsConverter());
        return InputOutput.of(outputType(), input.sample(), RDDs.alignmentRecordRDD(pipe));
    }
}
