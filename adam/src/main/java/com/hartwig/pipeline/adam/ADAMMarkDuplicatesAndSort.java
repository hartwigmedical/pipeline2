package com.hartwig.pipeline.adam;

import java.io.IOException;

import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Stage;

import org.apache.spark.storage.StorageLevel;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

class ADAMMarkDuplicatesAndSort implements Stage<Sample, AlignmentRecordRDD> {

    private final JavaADAMContext javaADAMContext;

    ADAMMarkDuplicatesAndSort(final JavaADAMContext javaADAMContext) {
        this.javaADAMContext = javaADAMContext;
    }

    @Override
    public DataSource<Sample, AlignmentRecordRDD> datasource() {
        return new AlignmentRDDSource(OutputType.ALIGNED, javaADAMContext);
    }

    @Override
    public InputOutput<Sample, AlignmentRecordRDD> execute(InputOutput<Sample, AlignmentRecordRDD> input) throws IOException {
        AlignmentRecordRDD persistedRDD = (AlignmentRecordRDD) input.payload().markDuplicates().persist(StorageLevel.MEMORY_AND_DISK_SER());
        return InputOutput.of(outputType(), input.entity(), persistedRDD.sortReadsByReferencePositionAndIndex());
    }

    @Override
    public OutputType outputType() {
        return OutputType.DUPLICATE_MARKED;
    }
}
