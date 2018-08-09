package com.hartwig.pipeline.adam;

import java.util.List;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.MoreExecutors;
import com.hartwig.io.DataLocation;
import com.hartwig.io.OutputType;
import com.hartwig.patient.KnownIndels;
import com.hartwig.patient.ReferenceGenome;
import com.hartwig.pipeline.BamCreationPipeline;
import com.hartwig.pipeline.QCResult;
import com.hartwig.pipeline.QualityControl;

import org.apache.hadoop.fs.FileSystem;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.jetbrains.annotations.NotNull;

public class ADAMPipelines {

    public static BamCreationPipeline bamCreation(final ADAMContext adamContext, final FileSystem fileSystem, final String workingDirectory,
            final String referenceGenomePath, final List<String> knownIndelPaths, final int bwaThreads, final boolean doQC,
            final boolean parallel, final boolean saveAsFile) {
        JavaADAMContext javaADAMContext = new JavaADAMContext(adamContext);
        ReferenceGenome referenceGenome = ReferenceGenome.of(referenceGenomePath);
        DataLocation dataLocation = new DataLocation(fileSystem, workingDirectory);
        return BamCreationPipeline.builder()
                .readCountQCFactory(ADAMReadCountCheck::from)
                .referenceFinalQC(ifEnabled(doQC,
                        ADAMFinalBAMQC.of(javaADAMContext, referenceGenome, CoverageThreshold.of(10, 90), CoverageThreshold.of(20, 70))))
                .tumorFinalQC(ifEnabled(doQC,
                        ADAMFinalBAMQC.of(javaADAMContext, referenceGenome, CoverageThreshold.of(30, 80), CoverageThreshold.of(60, 65))))
                .alignment(new ADAMBwa(referenceGenome, adamContext, bwaThreads))
                .alignmentDatasource(new HDFSAlignmentRDDSource(OutputType.ALIGNED, javaADAMContext, dataLocation))
                .finalDatasource(new HDFSAlignmentRDDSource(OutputType.INDEL_REALIGNED, javaADAMContext, dataLocation))
                .addBamEnrichment(new ADAMMarkDuplicatesAndSort(javaADAMContext, dataLocation))
                .addBamEnrichment(new ADAMRealignIndels(KnownIndels.of(knownIndelPaths), referenceGenome, javaADAMContext, dataLocation))
                .bamStore(new HDFSBamStore(dataLocation, fileSystem, saveAsFile))
                .executorService(parallel ? Executors.newFixedThreadPool(2) : MoreExecutors.sameThreadExecutor())
                .build();
    }

    @NotNull
    private static QualityControl<AlignmentRecordRDD> ifEnabled(final boolean doQC, final ADAMFinalBAMQC finalBAMQC) {
        return doQC ? finalBAMQC : alignments -> QCResult.ok();
    }
}
