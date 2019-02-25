package com.hartwig.pipeline.adam;

import java.util.List;
import java.util.stream.Collectors;

import com.hartwig.io.DataLocation;
import com.hartwig.io.FinalDataLocation;
import com.hartwig.io.OutputType;
import com.hartwig.patient.KnownIndels;
import com.hartwig.patient.ReferenceGenome;
import com.hartwig.pipeline.BamCreationPipeline;
import com.hartwig.pipeline.HadoopStatusReporter;
import com.hartwig.pipeline.QCResult;
import com.hartwig.pipeline.QualityControl;
import com.hartwig.pipeline.metrics.Monitor;

import org.apache.hadoop.fs.FileSystem;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;
import org.jetbrains.annotations.NotNull;

public class Pipelines {

    public static BamCreationPipeline bamCreation(final ADAMContext adamContext, final FileSystem fileSystem,
            final Monitor monitor, final String workingDirectory, final String referenceGenomePath, final List<String> knownIndelPaths,
            final int bwaThreads, final boolean doQC, final boolean mergeFinalFile) {
        JavaADAMContext javaADAMContext = new JavaADAMContext(adamContext);
        ReferenceGenome referenceGenome = ReferenceGenome.of(fileSystem.getUri() + referenceGenomePath);
        DataLocation finalDataLocation = new FinalDataLocation(fileSystem, workingDirectory);
        KnownIndels knownIndels = KnownIndels.of(knownIndelsFSPaths(fileSystem, knownIndelPaths));
        return BamCreationPipeline.builder()
                .finalQC(ifEnabled(doQC,
                        FinalBAMQC.of(javaADAMContext, referenceGenome, CoverageThreshold.of(10, 90), CoverageThreshold.of(20, 70))))
                .alignment(new Bwa(referenceGenome, adamContext, fileSystem, bwaThreads))
                .finalDatasource(new HDFSAlignmentRDDSource(OutputType.INDEL_REALIGNED, javaADAMContext, finalDataLocation))
                .finalBamStore(new HDFSBamStore(finalDataLocation, fileSystem, mergeFinalFile)).bamEnrichment(new MarkDups())
                .statusReporter(new HadoopStatusReporter(fileSystem, workingDirectory))
                .monitor(monitor)
                .build();
    }

    private static List<String> knownIndelsFSPaths(final FileSystem fileSystem, final List<String> knownIndelPaths) {
        return knownIndelPaths.stream().map(path -> fileSystem.getUri() + path).collect(Collectors.toList());
    }

    @NotNull
    private static QualityControl<AlignmentRecordDataset> ifEnabled(final boolean doQC,
            final QualityControl<AlignmentRecordDataset> finalBAMQC) {
        return doQC ? finalBAMQC : alignments -> QCResult.ok();
    }
}
