package com.hartwig.bam.adam;

import static java.util.Collections.singletonList;

import static scala.collection.JavaConverters.asScalaBufferConverter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.hartwig.bam.AlignmentStage;
import com.hartwig.exception.Exceptions;
import com.hartwig.io.InputOutput;
import com.hartwig.patient.Lane;
import com.hartwig.patient.ReferenceGenome;
import com.hartwig.patient.Sample;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkFiles;
import org.apache.spark.storage.StorageLevel;
import org.bdgenomics.adam.api.java.FragmentsToAlignmentRecordsConverter;
import org.bdgenomics.adam.models.ReadGroup;
import org.bdgenomics.adam.models.ReadGroupDictionary;
import org.bdgenomics.adam.models.SequenceDictionary;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.fragment.FragmentDataset;
import org.bdgenomics.adam.rdd.fragment.InterleavedFASTQInFormatter;
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;
import org.bdgenomics.adam.rdd.read.AnySAMOutFormatter;

import htsjdk.samtools.ValidationStringency;
import scala.Option;

class Bwa implements AlignmentStage {

    private final ADAMContext adamContext;
    private final ReferenceGenome referenceGenome;
    private final FileSystem fileSystem;
    private final int bwaThreads;

    Bwa(final ReferenceGenome referenceGenome, final ADAMContext adamContext, final FileSystem fileSystem, int bwaThreads) {
        this.adamContext = adamContext;
        this.referenceGenome = referenceGenome;
        this.fileSystem = fileSystem;
        this.bwaThreads = bwaThreads;
    }

    @Override
    public InputOutput<AlignmentRecordDataset> execute(InputOutput<AlignmentRecordDataset> input) {
        SequenceDictionary sequenceDictionary = adamContext.loadSequenceDictionary(referenceGenome.path() + ".dict");
        Sample sample = input.sample();
        List<AlignmentRecordDataset> laneRdds =
                sample.lanes().parallelStream().map(lane -> adamBwa(sequenceDictionary, sample, lane)).collect(Collectors.toList());
        if (!laneRdds.isEmpty()) {
            return InputOutput.of(sample,
                    laneRdds.get(0).<AlignmentRecordDataset>union(asScalaBufferConverter(laneRdds.subList(1, laneRdds.size())).asScala()));
        }
        throw Exceptions.noLanesInSample();
    }

    private AlignmentRecordDataset adamBwa(final SequenceDictionary sequenceDictionary, final Sample sample, final Lane lane) {
        FragmentDataset fragmentDataset = adamContext.loadPairedFastq(lane.firstOfPairPath(),
                lane.secondOfPairPath(),
                Option.empty(),
                Option.apply(StorageLevel.DISK_ONLY()),
                ValidationStringency.STRICT).toFragments();
        initializeBwaSharedMemoryPerExecutor(fragmentDataset);
        return RDDs.persistDisk(RDDs.AlignmentRecordDataset(fragmentDataset.pipe(BwaCommand.tokens(referenceGenome,
                sample,
                lane,
                bwaThreads),
                new ArrayList<>(),
                Collections.emptyMap(),
                0,
                InterleavedFASTQInFormatter.class,
                new AnySAMOutFormatter(),
                new FragmentsToAlignmentRecordsConverter())
                .replaceReadGroups(recordDictionary(recordGroup(sample, lane)))
                .replaceSequences(sequenceDictionary)));
    }

    private void initializeBwaSharedMemoryPerExecutor(final FragmentDataset FragmentDataset) {
        for (String file : IndexFiles.resolve(fileSystem, referenceGenome)) {
            adamContext.sc().addFile(file);
        }
        final String path = referenceGenome.path();
        FragmentDataset.jrdd().foreach(fragment -> InitializeBwaSharedMemory.run(SparkFiles.get(new Path(path).getName())));
    }

    private ReadGroupDictionary recordDictionary(final ReadGroup recordGroup) {
        return new ReadGroupDictionary(asScalaBufferConverter(singletonList(recordGroup)).asScala());
    }

    private ReadGroup recordGroup(final Sample sample, final Lane lane) {
        return new ReadGroup(sample.name(),
                lane.recordGroupId(),
                Option.empty(),
                Option.empty(),
                Option.empty(),
                Option.empty(),
                Option.empty(),
                Option.apply(sample.name()),
                Option.empty(),
                Option.apply("ILLUMINA"),
                Option.apply(lane.flowCellId()));
    }
}