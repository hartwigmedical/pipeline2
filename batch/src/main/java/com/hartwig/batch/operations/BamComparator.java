package com.hartwig.batch.operations;

import com.google.common.base.Stopwatch;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.util.AsyncBufferedIterator;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BamComparator {
    private static final Logger logger = LoggerFactory.getLogger(BamComparator.class);
    private File referenceSequence;
    private int numberOfCores;
    private ExecutorService executorService;

    public BamComparator(File referenceSequence, int numberOfCores) {
        this.referenceSequence = referenceSequence;
        this.numberOfCores = numberOfCores;
        executorService = Executors.newWorkStealingPool(numberOfCores);
    }

    public static void main(String[] args) {
        File rg = new File("/home/ned/source/hartwig/data/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.fasta");
        new BamComparator(rg, 8).compare("/home/ned/source/hartwig/data/colo829_chr20/COLO829v003R_chr20.sorted.bam",
                "/home/ned/source/hartwig/data/colo829_chr20/COLO829v003R_chr20.sorted.bam.cram.bam", true);
    }

    public BamComparisonOutcome compare(String bamOne, String bamTwo, boolean compareHeaders) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        logger.info("Thoroughly comparing \"{}\" to \"{}\"", bamOne, bamTwo);
        try {
            SamReader samOne = open(bamOne);
            SamReader samTwo = open(bamTwo);

            if (compareHeaders && !areHeadersEqualIgnoringRefGenomeUrl(samOne.getFileHeader(), samTwo.getFileHeader())) {
                logger.error("Headers are not equal!");
                logger.error("<<<<<<<<\n{}\n========{}\n>>>>>>>>", samOne.getFileHeader().getSAMString(),
                        samTwo.getFileHeader().getSAMString());
                return new BamComparisonOutcome(false, "Headers are not equal (ignoring ref genome URLs)");
            }

            AsyncBufferedIterator<SAMRecord> itOne = new AsyncBufferedIterator<>(samOne.iterator(), 100);
            AsyncBufferedIterator<SAMRecord> itTwo = new AsyncBufferedIterator<>(samTwo.iterator(), 100);

            long position = 0;
            while (itOne.hasNext()) {
                position++;
                if (itTwo.hasNext()) {
                    exec(itOne.next(), itTwo.next(), position);
                }
                if (position % 5_000_000 == 0) {
                    logger.info("{} records inspected", position);
                }
            }
            if (itTwo.hasNext()) {
                return new BamComparisonOutcome(false, "There is extra data in " + bamTwo);
            }
            return new BamComparisonOutcome(true, "All " + position + " records compared equal");
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            stopwatch.stop();
            logger.info("Completed comparison in {} seconds", stopwatch.elapsed(TimeUnit.SECONDS));
        }
    }

    private void exec(SAMRecord one, SAMRecord two, long position) {
        executorService.execute(() -> {
            if (!one.equals(two)) {
                throw new IllegalStateException("Difference found at location " + position + ":\n<<<<" + one.getSAMString() + "\n====\n" + two.getSAMString() + "\n>>>>");
            }
        });
    }

    private boolean areHeadersEqualIgnoringRefGenomeUrl(SAMFileHeader one, SAMFileHeader two) {
        if (one.getReadGroups().size() != two.getReadGroups().size()) {
            return false;
        }

        for (SAMReadGroupRecord readGroup : one.getReadGroups()) {
            if (!two.getReadGroups().contains(readGroup)) {
                return false;
            }
        }

        Set<Map.Entry<String, String>> attributes = one.getAttributes();
        for (Map.Entry<String, String> attribute: attributes) {
            if (! two.getAttributes().contains(attribute)) {
                return false;
            }
        }

        SAMSequenceDictionary sequenceDictionary = one.getSequenceDictionary();
        if (sequenceDictionary.size() != two.getSequenceDictionary().size()) {
            return false;
        }

        for (int i = 0; i < sequenceDictionary.getSequences().size(); i++) {
            SAMSequenceRecord recordOne = one.getSequenceDictionary().getSequences().get(i);
            SAMSequenceRecord recordTwo = two.getSequenceDictionary().getSequences().get(i);
            // In the headers I looked at there were only the "M5" and "UR" attributes, the second of which contains
            // the URL, which varies depending on the local path on the machine it was done on. It's possible we
            // could miss some attributes of course if other BAMs have other attributes in addition to these two.
            if (!(recordOne.getSequenceName().equals(recordTwo.getSequenceName()))
                    || recordOne.getSequenceIndex() != recordTwo.getSequenceIndex()
                    || recordOne.getSequenceLength() != recordTwo.getSequenceLength()) {
                return false;
            }
            String m5 = "M5";
            String m51 = recordOne.getAttribute(m5);
            String m52 = recordTwo.getAttribute(m5);
            if ((m51 == null && m52 != null)
                    || (m51 != null && m52 == null)
                    || !(recordTwo.getAttribute(m5).equals(recordTwo.getAttribute(m5)))) {
                return false;
            }
        }
        return true;
    }

    private SamReader open(String bamPath) throws IOException {
        InputStream streamOne = FileUtils.openInputStream(new File(bamPath));
        SamReaderFactory samReaderFactory = SamReaderFactory.make().referenceSequence(referenceSequence);
        return samReaderFactory.open(SamInputResource.of(streamOne));
    }

    public static class BamComparisonOutcome {
        protected final boolean areEqual;
        protected final String reason;

        private BamComparisonOutcome(boolean areEqual, String reason) {
            this.areEqual = areEqual;
            this.reason = reason;
        }
    }
}
