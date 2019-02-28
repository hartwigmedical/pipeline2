package com.hartwig.pipeline.after;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.metrics.Metric;
import com.hartwig.pipeline.metrics.Monitor;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.ivy.util.CopyProgressEvent;
import org.apache.ivy.util.CopyProgressListener;
import org.apache.ivy.util.FileUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BamMetricsPipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(BamIndexPipeline.class);
    private final FileSystem fileSystem;
    private final String sourceBamDirectory;
    private final String sourceRefGenomeDirectory;
    private final Monitor monitor;
    private final PicardWGSMetrics picardWGSMetrics;

    private BamMetricsPipeline(final FileSystem fileSystem, final String sourceBamDirectory, final String sourceRefGenomeDirectory,
            final Monitor monitor, final PicardWGSMetrics picardWGSMetrics) {
        this.fileSystem = fileSystem;
        this.sourceBamDirectory = sourceBamDirectory;
        this.sourceRefGenomeDirectory = sourceRefGenomeDirectory;
        this.monitor = monitor;
        this.picardWGSMetrics = picardWGSMetrics;
    }

    public void execute(Sample sample) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();
        String bamFileLocation = Bams.name(sample, sourceBamDirectory, Bams.SORTED);

        String workingDir = System.getProperty("user.dir");
        String localBamFile = workingDir + sample.name() + ".bam";

        LOGGER.info("Copying BAM file to [{}]", localBamFile);
        FileUtil.copy(fileSystem.open(new Path(bamFileLocation)), new File(localBamFile), noop());
        LOGGER.info("Copy complete");

        String localRefGenomeDirectory = workingDir + "refGenome";
        RemoteIterator<LocatedFileStatus> fileIterator = fileSystem.listFiles(new Path(sourceRefGenomeDirectory), false);
        String refGenomeFastaPath = null;
        while (fileIterator.hasNext()) {
            LocatedFileStatus file = fileIterator.next();
            String localFilePath = localRefGenomeDirectory + "/" + file.getPath().getName();

            LOGGER.info("Copying ref genome file [{}]", localFilePath);
            FileUtil.copy(fileSystem.open(file.getPath()), new File(localFilePath), noop());

            if (localFilePath.endsWith(".fasta")) {
                refGenomeFastaPath = localFilePath;
            }
        }

        assert refGenomeFastaPath != null;
        String outputFile = picardWGSMetrics.execute(sample, workingDir, localBamFile, refGenomeFastaPath);

        FileUtil.copy(new FileInputStream(outputFile),
                fileSystem.create(new Path(Bams.name(sample, sourceBamDirectory, Bams.SORTED) + ".wgsmetrics")),
                noop());

        long endTime = System.currentTimeMillis();
        monitor.update(Metric.spentTime("BAM_METRICS", endTime - startTime));
    }

    public static BamMetricsPipeline create(final FileSystem fileSystem, final String bamDirectory, final String sourceRefGenomeDirectory,
            final Monitor monitor) {
        return new BamMetricsPipeline(fileSystem, bamDirectory, sourceRefGenomeDirectory, monitor, new PicardWGSMetrics());
    }

    @NotNull
    private static CopyProgressListener noop() {
        return new CopyProgressListener() {
            @Override
            public void start(final CopyProgressEvent copyProgressEvent) {
            }

            @Override
            public void progress(final CopyProgressEvent copyProgressEvent) {
            }

            @Override
            public void end(final CopyProgressEvent copyProgressEvent) {
            }
        };
    }
}
