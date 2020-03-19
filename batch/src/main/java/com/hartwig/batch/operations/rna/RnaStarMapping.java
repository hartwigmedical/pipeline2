package com.hartwig.batch.operations.rna;

import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.batch.operations.OperationDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.SambambaCommand;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

public class RnaStarMapping implements BatchOperation {

    private static final String REF_GENCODE_37 = "/opt/resources/hs37d5_GENCODE19";
    // private static final String RNA_FASTQ_FILE_LIST = "rna_sample_fastqs_20200318.txt";

    @Override
    public VirtualMachineJobDefinition execute(
            InputBundle inputs, RuntimeBucket bucket, BashStartupScript startupScript, RuntimeFiles executionFlags) {

        InputFileDescriptor descriptor = inputs.get();

        final String batchInputs = descriptor.value();
        final String[] batchItems = batchInputs.split(",");

        if(batchItems.length != 2)
        {
            System.out.print(String.format("invalid input arguments(%s) - expected SampleId,PathToFastqFiles", batchInputs));
            return null;
        }

        final String sampleId = batchItems[0];
        final String fastqFilelist = batchItems[1];

        // download list of fastq files
        // startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s %s",
        //        "gs://rna-fastqs", RNA_FASTQ_FILE_LIST, VmDirectories.INPUT));

        // final String fastqFilelist = VmDirectories.INPUT + "/" + RNA_FASTQ_FILE_LIST;


        final List<String> sampleFastqFiles = getSampleFastqFileList(sampleId, fastqFilelist);

        if(fastqFilelist.isEmpty()) {
            System.out.print(String.format("sampleId(%s) fastq files not found", sampleId));
            return null;
        }

        // copy down FASTQ files for this sample
        for(final String fastqFile : sampleFastqFiles)
        {
            startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", fastqFile, VmDirectories.INPUT));
        }

        // locate the FASTQ files for reads 1 and 2
        final String r1Files = format("$(ls %s/*_R1* | tr '\\n' ',')", VmDirectories.INPUT);
        final String r2Files = format("$(ls %s/*_R2* | tr '\\n' ',')", VmDirectories.INPUT);

        // logging
        final String threadCount = Bash.allCpus();

        startupScript.addCommand(() -> format("cd %s", VmDirectories.OUTPUT));

        // run the STAR mapper
        final String[] starArgs = {"--runThreadN", threadCount, "--genomeDir", REF_GENCODE_37, "--genomeLoad", "NoSharedMemory",
                "--readFilesIn", r1Files, r2Files, "--readFilesCommand", "zcat", "--outSAMtype", "BAM", "Unsorted",
                "--outSAMunmapped", "Within", "--outBAMcompression", "0", "--outSAMattributes", "All",
                "--outFilterMultimapNmax", "10", "--outFilterMismatchNmax", "3", "limitOutSJcollapsed", "3000000",
                "--chimSegmentMin", "10", "--chimOutType", "WithinBAM", "SoftClip", "--chimJunctionOverhangMin", "10", "--chimSegmentReadGapMax", "3",
                "--chimScoreMin", "1", "--chimScoreDropMax", "30", "--chimScoreJunctionNonGTAG", "0", "--chimScoreSeparation", "1",
                "--outFilterScoreMinOverLread", "0.33", "--outFilterMatchNminOverLread", "0.33", "--outFilterMatchNmin", "35",
                "--alignSplicedMateMapLminOverLmate", "0.33", "--alignSplicedMateMapLmin", "35", "--alignSJstitchMismatchNmax", "5", "-1", "5", "5"};

        startupScript.addCommand(new VersionedToolCommand("star", "STAR", "2.7.3a", starArgs));

        final String bamFile = "Aligned.out.bam";

        // sort the BAM
        final String sortedBam = sampleId + ".sorted.bam";

        final String[] sortArgs = {"sort", "-@", threadCount, "-m", "2G", "-T", "tmp", "-O", "bam", bamFile, "-o", sortedBam};

        startupScript.addCommand(new VersionedToolCommand("samtools", "samtools", Versions.SAMTOOLS, sortArgs));

        // mark duplicate fragment reads within the BAM
        final String sortedDedupedBam = sampleId + ".sorted.dups.bam";

        final String[] dupArgs = {"markdup", "-t", threadCount, "--overflow-list-size=45000000", sortedBam, sortedDedupedBam};

        startupScript.addCommand(new SambambaCommand(dupArgs));

        final String[] indexArgs = {"index", sortedDedupedBam};

        startupScript.addCommand(new VersionedToolCommand("samtools", "samtools", Versions.SAMTOOLS, indexArgs));

        // clean up intermediary BAMs
        startupScript.addCommand(() -> format("rm -f %s", bamFile));
        startupScript.addCommand(() -> format("rm -f %s", sortedBam));

        // run QC stats on the fast-Qs as well
        /*
        final String fastqcOutputDir = format("%s/fastqc", VmDirectories.OUTPUT);
        startupScript.addCommand(() -> format("mkdir %s", fastqcOutputDir));

        final String allFastQs = format("%s/*gz", VmDirectories.INPUT);
        final String[] fastqcArgs = {"-o", fastqcOutputDir, allFastQs};

        // TEMP until reimage has taken place
        // startupScript.addCommand(() -> format("chmod a+x /opt/tools/fastqc/0.11.4/fastqc"));

        // currently not working, not sure why
        startupScript.addCommand(new VersionedToolCommand("fastqc", "fastqc", "0.11.4", fastqcArgs));
        */

        // upload the results
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "star"), executionFlags));

        // copy results to rna-analysis location on crunch


        startupScript.addCommand(() -> format("gsutil -m cp %s/* gs://rna-cohort/%s/", VmDirectories.OUTPUT, sampleId));

        // startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of("rna-cohort", sampleId), executionFlags));

        return ImmutableVirtualMachineJobDefinition.builder().name("rna-star-mapping").startupCommand(startupScript)
                .namespacedResults(ResultsDirectory.defaultDirectory()).workingDiskSpaceGb(200)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(12, 36)).build();
    }

    private final List<String> getSampleFastqFileList(final String sampleId, final String fastqFilelist)
    {
        if (!Files.exists(Paths.get(fastqFilelist)))
        {
            return Lists.newArrayList();
        }

        try
        {
            final List<String> fileContents = Files.readAllLines(new File(fastqFilelist).toPath());

            if(fileContents.isEmpty())
                return Lists.newArrayList();

            return fileContents.stream().filter(x -> x.contains(sampleId)).collect(Collectors.toList());
        }
        catch (IOException e)
        {
            //
            return Lists.newArrayList();
        }

    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("RnaStarMapping", "Generate BAMs from RNA FASTQs",
                OperationDescriptor.InputType.FLAT);
    }

}
