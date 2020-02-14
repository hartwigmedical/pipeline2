package com.hartwig.batch.operations;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

import java.io.File;

import static java.lang.String.format;

public class SamtoolsCramToBam implements BatchOperation {
    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket bucket,
                                               final BashStartupScript startupScript, final RuntimeFiles executionFlags) {
        InputFileDescriptor input = inputs.get();
        String outputFile = VmDirectories.outputFile(new File(input.remoteFilename()).getName().replaceAll("\\.bam$", ".cram"));
        String localInput = format("%s/%s", VmDirectories.INPUT, new File(input.remoteFilename()).getName());
        startupScript.addCommand(() -> input.toCommandForm(localInput));
        startupScript.addCommand(() -> "tar -C /opt/tools/samtools/1.9 -xf /opt/tools/samtools/1.9/samtools.tar.gz");
        startupScript.addCommand(
                new VersionedToolCommand("samtools",
                        "samtools",
                        Versions.SAMTOOLS,
                        "view",
                        "-h",
                        "-T",
                        Resource.REFERENCE_GENOME_FASTA,
                        "-o",
                        outputFile,
                        "-O",
                        "cram,store_md=1,store_nm=1",
                        "-@",
                        Bash.allCpus(),
                        localInput));
        startupScript.addCommand(() -> "gsutil cp gs://batch-samtoolscram-ned/pipeline5.jar /tmp/");
        startupScript.addCommand(() -> "gsutil cp gs://batch-samtoolscram-ned/bamhash_checksum_bam /tmp/");
        startupScript.addCommand(() -> "chmod +x /tmp/bamhash_checksum_bam");
        startupScript.addCommand(() -> format("java -cp /tmp/pipeline5.jar com.hartwig.batch.operations.BamToCramValidator thorough %s %s", localInput, outputFile));

        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "samtools"), executionFlags));
        return VirtualMachineJobDefinition.builder().name("samtoolscram").startupCommand(startupScript)
                .namespacedResults(ResultsDirectory.defaultDirectory())
                .performanceProfile(VirtualMachinePerformanceProfile.custom(4, 4)).build();
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("SamtoolsCramToBam", "Produce a CRAM file from each inputs BAM",
                OperationDescriptor.InputType.FLAT);
    }
}
