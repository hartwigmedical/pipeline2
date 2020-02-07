package com.hartwig.batch.operations;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.unix.PipeCommands;
import com.hartwig.pipeline.tools.Versions;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.lang.String.format;
import static java.util.Arrays.asList;

public class BamToCramValidator {
    private static final Logger logger = LoggerFactory.getLogger(BamToCramValidator.class);
    private ExecutorService executorService;
    private int cores;

    private BamToCramValidator(int cores) {
        executorService = Executors.newFixedThreadPool(cores);
        this.cores = cores;
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            throw new IllegalArgumentException("USAGE: [thorough|quick] [# of cores] [input.bam] [input.cram]");
        }
        try {
            // -Dsamjdk.reference_fasta=/home/ned/source/hartwig/data/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.fasta
//            env.put("REF_PATH", "/home/ned/source/hartwig/data/reference_genome");

            File refDir;
            if (System.getenv().containsKey("REF_PATH")) {
                refDir = new File(System.getenv("REF_PATH"));
            } else {
                refDir = new File("/opt/resources/reference_genome");
            }
            File refFasta = new File(refDir, "Homo_sapiens.GRCh37.GATK.illumina.fasta");
            new BamToCramValidator(Integer.parseInt(args[1])).validate(args, refFasta);
            System.exit(0);
        } catch (Exception e) {
            logger.error("Unexpected exception!", e);
            System.exit(1);
        }
    }

    private void validate(String[] args, File referenceSequence) {
        try {
            String one = ensureBam(args[2], referenceSequence);
            String two = ensureBam(args[3], referenceSequence);
            if (args[0].equalsIgnoreCase("quick")) {
                List<Future<String>> futures = execute(asList(flagstat(one), flagstat(two), hash(one), hash(two)));
                assertEqualContents(futures.get(0).get(), futures.get(1).get());
                assertEqualContents(futures.get(2).get(), futures.get(3).get());
                logger.info("Quick validation succeeded");
            } else {
                Callable<String> bamComparison = () -> {
                    BamComparator.BamComparisonOutcome result = new BamComparator(referenceSequence, cores).compare(one, two, true);
                    if (result.areEqual) {
                        return result.reason;
                    } else {
                        throw new RuntimeException(result.reason);
                    }
                };
                List<Callable<String>> callables = asList(flagstat(one), flagstat(two), bamComparison);
                List<Future<String>> futures = execute(callables);
                assertEqualContents(futures.get(0).get(), futures.get(1).get());
                logger.info("Thorough validation succeeded: " + futures.get(2).get());
            }
        } catch (ExecutionException | InterruptedException | IOException e) {
            throw new RuntimeException("Validation failed", e);
        }
    }

    private String ensureBam(String inputFile, File referenceSequence) throws InterruptedException, ExecutionException {
        if (inputFile.toLowerCase().endsWith(".cram")) {
            String newBam = inputFile + ".bam";
            logger.info("Converting " + inputFile + " to BAM " + newBam);
            Callable<String> callback = shell(new VersionedToolCommand("samtools", "samtools",
                    Versions.SAMTOOLS, "view", "-O", "bam,decode_md=0", "-o", newBam, "-@", "$(nproc)",
                    "-T", referenceSequence.getAbsolutePath(), inputFile), newBam);
            return execute(asList(callback)).get(0).get();
        } else {
            return inputFile;
        }
    }

    private List<Future<String>> execute(List<Callable<String>> callables) {
        try {
            return executorService.invokeAll(callables);
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while executing", e);
        }
    }

    private void assertEqualContents(String pathA, String pathB) throws IOException {
        if (!FileUtils.contentEquals(new File(pathA), new File(pathB))) {
            throw new RuntimeException(format("Contents of \"%s\" != \"%s\"", pathA, pathB));
        }
    }

    private Callable<String> flagstat(String inputFile) {
        String outputFile = inputFile + ".flagstat";
        return shell(new PipeCommands(new VersionedToolCommand("sambamba",
                "sambamba",
                Versions.SAMBAMBA,
                "flagstat",
                "-t",
                Bash.allCpus(),
                inputFile), () -> "tee " + outputFile), outputFile);
    }

    private Callable<String> hash(String inputFile) {
        String outputFile = inputFile + ".hash";
        return shell(() -> format("/tmp/bamhash_checksum_bam %s > %s", inputFile, outputFile), outputFile);
    }

    private Callable<String> shell(BashCommand command, String outputPath) {
        return () -> {
            ProcessBuilder builder = new ProcessBuilder();
            builder.command("/bin/bash", "-exc", command.asBash()).inheritIO();
            Process process = builder.start();
            int exitCode = process.waitFor();
            // Processes.run(...)
            logger.info("Process output: {}", IOUtils.toString(process.getInputStream(), StandardCharsets.UTF_8));
            if (exitCode != 0) {
                throw new RuntimeException(format("Command returned %d: \"%s\"", exitCode, command.asBash()));
            }
            return outputPath;
        };
    }
}
