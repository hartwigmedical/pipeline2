package com.hartwig.pipeline.calling.somatic;

import static com.hartwig.pipeline.resource.ResourceNames.BEDS;
import static com.hartwig.pipeline.resource.ResourceNames.MAPPABILITY;
import static com.hartwig.pipeline.resource.ResourceNames.PON;
import static com.hartwig.pipeline.resource.ResourceNames.SAGE;
import static com.hartwig.pipeline.resource.ResourceNames.STRELKA_CONFIG;

import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.FinalSubStage;
import com.hartwig.pipeline.calling.SubStageInputOutput;
import com.hartwig.pipeline.calling.substages.CosmicAnnotation;
import com.hartwig.pipeline.calling.substages.SnpEff;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.UnzipToDirectoryCommand;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.StartupScriptComponent;
import com.hartwig.pipeline.report.ZippedVcfAndIndexComponent;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;

public class SomaticCaller extends TertiaryStage<SomaticCallerOutput> {

    public static final String NAMESPACE = "somatic_caller";

    private OutputFile outputFile;
    private OutputFile sageOutputFile;

    public SomaticCaller(final AlignmentPair alignmentPair) {
        super(alignmentPair);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {

        List<BashCommand> commands = Lists.newArrayList();

        String tumorBamPath = getTumorBamDownload().getLocalTargetPath();
        String referenceBamPath = getReferenceBamDownload().getLocalTargetPath();
        String referenceGenomePath = Resource.REFERENCE_GENOME_FASTA;
        String tumorSampleName = metadata.tumor().sampleName();
        String referenceSampleName = metadata.reference().sampleName();
        String knownHotspotsTsv = Resource.of(SAGE, "KnownHotspots.tsv");
        SubStageInputOutput sageOutput = new SageHotspotsApplication(knownHotspotsTsv,
                Resource.of(SAGE, "CodingRegions.bed"),
                referenceGenomePath,
                tumorBamPath,
                referenceBamPath,
                tumorSampleName,
                referenceSampleName).andThen(new SageHotspotFiltersAndAnnotations(tumorSampleName))
                .andThen(new SageHotspotPonAnnotation(Resource.of(SAGE, "SAGE_PON.vcf.gz")))
                .andThen(new SageHotspotPonFilter())
                .apply(SubStageInputOutput.empty(tumorSampleName));

        commands.addAll(sageOutput.bash());

        commands.add(new UnzipToDirectoryCommand(VmDirectories.RESOURCES, Resource.SNPEFF_DB));

        SubStageInputOutput mergedOutput = new Strelka(referenceBamPath,
                tumorBamPath,
                Resource.of(STRELKA_CONFIG, "strelka_config_bwa_genome.ini"),
                referenceGenomePath).andThen(new MappabilityAnnotation(Resource.of(MAPPABILITY, "out_150_hg19.mappability.bed.gz"),
                Resource.of(MAPPABILITY, "mappability.hdr")))
                .andThen(new PonAnnotation("germline.pon", Resource.of(PON, "GERMLINE_PON.vcf.gz"), "GERMLINE_PON_COUNT"))
                .andThen(new PonAnnotation("somatic.pon", Resource.of(PON, "SOMATIC_PON.vcf.gz"), "SOMATIC_PON_COUNT"))
                .andThen(new StrelkaPostProcess(tumorSampleName,
                        Resource.of(BEDS, "NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed"),
                        tumorBamPath))
                .andThen(new PonFilter())
                .andThen(new SageHotspotsAnnotation(knownHotspotsTsv, sageOutput.outputFile().path()))
                .andThen(new SnpEff(Resource.SNPEFF_CONFIG))
                .andThen(new DbSnpAnnotation(Resource.DBSNPS_VCF))
                .andThen(FinalSubStage.of(new CosmicAnnotation(Resource.COSMIC_VCF_GZ, "ID,INFO")))
                .apply(SubStageInputOutput.empty(tumorSampleName));
        commands.addAll(mergedOutput.bash());

        outputFile = mergedOutput.outputFile();
        sageOutputFile = sageOutput.outputFile();
        return commands;
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.somaticCalling(bash, resultsDirectory);
    }

    @Override
    public SomaticCallerOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return SomaticCallerOutput.builder()
                .status(jobStatus)
                .maybeFinalSomaticVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(outputFile.fileName())))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.from(),
                        outputFile.fileName(),
                        OutputFile.of(metadata.tumor().sampleName(), "somatic_caller_post_processed", OutputFile.GZIPPED_VCF, false)
                                .fileName(),
                        resultsDirectory))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.from(),
                        sageOutputFile.fileName(),
                        OutputFile.of(metadata.tumor().sampleName(), "sage_hotspots", OutputFile.GZIPPED_VCF, false).fileName(),
                        resultsDirectory))
                .addReportComponents(new EntireOutputComponent(bucket,
                        Folder.from(),
                        NAMESPACE,
                        "strelkaAnalysis/",
                        resultsDirectory,
                        s -> s.contains("chromosomes") || s.contains("Makefile") || s.contains("task.complete")))
                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, Folder.from(), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, NAMESPACE, Folder.from()))
                .build();
    }

    @Override
    public SomaticCallerOutput skippedOutput(final SomaticRunMetadata metadata) {
        return SomaticCallerOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runSomaticCaller();
    }
}