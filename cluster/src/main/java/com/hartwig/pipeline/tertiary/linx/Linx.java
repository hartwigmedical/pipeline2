package com.hartwig.pipeline.tertiary.linx;

import static com.hartwig.pipeline.resource.ResourceNames.ENSEMBL;
import static com.hartwig.pipeline.resource.ResourceNames.KNOWLEDGEBASES;
import static com.hartwig.pipeline.resource.ResourceNames.REFERENCE_GENOME;
import static com.hartwig.pipeline.resource.ResourceNames.SV;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

public class Linx implements Stage<LinxOutput, SomaticRunMetadata> {

    public static final String NAMESPACE = "linx";
    private final InputDownload purpleOutputDirDownload;
    private final InputDownload purpleStructuralVcfDownload;

    public Linx(PurpleOutput purpleOutput) {
        purpleOutputDirDownload = new InputDownload(purpleOutput.outputDirectory());
        purpleStructuralVcfDownload = new InputDownload(purpleOutput.structuralVcf());
    }

    @Override
    public List<BashCommand> inputs() {
        return Collections.singletonList(purpleOutputDirDownload);
    }

    @Override
    public List<ResourceDownload> resources(final Storage storage, final String resourceBucket, final RuntimeBucket bucket) {
        return ImmutableList.of(ResourceDownload.from(storage, resourceBucket, REFERENCE_GENOME, bucket),
                ResourceDownload.from(storage, resourceBucket, SV, bucket),
                ResourceDownload.from(storage, resourceBucket, KNOWLEDGEBASES, bucket),
                ResourceDownload.from(storage, resourceBucket, ENSEMBL, bucket));
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata, final Map<String, ResourceDownload> resources) {
        ResourceDownload svResources = resources.get(SV);
        ResourceDownload knowledgebases = resources.get(KNOWLEDGEBASES);
        return Collections.singletonList(new LinxCommand(metadata.tumor().sampleName(),
                purpleStructuralVcfDownload.getLocalTargetPath(),
                purpleOutputDirDownload.getLocalTargetPath(),
                resources.get(REFERENCE_GENOME).find("fasta"),
                VmDirectories.OUTPUT,
                svResources.find("fragile_sites_hmf.csv"),
                svResources.find("line_elements.csv"),
                svResources.find("heli_rep_origins.bed"),
                svResources.find("viral_host_ref.csv"),
                ResourceDownload.RESOURCES_PATH,
                knowledgebases.find("knownFusionPairs.csv"),
                knowledgebases.find("knownPromiscuousFive.csv"),
                knowledgebases.find("knownPromiscuousThree.csv")));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.linx(bash, resultsDirectory);
    }

    @Override
    public LinxOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return LinxOutput.builder()
                .status(jobStatus)
                .addReportComponents(new EntireOutputComponent(bucket, Folder.from(), NAMESPACE, resultsDirectory))
                .build();
    }

    @Override
    public LinxOutput skippedOutput(final SomaticRunMetadata metadata) {
        return LinxOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && !arguments.shallow();
    }
}
