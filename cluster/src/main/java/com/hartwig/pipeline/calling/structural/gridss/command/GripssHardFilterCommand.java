package com.hartwig.pipeline.calling.structural.gridss.command;

public class GripssHardFilterCommand extends GripssCommand {

    public GripssHardFilterCommand(final String inputVcf, final String outputVcf) {
        super("com.hartwig.hmftools.gripss.GripssHardFilterApplicationKt",
                "-input_vcf",
                inputVcf,
                "-output_vcf",
                outputVcf);
    }
}
