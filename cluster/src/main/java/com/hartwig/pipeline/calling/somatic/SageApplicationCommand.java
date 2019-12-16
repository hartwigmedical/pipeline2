package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.execution.vm.Bash;

class SageApplicationCommand extends SageCommand {
    SageApplicationCommand(final String tumorSampleName, final String tumorBamPath, final String referenceSampleName,
            final String referenceBamPath, final String hotspots, final String panel, final String referenceGenomePath,
            final String outputVcf) {
        super("com.hartwig.hmftools.sage.SageApplication",
                "-tumor",
                tumorSampleName,
                "-tumor_bam",
                tumorBamPath,
                "-reference",
                referenceSampleName,
                "-reference_bam",
                referenceBamPath,
                "-hotspots",
                hotspots,
                "-panel",
                panel,
                "-ref_genome",
                referenceGenomePath,
                "-threads",
                Bash.allCpus(),
                "-out",
                outputVcf);
    }
}
