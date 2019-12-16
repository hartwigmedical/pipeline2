package com.hartwig.pipeline.calling.somatic;

class SageHotspotAnnotationCommand extends SageHotspotCommand {
    SageHotspotAnnotationCommand(final String sourceVcf, final String hotspotVcf, final String knownHotspots, final String outputVcf) {
        super("com.hartwig.hmftools.sage.SageHotspotAnnotation",
                "-source_vcf",
                sourceVcf,
                "-hotspot_vcf",
                hotspotVcf,
                "-known_hotspots",
                knownHotspots,
                "-out",
                outputVcf);
    }
}
