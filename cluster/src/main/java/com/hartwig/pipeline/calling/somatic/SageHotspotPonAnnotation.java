package com.hartwig.pipeline.calling.somatic;

import static com.google.common.collect.Lists.newArrayList;

class SageHotspotPonAnnotation extends BcfToolsAnnotation {

    SageHotspotPonAnnotation(String pon) {
        super("sage.hotspots.pon", newArrayList(pon, "-c", "SAGE_PON_COUNT"));
    }
}
