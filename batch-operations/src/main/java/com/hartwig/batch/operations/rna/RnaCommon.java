package com.hartwig.batch.operations.rna;

import static com.hartwig.pipeline.resource.RefGenomeVersion.RG_37;

import com.hartwig.pipeline.resource.RefGenomeVersion;

public class RnaCommon {

    public static final String RNA_RESOURCES = "gs://rna-resources";
    public static final String RNA_COHORT_LOCATION_HG37 = "gs://rna-cohort";
    public static final String RNA_COHORT_LOCATION_HG38 = "gs://rna-cohort-38";

    public static final String REF_GENCODE_37_DIR = "hs37d5_GENCODE19";

    public static final int MAX_EXPECTED_BAM_SIZE_GB = 70;

    public static String getRnaResourceDirectory(final RefGenomeVersion version, final String resourceDir)
    {
        if(version == RG_37)
            return String.format("%s/%s/37", RNA_RESOURCES, resourceDir);
        else
            return String.format("%s/%s/38", RNA_RESOURCES, resourceDir);
    }

    public static String getRnaCohortDirectory(final RefGenomeVersion version)
    {
        return version == RG_37 ? RNA_COHORT_LOCATION_HG37 : RNA_COHORT_LOCATION_HG38;
    }

}
