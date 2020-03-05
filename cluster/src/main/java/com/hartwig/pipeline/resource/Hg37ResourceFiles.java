package com.hartwig.pipeline.resource;

import static com.hartwig.pipeline.resource.ResourceNames.AMBER_PON;
import static com.hartwig.pipeline.resource.ResourceNames.GC_PROFILE;
import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_REPEAT_MASKER_DB;
import static com.hartwig.pipeline.resource.ResourceNames.MAPPABILITY;
import static com.hartwig.pipeline.resource.ResourceNames.REFERENCE_GENOME;
import static com.hartwig.pipeline.resource.ResourceNames.SAGE;
import static com.hartwig.pipeline.resource.ResourceNames.SNPEFF;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public class Hg37ResourceFiles implements ResourceFiles
{
    public static final String HG37_DIRECTORY = "hg37";

    public RefGenomeVersion version() { return RefGenomeVersion.HG37; }

    public String versionDirectory() { return HG37_DIRECTORY; }

    private String formPath(String name, String file) {
        return String.format("%s/%s/%s/%s", VmDirectories.RESOURCES, name, versionDirectory(), file);
    }

    private static final String REF_GENOME_FASTA_HG37_FILE = "Homo_sapiens.GRCh37.GATK.illumina.fasta";

    public static final String REFERENCE_GENOME_FASTA_HG37 =
            String.format("%s/%s/%s/%s", VmDirectories.RESOURCES, REFERENCE_GENOME, HG37_DIRECTORY, REF_GENOME_FASTA_HG37_FILE);

    public String refGenomeFile() { return formPath(REFERENCE_GENOME, REF_GENOME_FASTA_HG37_FILE); }

    public String gcProfileFile() { return formPath(GC_PROFILE,"GC_profile.1000bp.cnp"); }

    public String germlineHetPon() { return formPath(AMBER_PON, "GermlineHetPon.hg19.vcf.gz"); }

    public String gridssRepeatMaskerDb() { return formPath(GRIDSS_REPEAT_MASKER_DB,"hg19.fa.out"); }

    public String snpEffDb() { return formPath(SNPEFF,"snpEff_v4_3_GRCh37.75.zip"); }

    public String sageKnownHotspots() { return formPath(SAGE, "KnownHotspots.hg19.vcf.gz"); }

    public String sageActionableCodingPanel() { return formPath(SAGE, "ActionableCodingPanel.hg19.bed.gz"); }

    public String out150Mappability() { return formPath(MAPPABILITY, "out_150_hg19.mappability.bed.gz"); }

    public String sageGermlinePon() { return formPath(SAGE, "SageGermlinePon.hg19.vcf.gz"); }

    public String sageSomaticPon() { return formPath(SAGE, "SAGE_PON.vcf.gz"); }

}
