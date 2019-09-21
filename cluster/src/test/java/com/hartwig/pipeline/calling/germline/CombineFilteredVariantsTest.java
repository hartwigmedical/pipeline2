package com.hartwig.pipeline.calling.germline;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;

import org.junit.Test;

public class CombineFilteredVariantsTest extends SubStageTest{

    @Override
    public SubStage createVictim() {
        return new CombineFilteredVariants("other.vcf", "referenceSampleName.fasta");
    }

    @Override
    public String expectedPath() {
        return outFile("referenceSampleName.filtered_variants.vcf");
    }

    @Test
    public void combinesVariantsWithGatk() {
        assertThat(output.currentBash().asUnixString()).contains("java -Xmx20G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T "
                + "CombineVariants -V " + outFile("referenceSampleName.strelka.vcf") + " -V other.vcf -o "
                + expectedPath() + " -R referenceSampleName.fasta --assumeIdenticalSamples");
    }

    @Override
    protected String sampleName() {
        return "referenceSampleName";
    }
}