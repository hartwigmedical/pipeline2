package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.calling.substages.CosmicAnnotation;

import org.junit.Test;

public class CosmicAnnotationTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new CosmicAnnotation("cosmic.vcf.gz", "ID,INFO");
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.cosmic.annotated.vcf.gz";
    }

    @Test
    public void runsBcfToolsDbSnpAnnotation() {
        assertThat(bash()).contains(
                "/opt/tools/bcftools/1.9/bcftools annotate -a cosmic.vcf.gz -c ID,INFO "
                        + "/data/output/tumor.strelka.vcf -O z -o /data/output/tumor.cosmic.annotated.vcf.gz");
    }

    @Test
    public void runsTabix() {
        assertThat(bash()).contains(
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.cosmic.annotated.vcf.gz -p vcf");
    }

}