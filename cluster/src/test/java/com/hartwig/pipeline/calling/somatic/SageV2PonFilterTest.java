package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;

import org.junit.Test;

public class SageV2PonFilterTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new SageV2PonFilter();
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.sage.pon.filter.vcf.gz";
    }

    @Test
    public void runsTwoPipedBcfToolsFilterCommandInSubshell() {
        assertThat(bash()).contains("("
                + "/opt/tools/bcftools/1.3.1/bcftools filter -e 'PON_COUNT!=\".\" && INFO/TIER=\"HOTSPOT\" && PON_MAX>=5 && PON_COUNT >= 10' -s SAGE_PON -m+ /data/output/tumor.strelka.vcf -O u | "
                + "/opt/tools/bcftools/1.3.1/bcftools filter -e 'PON_COUNT!=\".\" && INFO/TIER=\"PANEL\" && PON_MAX>=5 && PON_COUNT >= 3' -s SAGE_PON -m+ -O u | "
                + "/opt/tools/bcftools/1.3.1/bcftools filter -e 'PON_COUNT!=\".\" && INFO/TIER!=\"HOTSPOT\" && INFO/TIER!=\"PANEL\" && PON_COUNT >= 3' -s SAGE_PON -m+ "
                + "-O z -o /data/output/tumor.sage.pon.filter.vcf.gz");
    }

    @Test
    public void runsTabix() {
        assertThat(bash()).contains(
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.pon.filter.vcf.gz -p vcf");
    }
}