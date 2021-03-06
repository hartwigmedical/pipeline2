package com.hartwig.pipeline.calling.sage;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.stages.SubStage;

import org.junit.Test;

public class PonFilterHg38Test extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new PonFilter(RefGenomeVersion.V38);
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.sage.pon.filter.vcf.gz";
    }

    @Test
    public void runsTwoPipedBcfToolsFilterCommandInSubshell() {
        assertThat(bash()).contains("("
                + "/opt/tools/bcftools/1.9/bcftools filter -e 'PON_COUNT!=\".\" && INFO/TIER=\"HOTSPOT\" && PON_MAX>=5 && PON_COUNT >= 5' -s PON -m+ /data/output/tumor.strelka.vcf -O u | "
                + "/opt/tools/bcftools/1.9/bcftools filter -e 'PON_COUNT!=\".\" && INFO/TIER=\"PANEL\" && PON_MAX>=5 && PON_COUNT >= 2' -s PON -m+ -O u | "
                + "/opt/tools/bcftools/1.9/bcftools filter -e 'PON_COUNT!=\".\" && INFO/TIER!=\"HOTSPOT\" && INFO/TIER!=\"PANEL\" && PON_COUNT >= 2' -s PON -m+ "
                + "-O z -o /data/output/tumor.sage.pon.filter.vcf.gz");
    }

    @Test
    public void runsTabix() {
        assertThat(bash()).contains(
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.pon.filter.vcf.gz -p vcf");
    }
}