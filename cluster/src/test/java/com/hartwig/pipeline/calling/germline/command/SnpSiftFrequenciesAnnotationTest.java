package com.hartwig.pipeline.calling.germline.command;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;

import org.junit.Test;

public class SnpSiftFrequenciesAnnotationTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new SnpSiftFrequenciesAnnotation("gonl_v5.vcf.gz", "snpEff.config");
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.gonlv5.annotated.vcf.gz";
    }

    @Test
    public void runsSnpSiftFrequenciesAnnotation() {
        assertThat(output.currentBash().asUnixString()).contains("(java -Xmx20G -jar /data/tools/snpEff/4.3s/SnpSift.jar annotate -c "
                + "snpEff.config -tabix -name GoNLv5 -info AF,AN,AC gonl_v5.vcf.gz /data/output/tumor.strelka.vcf > "
                + "/data/output/tumor.gonlv5.annotated.vcf)");
    }

    @Test
    public void runsBgZip() {
        assertThat(output.currentBash().asUnixString()).contains("bgzip -f /data/output/tumor.gonlv5.annotated.vcf");
    }

    @Test
    public void runsTabix() {
        assertThat(output.currentBash().asUnixString()).contains("tabix /data/output/tumor.gonlv5.annotated.vcf.gz -p vcf");
    }
}