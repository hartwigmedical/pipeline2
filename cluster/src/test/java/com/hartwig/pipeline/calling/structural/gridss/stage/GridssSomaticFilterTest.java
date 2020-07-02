package com.hartwig.pipeline.calling.structural.gridss.stage;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class GridssSomaticFilterTest extends SubStageTest {
    @Override
    public SubStage createVictim() {
        return new GridssSomaticFilter(TestInputs.HG37_RESOURCE_FILES);
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.gridss.somatic.vcf.gz";
    }

    @Test
    public void expectedOutput() {
        assertThat(bash()).contains("java -Xmx16G -cp /opt/tools/gripss/1.0/gripss.jar com.hartwig.hmftools.gripss.GripssApplicationKt "
                + "-ref_genome /opt/resources/reference_genome/hg37/Homo_sapiens.GRCh37.GATK.illumina.fasta "
                + "-breakpoint_hotspot /opt/resources/knowledgebases/hg37/KnownFusionPairs.hg19.bedpe "
                + "-breakend_pon /opt/resources/gridss_pon/hg37/gridss_pon_single_breakend.bed "
                + "-breakpoint_pon /opt/resources/gridss_pon/hg37/gridss_pon_breakpoint.bedpe "
                + "-input_vcf /data/output/tumor.strelka.vcf "
                + "-output_vcf /data/output/tumor.gridss.somatic.vcf.gz"
        );
    }

}