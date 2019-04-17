package com.hartwig.pipeline.calling.germline;

import org.junit.Test;

import static org.apache.commons.lang.RandomStringUtils.randomAlphabetic;
import static org.assertj.core.api.Assertions.assertThat;

public class GatkHaplotypeCallerTest {
    @Test
    public void shouldGenerateCorrectCommandFromArguments() {
        String jar = randStr();
        String inputBam = randStr();
        String referenceFasta = randStr();
        String outputVcf = randStr();

        String expected = "java -jar " + jar + " --analysis_type HaplotypeCaller --input_file " + inputBam
                + " -o " + outputVcf + " --reference_sequence " + referenceFasta
//                .append(" -D path_to_dbsnp")
                + " -nct $(grep -c '^processor' /proc/cpuinfo)"
                + " -variant_index_type LINEAR -variant_index_parameter 128000 -stand_call_conf 15.0"
                + " -ERC GVCF -GQB 5 -GQB 10 -GQB 15 -GQB 20 -GQB 30 -GQB 40 -GQB 50 -GQB 60 --sample_ploidy 2";

        assertThat(new GatkHaplotypeCaller(jar, inputBam, referenceFasta, outputVcf).buildCommand()).isEqualTo(expected);
    }

    private static String randStr() {
        return randomAlphabetic(16);
    }
}