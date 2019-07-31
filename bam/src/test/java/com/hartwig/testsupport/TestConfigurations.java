package com.hartwig.testsupport;

import static com.hartwig.support.test.Resources.testResource;

import com.google.common.collect.ImmutableMap;
import com.hartwig.bam.runtime.configuration.Configuration;
import com.hartwig.bam.runtime.configuration.ImmutableConfiguration;
import com.hartwig.bam.runtime.configuration.ImmutablePatientParameters;
import com.hartwig.bam.runtime.configuration.ImmutablePipelineParameters;
import com.hartwig.bam.runtime.configuration.ImmutableReferenceGenomeParameters;
import com.hartwig.bam.runtime.configuration.ReferenceGenomeParameters;

public class TestConfigurations {
    private static final String PATIENT_DIR = "patients";

    public static final String HUNDREDK_READS_HISEQ_PATIENT_NAME = "TESTX";

    public static final ReferenceGenomeParameters REFERENCE_GENOME_PARAMETERS =
            ImmutableReferenceGenomeParameters.builder().directory(testResource("reference_genome/")).file("reference.fasta").build();

    private static final ImmutablePatientParameters.Builder DEFAULT_PATIENT_BUILDER = ImmutablePatientParameters.builder();

    private static final ImmutableConfiguration.Builder DEFAULT_CONFIG_BUILDER = ImmutableConfiguration.builder()
            .spark(ImmutableMap.of("master", "local[2]"))
            .pipeline(ImmutablePipelineParameters.builder().hdfs("file:///").build())
            .referenceGenome(REFERENCE_GENOME_PARAMETERS);

    public static final Configuration HUNDREDK_READS_HISEQ =
            DEFAULT_CONFIG_BUILDER.patient(DEFAULT_PATIENT_BUILDER.directory(testResource(PATIENT_DIR + "/100k_reads_hiseq"))
                    .name(HUNDREDK_READS_HISEQ_PATIENT_NAME)
                    .build()).build();
}