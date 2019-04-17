package com.hartwig.pipeline.resource;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;

import org.immutables.value.Value;

@Value.Immutable
public interface Resources {

    Resource referenceGenome();

    Resource knownIndels();

    Resource knownSnps();

    Resource strelkaConfig();

    static Resources from(Storage storage, Arguments arguments) {
        Resource referenceGenome = new Resource(storage, arguments.referenceGenomeBucket(), "reference_genome", new ReferenceGenomeAlias());
        Resource knownIndels = new Resource(storage, arguments.knownIndelsBucket(), "known_indels");
        Resource knownSnps = new Resource(storage, "known_snps", "known_snps");
        Resource strelkaConfig = new Resource(storage, "strelka_config", "strelka_config");
        return ImmutableResources.builder()
                .referenceGenome(referenceGenome)
                .knownIndels(knownIndels)
                .knownSnps(knownSnps)
                .strelkaConfig(strelkaConfig)
                .build();
    }
}