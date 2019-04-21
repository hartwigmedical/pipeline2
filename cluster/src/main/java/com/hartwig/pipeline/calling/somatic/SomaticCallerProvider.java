package com.hartwig.pipeline.calling.somatic;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.resource.GATKDictAlias;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.resource.ReferenceGenomeAlias;
import com.hartwig.pipeline.resource.Resource;

public class SomaticCallerProvider {

    private final Arguments arguments;
    private final GoogleCredentials credentials;
    private final Storage storage;

    private SomaticCallerProvider(final Arguments arguments, final GoogleCredentials credentials, final Storage storage) {
        this.arguments = arguments;
        this.credentials = credentials;
        this.storage = storage;
    }

    public static SomaticCallerProvider from(final Arguments arguments, final GoogleCredentials credentials, final Storage storage) {
        return new SomaticCallerProvider(arguments, credentials, storage);
    }

    public SomaticCaller get() {
        return new SomaticCaller(arguments,
                new ComputeEngine(arguments, credentials, storage),
                storage,
                new Resource(storage,
                        arguments.referenceGenomeBucket(),
                        "reference_genome",
                        new ReferenceGenomeAlias().andThen(new GATKDictAlias())),
                new Resource(storage, "strelka_config", "strelka_config"),
                new Resource(storage, "hg19_mappability_tracks", "hg19_mappability_tracks"),
                ResultsDirectory.defaultDirectory());
    }
}
