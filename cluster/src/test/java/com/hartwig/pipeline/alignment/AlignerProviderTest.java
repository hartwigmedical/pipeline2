package com.hartwig.pipeline.alignment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class AlignerProviderTest {

    private static final Arguments LOCAL_ARGUMENTS = Arguments.testDefaults();
    private GoogleCredentials credentials;
    private Storage storage;

    @Before
    public void setUp() throws Exception {
        credentials = mock(GoogleCredentials.class);
        storage = mock(Storage.class);
    }

    @Test
    public void wiresUpBootstrapWithLocalDependencies() throws Exception {
        AlignerProvider victim = AlignerProvider.from(credentials, storage, LOCAL_ARGUMENTS);
        assertThat(victim.get()).isNotNull();
        assertThat(victim).isInstanceOf(AlignerProvider.LocalAlignerProvider.class);
    }

    @Ignore
    @Test
    public void wiresUpBootstrapWithSbpDependencies() throws Exception {
        AlignerProvider victim = AlignerProvider.from(credentials, storage, Arguments.testDefaultsBuilder().build());
        assertThat(victim.get()).isNotNull();
        assertThat(victim).isInstanceOf(AlignerProvider.SbpAlignerProvider.class);
    }
}
