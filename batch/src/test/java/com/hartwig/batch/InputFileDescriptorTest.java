package com.hartwig.batch;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public class InputFileDescriptorTest {
    private String project;
    private String local;
    private String remote;
    private ImmutableInputFileDescriptor.Builder builder;

    @Before
    public void setup() {
        project = "my-project";
        local = "local_dest";
        remote = "some.remote.file";
        builder = InputFileDescriptor.builder().billedProject(project);
    }

    @Test
    public void shouldAddProtocolIfLeftOutOfFilenames() {
        assertCommandForm(builder.remoteFilename(format("gs://%s", remote)).build().toCommandForm(local), remote);
    }

    @Test
    public void shouldNotDoubleAddProtocolIfIncludedInFilenames() {
        assertCommandForm(builder.remoteFilename(remote).build().toCommandForm(local), remote);
    }

    private void assertCommandForm(String commandForm, String remoteFile) {
        assertThat(commandForm).isEqualTo(format("gsutil -q -u %s cp gs://%s %s", project, remoteFile, local));
    }

}