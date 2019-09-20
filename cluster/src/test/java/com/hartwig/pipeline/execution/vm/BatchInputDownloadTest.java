package com.hartwig.pipeline.execution.vm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hartwig.pipeline.testsupport.CommonTestEntities;

import org.junit.Test;

public class BatchInputDownloadTest implements CommonTestEntities {
    @Test
    public void shouldCreateSingleGsCopyCommandForAllInputsToInputDirectory() {
        InputDownload inputOne = mock(InputDownload.class);
        InputDownload inputTwo = mock(InputDownload.class);
        BatchInputDownload download = new BatchInputDownload(inputOne, inputTwo);
        when(inputOne.getRemoteSourcePath()).thenReturn("gs://remote/location.1");
        when(inputTwo.getRemoteSourcePath()).thenReturn("gs://remoter/location.2");

        String allInputSources = inputOne.getRemoteSourcePath() + " " + inputTwo.getRemoteSourcePath();
        assertThat(download.asBash()).isEqualTo("gsutil -qm cp " + allInputSources + " " + CommonTestEntities.IN_DIR);
    }
}
