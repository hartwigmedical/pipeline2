package com.hartwig.bcl2fastq;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Optional;

import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.common.collect.ImmutableList;
import com.hartwig.bcl2fastq.conversion.Conversion;
import com.hartwig.bcl2fastq.conversion.ConvertedFastq;
import com.hartwig.bcl2fastq.conversion.ConvertedSample;
import com.hartwig.bcl2fastq.conversion.ConvertedUndetermined;
import com.hartwig.bcl2fastq.conversion.ImmutableConvertedUndetermined;
import com.hartwig.pipeline.storage.GsUtilFacade;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.testsupport.TestBlobs;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class OutputCopierTest {
    private static final String NA = "na";
    private static final String OUTPUT_SERVICE_ACCOUNT_EMAIL = "output@serviceaccount.com";
    private Bcl2fastqArguments arguments;
    private GsUtilFacade gsUtil;
    private OutputCopier victim;
    private RuntimeBucket runtimeBucket;
    private String runtimeBucketName;
    private String runtimePath;
    private Bucket bucket;

    @Before
    public void setup() {
        arguments = arguments();
        gsUtil = mock(GsUtilFacade.class);
        runtimeBucket = mock(RuntimeBucket.class);
        runtimePath = "results/directory";
        bucket = mock(Bucket.class);
        when(runtimeBucket.getUnderlyingBucket()).thenReturn(bucket);
        victim = new OutputCopier(arguments, runtimeBucket, gsUtil);
    }

    @Test
    public void shouldDoNothingOnEmptyConversion() {
        Conversion conversion = Conversion.builder().flowcell("flow").undetermined(emptyUndetermined()).build();
        victim.accept(conversion);
        verifyZeroInteractions(gsUtil);
    }

    @Test
    public void shouldCopyBothOutputFilesFromEachConvertedFastq() {
        String fastqaPathR1 = "fastqaPathR1";
        String fastqaPathR2 = "fastqaPathR2";
        String fastqbPathR1 = "fastqbPathR1";
        String fastqbPathR2 = "fastqbPathR2";
        String fastqcPathR1 = "fastqcPathR1";
        String fastqcPathR2 = "fastqcPathR2";

        Bucket bucket = mock(Bucket.class);
        when(runtimeBucket.getUnderlyingBucket()).thenReturn(bucket);
        runtimeBucketName = "runtime-bucket";
        when(bucket.getName()).thenReturn(runtimeBucketName);

        ConvertedFastq fastqA = mockFastq(fastqaPathR1, fastqaPathR2);
        ConvertedFastq fastqB = mockFastq(fastqbPathR1, fastqbPathR2);
        ConvertedFastq fastqC = mockFastq(fastqcPathR1, fastqcPathR2);

        Conversion conversion = mock(Conversion.class);
        ConvertedSample sampleA = mock(ConvertedSample.class);
        ConvertedSample sampleB = mock(ConvertedSample.class);
        when(conversion.samples()).thenReturn(ImmutableList.of(sampleA, sampleB));
        when(sampleA.fastq()).thenReturn(ImmutableList.of(fastqA, fastqB));
        when(sampleB.fastq()).thenReturn(ImmutableList.of(fastqC));

        Blob firstBlob = returnBlob(fastqaPathR1, bucket);
        Blob secondBlob = returnBlob(fastqaPathR2, bucket);
        returnBlob(fastqbPathR1, bucket);
        returnBlob(fastqbPathR2, bucket);
        returnBlob(fastqcPathR1, bucket);
        returnBlob(fastqcPathR2, bucket);
        victim.accept(conversion);

        verifyCopy(fastqaPathR1);
        verifyCopy(fastqaPathR2);
        verifyCopy(fastqbPathR1);
        verifyCopy(fastqbPathR2);
        verifyCopy(fastqcPathR1);
        verifyCopy(fastqcPathR2);

        ArgumentCaptor<Acl> firstAclCaptor = ArgumentCaptor.forClass(Acl.class);
        ArgumentCaptor<Acl> secondAclCaptor = ArgumentCaptor.forClass(Acl.class);
        verify(firstBlob).createAcl(firstAclCaptor.capture());
        verify(secondBlob).createAcl(secondAclCaptor.capture());
        verifyAcl(firstAclCaptor.getValue());
        verifyAcl(secondAclCaptor.getValue());
    }

    public Blob returnBlob(final String path, final Bucket bucket) {
        Blob blob = TestBlobs.blob(path);
        when(bucket.get("results/directory/" + path)).thenReturn(blob);
        return blob;
    }

    @Test
    public void addsOutputServiceAccountEmailToAcl() {
        Conversion conversion = Conversion.builder().flowcell("flow").undetermined(emptyUndetermined()).build();
        victim.accept(conversion);
        ArgumentCaptor<Acl> createdAcl = ArgumentCaptor.forClass(Acl.class);
        verify(bucket).createAcl(createdAcl.capture());
        Acl result = createdAcl.getValue();
        verifyAcl(result);
    }

    @NotNull
    public ImmutableConvertedUndetermined emptyUndetermined() {
        return ConvertedUndetermined.builder().yieldQ30(0).yield(0).build();
    }

    public void verifyAcl(final Acl result) {
        assertThat(((Acl.User) result.getEntity()).getEmail()).isEqualTo(OUTPUT_SERVICE_ACCOUNT_EMAIL);
        assertThat(result.getRole()).isEqualTo(Acl.Role.READER);
    }

    private ConvertedFastq mockFastq(String outputPathR1, String outputPathR2) {
        ConvertedFastq fastq = mock(ConvertedFastq.class);
        when(fastq.pathR1()).thenReturn(format("%s/%s", runtimePath, outputPathR1));
        when(fastq.outputPathR1()).thenReturn(outputPathR1);
        when(fastq.pathR2()).thenReturn(Optional.of(format("%s/%s", runtimePath, outputPathR2)));
        when(fastq.outputPathR2()).thenReturn(Optional.of(outputPathR2));
        return fastq;
    }

    private void verifyCopy(String source) {
        verify(gsUtil).copy(format("gs://%s/%s/%s", runtimeBucketName, runtimePath, source),
                format("gs://%s/%s", arguments.outputBucket(), source));
    }

    private Bcl2fastqArguments arguments() {

        return Bcl2fastqArguments.builder()
                .inputBucket(NA)
                .privateKeyPath(NA)
                .sbpApiUrl(NA)
                .serviceAccountEmail(NA)
                .flowcell(NA)
                .outputServiceAccountEmail(OUTPUT_SERVICE_ACCOUNT_EMAIL)
                .outputProject(NA)
                .outputBucket(NA)
                .outputPrivateKeyPath(NA)
                .cleanup(false)
                .project(NA)
                .cloudSdkPath(NA)
                .region(NA)
                .forensicBucket(NA)
                .usePreemptibleVms(false)
                .useLocalSsds(false)
                .build();
    }
}