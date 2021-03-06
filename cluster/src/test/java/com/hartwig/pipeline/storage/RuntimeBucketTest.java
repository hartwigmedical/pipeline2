package com.hartwig.pipeline.storage;

import static com.hartwig.pipeline.testsupport.TestInputs.defaultSomaticRunMetadata;
import static com.hartwig.pipeline.testsupport.TestInputs.referenceRunMetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.hartwig.pipeline.Arguments;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class RuntimeBucketTest {

    private static final String NAMESPACE = "test";
    private static final String REGION = "region";
    private static final String NAMESPACED_BLOB = "test/path/to/blob";
    private Storage storage;
    private ArgumentCaptor<BucketInfo> bucketInfo;
    private Bucket bucket;

    @Before
    public void setUp() throws Exception {
        storage = mock(Storage.class);
        bucket = mock(Bucket.class);
        bucketInfo = ArgumentCaptor.forClass(BucketInfo.class);
        when(storage.create(bucketInfo.capture())).thenReturn(bucket);
    }

    @Test
    public void createsBucketIdFromSampleName() {
        RuntimeBucket.from(storage,
                NAMESPACE,
                referenceRunMetadata(),
                Arguments.testDefaultsBuilder().profile(Arguments.DefaultsProfile.PRODUCTION).build());
        assertThat(bucketInfo.getValue().getName()).isEqualTo("run-reference-test");
    }

    @Test
    public void createsBucketIdFromTumorAndReferenceName() {
        RuntimeBucket.from(storage,
                NAMESPACE,
                defaultSomaticRunMetadata(),
                Arguments.testDefaultsBuilder().profile(Arguments.DefaultsProfile.PRODUCTION).build());
        assertThat(bucketInfo.getValue().getName()).isEqualTo("run-reference-tumor-test");
    }

    @Test
    public void setsRegionToArguments() {
        RuntimeBucket.from(storage, NAMESPACE, referenceRunMetadata(), Arguments.testDefaultsBuilder().region(REGION).build());
        assertThat(bucketInfo.getValue().getLocation()).isEqualTo(REGION);
    }

    @Test
    public void usesRegionalStorageClass() {
        defaultBucket();
        assertThat(bucketInfo.getValue().getStorageClass()).isEqualTo(StorageClass.REGIONAL);
    }

    @Test
    public void namespacesGetOperation() {
        RuntimeBucket victim = defaultBucket();
        ArgumentCaptor<String> blobNameCaptor = ArgumentCaptor.forClass(String.class);
        victim.get("/path/to/blob");
        verify(bucket).get(blobNameCaptor.capture());
        assertThat(blobNameCaptor.getValue()).isEqualTo(NAMESPACED_BLOB);
    }

    @Test
    public void doesNotNamespaceIfNamespacedAlready() {
        RuntimeBucket victim = defaultBucket();
        ArgumentCaptor<String> blobNameCaptor = ArgumentCaptor.forClass(String.class);
        victim.get("test/path/to/blob");
        verify(bucket).get(blobNameCaptor.capture());
        assertThat(blobNameCaptor.getValue()).isEqualTo(NAMESPACED_BLOB);
    }

    @Test
    public void namespacesCreateOperationBytes() {
        RuntimeBucket victim = defaultBucket();
        ArgumentCaptor<String> blobNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<byte[]> contentCaptor = ArgumentCaptor.forClass(byte[].class);
        byte[] content = {};
        victim.create("/path/to/blob", content);
        verify(bucket).create(blobNameCaptor.capture(), contentCaptor.capture());
        assertThat(blobNameCaptor.getValue()).isEqualTo(NAMESPACED_BLOB);
    }

    @Test
    public void namespacesCreateOperationStream() {
        RuntimeBucket victim = defaultBucket();
        ArgumentCaptor<String> blobNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<InputStream> contentCaptor = ArgumentCaptor.forClass(InputStream.class);
        InputStream content = new ByteArrayInputStream(new byte[] {});
        victim.create("/path/to/blob", content);
        verify(bucket).create(blobNameCaptor.capture(), contentCaptor.capture());
        assertThat(blobNameCaptor.getValue()).isEqualTo(NAMESPACED_BLOB);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void namespacesListOperationNoPrefix() {
        RuntimeBucket victim = defaultBucket();
        ArgumentCaptor<Storage.BlobListOption> listOptionCaptor = ArgumentCaptor.forClass(Storage.BlobListOption.class);
        Page<Blob> page = mock(Page.class);
        when(bucket.list(listOptionCaptor.capture())).thenReturn(page);
        victim.list();
        assertThat(listOptionCaptor.getValue()).isEqualTo(Storage.BlobListOption.prefix("test"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void namespacesListOperationPrefix() {
        RuntimeBucket victim = defaultBucket();
        ArgumentCaptor<Storage.BlobListOption> listOptionCaptor = ArgumentCaptor.forClass(Storage.BlobListOption.class);
        Page<Blob> page = mock(Page.class);
        when(bucket.list(listOptionCaptor.capture())).thenReturn(page);
        victim.list("prefix");
        assertThat(listOptionCaptor.getValue()).isEqualTo(Storage.BlobListOption.prefix("test/prefix"));
    }

    @Test
    public void usesCmek() {
        String keyName = "key";
        Arguments arguments = Arguments.testDefaultsBuilder().cmek(keyName).build();
        RuntimeBucket.from(storage, NAMESPACE, referenceRunMetadata(), arguments);
        assertThat(bucketInfo.getValue().getDefaultKmsKeyName()).isEqualTo(keyName);
    }

    @NotNull
    private RuntimeBucket defaultBucket() {
        return RuntimeBucket.from(storage, NAMESPACE, referenceRunMetadata(), Arguments.testDefaults());
    }
}