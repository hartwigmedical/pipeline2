package com.hartwig.pipeline.alignment;

import java.util.concurrent.Executors;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.sample.FileSystemSampleSource;
import com.hartwig.pipeline.alignment.sample.GoogleStorageSampleSource;
import com.hartwig.pipeline.alignment.sample.SampleSource;
import com.hartwig.pipeline.alignment.sample.SbpS3SampleSource;
import com.hartwig.pipeline.alignment.sample.SbpSampleReader;
import com.hartwig.pipeline.alignment.vm.VmAligner;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.storage.CloudCopy;
import com.hartwig.pipeline.storage.CloudSampleUpload;
import com.hartwig.pipeline.storage.GSUtilCloudCopy;
import com.hartwig.pipeline.storage.GoogleStorageFileSource;
import com.hartwig.pipeline.storage.LocalFileSource;
import com.hartwig.pipeline.storage.RCloneCloudCopy;
import com.hartwig.pipeline.storage.SampleUpload;
import com.hartwig.pipeline.storage.SbpS3FileSource;

public abstract class AlignerProvider {

    private final GoogleCredentials credentials;
    private final Storage storage;
    private final Arguments arguments;

    AlignerProvider(final GoogleCredentials credentials, final Storage storage, final Arguments arguments) {
        this.credentials = credentials;
        this.storage = storage;
        this.arguments = arguments;
    }

    protected Arguments getArguments() {
        return arguments;
    }

    abstract Aligner wireUp(GoogleCredentials credentials, Storage storage, AlignmentOutputStorage alignmentOutputStorage,
            ResultsDirectory resultsDirectory) throws Exception;

    public Aligner get() throws Exception {
        ResultsDirectory resultsDirectory = ResultsDirectory.defaultDirectory();
        AlignmentOutputStorage alignmentOutputStorage = new AlignmentOutputStorage(storage, arguments, resultsDirectory);
        return wireUp(credentials, storage, alignmentOutputStorage, resultsDirectory);
    }

    public static AlignerProvider from(GoogleCredentials credentials, Storage storage, Arguments arguments) {
        if (arguments.sbpApiRunId().isPresent() || arguments.sbpApiSampleId().isPresent()) {
            return new SbpAlignerProvider(credentials, storage, arguments);
        }
        return new LocalAlignerProvider(credentials, storage, arguments);
    }

    static class LocalAlignerProvider extends AlignerProvider {

        private LocalAlignerProvider(final GoogleCredentials credentials, final Storage storage, final Arguments arguments) {
            super(credentials, storage, arguments);
        }

        @Override
        Aligner wireUp(GoogleCredentials credentials, Storage storage, AlignmentOutputStorage alignmentOutputStorage,
                ResultsDirectory resultsDirectory) throws Exception {
            SampleSource sampleSource = getArguments().upload()
                    ? new FileSystemSampleSource(getArguments().sampleDirectory())
                    : new GoogleStorageSampleSource(storage, getArguments());
            GSUtilCloudCopy gsUtilCloudCopy = new GSUtilCloudCopy(getArguments().cloudSdkPath());
            SampleUpload sampleUpload = new CloudSampleUpload(new LocalFileSource(), gsUtilCloudCopy);
            return AlignerProvider.constructVmAligner(getArguments(),
                    credentials,
                    storage,
                    sampleSource,
                    sampleUpload,
                    resultsDirectory,
                    alignmentOutputStorage);
        }
    }

    static class SbpAlignerProvider extends AlignerProvider {

        private SbpAlignerProvider(final GoogleCredentials credentials, final Storage storage, final Arguments arguments) {
            super(credentials, storage, arguments);
        }

        @Override
        Aligner wireUp(GoogleCredentials credentials, Storage storage, AlignmentOutputStorage alignmentOutputStorage,
                ResultsDirectory resultsDirectory) throws Exception {
            SbpRestApi sbpRestApi = SbpRestApi.newInstance(getArguments().sbpApiUrl());
            SampleSource sampleSource = new SbpS3SampleSource(new SbpSampleReader(sbpRestApi));
            CloudCopy cloudCopy = new RCloneCloudCopy(getArguments().rclonePath(),
                    getArguments().rcloneGcpRemote(),
                    getArguments().rcloneS3RemoteDownload(),
                    ProcessBuilder::new);
            SampleUpload sampleUpload =
                    new CloudSampleUpload(getArguments().gsFastq() ? new GoogleStorageFileSource() : new SbpS3FileSource(), cloudCopy);
            return AlignerProvider.constructVmAligner(getArguments(),
                    credentials,
                    storage,
                    sampleSource,
                    sampleUpload,
                    resultsDirectory,
                    alignmentOutputStorage);
        }
    }

    private static Aligner constructVmAligner(final Arguments arguments, final GoogleCredentials credentials, final Storage storage,
            final SampleSource sampleSource, final SampleUpload sampleUpload, final ResultsDirectory resultsDirectory,
            final AlignmentOutputStorage alignmentOutputStorage) throws Exception {
        ComputeEngine computeEngine = ComputeEngine.from(arguments, credentials, arguments.shallow());
        return new VmAligner(arguments,
                computeEngine,
                storage,
                sampleSource,
                sampleUpload,
                resultsDirectory,
                alignmentOutputStorage,
                Executors.newCachedThreadPool());
    }
}
