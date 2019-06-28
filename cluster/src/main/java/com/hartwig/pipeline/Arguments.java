package com.hartwig.pipeline;

import java.util.Optional;

import org.immutables.value.Value;

@Value.Immutable
public interface Arguments {

    String EMPTY = "";

    enum DefaultsProfile {
        PRODUCTION,
        DEVELOPMENT
    }

    enum Mode {
        SINGLE_SAMPLE,
        SOMATIC
    }

    boolean forceJarUpload();

    boolean cleanup();

    boolean usePreemptibleVms();

    boolean upload();

    boolean runBamMetrics();

    boolean runAligner();

    boolean runSnpGenotyper();

    boolean runGermlineCaller();

    boolean runSomaticCaller();

    boolean runStructuralCaller();

    boolean runTertiary();

    DefaultsProfile profile();

    Mode mode();

    String project();

    String version();

    String region();

    String jarDirectory();

    String sampleDirectory();

    String sampleId();

    String setId();

    String privateKeyPath();

    String serviceAccountEmail();

    String sbpApiUrl();

    String sbpS3Url();

    String nodeInitializationScript();

    String cloudSdkPath();

    String rclonePath();

    String rcloneGcpRemote();

    String rcloneS3Remote();

    String resourceBucket();

    String toolsBucket();

    String patientReportBucket();

    Optional<Integer> sbpApiSampleId();

    Optional<Integer> sbpApiRunId();

    Optional<String> runId();

    static ImmutableArguments.Builder builder() {
        return ImmutableArguments.builder();
    }

    static Arguments defaults(String profileString) {
        return defaultsBuilder(profileString).build();
    }

    static Arguments testDefaults() {
        return testDefaultsBuilder().build();
    }

    static ImmutableArguments.Builder testDefaultsBuilder() {
        return defaultsBuilder(DefaultsProfile.DEVELOPMENT.name());
    }

    static ImmutableArguments.Builder defaultsBuilder(String profileString) {
        DefaultsProfile profile = DefaultsProfile.valueOf(profileString.toUpperCase());
        if (profile.equals(DefaultsProfile.PRODUCTION)) {
            return ImmutableArguments.builder()
                    .profile(profile)
                    .mode(DEFAULT_MODE)
                    .rclonePath(DEFAULT_PRODUCTION_RCLONE_PATH)
                    .rcloneGcpRemote(DEFAULT_PRODUCTION_RCLONE_GCP_REMOTE)
                    .rcloneS3Remote(DEFAULT_PRODUCTION_RCLONE_S3_REMOTE)
                    .region(DEFAULT_PRODUCTION_REGION)
                    .project(DEFAULT_PRODUCTION_PROJECT)
                    .version(DEFAULT_PRODUCTION_VERSION)
                    .sampleDirectory(DEFAULT_PRODUCTION_SAMPLE_DIRECTORY)
                    .nodeInitializationScript(DEFAULT_PRODUCTION_NODE_INIT)
                    .sbpApiUrl(DEFAULT_PRODUCTION_SBP_API_URL)
                    .sbpS3Url(DEFAULT_PRODUCTION_SBP_S3_URL)
                    .jarDirectory(DEFAULT_PRODUCTION_JAR_LIB)
                    .privateKeyPath(DEFAULT_PRODUCTION_KEY_PATH)
                    .serviceAccountEmail(DEFAULT_PRODUCTION_SERVICE_ACCOUNT_EMAIL)
                    .cloudSdkPath(DEFAULT_PRODUCTION_CLOUD_SDK_PATH)
                    .forceJarUpload(false)
                    .cleanup(true)
                    .usePreemptibleVms(true)
                    .upload(true)
                    .runBamMetrics(true)
                    .runAligner(true)
                    .runSnpGenotyper(true)
                    .runGermlineCaller(true)
                    .runSomaticCaller(true)
                    .runStructuralCaller(true)
                    .runTertiary(true)
                    .sampleId(EMPTY)
                    .setId(EMPTY)
                    .toolsBucket(DEFAULT_PRODUCTION_COMMON_TOOLS_BUCKET)
                    .resourceBucket(DEFAULT_PRODUCTION_RESOURCE_BUCKET)
                    .patientReportBucket(DEFAULT_PRODUCTION_PATIENT_REPORT_BUCKET);
        } else {
            return ImmutableArguments.builder()
                    .profile(profile)
                    .mode(DEFAULT_MODE)
                    .region(DEFAULT_DEVELOPMENT_REGION)
                    .project(DEFAULT_DEVELOPMENT_PROJECT)
                    .version(DEFAULT_DEVELOPMENT_VERSION)
                    .sampleDirectory(DEFAULT_DEVELOPMENT_SAMPLE_DIRECTORY)
                    .nodeInitializationScript(DEFAULT_DEVELOPMENT_NODE_INIT)
                    .jarDirectory(DEFAULT_DEVELOPMENT_JAR_LIB)
                    .privateKeyPath(DEFAULT_DEVELOPMENT_KEY_PATH)
                    .cloudSdkPath(DEFAULT_DEVELOPMENT_CLOUD_SDK_PATH)
                    .serviceAccountEmail(DEFAULT_DEVELOPMENT_SERVICE_ACCOUNT_EMAIL)
                    .forceJarUpload(false)
                    .cleanup(true)
                    .usePreemptibleVms(true)
                    .upload(true)
                    .runBamMetrics(true)
                    .runAligner(true)
                    .runSnpGenotyper(true)
                    .runGermlineCaller(true)
                    .runSomaticCaller(true)
                    .runTertiary(true)
                    .runStructuralCaller(true)
                    .rclonePath(NOT_APPLICABLE)
                    .rcloneS3Remote(NOT_APPLICABLE)
                    .rcloneGcpRemote(NOT_APPLICABLE)
                    .sbpS3Url(EMPTY)
                    .sbpApiUrl(NOT_APPLICABLE)
                    .sampleId(EMPTY)
                    .setId(EMPTY)
                    .toolsBucket(DEFAULT_DEVELOPMENT_COMMON_TOOLS_BUCKET)
                    .resourceBucket(DEFAULT_DEVELOPMENT_RESOURCE_BUCKET)
                    .patientReportBucket(DEFAULT_DEVELOPMENT_PATIENT_REPORT_BUCKET);
        }
    }

    static String workingDir() {
        return System.getProperty("user.dir");
    }

    Mode DEFAULT_MODE = Mode.SINGLE_SAMPLE;

    String DEFAULT_PRODUCTION_RCLONE_PATH = "/usr/bin";
    String DEFAULT_PRODUCTION_RCLONE_GCP_REMOTE = "gs";
    String DEFAULT_PRODUCTION_RCLONE_S3_REMOTE = "s3";
    String DEFAULT_PRODUCTION_REGION = "europe-west4";
    String DEFAULT_PRODUCTION_PROJECT = "hmf-pipeline-prod-e45b00f2";
    String DEFAULT_PRODUCTION_VERSION = "";
    String DEFAULT_PRODUCTION_SAMPLE_DIRECTORY = "/samples";
    String DEFAULT_PRODUCTION_NODE_INIT = "node-init.sh";
    String DEFAULT_PRODUCTION_SBP_API_URL = "http://hmfapi";
    String DEFAULT_PRODUCTION_SBP_S3_URL = "https://s3.object02.schubergphilis.com";
    String DEFAULT_PRODUCTION_JAR_LIB = "/usr/share/pipeline5";
    String DEFAULT_PRODUCTION_KEY_PATH = "/secrets/bootstrap-key.json";
    String DEFAULT_PRODUCTION_CLOUD_SDK_PATH = "/usr/lib/google-cloud-sdk/bin";
    String DEFAULT_PRODUCTION_SERVICE_ACCOUNT_EMAIL = String.format("bootstrap@%s.iam.gserviceaccount.com", DEFAULT_PRODUCTION_PROJECT);
    String DEFAULT_PRODUCTION_RESOURCE_BUCKET = "common-resources-prod";
    String DEFAULT_PRODUCTION_COMMON_TOOLS_BUCKET = "common-tools-prod";
    String DEFAULT_PRODUCTION_PATIENT_REPORT_BUCKET = "pipeline-output-prod";

    String NOT_APPLICABLE = "N/A";
    String DEFAULT_DEVELOPMENT_REGION = "europe-west4";
    String DEFAULT_DEVELOPMENT_PROJECT = "hmf-pipeline-development";
    String DEFAULT_DEVELOPMENT_VERSION = "local-SNAPSHOT";
    String DEFAULT_DEVELOPMENT_SAMPLE_DIRECTORY = workingDir() + "/samples";
    String DEFAULT_DEVELOPMENT_NODE_INIT = workingDir() + "/cluster/src/main/resources/node-init.sh";
    String DEFAULT_DEVELOPMENT_JAR_LIB = workingDir() + "/system/target";
    String DEFAULT_DEVELOPMENT_KEY_PATH = workingDir() + "/bootstrap-key.json";
    String DEFAULT_DEVELOPMENT_CLOUD_SDK_PATH = System.getProperty("user.home") + "/gcloud/google-cloud-sdk/bin";
    String DEFAULT_DEVELOPMENT_SERVICE_ACCOUNT_EMAIL = String.format("bootstrap@%s.iam.gserviceaccount.com", DEFAULT_DEVELOPMENT_PROJECT);
    String DEFAULT_DEVELOPMENT_RESOURCE_BUCKET = "common-resources";
    String DEFAULT_DEVELOPMENT_COMMON_TOOLS_BUCKET = "common-tools";
    String DEFAULT_DEVELOPMENT_PATIENT_REPORT_BUCKET = "pipeline-output-dev";
}
