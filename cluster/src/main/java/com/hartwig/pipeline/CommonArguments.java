package com.hartwig.pipeline;

import java.util.Optional;

import org.apache.commons.cli.CommandLine;

public interface CommonArguments {

    String PROJECT = "project";
    String REGION = "region";
    String LOCAL_SSDS = "local_ssds";
    String PREEMPTIBLE_VMS = "preemptible_vms";
    String STORAGE_KEY_PATH = "storage_key_path";
    String SERVICE_ACCOUNT_EMAIL = "service_account_email";
    String CLOUD_SDK = "cloud_sdk";
    String PRIVATE_KEY_PATH = "private_key_path";
    String CMEK = "cmek";
    String PRIVATE_NETWORK = "private_network";

    String CMEK_DESCRIPTION = "The name of the Customer Managed Encryption Key. When this flag is populated all runtime "
            + "buckets will use this key.";
    String PRIVATE_NETWORK_DESCRIPTION =  "The name of the private network to use. Specifying a value here will use this "
            + "network and subnet of the same name and disable external IPs. Ensure the network has been created in GCP before enabling "
            + "this flag";

    String project();

    Optional<String> privateKeyPath();

    String cloudSdkPath();

    String region();

    boolean usePreemptibleVms();

    boolean useLocalSsds();

    Optional<String> privateNetwork();

    String serviceAccountEmail();

    Optional<String> cmek();

    Optional<String> runId();

    Optional<Integer> sbpApiRunId();

    static Optional<String> privateKey(CommandLine commandLine) {
        if (commandLine.hasOption(PRIVATE_KEY_PATH)) {
            return Optional.of(commandLine.getOptionValue(PRIVATE_KEY_PATH));
        }
        return Optional.empty();
    }
}
