package com.hartwig.pipeline.execution.vm;

import com.hartwig.pipeline.execution.JobDefinition;

import org.immutables.value.Value;

@Value.Immutable
public interface VirtualMachineJobDefinition extends JobDefinition<VirtualMachinePerformanceProfile> {

    String STANDARD_IMAGE = "diskimager-standard";

    String imageFamily();

    String startupCommand();

    String completionFlagFile();

    static VirtualMachineJobDefinition germlineCalling(BashStartupScript startupScript) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("germline")
                .imageFamily(STANDARD_IMAGE)
                .startupCommand(startupScript.asUnixString())
                .completionFlagFile(startupScript.completionFlag())
                .performanceProfile(VirtualMachinePerformanceProfile.defaultVm())
                .build();
    }

    static VirtualMachineJobDefinition somaticCalling(BashStartupScript startupScript) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("strelka")
                .imageFamily(STANDARD_IMAGE)
                .startupCommand(startupScript.asUnixString())
                .completionFlagFile(startupScript.completionFlag())
                .performanceProfile(VirtualMachinePerformanceProfile.defaultVm())
                .build();
    }
}