package com.hartwig.pipeline.execution.vm;

import com.hartwig.pipeline.execution.JobDefinition;

import org.immutables.value.Value;

@Value.Immutable
public interface VirtualMachineJobDefinition extends JobDefinition<VirtualMachinePerformanceProfile> {

    String STANDARD_IMAGE = "diskimager-standard";

    @Value.Default
    default String imageFamily(){
        return STANDARD_IMAGE;
    }

    BashStartupScript startupCommand();

    @Override
    @Value.Default
    default VirtualMachinePerformanceProfile performanceProfile(){
        return VirtualMachinePerformanceProfile.defaultVm();
    }

    static ImmutableVirtualMachineJobDefinition.Builder builder() {
        return ImmutableVirtualMachineJobDefinition.builder();
    }

    static VirtualMachineJobDefinition germlineCalling(BashStartupScript startupScript) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("germline")
                .startupCommand(startupScript)
                .performanceProfile(VirtualMachinePerformanceProfile.highCpuVm())
                .build();
    }

    static VirtualMachineJobDefinition somaticCalling(BashStartupScript startupScript) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("strelka")
                .startupCommand(startupScript)
                .build();
    }
}