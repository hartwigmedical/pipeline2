package com.hartwig.pipeline.calling.command;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.joining;

public class VersionedToolCommand implements BashCommand {

    private final String toolName;
    private final String toolBinaryName;
    private final String version;
    private final List<String> arguments;

    public VersionedToolCommand(final String toolName, final String toolBinaryName, final String version, final String... arguments) {
        this.toolName = toolName;
        this.toolBinaryName = toolBinaryName;
        this.version = version;
        this.arguments = Arrays.asList(arguments);
    }

    @Override
    public String asBash() {
        return String.format("%s/%s/%s/%s %s",
                VmDirectories.TOOLS,
                toolName,
                version,
                toolBinaryName,
                arguments.stream().collect(joining(" ")));
    }
}