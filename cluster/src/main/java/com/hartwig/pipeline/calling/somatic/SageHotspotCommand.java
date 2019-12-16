package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.execution.vm.JavaClassCommand;
import com.hartwig.pipeline.tools.Versions;

class SageHotspotCommand extends JavaClassCommand {
    SageHotspotCommand(final String mainClass, final String... arguments) {
        super("sage", Versions.SAGE, "sage.jar", mainClass, "8G", arguments);
    }
}
