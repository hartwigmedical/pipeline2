package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;

class BcfToolsAnnotation extends SubStage {

    private final List<String> annotationArguments;

    BcfToolsAnnotation(final String name, final List<String> annotationArguments) {
        super(name + ".annotated", OutputFile.GZIPPED_VCF);
        this.annotationArguments = annotationArguments;
    }

    @Override
    BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        return bash.addCommand(new BcfToolsAnnotationCommand(annotationArguments, input.path(), output.path()))
                .addCommand(new TabixCommand(output.path()));
    }
}