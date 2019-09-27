package com.hartwig.pipeline.alignment.vm;

class SambambaViewCommand extends SambambaCommand {

    SambambaViewCommand() {
        super("view", "-f", "cram", "-S", "-l0", "/dev/stdin");
    }
}
