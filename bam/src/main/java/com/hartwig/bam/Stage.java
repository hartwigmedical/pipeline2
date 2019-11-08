package com.hartwig.bam;

import java.io.IOException;

import com.hartwig.io.InputOutput;

public interface Stage<I, O> {

    InputOutput<O> execute(InputOutput<I> input) throws IOException;
}