package com.hartwig.bam.after;

import java.io.IOException;

import com.hartwig.patient.Sample;

public interface SortAndIndexer {
    void execute(Sample sample, String sourceBam) throws IOException, InterruptedException;
}