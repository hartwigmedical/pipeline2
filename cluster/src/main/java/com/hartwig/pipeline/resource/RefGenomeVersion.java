package com.hartwig.pipeline.resource;

public enum RefGenomeVersion {
    V37("hg19", "HG37", "37", "37", "HG19"),
    V38("hg38", "HG38", "38", "38", "HG38"),
    V38_ALT("hg38", "HG38", "38", "38_ALT", "HG38");

    private final String sage;
    private final String linx;
    private final String resources;
    private final String pipeline;
    private final String chord;

    RefGenomeVersion(final String sage, final String linx, final String resources, final String pipeline, final String chord) {
        this.sage = sage;
        this.linx = linx;
        this.resources = resources;
        this.pipeline = pipeline;
        this.chord = chord;
    }

    public String sage() {
        return sage;
    }

    public String resources() {
        return resources;
    }

    public String pipeline() {
        return pipeline;
    }

    public String chord() {
        return chord;
    }

    public String linx() {
        return linx;
    }
}
