package com.hartwig.pipeline.tertiary.peach;

import static com.hartwig.pipeline.Arguments.testDefaultsBuilder;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

public class PeachTest extends TertiaryStageTest<PeachOutput> {
    @Override
    public void disabledAppropriately() {
        assertThat(victim.shouldRun(testDefaultsBuilder().runTertiary(false).shallow(false).build())).isFalse();
    }

    @Override
    public void enabledAppropriately() {
        assertThat(victim.shouldRun(testDefaultsBuilder().runTertiary(true).shallow(false).build())).isTrue();
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(new AddDatatype(DataType.PEACH_CALLS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Peach.NAMESPACE, "tumor.peach.calls.tsv")),
                new AddDatatype(DataType.PEACH_GENOTYPE,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Peach.NAMESPACE, "tumor.peach.genotype.tsv")));
    }

    @Override
    protected void setupPersistedDataset() {
        // no persistence for this stage
    }

    @Override
    protected void validatePersistedOutput(final PeachOutput output) {
        // no persistence for this stage
    }

    @Override
    protected Stage<PeachOutput, SomaticRunMetadata> createVictim() {
        return new Peach(TestInputs.purpleOutput(), TestInputs.REF_GENOME_37_RESOURCE_FILES);
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList("/opt/tools/peach/1.0_venv/bin/python /opt/tools/peach/1.0/src/main.py "
                + "/data/input/tumor.purple.germline.vcf.gz tumor reference 1.0 /data/output /opt/resources/peach/37/min_DPYD.json "
                + "/usr/bin/vcftools");
    }

    @Override
    protected void validateOutput(final PeachOutput output) {
        // no additional validation
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected List<String> expectedInputs() {
        return List.of(input(expectedRuntimeBucketName() + "/purple/tumor.purple.germline.vcf.gz", "tumor.purple.germline.vcf.gz"));
    }
}