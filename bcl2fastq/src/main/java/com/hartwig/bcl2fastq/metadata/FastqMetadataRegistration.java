package com.hartwig.bcl2fastq.metadata;

import static java.lang.String.format;

import java.util.Base64;
import java.util.List;
import java.util.function.Consumer;

import com.hartwig.bcl2fastq.conversion.Conversion;
import com.hartwig.bcl2fastq.conversion.ConvertedFastq;
import com.hartwig.bcl2fastq.conversion.ConvertedSample;
import com.hartwig.bcl2fastq.conversion.WithYieldAndQ30;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FastqMetadataRegistration implements Consumer<Conversion> {

    private final static Logger LOGGER = LoggerFactory.getLogger(FastqMetadataRegistration.class);

    private final SbpFastqMetadataApi sbpApi;
    private final String outputBucket;
    private final String log;

    public FastqMetadataRegistration(final SbpFastqMetadataApi sbpApi, final String outputBucket, final String log) {
        this.sbpApi = sbpApi;
        this.outputBucket = outputBucket;
        this.log = log;
    }

    @Override
    public void accept(final Conversion conversion) {
        SbpFlowcell sbpFlowcell = sbpApi.getFlowcell(conversion.flowcell());
        if (sbpFlowcell != null) {
            LOGGER.info("Updating SBP API with conversion results for SBP flowcell [{}]", sbpFlowcell.id());
            double percUndeterminedYield = (conversion.undetermined().yield() / (double) conversion.yield()) * 100;
            boolean flowcellQCPass = QualityControl.errorsInLogs(log) && QualityControl.undeterminedReadPercentage(percUndeterminedYield)
                    && QualityControl.minimumYield(conversion);
            for (ConvertedSample sample : conversion.samples()) {
                SbpSample sbpSample = sbpApi.findOrCreate(sample.barcode(), sample.project());
                if (sbpSample.status().equals(SbpSample.STATUS_READY)) {
                    LOGGER.warn("Sample {} is Ready but got additional data from flowcell {}}. " + "Please verify with lab how to proceed.",
                            sbpSample.barcode(),
                            sbpFlowcell.flowcell_id());
                }
                for (ConvertedFastq convertedFastq : sample.fastq()) {
                    SbpLane sbpLane = sbpApi.findOrCreate(SbpLane.builder()
                            .flowcell_id(sbpFlowcell.id())
                            .name(lane(convertedFastq.id().lane()))
                            .build());

                    sbpApi.create(SbpFastq.builder()
                            .sample_id(sbpSample.id().orElseThrow())
                            .lane_id(sbpLane.id().orElseThrow())
                            .bucket(outputBucket)
                            .name_r1(convertedFastq.outputPathR1())
                            .size_r1(convertedFastq.sizeR1())
                            .hash_r1(convertMd5ToSbpFormat(convertedFastq.md5R1()))
                            .name_r2(convertedFastq.outputPathR2().orElse(""))
                            .size_r2(convertedFastq.sizeR2())
                            .hash_r2(convertMd5ToSbpFormat(convertedFastq.md5R2().orElse("")))
                            .yld(convertedFastq.yield())
                            .q30(Q30.of(convertedFastq))
                            .qc_pass(flowcellQCPass && QualityControl.minimumQ30(convertedFastq, sbpSample.q30_req().orElse(0d)))
                            .build());
                }
                List<SbpFastq> sampleFastq = sbpApi.getFastqs(sbpSample);
                updateSampleYieldAndStatus(sample, sbpSample, sampleFastq);
            }
            SbpFlowcell updated = sbpApi.updateFlowcell(SbpFlowcell.builderFrom(sbpFlowcell)
                    .status(SbpFlowcell.STATUS_CONVERTED)
                    .undet_rds_p_pass(flowcellQCPass)
                    .yld(conversion.yield())
                    .q30(Q30.of(conversion))
                    .undet_rds(conversion.undetermined().yield())
                    .undet_rds_p(percUndeterminedYield * 100)
                    .build());
            SbpFlowcell withTimestamp = sbpApi.updateFlowcell(SbpFlowcell.builderFrom(updated)
                    .undet_rds_p_pass(flowcellQCPass)
                    .convertTime(updated.updateTime())
                    .build());
            LOGGER.info("Updated flowcell [{}]", withTimestamp);
        } else {
            throw new IllegalStateException(String.format(
                    "No flowcell found in SBP API for name [%s]. Check the API and ensure its registered and run" + "bcl2fastq again.",
                    conversion.flowcell()));
        }
    }

    private String convertMd5ToSbpFormat(String originalMd5) {
        return new String(Hex.encodeHex(Base64.getDecoder().decode(originalMd5)));
    }

    private String lane(final int laneNumber) {
        return format("L00%s", laneNumber);
    }

    private void updateSampleYieldAndStatus(final ConvertedSample sample, final SbpSample sbpSample, final List<SbpFastq> validFastq) {
        ImmutableSbpSample.Builder sampleUpdate = SbpSample.builder().from(sbpSample);
        long totalSampleYield = validFastq.stream().filter(SbpFastq::qc_pass).mapToLong(f -> f.yld().orElse(0L)).sum();
        long totalSampleYieldQ30 =
                validFastq.stream().filter(SbpFastq::qc_pass).mapToLong(f -> (long) (f.q30().orElse(0d) / 100 * f.yld().orElse(0L))).sum();
        double sampleQ30 = Q30.of(new WithYieldAndQ30() {
            @Override
            public long yield() {
                return totalSampleYield;
            }

            @Override
            public long yieldQ30() {
                return totalSampleYieldQ30;
            }
        });
        if (sbpSample.yld_req().isPresent() && sbpSample.q30_req().isPresent()) {
            long yldRequired = sbpSample.yld_req().get();
            double q30Required = sbpSample.q30_req().get();
            if (yldRequired < totalSampleYield && q30Required < sampleQ30) {
                sampleUpdate.status(SbpSample.STATUS_READY);
            } else {
                sampleUpdate.status(SbpSample.STATUS_INSUFFICIENT_QUALITY);
            }

        } else {
            sampleUpdate.status(SbpSample.STATUS_UNREGISTERED);
        }
        sbpApi.updateSample(sampleUpdate.name(sample.sample()).yld(totalSampleYield).q30(sampleQ30).build());
    }
}
