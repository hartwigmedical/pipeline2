package com.hartwig.pipeline.runtime;

import java.io.IOException;

import com.hartwig.patient.Patient;
import com.hartwig.patient.Sample;
import com.hartwig.patient.input.PatientReader;
import com.hartwig.pipeline.BamCreationPipeline;
import com.hartwig.pipeline.GunZip;
import com.hartwig.pipeline.adam.Pipelines;
import com.hartwig.pipeline.metrics.Monitor;
import com.hartwig.pipeline.runtime.configuration.Configuration;
import com.hartwig.pipeline.runtime.configuration.YAMLConfigurationReader;
import com.hartwig.pipeline.runtime.spark.SparkContexts;
import com.hartwig.support.hadoop.Hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineRuntime {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineRuntime.class);
    private final Configuration configuration;
    private final Monitor monitor;
    private final boolean alreadyUnzipped;

    PipelineRuntime(final Configuration configuration, final Monitor monitor, final boolean alreadyUnzipped) {
        this.configuration = configuration;
        this.monitor = monitor;
        this.alreadyUnzipped = alreadyUnzipped;
    }

    void start() {
        JavaSparkContext javaSparkContext = SparkContexts.create("ADAM", configuration);
        SparkContext sparkContext = javaSparkContext.sc();
        try {
            FileSystem fileSystem = Hadoop.fileSystem(configuration.pipeline().hdfs());
            ADAMContext adamContext = new ADAMContext(sparkContext);
            BamCreationPipeline adamPipeline = Pipelines.bamCreationConsolidated(adamContext,
                    fileSystem,
                    monitor,
                    configuration.pipeline().resultsDirectory(),
                    configuration.referenceGenome().path(),
                    configuration.knownIndel().paths(),
                    configuration.pipeline().bwa().threads(),
                    false,
                    false);
            Patient patient = PatientReader.fromHDFS(fileSystem, configuration.patient().directory(), configuration.patient().name());
            Sample sample = GunZip.execute(fileSystem, javaSparkContext, patient.reference(), alreadyUnzipped);
            adamPipeline.execute(sample);
        } catch (Exception e) {
            LOGGER.error("Fatal error while running ADAM pipeline. See stack trace for more details", e);
            throw new RuntimeException(e);
        } finally {
            LOGGER.info("Pipeline complete, stopping spark context");
            sparkContext.stop();
            LOGGER.info("Spark context stopped");
        }
        System.exit(0);
    }

    public static void main(String[] args) {
        Configuration configuration;
        try {
            configuration = YAMLConfigurationReader.from(System.getProperty("user.dir"));
            new PipelineRuntime(configuration, Monitor.noop(), false).start();
        } catch (IOException e) {
            LOGGER.error("Unable to read configuration. Check configuration in /conf/pipeline.yaml", e);
        }
    }
}
