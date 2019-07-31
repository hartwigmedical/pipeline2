package com.hartwig.bam.runtime;

import java.io.IOException;

import com.hartwig.bam.BamCreationPipeline;
import com.hartwig.bam.adam.Pipelines;
import com.hartwig.bam.runtime.configuration.Configuration;
import com.hartwig.bam.runtime.configuration.YAMLConfigurationReader;
import com.hartwig.bam.runtime.spark.SparkContexts;
import com.hartwig.patient.input.PatientReader;
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

    PipelineRuntime(final Configuration configuration) {
        this.configuration = configuration;
    }

    void start() {
        JavaSparkContext javaSparkContext = SparkContexts.create("ADAM", configuration);
        SparkContext sparkContext = javaSparkContext.sc();
        try {
            FileSystem fileSystem = Hadoop.fileSystem(configuration.pipeline().hdfs());
            ADAMContext adamContext = new ADAMContext(sparkContext);
            BamCreationPipeline adamPipeline = Pipelines.bamCreationConsolidated(adamContext,
                    fileSystem, configuration.pipeline().resultsDirectory(),
                    configuration.referenceGenome().path(),
                    configuration.pipeline().bwa().threads(), configuration.pipeline().saveResultsAsSingleFile());
            adamPipeline.execute(PatientReader.fromHDFS(fileSystem, configuration.patient().directory(), configuration.patient().name())
                    .reference());
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
            new PipelineRuntime(configuration).start();
        } catch (IOException e) {
            LOGGER.error("Unable to read configuration. Check configuration in /conf/pipeline.yaml", e);
        }
    }
}