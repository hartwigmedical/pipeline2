# Hartwig Medical Foundation - Pipeline5

### 1.1 Introduction
Pipeline5 (Pv5) is a processing and analysis pipeline for high throughput DNA sequencing data, build for Hartwig Medical Foundation (HMF). The goals of the project are to deliver best in class performance using modern big data software, and scalability using cloud infrastructure. Pv5 succeeds Pipeline4, which uses a central GridEngine HPC to parallelize workloads. While this approach has worked for HMF for the last 10 years, it has some key drawbacks:
GridEngine itself is no longer evolving
Requires centrally managed infrastructure which is constantly under or over utilized.
The codebase is difficult to change due to brittle and untested scripts

To this end, Pv5 uses ADAM, Spark and Google Cloud Platform. Following is an overview of how they come together to form the Pv5 architecture.

### 1.2 Google Cloud Platform
We evaluated both [Google Cloud Platform](https://cloud.google.com/) (GCP) and [Amazon Web Services](https://aws.amazon.com/) (AWS) as cloud infrastructure providers. While both are great platforms and offer roughly equivalent services, we chose GCP for the following reasons (all at the time of evaluation, fall 2019):
- GCP is less expensive for our workload.
- GCP has a simpler pricing model for compute.
- GCP has a more user friendly console for monitoring and operations
- GCP’s managed Hadoop offering, Dataproc, has faster startup times

The last point was quite important to us due to a design choice made to address resource contention. Pv5 uses ephemeral clusters tailored to each patient. This way at any point we are only using exactly the resources we need for our workload, never having to queue or idle. To make this possible we spin up large clusters quickly as new patients come in and tear them down again once the patient processing is complete.

Pv5 makes use of the following GCP services:
[Google Cloud Storage](https://cloud.google.com/storage/) to store transient patient data, supporting resources, configuration and tools.
[Google Dataproc](https://cloud.google.com/dataproc/) to run ADAM/Spark workloads, currently only used in alignment (more on this later).
[Google Compute Engine](https://cloud.google.com/compute/) to run workloads using tools not yet compatible with Spark (samtools, GATK, strelka, etc)

### 1.3 ADAM, Spark and Dataproc
[ADAM](https://github.com/bigdatagenomics/adam) is a genomic analysis modeling and processing framework built on Apache Spark. Please see [the documentation](https://adam.readthedocs.io/en/latest/) for a complete description of the ADAM ecosystem and goals. At this point we only use the core APIs and datamodels. [Apache Spark](https://spark.apache.org/) is an analytics engine used for large scale data processing, also best to read their own docs for a complete description.

We use ADAM to parallelize processing of FASTQ and BAM files using hadoop. ADAM provides an avro datamodel to read and persist this data from and to HDFS, and the ability to hold the massive datasets in memory as they are processed.

Google Cloud Platform offers Dataproc as a managed spark environment. Each patient gets its own Dataproc cluster which is started when their FASTQ lands and deleted when tertiary analysis is complete. Dataproc includes a connector to use Google Storage as HDFS. With this we can have these transient compute clusters, with permanent distributed data storage.

### 1.4 Google Compute Engine
Not all the algorithms in our pipeline are currently suited to ADAM. For these tools we’ve developed a small framework to run them on VMs in Java. To accomplish this we’ve created a standard VM image containing a complete repository of external and internal tools, and OS dependencies.

Using internal APIs we launch VM jobs by generating a bash startup script which will copy inputs and resources, run the tools themselves, and copy the final results (or any errors) back up into google storage.

VMs performance profiles can be created to use Google’s standard machine type or custom cpu/ram combinations based on the workload’s requirements.

### 1.5 Pipeline Stages
The pipeline first runs primary and secondary analysis on a reference (blood/normal) sample and tumor sample before comparing them in the final somatic pipeline. Steps 1.5.1-1.5.5 are run in the single sample pipeline, where 1.5.6-1.5.11 are run in the somatic. Alignment is the only step which uses Google Dataproc, while the rest run in vanilla Google Compute Engine.

#### 1.5.1 Alignment
Under normal circumstances Pv5 starts with the input of one to n paired-end FASTQ files produced by sequencing. The first task of the pipeline is to align these reads to the human reference genome. The algorithm behind this process is BWA. This is where we most take advantage of ADAM, Spark and Dataproc. ADAM gives us an API to run external tools against small blocks of data in parallel. Using this API we run thousands of BWA processes in parallel then combine the alignment results into a single BAM file and persist it.

Worth noting there is both a pre-processing and post-processing step done here. Before alignment, we run a small Spark cluster to simply gunzip the data. Our input FASTQs come in gzipp’ed (not bgzipped) so cannot be properly parallelized by Spark in that state. After the alignment is complete, we run a sambamba sort and index, as the ADAM sort we’ve found unstable and does not perform indexing.
#### 1.5.2 WGS Metrics
Our downstream QC tools require certain metrics about the BAM. These are produced using Picard tools [CollectWgsMetrics](https://software.broadinstitute.org/gatk/documentation/tooldocs/4.0.0.0/picard_analysis_CollectWgsMetrics.php).
#### 1.5.3 Samtools Flagstat
[Samtools](http://www.htslib.org/doc/samtools.html) flag statistics are not consumed by any downstream stages, but very useful in ad hoc QC and analysis.
#### 1.5.4 SnpGenotype (GATK UnifiedGenotyper)
Also used in final QC, GATK’s [UnifiedGenotyper](https://software.broadinstitute.org/gatk/documentation/tooldocs/3.8-0/org_broadinstitute_gatk_tools_walkers_genotyper_UnifiedGenotyper.php) is used to call variants around 26 specific location. The results are used as a final sanity check of the pipeline output.
#### 1.5.5 Germline Calling (GATK HaplotypeCaller)
GATk’s [HaplotypeCaller](https://software.broadinstitute.org/gatk/documentation/tooldocs/3.8-0/org_broadinstitute_gatk_tools_walkers_haplotypecaller_HaplotypeCaller.php) is used to call germline variants on the reference sample only.
#### 1.5.6 Somatic Variant Calling (Strelka)
[Strelka2](https://github.com/Illumina/strelka) from illumina is used to call SNP and INDEL variants between the tumor/reference pair.
#### 1.5.7 Structural Variant Calling (GRIDSS)
[GRIDSS](https://github.com/PapenfussLab/gridss) is used to call structural variants between the tumor/reference pair.
#### 1.5.8 Cobalt
[Cobalt](https://github.com/hartwigmedical/hmftools/tree/master/count-bam-lines) is an HMF in-house tool used to determine read depth ratios.
#### 1.5.8 Amber
[Amber](https://github.com/hartwigmedical/hmftools/tree/master/amber) is an HMF in-house tool used to determine the B allele frequency of a tumor/reference pair.
#### 1.5.9 Purple
[Purple](https://github.com/hartwigmedical/hmftools/tree/master/purity-ploidy-estimator) is an HMF in-house tool which combines the read-depth ratios, BAF, and variants to produce the pipeline final output, used both in the final report and exposed to research.
#### 1.5.10 Health Check
Final QC of the purple results and BAM metrics.

### 1.7 Production Environment at Schuberg Phillis
### 2. Developers Guide