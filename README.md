# AmpRecon - GenRe.1

AmpRecon - GenRe.1 is a forked of the [genomic-surveillance/AmpRecon](https://github.com/genomic-surveillance/AmpRecon/tree/f73fd7c660fad9f0fc5594e0275ae7ba026ef5e8) pipeline, maintained by [Core GenRe-Mekong Team](https://github.com/GenRe-Mekong).

AmpRecon is a bioinformatics analysis pipeline for analyzing amplicon sequencing data from the malaria parasite _Plasmodium falciparum_. The pipeline currently supports amplicon from three panels including GRC1, GRC2, and Spec. It takes paired-end short-reads `fastq` from each panel as input, align them to their respective reference panels, and performs varints calling to identify genetic variations.

The primary output is a [Genetic Report Card (GRC)](https://www.malariagen.net/wp-content/uploads/2023/10/GRC_UserGuide_July2023.pdf) in CSV format, which summarizes key genetic information for each sample. This including the detection of drug resistance genes (i.e. kelch and plasmepsin), nucleotide and amino acid barcode, and an estimation of complexity of infection (COI).

By default, AmpRecon is configured to process data from _P. falciparum_. However, it can also be used to analyze _Plasmodium vivax_ data by using the provided [configuration file](#p-vivax-configuration-file).

![workflow_outline](./assets/workflow_outline.png)
<p style="text-align: center;"><i>An outline of the workflow.</i></p>

- [AmpRecon - GenRe.1](#amprecon---genre1)
  - [Quick-Start Guide](#quick-start-guide)
    - [TLDR](#tldr)
  - [Installation](#installation)
    - [Prerequisites](#prerequisites)
    - [Software Dependencies](#software-dependencies)
  - [Usage](#usage)
    - [Reference Panel](#reference-panel)
    - [Input Manifest](#input-manifest)
      - [FastQ Manifest](#fastq-manifest)
      - [CRAM Manifest](#cram-manifest)
      - [Metadata](#metadata)
  - [Running on Computing Cluster](#running-on-computing-cluster)
    - [Containers Caching](#containers-caching)
    - [Specific Cluster Configuration](#specific-cluster-configuration)
  - [Pipeline Configuration](#pipeline-configuration)
    - [Configable Parameters](#configable-parameters)
      - [I/O](#io)
      - [Panel and GRC resources](#panel-and-grc-resources)
      - [Genotyping setting](#genotyping-setting)
      - [McCOIL setting](#mccoil-setting)
  - [_P. vivax_ Configuration File](#p-vivax-configuration-file)
  - [Output](#output)
  - [Authors and Acknowledgements](#authors-and-acknowledgements)

## Quick-Start Guide

AmpRecon - GenRe.1 is built and tested with Nextflow [version 22.04](https://github.com/nextflow-io/nextflow/releases/tag/v22.04.4). It is recommended to use with either [Docker](https://docs.docker.com/engine/install/ubuntu/#installation-methods) or [Singularity](https://github.com/sylabs/singularity). Assuming you already have [Nextflow](https://github.com/nextflow-io/nextflow), and [Singularity](https://github.com/sylabs/singularity) or [Docker](https://docs.docker.com/engine/install/ubuntu/#installation-methods) installed, you can get started by following these steps:

1. Clone the repository:

   ```bash
   git clone https://github.com/thavinb/AmpRecon.git
   cd AmpRecon
   ```

2. Prepare your input samplesheet:

   Create a tab-separated file (e.g., samplesheet.tsv) with the following columns.
   `samplesheet.tsv`

   ```bash
   sample_id	primer_panel	fastq1_path	fastq2_path
   sample01	PFA_GRC1_v1.0	/path/to/reads/sample01_grc1_R1.fastq.gz	/path/to/reads/sample01_grc1_R2.fastq.gz
   sample01	PFA_GRC2_v1.0	/path/to/reads/sample01_grc2_R1.fastq.gz	/path/to/reads/sample01_grc2_R2.fastq.gz
   sample01	PFA_Spec	/path/to/reads/sample01_spec_R1.fastq.gz	/path/to/reads/sample01_spec_R2.fastq.gz
   sample02	PFA_GRC1_v1.0	/path/to/reads/sample02_grc1_R1.fastq.gz	/path/to/reads/sample02_grc1_R2.fastq.gz
   sample02	PFA_GRC2_v1.0	/path/to/reads/sample02_grc2_R1.fastq.gz	/path/to/reads/sample02_grc2_R2.fastq.gz
   sample02	PFA_Spec	/path/to/reads/sample02_spec_R1.fastq.gz	/path/to/reads/sample02_spec_R2.fastq.gz
   ```

3. Run the pipeline:

   ```bash
   # If using docker
   nextflow run main.nf \
    -profile docker     \
    --batch_id RUN00001 \ # define by user
    --manifest samplesheet.tsv 

   # If using singularity
   nextflow run main.nf  \
    -profile singularity \
    --batch_id RUN00001  \ # define by user
    --manifest samplesheet.tsv 
   ```

### TLDR

   ```bash
   nextflow run thavinb/AmpRecon \
   # -profile docker             \ # If using docker
    --batch_id RUN00001          \ # define by user
    --manifest samplesheet.tsv 
   ```


## Installation

### Prerequisites

Before you begin, ensure you have the following installed on a Linux-based operating system:

- [Nextflow](https://www.nextflow.io/docs/latest/install.html#self-install) (≥22.10.21).
- A container engine — either [Docker](https://docs.docker.com/engine/install/ubuntu/#installation-methods) or [Singularity](https://docs.sylabs.io/guides/3.5/user-guide/quick_start.html#quick-installation-steps) is highly recommended, as it simplifies management of software dependencies.

> [!NOTE]
> This versions aim to improves **portability** by relying on publicly available images from the [BioContainers](https://biocontainers.pro/) repository. The pipeline now also supports both **Docker** and **Singularity (Apptainer)** profiles for flexible execution in local and HPC environments.

1. Install [Nextflow](https://www.nextflow.io/docs/latest/install.html#self-install):

   Install the latest version of Nextflow by running:

   ```bash
   curl -s https://get.nextflow.io | bash
   # This will install the latest version
   # Move the nextflow executable to a directory in your $PATH
   # e.g., sudo mv nextflow /usr/local/bin/
   ```

2. Install a Container Engine:

   - [Docker](https://docs.docker.com/engine/install/ubuntu/#installation-methods) is a popular choice for local machines and cloud environments. To run using the `Docker`, use the `-profile docker` flag.

   ```bash
   nextflow run main.nf \
     -profile docker \
     --batch_id RUN00001 \
     --manifest samplesheet.tsv
   ```

   - [Singularity](https://docs.sylabs.io/guides/3.5/user-guide/quick_start.html#quick-installation-steps) is used in most HPC environments. To run using the `Singularity`, use the `-profile singularity` flag. See also: [Running on Computing Cluster](#running-on-computing-cluster)).

   ```bash
   nextflow run main.nf \
     -profile singularity \
     --batch_id RUN00001 \
     --manifest samplesheet.tsv
   ```

3. Download the Pipeline:

   Clone the repository to get the pipeline code on local system:

   ```bash
   git clone https://github.com/thavinb/AmpRecon.git
   ```

>[!IMPORTANT]
>If no container engine is available (or deliberately not to use any), you can run the pipeline using `-profile run_locally`. Please note that every programs of an appropiate version needed to be callable on your local computer using the same command in what define in containers (e.g. `python` as `python` not `python3`, `samtools coverage`). The versions tested for compatibility are listed below.

### Software Dependencies

AmpRecon pipeline has been extensively tested and is designed to run within a containerized environment (e.g., `Docker` or `Singularity`). The container environment provides the portability of the exact same software environment, regardless of the underlying hardware it runs on, which, in turn, should ensures the reproducibility of the pipeline.

The following table lists the key software versions included in the tested container image:

| Software          | Version |
| ----------------- | ------- |
| Staden IO Library | 1.15.1  |
| BioBAMBAM         | 2.0.79  |
| Pandas            | 2.2.1   |
| FastQC            | 0.12.1  |
| MultiQC           | 1.27    |
| BWA               | 0.7.17  |
| Samtools          | 1.8     |
| BCFtools          | 1.8     |
| PyVCF             | 0.6.8   |
| PySam             | 0.23.3  |
| R Base            | 4.3.1   |

⚠️If you must run the pipeline using **locally installed softwares** instead of the container, please ensure that your local versions exactly match those listed in the table above.

## Usage

### Reference Panel

Before creating the input manifest, it's worth understanding the reference panels used by the pipeline. The pipeline is designed to analyze amplicon sequences from three specific panels. Therefore, each sample should include three corresponding fastq files, one for each panel.

By default, the pipeline points to a valid `panels_settings.csv` file containing key information for each panel, including:

- The path to the **indexed reference panel fasta file**.
- The path to the **design file** (e.g., regions.txt) that specifies the targeted regions to generate per-panel statistics.
- The path to the **target SNP list** (e.g., annotation.vcf) for variant calling.

If you need to overiding the reference panel, you can provide a custom  `panels_settings.csv` file using the `--panels_settings` parameter. The file must follow this strict format:

| panel_name | reference_file | design_file | snp_list |
| --- | --- | --- | --- |
| PFA_GRC1_v1.0 | /path/to/PFA_GRC1_v1.0.fasta | /path/to/PFA_GRC1_v1.0.regions.txt | /path/to/PFA_GRC1_v1.0.annotation.vcf |
| PFA_GRC2_v1.0 | /path/to/PFA_GRC2_v1.0.fasta | /path/to/PFA_GRC2_v1.0.regions.txt | /path/to/PFA_GRC2_v1.0.annotation.vcf |
| PFA_Spec | /path/to/PFA_Spec.fasta | /path/to/PFA_Spec.regions.txt | /path/to/PFA_Spec.annotation.vcf |

>[!IMPORTANT]
The `panel_name` column values must **exactly match** the corresponding `primer_panel` entries in the [input manifest](#input-manifest) file to ensures proper alignment between panel configurations and sample data. The default `panel_settings.csv` file defines three standard panel names:
>
> - `PFA_GRC1_v1.0` for `GRC1`
> - `PFA_GRC2_v1.0` for `GRC2`
> - `PFA_Spec` for `SPEC`

### Input Manifest

AmpRecon accepts input through a manifest file, which is a tab-separated text file that specifying the paths to the fastq files for each sample and panel. By default, AmpRecon expects fastq files. However, you can also provide CRAM or BAM files using `--execution_mode cram` on the command line.

The required structure of the manifest file depends on the selected execution mode. Each mode has a distinct format that must be followed exactly.

#### FastQ Manifest

By default, AmpRecon expects fastq files as input. The input manifest must be in tab-seperated format, follows a strict header:

| sample_id | primer_panel | fastq1_path | fastq2_path |
|-----------|--------------|-------------|-------------|
|sample01   |PFA_GRC1_v1.0 |/path/to/reads/sample01_grc1_R1.fastq.gz| /path/to/reads/sample01_grc1_R2.fastq.gz |
|sample01   |PFA_GRC2_v1.0 |/path/to/reads/sample01_grc2_R1.fastq.gz| /path/to/reads/sample01_grc2_R2.fastq.gz |
|sample01   |PFA_Spec      |/path/to/reads/sample01_spec_R1.fastq.gz| /path/to/reads/sample01_spec_R2.fastq.gz |
|sample02   |PFA_GRC1_v1.0 |/path/to/reads/sample02_grc1_R1.fastq.gz| /path/to/reads/sample02_grc1_R2.fastq.gz |
|sample02   |PFA_GRC2_v1.0 |/path/to/reads/sample02_grc2_R1.fastq.gz| /path/to/reads/sample02_grc2_R2.fastq.gz |
|sample02   |PFA_Spec      |/path/to/reads/sample02_spec_R1.fastq.gz| /path/to/reads/sample02_spec_R2.fastq.gz |

```bash
nextflow run main.nf \
  -profile docker \
  --batch_id RUN00001 \
  --manifest /path/to/samplesheet.tsv
```

#### CRAM Manifest

Alternatively, you can use cram or bam file as input by adding `--execution_mode cram` to the command. This requires a `tab-seperated` manifest file with a differnt header:

| sample_id | primer_panel | cram_path |
|----------|---------------|----------------------------|
| sample01 | PFA_GRC1_v1.0 | /seq/sample01/sample01_1#55.cram |
| sample01 | PFA_GRC2_v1.0 | /seq/sample01/sample01_1#149.cram |
| sample01 | PFA_Spec      | /seq/sample01/sample01_1#808.cram |

```bash
nextflow run main.nf \
  -profile docker \
  --execution_mode cram \ 
  --batch_id RUN00001 \
  --manifest /path/to/samplesheet.tsv 
```  

>[!NOTE]
>**_Relative paths_** (i.e., `path/to/file.fastq`) are interpreted from the directory where you execute the pipeline. To ensure the pipeline can always find your files, regardless of where you run it from, please use the **_absolute path_** (i.e., `/home/user/path/to/file.fastq`).

#### Metadata

To include additional metadata in the pipeline results (e.g., Collection Site, Collection Date, etc.), you can supplies the metadata file with any extra column via the `--metadata` with the headers of your choice. Ensure that the custom headers do not conflict with the required headers or [reserved result headers](https://www.malariagen.net/wp-content/uploads/2023/10/GRC_UserGuide_July2023.pdf).

The file is to be in the tab-delimited format with same sample identifier in the column `sample_id` as the input manifest file.

**Example:**

| sample_id | collection_site | collection_date |
|-----------|-----------------|-----------------|
|sample01   |    Site_A       | 2025-01-15      |
|sample02   |    Site_A       | 2025-01-15      |
|sample03   |    Site_B       | 2025-03-11      |

[**(&uarr;)**](#amprecon---genre1)

## Running on Computing Cluster

HPC/compute cluster may have different execution environment. To ensure successful job submission and piepline execution, two main things to addressed:

### Containers Caching

When running using the `-profile singularity`, Nextflow will automatically download the required container image at **runtime** and cache it in the `containers` directory. However, this can cause issues on a cluster in the first run, due to often lack of internet access on compute node.

The pipeline provide the `containers/pull_containers.sh` bash script to download all the images beforehand to the `containers` directory to prevent any downloads at runtime.

```bash
# After cloning the code
cd AmpRecon
cd containers
./pull_containers.sh
```

### Specific Cluster Configuration

To run of computing cluster, `Nextflow` must be explicitly told which executor to use for submitting tasks (e.g., `slurm`, `sge`). Otherwise, all the jobs will be run locally where the pipeline is executed. This can be achieved by setting an executor in the profiles configuration. It is recommend to add an additional `profile` in `conf/profiles.config` to avoid altering any standard profile behavior. For more available options on executer and process, see the [Nextflow documentation](https://www.nextflow.io/docs/latest/config.html#config-profiles).

The following is an example profile for running on the BMRC Cluster:

`conf/profiles.conf`

```groovy
bmrc {
    singularity {
        enabled = true
        cacheDir = "${projectDir}/containers"
    }
    process {
        queue = "short" // partition name on the cluster
        errorStrategy = {task.attempt <= 3 ? 'retry' : 'terminate'}
        maxRetries = 2
    }
    executor {
        // https://www.nextflow.io/docs/latest/reference/config.html#executor
        name = "slurm" // executor 
        queueSize = 50
        pollInterval = "5 sec"
        submitRateLimit = '60sec'
    }
}
```

After adding the profile you can run using your custom profile name through `-profile`:

```bash
nextflow run main.nf \
  -profile bmrc \
  --batch_id RUN00001 \
  --manifest /path/to/samplesheet.tsv
```  

[**(&uarr;)**](#amprecon---genre1)  

## Pipeline Configuration

AmpRecon allows for workflow customization through various parameters. These parameters can be set on the command line in addition to the required parameters (e.g., `--batch_id` and `--manifest`). The following is a list of available configurable parameters.

### Configable Parameters

#### I/O

- `--batch_id`: A required user-defined identifier for the run. (Default: `null`).

- `--manifest`: Path to the manifest file. Note that the required header is different for each execution mode. See [Input Manifest](#cram-manifest). (Default: `null`).

- `--execution_mode`:  Set the entry point for the pipeline (`fastq` or `cram`). Default: `fastq`.

- `--containers_dir`: The directory path for caching `Singularity` container images. (Default: `$projectDir/containers`).

- `--results_dir`: The output directory where all results will be saved. (Default:`$launchDir/${batch_id}-${timestamp}/`).

#### Panel and GRC resources

- `--panel_setting`: Path to a CSV file that sets the reference (`fasta`), target sites for variant calling (`vcf`), and regions (`csv`) for each panel. See [Reference Panel](#reference-panel). (Default: `${projectDir}/ampreconresources/plasmodium/falciparum/general/panel_settings_pseudoreference.csv`).

- `--grc_settings_file_path`: Path to the GRC settings file. (Default: `$projectDir/ampreconresources/plasmodium/falciparum/grc_resources/grc_settings.json`).

- `--chrom_key_file_path`: Path to the chrom key file. (Default: `$projectDir/ampreconresources/plasmodium/falciparum/grc_resources/grc_settings.json`).

- `--kelch_reference_file_path`: Path to the kelch13 reference sequence file. (Default: `$projectDir/ampreconresources/plasmodium/falciparum/grc_resources/kelchReference.txt`).

- `--codon_key_file_path`: Path to the codon key file. (Default: `$projectDir/ampreconresources/plasmodium/shared/grc_resources/codonKey.txt`).

- `--drl_information_file_path`: Path to the drug resistance loci information file. (Default: `$projectDir/ampreconresources/plasmodium/falciparum/grc_resources/DRLinfo.txt`).

- `--no_kelch`: Boolean parameter to skip step of grc-_kelch13_ . (Dafault: `False`).

- `--no_plasmepsin`: Boolean parameter to skip the step of grc-_plasmepsin_. (Default: `False`).

- `--no_coi`: Boolean parameter to skip the grc-_coi_ step. (Default: `False`)

#### Genotyping setting

- `--mpileup_min_bq`: The minimum baseQ of bases to be used during `samtools mpileup` steps. (Default: `20`).

#### [McCOIL](https://github.com/AMarinhoSN/THEREALMcCOIL/blob/master/runMcCOIL.R) setting

- `--mccoil_ntotal`: The total number of MCMC iterations. (Default: `1000` )
- `--mccoil_nburnin`: The total number of burnin iterations. (Default: `100`)
- `--mccoil_seed`: Seed for random number generator. (Default: `123456`)
- `--mccoil_maxCOI`: Upper bound for COI. (Default: `25`)
- `--mccoil_maxMissTol`: Maximum tolerance for missing data for individuals/sample and site(Default: `20`)
- `--mccoil_e1`: The probability of calling homozygous loci heterozygous. (Default: `0.05`)
- `--mccoil_e2`: The probability of calling heterozygous loci homozygous. (Default: `0.05`)
- `--mccoil_m0`: Initial COI. (Default: `5`)

[**(&uarr;)**](#amprecon---genre1)

## _P. vivax_ Configuration File  

AmpRecon includes a predefined configuration file for generating the GRC for _P. vivax_. Users can apply this configuration by adding the `-c conf/pvivax.config` to the command line.

This will override the default parameters (i.e., reference panel and GRC settings) with the appropriate ones for the _P. vivax_ workflow.

```bash
nextflow run main.nf \
  -profile docker \
  -c conf/pvivax.config \
  --batch_id RUN00001 \
  --manifest pv_samplesheet.tsv
```

[**(&uarr;)**](#amprecon---genre1)  

## Output

After each run, AmpRecon produces the following directory structure:

```bash
RUN00001_20250101_000101/
├── bams
│   ├── sample01_PFA_GRC1.bam
│   ├── sample01_PFA_GRC1.bam.bai
│   ├── sample01_PFA_GRC2.bam
│   ├── sample01_PFA_GRC2.bam.bai
│   ├── sample01_PFA_Spec.bam
│   └── sample01_PFA_Spec.bam.bai
├── grc
│   └── RUN00001_GRC.txt
├── multiqc
│   ├── RUN000001_multiqc_report.html
│   └── RUN000001_multiqc_report_data/
├── pipeline_info
│   ├── execution_report.html
│   ├── execution_timeline.html
│   └── pipeline_dag.html
├── read_counts
│   ├── PFA_GRC1_v1.0_reads_per_region.csv
│   ├── PFA_GRC2_v1.0_reads_per_region.csv
│   └── PFA_Spec_reads_per_region.csv
└── vcfs
    ├── sample01_PFA_GRC1.vcf.gz
    ├── sample01_PFA_GRC1.vcf.gz.tbi
    ├── sample01_PFA_GRC2.vcf.gz
    ├── sample01_PFA_GRC2.vcf.gz.tbi
    ├── sample01_PFA_Spec.vcf.gz
    └── sample01_PFA_Spec.vcf.gz.tbi
```

- `bams/`
Contains sorted and indexed BAM files used in the genotyping process.
Each file corresponds to a sample–panel combination.

- `vcfs/`
Contains filtered and indexed VCF files produced after genotyping.
Each VCF represents the variant calls for a specific sample–panel pair.

- `grc/`
Contains the GRC summary file (e.g., RUN00001_GRC.txt), reporting aggregated per-panel results such as read counts and coverage statistics.

- `read_counts/`
Contains per-panel read counts by region. Each CSV file (`*_reads_per_region.csv`) provides statistics such as total reads and mapping quality for each panel region.

- `multiqc/`
Contains the consolidated MultiQC report (multiqc_report.html) summarizing quality metrics of sequence, mapping, and variant calling across all samples and panels.

- `pipeline_info/`
Includes Nextflow’s execution metadata such as reports, timelines, and the pipeline DAG, useful for reproducibility and debugging.

[**(&uarr;)**](#amprecon---genre1)  

## Authors and Acknowledgements

This pipeline was originally developed and maintained by the [Data Analysis and Engineering Team](https://www.sanger.ac.uk/group/data-analysis-and-engineering/) at the Wellcome Sanger Institute’s [Genomic Surveillance Unit](https://www.sanger.ac.uk/collaboration/genomic-surveillance-unit/).  The methodology implemented by early versions of the pipeline is described in [Jacob et al. (2021)](https://doi.org/10.7554/eLife.62997).

Following the closure of GSU, the project has been transferred to and is now maintained and further develop by the [Core GenRe-Mekong Team](https://github.com/GenRe-Mekong).

[**(&uarr;)**](#amprecon---genre1)  
