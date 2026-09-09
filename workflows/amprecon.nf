// Copyright (C) 2023 Genome Surveillance Unit/Genome Research Ltd.
// Copyright (C) 2025 GenRe-Mekong Core Team.
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { CRAM_TO_READS       } from '../subworkflows/cram_to_reads'
include { FASTQ_PREPROCESS    } from '../subworkflows/fastq_preprocess'
include { ALIGNMENT           } from '../subworkflows/alignment'
include { GENOTYPING          } from '../subworkflows/genotyping'
include { KRAKEN2             } from '../subworkflows/kraken2'
include { VARIANTS_TO_GRCS    } from '../subworkflows/variants_to_grcs'
include { PIPELINE_COMPLETION } from '../subworkflows/utils'
include { resolvePath         } from '../subworkflows/utils'

include { FASTQC              } from '../modules/fastqc/main'
include { MULTIQC             } from '../modules/multiqc/main'
include { write_vcfs_manifest } from '../modules/write_vcfs_manifest.nf'

/* include { paramsSummaryMap       } from 'plugin/nf-schema' */
/* include { paramsSummaryMultiqc   } from '../subworkflows/nf-core/utils_nfcore_pipeline' */
/* include { softwareVersionsToYAML } from '../subworkflows/nf-core/utils_nfcore_pipeline' */
/* include { methodsDescriptionText } from '../subworkflows/local/utils_nfcore_test_pipeline' */

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow AMPRECON {

    take:
    mode
    manifest
    input_ch // cram/fastq channel 
    kraken
    kraken_db_ch
    chrom_key
    codon_key
    drl_info
    qpcr_ch
    multiqc_config
    multiqc_logo
	outdir

    main:

    def ch_versions = channel.empty()
    def ch_multiqc_files = channel.empty()

    if (mode == "cram") {

        CRAM_TO_READS(input_ch)

        fastq_ch = CRAM_TO_READS.out.fastq
        ch_versions = ch_versions.mix(CRAM_TO_READS.out.versions)

    } else if (mode == "fastq"){

        FASTQ_PREPROCESS(input_ch)

        fastq_ch = FASTQ_PREPROCESS.out.fastq
        ch_versions = ch_versions.mix(FASTQ_PREPROCESS.out.versions)

    }

    //
    // FastQC
    //

    FASTQC(fastq_ch)

    ch_versions = ch_versions.mix(FASTQC.out.versions.first())
    ch_multiqc_files = ch_multiqc_files.mix(FASTQC.out.zip.collect{it[1]})

    //
    // KRAKEN2 (OPTIONAL)
    //
    
    if (kraken) {
        KRAKEN2(
            fastq_ch,
            kraken_db_ch
        )

        k2_species = KRAKEN2.out.k2_species
    } else {

        k2_species = Channel.empty()

    }

    //
    // ALIGNMENT
    //

    ALIGNMENT(
        fastq_ch
    )

    ch_versions = ch_versions.mix(ALIGNMENT.out.versions)
    ch_multiqc_files = ch_multiqc_files.mix(ALIGNMENT.out.mqc)

    //
    // GENOTYPING
    //

    GENOTYPING(
        ALIGNMENT.out.bam
    )

    ch_versions = ch_versions.mix(GENOTYPING.out.versions)
    ch_multiqc_files = ch_multiqc_files.mix(GENOTYPING.out.mqc)

    //
    // GRC CREATION
    //
    GENOTYPING.out.vcf
        | map { it ->
            def (meta, vcf, tbi) = it[0..2]
            tuple( meta.id ,vcf ) }
        | multiMap { it ->
            id:  it[0]
            vcf: it[1]
        }
        | set { vcf_ch }

    write_vcfs_manifest(vcf_ch.id.collect(), vcf_ch.vcf.collect())
    lanelet_manifest_file = write_vcfs_manifest.out

    VARIANTS_TO_GRCS(
        manifest,
        lanelet_manifest_file,
        chrom_key,
        codon_key,
        drl_info,
        qpcr_ch,
        k2_species
    )

    ch_versions = ch_versions.mix(VARIANTS_TO_GRCS.out.versions)

    MULTIQC(
        ch_multiqc_files.collect(),
        params.multiqc_config,
        [],
        params.multiqc_logo,
        [],
        [],
        ch_versions.collect()
    )

    emit:
    grc            = VARIANTS_TO_GRCS.out.grc
    mqc            = MULTIQC.out.report
    versions       = ch_versions                 
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    THE END
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
