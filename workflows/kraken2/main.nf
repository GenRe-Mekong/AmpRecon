#!/usr/bin/env nextflow
// Copyright (C) 2026 GenRe-Mekong Core Team.

// import modules
include { KRAKEN2_RUN                   } from '../../modules/kraken2/main.nf'
include { KRAKEN2_POSTPROCESS           } from '../../modules/kraken2_postprocess/main.nf'

workflow KRAKEN2 {
    take:
        fastq_ch
        kraken_db

    main:

        ch_versions = Channel.empty()

        // filter only for SPEC panel
        fastq_spec_ch = fastq_ch.filter { meta, reads ->
            meta.panel == "PFA_Spec"
        }
        KRAKEN2_RUN(
            fastq_spec_ch,
            kraken_db
        )
        ch_versions = ch_versions.mix(KRAKEN2_RUN.out.versions)

        KRAKEN2_POSTPROCESS(
            KRAKEN2_RUN.out.report.map{ meat, report -> report }.collect()
        )

    emit:
        k2_species = KRAKEN2_POSTPROCESS.out.species
        versions   = ch_versions
}
