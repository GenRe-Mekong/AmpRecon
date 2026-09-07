#!/usr/bin/env nextflow
// Copyright (C) 2023 Genome Surveillance Unit/Genome Research Ltd.
// Copyright (C) 2025 GenRe-Mekong Core Team.

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    genre-mekong/amprecon
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Github : https://github.com/GenRe-Mekong/AmpRecon
----------------------------------------------------------------------------------------
*/

// --- import modules ---------------------------------------------------------

include { PIPELINE_INIT       } from './subworkflows/utils'
include { AMPRECON            } from './workflows/amprecon'

// Main entry-point workflow
workflow GENREMEKONG_AMPRECON {

    take:

    manifest
    input_ch // fastq/cram channel
    kraken_db_ch
    qpcr_ch

    main: 

    AMPRECON(
        params.execution_mode,
        manifest,
        input_ch,
        params.kraken,
        kraken_db_ch,
		params.chrom_key_file_path,
        params.codon_key_file_path,
        params.drl_information_file_path,
        qpcr_ch,
		params.multiqc_config,
		params.multiqc_logo,
		params.results_dir
    )

   emit:
    grc = AMPRECON.out.grc
    mqc = AMPRECON.out.mqc

}

// --- Execute Main Workflow -------------------------------------------------
workflow {

    main:

    PIPELINE_INIT (
		params.help,
        params.monochrome_logs,
        params.results_dir,
        params.manifest,
        params.qpcr
    )


    GENREMEKONG_AMPRECON (
        PIPELINE_INIT.out.manifest,
        PIPELINE_INIT.out.input_ch,
        PIPELINE_INIT.out.kraken_db_ch,
        PIPELINE_INIT.out.qpcr_ch
    )
}

// --- On Completion ---------------------------------------------------------
// TODO: fix completion message.
workflow.onComplete {
    if (workflow.exitStatus == 0) {
        log.info """
            ===========================================
            Finished in ${workflow.duration}
            Results directory ==> ${params.results_dir}
            """
            .stripIndent()
    } else {
        log.info '''
            ===========================================
            Finished with errors!
            '''
            .stripIndent()
    }
}
