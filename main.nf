#!/usr/bin/env nextflow
// Copyright (C) 2023 Genome Surveillance Unit/Genome Research Ltd.


// --- import modules ---------------------------------------------------------

include { PIPELINE_INIT       } from './workflows/utils'
include { CRAM_TO_READS       } from './workflows/cram_to_reads'
include { FASTQ_PREPROCESS    } from './workflows/fastq_preprocess'
include { FASTQC              } from './modules/fastqc/main'
include { ALIGNMENT           } from './workflows/alignment'
include { GENOTYPING          } from './workflows/genotyping'
include { VARIANTS_TO_GRCS    } from './workflows/variants_to_grcs'
include { MULTIQC             } from './modules/multiqc/main'
include { PIPELINE_COMPLETION } from './workflows/utils'
include { resolvePath         } from './workflows/utils'
include { write_vcfs_manifest } from './modules/write_vcfs_manifest.nf'

// Main entry-point workflow
workflow AMPRECON {

// -- MAIN-EXECUTION ------------------------------------------------------
    ch_versions = Channel.empty()
    ch_multiqc_files = Channel.empty()
    def manifest = resolvePath(params.manifest)
    PIPELINE_INIT (
        params.help, 
        params.monochrome_logs, 
        params.results_dir, 
        manifest,
        params.qpcr
    ) 


    if (params.execution_mode == "cram") {
        CRAM_TO_READS(
            PIPELINE_INIT.out.input_ch
        )
        fastq_ch = CRAM_TO_READS.out.fastq
        ch_versions = ch_versions.mix(CRAM_TO_READS.out.versions)
    } else {
        FASTQ_PREPROCESS(PIPELINE_INIT.out.input_ch)
        fastq_ch = FASTQ_PREPROCESS.out.fastq
        ch_versions = ch_versions.mix(FASTQ_PREPROCESS.out.versions)
    }

    
    //
    // QUALITY CHECK
    //
    FASTQC(fastq_ch) 
    ch_versions = ch_versions.mix(FASTQC.out.versions.first())
	ch_multiqc_files = ch_multiqc_files.mix(FASTQC.out.zip.collect{it[1]})

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
        params.chrom_key_file_path,
        params.kelch_reference_file_path,
        params.codon_key_file_path,
        params.drl_information_file_path,
        PIPELINE_INIT.out.qpcr_ch
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
    // TODO:
    PIPELINE_COMPLETION()

    emit: 
    grc = VARIANTS_TO_GRCS.out.grc

}


// --- Execute Main Workflow -------------------------------------------------
workflow {
    AMPRECON()
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
		log.info """
			===========================================
			Finished with errors!
			"""
			.stripIndent()
	}
}







