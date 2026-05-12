#!/usr/bin/env nextflow
// Copyright (C) 2023 Genome Surveillance Unit/Genome Research Ltd.

/*
    | READS_TO_VARIANTS |---------------------------------------------
    
    This workflow takes FASTQ files from iRODS and aligns the 
    reads to a reference genome and performing a series of commands to
    produce a coordinate sorted, indexed BAM file.

    Per amplicon panel region read counts are determined from these BAM 
    files. These per Amplicon panel regions are specificied through
    a supplied annotation file.
    
    Variant call files are also produced from these alignment files.
    A genotyping workflows is run to produce variant call files, 
    which are restricted to specific regions using a supplied SNP list.

    A channel containing FASTQ file and associated reference genome is
    is supplied to the workflow. This is in addition to channels that
    link SNP list, annotation file and sample ID with file ID. A 
    manifest linking sample IDs with associated VCF lanelet files is
    output by this workflow.
    ------------------------------------------------------------------
*/

// enable dsl2
nextflow.enable.dsl = 2

// import workflows
include { BWA_MEM             } from '../../modules/bwa_mem/main'
include { SAMTOOLS_SORT_INDEX } from '../../modules/samtools_sort_index/main'
include { SAMTOOLS_FLAGSTATS  } from '../../modules/samtools_flagstats/main'
include { SAMTOOLS_COVERAGE   } from '../../modules/samtools_coverage/main'

include { READ_COUNTS } from '../reads_counts/main'
/* include { bam_merge_and_index } from '../modules/bam_merge_and_index.nf' */
/*
This workflow takes fastq files and outputs vcfs
*/

workflow ALIGNMENT {
    take:
        fastq_ch // tuple ( meta, fastq )
        /* annotations_ch // tuple (panel_name, annotation_file) */

    main:

        ch_versions = Channel.empty()
		ch_multiqc_files = Channel.empty()


        //
        // MODULE: BWA
		// alignment
        //
        
        // Staging reference files
        def fastq_ch = fastq_ch.map {
            def (meta, fastq) = it[0..1]
            def fasta = file("${meta.reference.fasta}*")
            return tuple(meta, fastq, fasta)
        }

        BWA_MEM(fastq_ch)  

        ch_versions = ch_versions.mix(BWA_MEM.out.versions.first())

        //
        // MODULE: SAMTOOLS
		// sort and index
        //
        SAMTOOLS_SORT_INDEX(
            BWA_MEM.out.sam
        ) 
        ch_versions = ch_versions.mix(SAMTOOLS_SORT_INDEX.out.versions.first())
		
 
		// 
		// Get mapping statistic 
		SAMTOOLS_FLAGSTATS(
			SAMTOOLS_SORT_INDEX.out.bam
		)
        ch_versions = ch_versions.mix(SAMTOOLS_FLAGSTATS.out.versions.first())
		ch_multiqc_files = ch_multiqc_files.mix(SAMTOOLS_FLAGSTATS.out.flagstats.collect{it[1]})

		SAMTOOLS_COVERAGE(
			SAMTOOLS_SORT_INDEX.out.bam
		)
        ch_versions = ch_versions.mix(SAMTOOLS_COVERAGE.out.versions.first())
		ch_multiqc_files = ch_multiqc_files.mix(SAMTOOLS_COVERAGE.out.coverage.collect{it[1]})
			
        // read counts
        // TODO: try exclude annotation_ch 
        READ_COUNTS(SAMTOOLS_SORT_INDEX.out.bam)
        /* READ_COUNTS(ALIGNMENT.out, alignment_ref_ch, annotations_ch) */

        // -- | SET SAMPLE_TAG as channels keys | --
        // Here we set the sample_tag ("{sample_id}_{panel_name}") as the key for the channels (instead of the file_id)
        // Which means all lanelets for a single sample were merged into a single entity and should be treated as such 
        // for downstream processes.

    emit:
        bam      = SAMTOOLS_SORT_INDEX.out.bam
		mqc		 = ch_multiqc_files
        versions = ch_versions
}

