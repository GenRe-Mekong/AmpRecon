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
include { BWA_MEM             as ALIGNMENT      } from '../../bwa'
include { SAMTOOLS_SORT_INDEX as BAM_SORT_INDEX } from '../../bwa'

include { READ_COUNTS } from './read_counts.nf'
include { GENOTYPING_BCFTOOLS } from './genotyping_bcftools.nf'
include { write_vcfs_manifest } from '../modules/write_vcfs_manifest.nf'
include { bam_merge_and_index } from '../modules/bam_merge_and_index.nf'
/*
This workflow takes fastq files and outputs vcfs
*/

workflow READS_TO_VARIANTS {
    take:
        fastq_ch // tuple ( meta, fastq )
        /* annotations_ch // tuple (panel_name, annotation_file) */

    main:

        ch_versions = Channel.empty()

        //
        // MODULE: BWA
        //
        BWA(fastq_ch)  
        ch_versions = ch_versions.mix(ALIGNMENT.out.versions.first())

        //
        // MODULE: SAMTOOLS
        //
        BAM_SORT_INDEX(
            ALIGNMENT.out.sam
        )
        ch_versions = ch_versions.mix(BAM_SORT_INDEX.out.versions.first())
 
        // read counts
        // TODO: try exclude annotation_ch 
        /* READ_COUNTS(ALIGNMENT.out, alignment_ref_ch, annotations_ch) */

        // TODO: Kept for backward compatibility
        // TODO: In case of more than one sequence in the same samples
        // Merge sample ids with duplicated lanelets
        /* ALIGNMENT.out // tuple (file_id, bam_file, bai_file)) */
        /*     | join( file_id_to_sample_id_ch ) // tuple (file_id, bam_file, bai_file, sample_id) */
        /*     | join ( file_id_reference_files_ch ) // tuple (file_id, bam_file, bai_file, sample_id, panel_name, reference_fasta_file, snp_list) */
        /*     | map { it -> tuple("${it[3]}_${it[4]}", it[1]) } // tuple("{sample_id}_{panel_name}", bam_file) */
        /*     | groupTuple() */
        /*     | set { bam_merge_ch } // tuple("sample_id_panel_name", [bam_file_1, bam_file_N]) N number of multiples */ 

        /* bam_merge_and_index( bam_merge_ch ) */

        // -- | SET SAMPLE_TAG as channels keys | --
        // Here we set the sample_tag ("{sample_id}_{panel_name}") as the key for the channels (instead of the file_id)
        // Which means all lanelets for a single sample were merged into a single entity and should be treated as such 
        // for downstream processes.
        /* file_id_reference_files_ch */
        /*     | join( file_id_to_sample_id_ch )  // tuple (file_id, panel_name, reference_fasta_file, snp_list, sample_id) */
        /*     | map { it -> tuple("${it[4]}_${it[1]}", it[0], it[1], it[2], it[3], it[4] ) } */ 
        /*     | set { sample_key_ref_ch } // tuple ("{sample_id}_{panel_name}", file_id, panel_name, reference_fasta_file, snp_list, sample_id) */

        // Genotyping
        /* sample_key_ref_ch */
        /*     | map{it -> tuple(it[0],it[1], it[3], it[4])} */
        /*     | set{genotyping_ref_ch} // tuple (sample_tag, fasta_file, snp_list, sample_key) */

        GENOTYPING_BCFTOOLS(
            bam_merge_and_index.out,
            genotyping_ref_ch
        )

        GENOTYPING_BCFTOOLS.out.set{vcf_files_ch}

        // Create channel of 2 lists: IDs and VCFs
        vcf_files_ch // tuple (sample_tag, vcf_path, vcf_index_path)
            | map{it -> tuple(it[0], it[1])} // tuple (sample_tag, vcf_path)
            | join(file_id_to_sample_id_ch) // tuple (sample_tag, vcf_path, sample_id)
            | multiMap { it ->
                id_list: it[2]
                vcf_list: it[1]
            }
            | set{lanelet_ch}
        
        // Write manifest of VCF files per sample IDs
        write_vcfs_manifest(lanelet_ch.id_list.collect(), lanelet_ch.vcf_list.collect())
        lanelet_manifest = write_vcfs_manifest.out // (manifest_file)

    emit:
        lanelet_manifest // (manifest_file)
}

workflow {
    // File required for Reads to Variants input channels
    channel_data = Channel.fromPath(params.channel_data_file, checkIfExists: true)
        .splitCsv(header: true, sep: '\t')

    // Reads to Variants input channels
    fastq_ch = channel_data.map { row -> tuple(row.file_id, row.fastq_file, row.reference_file) }
    file_id_reference_files_ch = channel_data.map { row -> tuple(row.file_id, row.panel_name, row.reference_file, row.snp_list) }
    annotations_ch = channel_data.map { row -> tuple(row.panel_name, file(row.annotation_file)) }.unique()
    file_id_to_sample_id_ch = channel_data.map { row -> tuple(row.file_id, row.sample_id) }

    // Run Reads to Variants workflow
    READS_TO_VARIANTS(fastq_ch, file_id_reference_files_ch, annotations_ch, file_id_to_sample_id_ch)
}
