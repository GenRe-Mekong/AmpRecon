#!/usr/bin/env nextflow
// Copyright (C) 2023 Genome Surveillance Unit/Genome Research Ltd.


/*
    | VARIANTS_TO_GRCS |-----------------------------------------
    
    This workflow takes a manifest file and and manifest for 
    samples and lanelet VCFs as input. The variant calls in these 
    VCFs are used to determine several key metrics, from which 
    metadata enriched GRCs and barcodes files are assembled. Several
    files are needed for these processes: 
    [1] Chrom key file that specifies amplicon regions, their genomic 
    coordinates and reference alleles is for genotype file creation. 
    [2] Codon key file for describing the genetic code by linking 
    codons with associated amino acid.
    [3] Kelch reference file which details the codons in the Kelch13
    region - genomic location of each base, base at each position 
    and amino acid.
    [4] DRL information file that describes the amino acid position,
    mutation name, and genomic position for each base in the locus 
    for key drug resistance loci.
    
    A GRC settings file must also be supplied to the pipeline. This 
    file details different key settings for GRC creation. These include
    minimum coverage values for Kelch13 mutation calling and species
    calling, Kelch13 regions, Plasmepsin loci genotypes and variants, 
    and amino acid calling / haplotype calling double heterozygous case
    haplotype. It also contains values for a speciation default species 
    and species order, species reference describing the allele for each 
    species at particular loci and barcoding reference information:
    chromosome, locus, reference allele.
    
    The lanelet VCFs specified in the lanelet manifest are used to
    create a genotype file. This genotype file is used throughout
    the workflow, for Kelch13 mutation calling, Plasmepsin copy
    number variation calling, drug resistance haplotype assembly,
    barcode assembly, species calling and complexity of infection 
    estimation. The output from these processes are assembled into 
    2 GRC files, which then have metadata from the manifest added 
    to them.

    One GRC files and a barcodes file are the outputs.
    ------------------------------------------------------------------
*/

// enable dsl2
nextflow.enable.dsl = 2

// import modules
include { assemble_genotype_file } from '../modules/grc_assemble_genotype_file.nf'
include { grc_kelch13_mutation_caller } from '../modules/grc_kelch13_mutation_caller.nf'
include { grc_plasmepsin_cnv_caller } from '../modules/grc_plasmepsin_cnv_caller.nf'
include { grc_speciate } from '../modules/grc_speciate.nf'
include { grc_barcoding } from '../modules/grc_barcoding.nf'
include { grc_estimate_coi } from '../modules/grc_estimate_coi.nf'
include { grc_amino_acid_caller } from '../modules/grc_amino_acid_caller.nf'
include { grc_assemble } from '../modules/grc_assemble.nf'
include { grc_add_metadata } from '../modules/grc_add_metadata.nf'
include { upload_pipeline_output_to_s3 } from '../modules/upload_pipeline_output_to_s3.nf'
include { grc_curator } from '../modules/grc_curator.nf'

workflow VARIANTS_TO_GRCS {
    take:
        manifest_file
        lanelet_manifest_file
        chrom_key_file
        kelch_reference_file
        codon_key_file
        drl_information_file

    main:
        // Write genotype file
        assemble_genotype_file(lanelet_manifest_file, chrom_key_file)
        genotype_files_ch = assemble_genotype_file.out
        
        // Call mutations at Kelch13 loci
        if (params.no_kelch == false){
            grc_kelch13_mutation_caller(genotype_files_ch, kelch_reference_file, codon_key_file)
            kelch_grc_ch = grc_kelch13_mutation_caller.out
        } else {
            kelch_grc_ch = Channel.empty()
        }

        // Call copy number variation at Plasmepsin breakpoint
        if (params.no_plasmepsin == false){
            grc_plasmepsin_cnv_caller(genotype_files_ch)
            plasmepsin_grc_ch = grc_plasmepsin_cnv_caller.out
        } else {
            plasmepsin_grc_ch = Channel.empty()
        }

        // Create barcodes
        grc_barcoding(genotype_files_ch)

        // Determine species
        grc_speciate(genotype_files_ch, grc_barcoding.out.barcoding_file)
        if (params.DEBUG_no_coi == false){
            // Complexity of infection estimation
            grc_estimate_coi(grc_barcoding.out.barcoding_file)
            coi_grc_ch = grc_estimate_coi.out
        }

        if (params.DEBUG_no_coi == true){
            coi_grc_ch = Channel.empty()
        }
        // Assemble drug resistance haplotypes and amino acid calls
        grc_amino_acid_caller(genotype_files_ch, drl_information_file, codon_key_file)

        // Assemble genetic report card file
        grc_speciate.out
            .concat(kelch_grc_ch)
            .concat(plasmepsin_grc_ch)
            .concat(grc_barcoding.out.barcoding_file)
            .concat(coi_grc_ch)
            .concat(grc_amino_acid_caller.out.drl_haplotypes)
            .concat(grc_amino_acid_caller.out.grc2)
            .concat(grc_barcoding.out.barcoding_split_out_file)
            .collect()
            .set{grc_components}
        grc_assemble(grc_components)

        grc_curator(grc_assemble.out)

        // Add metadata from manifest to GRC file
        grc_add_metadata(manifest_file, grc_curator.out.grc)

        // Workflow output channel
        grc = grc_add_metadata.out
    
        // upload final GRC and Genotype file to S3 bucket
        if (params.upload_to_s3){
            grc
                .concat(genotype_files_ch)
                .set{output_to_s3}
            upload_pipeline_output_to_s3(output_to_s3, "grc")
        }

    emit:
        grc
}

workflow {
    // Files required for GRC creation
    Channel.fromPath(params.grc_settings_file_path, checkIfExists: true)
    manifest_file = Channel.fromPath(params.manifest_path, checkIfExists: true)
    lanelet_manifest_file = Channel.fromPath(params.lanelet_manifest_path, checkIfExists: true)
    chrom_key_file = Channel.fromPath(params.chrom_key_file_path, checkIfExists: true)
    kelch_reference_file = Channel.fromPath(params.kelch_reference_file_path, checkIfExists: true)
    codon_key_file = Channel.fromPath(params.codon_key_file_path, checkIfExists: true)
    drl_information_file = Channel.fromPath(params.drl_information_file_path, checkIfExists: true)

    // Run GRC creation workflow
    VARIANTS_TO_GRCS(manifest_file, lanelet_manifest_file, chrom_key_file, kelch_reference_file, codon_key_file, drl_information_file)
}