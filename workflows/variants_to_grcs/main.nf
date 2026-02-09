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

// import modules
include { assemble_genotype_file       } from '../../modules/grc_assemble_genotype_file.nf'
include { grc_kelch13_mutation_caller  } from '../../modules/grc_kelch13_mutation_caller.nf'
include { grc_plasmepsin_cnv_caller    } from '../../modules/grc_plasmepsin_cnv_caller.nf'
include { grc_speciate                 } from '../../modules/grc_speciate.nf'
include { grc_barcoding                } from '../../modules/grc_barcoding.nf'
include { GRC_MCCOIL_INPUT             } from '../../modules/grc_mccoil/main.nf'
include { GRC_RUN_MCCOIL               } from '../../modules/grc_mccoil/main.nf'
include { GRC_PARSE_MCCOIL             } from '../../modules/grc_mccoil/main.nf'
include { grc_amino_acid_caller        } from '../../modules/grc_amino_acid_caller.nf'
include { grc_assemble 			       } from '../../modules/grc_assemble.nf'
include { GRC_ADD_METADATA             } from '../../modules/grc_add_metadata.nf'

workflow VARIANTS_TO_GRCS {
    take:
        manifest_file
        lanelet_manifest_file
        chrom_key_file
        kelch_reference_file
        codon_key_file
        drl_information_file

    main:

        def chrom_key_file = file("${chrom_key_file}")
        def kelch_reference_file = file("${kelch_reference_file}")
        def codon_key_file = file("${codon_key_file}")
        def drl_information_file = file("${drl_information_file}")
        def config = file("${params.grc_settings_file_path}")
        ch_versions = Channel.empty()

        // Write genotype file
        assemble_genotype_file(
            lanelet_manifest_file, 
            chrom_key_file
            )
        ch_versions = ch_versions.mix(assemble_genotype_file.out.versions.first())
        genotype_files_ch = assemble_genotype_file.out.mnf
        
        // Call mutations at Kelch13 loci
        if (params.no_kelch == false){
            grc_kelch13_mutation_caller(
                genotype_files_ch,
                kelch_reference_file,
                codon_key_file,
                config
                )
            kelch_grc_ch = grc_kelch13_mutation_caller.out
        } else {
            kelch_grc_ch = Channel.empty()
        }

        // Call copy number variation at Plasmepsin breakpoint
        if (params.no_plasmepsin == false){
            grc_plasmepsin_cnv_caller(genotype_files_ch, config)
            plasmepsin_grc_ch = grc_plasmepsin_cnv_caller.out
        } else {
            plasmepsin_grc_ch = Channel.empty()
        }

        // Create barcodes
        grc_barcoding(genotype_files_ch, config)

        // Determine species
        grc_speciate(genotype_files_ch, grc_barcoding.out.barcoding_file, config)

        // Complexity of infection estimation
        if (params.no_coi == false) {
            // Load LibPaths 
            rlibs_path = Channel.fromPath("${projectDir}/assets/R_libs")
            GRC_MCCOIL_INPUT(grc_barcoding.out.barcoding_file, config)
            GRC_RUN_MCCOIL(GRC_MCCOIL_INPUT.out.het, rlibs_path)
            GRC_PARSE_MCCOIL(GRC_RUN_MCCOIL.out.coi)
            coi_grc_ch = GRC_PARSE_MCCOIL.out.coi
            ch_versions = ch_versions.mix(GRC_RUN_MCCOIL.out.versions.first())
        } else {
            coi_grc_ch = Channel.empty()
        }

        // Assemble drug resistance haplotypes and amino acid calls
        grc_amino_acid_caller(
            genotype_files_ch,
            drl_information_file,
            codon_key_file,
            config
            )

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

        // Add metadata from manifest to GRC file
        GRC_ADD_METADATA(manifest_file, grc_assemble.out)

        // Workflow output channel
        grc = GRC_ADD_METADATA.out
    
    emit:
        grc
        versions = ch_versions
}
