// Copyright (C) 2023 Genome Surveillance Unit/Genome Research Ltd.

params.min_total_depth = 10
params.het_min_allele_depth = 5
params.het_min_allele_proportion = 0.10
params.chromosome_column_name = "Chrom_ID"
params.locus_column_name = "VarPos"

process assemble_genotype_file {
    /*
    Merges supplied VCF files into 1 genotype .tsv file. 
    Removes SNPs to be masked, updates co-ordinates to match
    those within supplied chromKey file and filtesr out
    alleles with low coverage.
    */

    label "pyvcf"
    label "grc_tools"

    input:
        path(vcf_manifest_file)
        path(chrom_key_file)

    output:
        path("${output_file_name}"), emit: mnf
        path("versions.yml"),        emit: versions

    script:
        output_file_name = "genotype_file.tsv"
        chromsome_column = params.chromosome_column_name
        locus_column = params.locus_column_name
        min_depth = params.min_total_depth
        min_allele_depth = params.het_min_allele_depth
        min_allele_proportion = params.het_min_allele_proportion

        """
        write_genotypes_file.py \
            --manifest_file ${vcf_manifest_file} \
            --output_file "${output_file_name}" \
            --chromKey_file "${chrom_key_file}" \
            --chromosome_column_name "${chromsome_column}" \
            --locus_column_name "${locus_column}" \
            --min_total_depth "${min_depth}" \
            --het_min_allele_depth "${min_allele_depth}" \
            --het_min_allele_proportion "${min_allele_proportion}"

        cat <<-END_VERSIONS > versions.yml
        "${task.process}":
            python: \$( python --version 2>&1 | cut -f2 -d ' ' )
            pyvcf: \$( pip show PyVCF | grep Version| cut -f2 -d ' ')
        END_VERSIONS
        """
}
