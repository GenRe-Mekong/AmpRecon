// Copyright (C) 2023 Genome Surveillance Unit/Genome Research Ltd.


process BCFTOOLS_MPILEUP {
    /*
    Creates an uncompressed BCF file containing calculated genotype likelihoods for every possible genomic position supported by the BAM
    */

    tag "${meta.uuid}"
    label "bcftools"
    label "process_low"

    input:
        tuple val(meta), path(bam), path(bai), path(target), path(fasta)

    output:
        tuple val(meta), path("*.bcf"), emit: bcf
        path "versions.yml"           , emit: versions

    script:
        def prefix = "${meta.uuid}"
        def min_bq = params.mpileup_min_bq ? params.mpileup_min_bq : 20
        def max_depth = params.mpileup_max_depth ? params.mpileup_max_depth : 50000

        """
        bcftools mpileup \
            --min-BQ ${min_bq} \
            --max-depth ${max_depth} \
            --annotate FORMAT/AD,FORMAT/DP \
            --targets-file "${target}" \
            --fasta-ref "${fasta}" \
            --output-type u \
            "${bam}" \
            > "${prefix}.bcf"
            
        cat <<-EOF > versions.yml
        "${task.process}": 
            bcftools: "\$( bcftools --version | grep -E '^bcftools' | cut -f2 -d ' ')"
            htslib: "\$( bcftools --version | grep -E '^Using htslib' | cut -f 3 -d ' ')"
        EOF
        """
}
