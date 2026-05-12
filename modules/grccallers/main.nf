// Copyright (C) 2023 Genome Surveillance Unit/Genome Research Ltd.

process GRC_KELCH13_MUTATION_CALLER {
    /*
    Calls non-synonymous mutations in asupplied region of the kelch13
    gene in each of the supplied genotype files. Uses a file detailing
    the kelch13 reference sequence, codon structure and amino acid
    translation. It uses a codon key file for translating nucleotide
    codons into associated amino acids. It also uses the kelch13 region
    supplied in the configuration file.
    Writes these kelch13 mutation calls to a single output tab-separated file.
    */
    label "grccallers"

    input:
        path(genotype_file)

    output:
        path("*compact.tsv"), emit: compact
        path("*long.tsv"),    emit: long_format

    script:

        def output = "k13_mut_call"
        def min_dp    = params.kelch13_min_dp    ? "--min-dp ${params.kelch13_min_dp}"       : ""
        def min_ad    = params.kelch13_min_ad    ? "--min-ad ${params.kelch13_min_ad}"       : ""
        def min_ratio = params.kelch13_min_ratio ? "--min-ratio ${params.kelch13_min_ratio}" : ""

        """
        grccallers k13 \\
            --genotype        ${genotype_file} \\
            --fasta           ${params.reference_genome}  \\
            --gff             ${params.reference_gff}     \\
            --who-list        ${params.kelch13_who_list}  \\
            --output-prefix   ${output}                   \\
            ${min_dp}    \\
            ${min_ad}    \\
            ${min_ratio} \\
            --log-level DEBUG

            
            # standardize column's name for downstream compatibility
            sed -i '1s/sample_id/ID/g'      ${output}_compact.tsv
            sed -i '1s/mutations/kelch13/g' ${output}_compact.tsv

            # TODO add versions.yml
        """
}