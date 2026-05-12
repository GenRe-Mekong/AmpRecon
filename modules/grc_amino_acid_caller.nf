// Copyright (C) 2023 Genome Surveillance Unit/Genome Research Ltd.

params.drl_haplotypes_output_filename = "drl_haplotype_calls.tsv"
params.grc2_intermediate_name = "GRC2.intermediate.tsv"

process grc_amino_acid_caller {
    label "py_pandas"
    label "grc_tools"
    input:
        path(genotype_file)
        path(drl_information_file)
        path(codon_key_file)
        path(config)

    output:
        path("${output_drl_haplotypes}"), emit: drl_haplotypes
        path("${output_grc_2}"), emit: grc2

    script:
        grc_settings = params.grc_settings_file_path
        output_drl_haplotypes = params.drl_haplotypes_output_filename
        output_grc_2 = params.grc2_intermediate_name

        """
        grc_amino_acid_caller.py \
            --genotype_files ${genotype_file} \
            --config "${config}"  \
            --output_grc1_file "${output_drl_haplotypes}" \
            --output_grc2_file "${output_grc_2}" \
            --drl_information_file "${drl_information_file}"  \
            --codon_key_file "${codon_key_file}"
        """

}
