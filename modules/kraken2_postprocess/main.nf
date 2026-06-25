// Copyright (C) 2026 GenRe Mekong Core Team

process KRAKEN2_POSTPROCESS {
    label "py_pandas"

    input:
        path(kraken_reports)

    output:
        path("species_call.tsv"), emit: species

    script:
        """
        kraken_postprocess.py \
            --suffix _PFA_Spec_report.txt \
            --min_reads 5 \
            --min_fraction 0.001 \
            --output species_call.tsv \
            --verbose
        """
}
