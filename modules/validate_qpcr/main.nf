// Copyright (C) 2026 GenRe-Mekong Core Team.

process VALIDATE_QPCR {

    tag "${qpcr_mnf.getBaseName()}"
    label 'py_pandas'

    input:
        path(qpcr_mnf)

    output:
        path("validated_qpcr_mnf.tsv"), emit: validated_qpcr_mnf
        path("versions.yml")          , emit: versions

    script:
        def force_fill_columns =  params.qpcr ? "" : "--force-fill-columns"
        """
        validate_qpcr.py \
            --qpcr-input ${qpcr_mnf} \
            ${force_fill_columns} \
            --output validated_qpcr_mnf.tsv

        cat <<-EOF > versions.yml
            ${task.process}:
                python: \$(python --version | cut -f2 -d ' ') 
        EOF
        """
}
