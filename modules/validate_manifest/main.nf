// Copyright (C) 2023 Genome Surveillance Unit/Genome Research Ltd.

process VALIDATE_MANIFEST {

    tag "${manifest.getBaseName()}"
    label 'py_pandas'

    input:
        path(manifest)
        path(panel_setting)
        val(execution_mode)

    output:
        val true,             emit: valid
        path("versions.yml"), emit: versions

    script:
        """
        validate_samplesheet.py ${manifest} ${panel_setting} ${execution_mode}

        cat <<-EOF > versions.yml
            ${task.process}:
                python: \$(python --version | cut -f2 -d ' ') 
        EOF
        """
}
