// Copyright (C) 2023 Genome Surveillance Unit/Genome Research Ltd.

process BAMADAPTERCLIP {
    /*
    Removes identified adapters from bam
    */

    tag "${meta.uuid}"
    label 'biobambam'
    label 'process_low'

    input:
        tuple val(meta), path(bam)

    output:
        tuple val(meta), path("*.clipped.bam"), emit: bam
        path "versions.yml"                   , emit: versions

    script:
        def verbose           = 1
        def compression_level = 0
        def prefix            = "${meta.uuid}"
        """
        bamadapterclip \
            verbose=${verbose} \
            level=${compression_level} \
            < "${bam}" \
            > "${prefix}.clipped.bam"

        cat <<-EOF > versions.yml 
        "${task.process}":
            biobambam: \$(bamadapterclip -v 2>&1 | head -n1 | sed -n 's/.*version //p' | sed 's/\\.\$//')
        EOF
        """

}
