// Copyright (C) 2023 Genome Surveillance Unit/Genome Research Ltd.

process SCRAMBLE_CRAM_TO_BAM {
    /*
    Converts a cram to bam.
    */
    tag "${meta.uuid}"
    label 'staden_io'
    label 'process_low'

    input:
        tuple val(meta), path(cram), path(fasta)
    output:
        tuple val(meta), path("*.bam"), emit: bam
        path "versions.yml"           , emit: versions

    script:
        def prefix = "${meta.uuid}"

        """
        scramble \
            -t 1 \
            -7 \
            -r "${meta.reference.fasta}" \
            -I cram \
            -O bam \
            < "${cram}" \
            > "${prefix}.bam"

        cat <<-EOF > versions.yml
        "${task.process}": 
            staden_io: \$(scramble -h | grep sCRAMble | sed 's/.*version //g')
        EOF
        """
}

