// Copyright (C) 2023 Genome Surveillance Unit/Genome Research Ltd.

process BWA_MEM {
    /*
    * Map reads to reference
    */
    tag "${meta.uuid}"
    label "bwa"

    input:
        tuple val(meta), path(reads), path(fasta)

    output:
        tuple val(meta), path("*.sam"), emit: sam
        path "versions.yml"           , emit: versions

    script:
        def prefix   = "${meta.uuid}"
        def reference = "${meta.reference.fasta}"

        """
        set -e
        set -o pipefail

        bwa mem \
            -Y \
            -K 100000000 \
            -t ${task.cpus} \
            ${reference} \
            ${reads} > ${prefix}.sam

        cat <<-EOF > versions.yml
        "${task.process}":
            bwa: \$( bwa 2>&1 | grep Version | cut -f2 -d ' ' )
        EOF
        """
}
