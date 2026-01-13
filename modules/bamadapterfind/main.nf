// Copyright (C) 2023 Genome Surveillance Unit/Genome Research Ltd.

process BAMADAPTERFIND {
    /*
    Searches for sequencing adapter contamination within a BAM  file.
    */

    tag "${meta.uuid}"
    label 'biobambam'
    label 'process_low'

    input:
        tuple val(meta), path(bam)

    output:
        tuple val(meta), path("*adapter.bam"), emit: bam
        path "versions.yml"                  , emit: versions

    script:
        def prefix        = task.ext.prefix ?: "${meta.uuid}"
        """
        bamadapterfind level=9 < ${bam} >${prefix}.adapter.bam
        cat <<-EOF > versions.yml
        "${task.process}":
            biobambam: \$(bamtofastq -v 2>&1 | head -n1 | sed -n 's/.*version //p' | sed 's/\\.\$//')
        EOF
        """
}

