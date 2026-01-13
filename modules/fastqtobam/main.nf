// Copyright (C) 2023 Genome Surveillance Unit/Genome Research Ltd.

process FASTQTOBAM {
    /*
    Converts FASTQ to BAM.
    */

    tag "${meta.uuid}"
    label 'biobambam'
    label 'process_low'

    input:
        tuple val(meta), path(fastq)

    output:
        tuple val(meta), path("*.bam"), emit: bam
        path "versions.yml"           , emit: versions

    script:
        def prefix = "${meta.uuid}"
        def gzip_status = "${fastq[0]}".endsWith("gz") ? 1 : 0
        """
        fastqtobam \
            level=9 \
            gz=${gzip_status} \
            namescheme=pairedfiles \
            ${fastq[0]} ${fastq[1]} > ${prefix}.bam

        cat <<-EOF > versions.yml
        "${task.process}":
            biobambam: \$(bamtofastq -v 2>&1 | head -n1 | sed -n 's/.*version //p' | sed 's/\\.\$//')
        EOF
        """
}
