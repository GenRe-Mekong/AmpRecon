// Copyright (C) 2026 GenRe Mekong Core Team

process KRAKEN2_RUN {
    /*
    * Map reads to reference
    */
    tag "${meta.uuid}"
    label "kraken2"

    input:
        tuple val(meta), path(reads)
        path kraken_db

    output:
        tuple val(meta), path("*_output.txt"), emit: output
        tuple val(meta), path("*_report.txt"), emit: report
        path "versions.yml"           , emit: versions

    script:
        def prefix    = "${meta.uuid}"
        def memory_mapping = task.executor in ['slurm', 'sge', 'awsbatch'] ? "" : "--memory-mapping"

        """
        kraken2 \
            --db ${kraken_db} \
            --threads ${task.cpus} \
            --unclassified-out ${prefix}#_unclassified.txt \
            --classified-out ${prefix}#_classified.txt \
            --output ${prefix}_output.txt \
            --report ${prefix}_report.txt \
            --paired \
            --use-names \
            ${memory_mapping} \
            ${reads}


        cat <<-EOF > versions.yml
        "${task.process}":
            kraken2: \$( kraken2 --version | grep version | cut -f 3 -d ' ' )
        EOF
        """
}
