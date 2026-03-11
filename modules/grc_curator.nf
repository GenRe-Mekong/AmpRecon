process grc_curator {
    publishDir "${params.results_dir}/grc/", overwrite: true, mode: "copy", pattern: "*flagged.tsv"
    publishDir "${params.results_dir}/grc/", overwrite: true, mode: "copy", pattern: "*removed.tsv"

    label "grc_tools"
    input:
        path(grc)

    output:
        path("*GRC_curated.txt"), emit: grc
        path("*flagged.tsv")    , emit: flagged, optional: true
        path("*removed.tsv")    , emit: removed, optional: true

    script:
        """
        grc_curator.py \
            --run_id ${params.batch_id} \
            --raw_grc_path ${grc} \
            --accepted_mutations_file ${params.accepted_mutations_file}
        """
}
