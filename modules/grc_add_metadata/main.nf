// Copyright (C) 2023 Genome Surveillance Unit/Genome Research Ltd.
// Copyright (C) 2026 GenreMekong Core Team

process GRC_ADD_METADATA {

    label "py_pandas"

    input:
        path(grc)
        path(metadata)

    output:
        path("*_GRC.txt"), emit: grc

    script:
        manifest_opt = params.metadata ? "" : "--allow_duplicates --exclude_meta_cols primer_panel"
        """
        grc_metadata.py \
            --grc_file  ${grc} \
            --meta_file ${metadata} \
            ${manifest_opt} \
            --output ${params.batch_id}_GRC.txt
        """
}
