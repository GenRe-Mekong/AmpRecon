// Copyright (C) 2023 Genome Surveillance Unit/Genome Research Ltd.

params.coi_output_filename = "coi.tsv"

process GRC_MCCOIL_INPUT {

    label "py_pandas"

    input:
        path(barcodes_grc)

    output:
        path("*.tsv"), emit: het

    script:

        def grc_settings = params.grc_settings_file_path
        coi_out_filename = params.coi_output_filename
        input_suffix = "myRun" // this could be the run id (or any sequence batch identifier)
        
        """
        # write McCOIL in
        grc_process_mccoil_io.py -write_mccoil_in \
                --barcodes_files ${barcodes_grc} --config ${grc_settings} \
                --output_file ${input_suffix}.tsv
        """
}
        
process GRC_RUN_MCCOIL {

    label "mccoil"

    input:
        path(het_data)

    output:
        path("*_summary.txt"), emit: coi
        path("versions.yml") , emit: versions

    script:
        def input_suffix = "myRun" // this could be the run id (or any sequence batch identifier)
        """
        runMcCOIL.R \
            -i ${het_data} \
            --outPrefix ${input_suffix} \
            --totalRun ${params.mccoil_ntotal} \
            --totalBurnIn ${params.mccoil_nburnin} \
            --seed ${params.mccoil_seed} \
            --maxCOI ${params.mccoil_maxCOI} \
            --M0 ${params.mccoil_m0} \
            --maxMissTol ${params.mccoil_maxMissTol} \
            --e1 ${params.mccoil_e1} \
            --e2 ${params.mccoil_e2}

        cat <<-EOF > versions.yml
        "${task.process}":
             rbase: \$( R --version | head -n1 | sed 's/R version //g' | cut -f1,2 -d ' ')
             THEREALMcCOIL: "AMarinhoSN/THEREALMcCOIL (@3b41fb2)"
        EOF
        """
}

process GRC_PARSE_MCCOIL {

    label "py_pandas"

    input:
        path(barcodes_grc)

    output:
        path("${coi_out_filename}"), emit: coi

    script:

        def grc_settings = params.grc_settings_file_path
        input_suffix = "myRun" // this could be the run id (or any sequence batch identifier)
        coi_out_filename = params.coi_output_filename
        
        """
        # write coi grc
        grc_process_mccoil_io.py -write_coi_grc \
            --mccoil_sum_file ${input_suffix}_summary.txt \
            --output_file ${coi_out_filename}
        """
}
