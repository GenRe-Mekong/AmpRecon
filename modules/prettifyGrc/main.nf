
process PRETTIFY_GRC {
    /*
    Adjust the column to compile with the GRC 1.4 format
    */

    label 'py_pandas'
    label 'process_low'

    input:
        path(grc_file)

    output:
        path("*_GRC.xlsx"), emit: xlsx

    script:
        """
        prettifyGrc.py \
            --run-id ${params.batch_id} \
            ${grc_file}  \
            ${params.batch_id}_GRC.xlsx
        """

}
