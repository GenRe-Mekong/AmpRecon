// Copyright (C) 2023 Genome Surveillance Unit/Genome Research Ltd.

include { READ_COUNT_PER_REGION } from '../../modules/read_counts_per_region/main'

workflow READ_COUNTS {
  take:
    bam_ch

  main:
    // brnch bam ch into each panel_name
    // collect bam and bai [ panel_name, [bam], [bai] ]
    bam_ch
        | map { it ->
            def (meta, bam, bai) = it
            tuple( meta.panel, meta.dsgn, meta.reference.fai, bam, bai )} 
        | groupTuple() // [ panel_name, [[fai,bam,bai], [fai,bam,bai]..] ]
        | map { it ->
            def (panel_name, dsgn, fai, bams, bais) = it
            tuple( panel_name, dsgn.first(), fai.first(), bams, bais ) }
        | set { per_panel_ch }

    // determine read counts per amplicon region
    READ_COUNT_PER_REGION(
        per_panel_ch
    )

}