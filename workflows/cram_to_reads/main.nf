//
// Subworkflow to covert CRAM file to FastQ
//

/*
-----------------------------------------------------------------------------------
    IMPORT MODULES
-----------------------------------------------------------------------------------
*/

include { SCRAMBLE_CRAM_TO_BAM            } from '../../modules/scramble/main'
include { BAMADAPTERCLIP as CLIP_ADAPTERS } from '../../modules/bamadapterclip/main'
include { BAMTOFASTQ     as BAM_TO_FASTQ  } from '../../modules/bamtofastq/main'

/*
-----------------------------------------------------------------------------------
    SUBWORKFLOW FOR INITIALISE PIPELINE
-----------------------------------------------------------------------------------
*/

workflow CRAM_TO_READS {
    take: 
        input_ch             //  tuple( metas, input )

    main:
 
        ch_versions = Channel.empty()
        
        // Staging reference files
        def input_ch = input_ch.map {
            def (meta, fastq) = it[0..1]
            def fasta = file("${meta.reference.fasta}*")
            return tuple(meta, fastq, fasta)
        }
        //
        // MODULE: SCRAMBLE
        // Covert CRAM to BAM
        //
        SCRAMBLE_CRAM_TO_BAM(input_ch)
        ch_versions = ch_versions.mix(SCRAMBLE_CRAM_TO_BAM.out.versions.first())


        //
        // MODULE: BAMADAPTERCLIP
        // Cut adapter marked within BAM
        //
        CLIP_ADAPTERS(SCRAMBLE_CRAM_TO_BAM.out.bam)
        ch_versions = ch_versions.mix(CLIP_ADAPTERS.out.versions.first())

        //
        // MODULE: BAMTOFASTQ
        // Convert BAM to FastQ
        //
        BAM_TO_FASTQ(CLIP_ADAPTERS.out.bam)
        ch_versions = ch_versions.mix(BAM_TO_FASTQ.out.versions.first())

    emit:
        fastq = BAM_TO_FASTQ.out.fastq
        versions = ch_versions
}
