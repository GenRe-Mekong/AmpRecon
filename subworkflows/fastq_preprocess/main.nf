//
// Subworkflow to preprocess FastQ file 
// *The BAM coversion is require to replicate the SANGER's step of CRAM entry point.
//

/*
-----------------------------------------------------------------------------------
    IMPORT MODULES
-----------------------------------------------------------------------------------
*/

include { FASTQTOBAM     as FASTQ_TO_BAM  } from '../../modules/fastqtobam/main'
include { BAMADAPTERFIND as FIND_ADAPTERS } from '../../modules/bamadapterfind/main'
include { BAMADAPTERCLIP as CLIP_ADAPTERS } from '../../modules/bamadapterclip/main'
include { BAMTOFASTQ     as BAM_TO_FASTQ  } from '../../modules/bamtofastq/main'

/*
-----------------------------------------------------------------------------------
    SUBWORKFLOW FOR INITIALISE PIPELINE
-----------------------------------------------------------------------------------
*/

workflow FASTQ_PREPROCESS {
    take: 
        input_ch             //  tuple( metas, input )

    main:
 
        ch_versions = Channel.empty()
        
        //
        // MODULE: FASTQTOBAM
        // Covert FASTQ to BAM
        //
        FASTQ_TO_BAM(input_ch)
        ch_versions = ch_versions.mix(FASTQ_TO_BAM.out.versions.first())


        //
        // MODULE: BAMADAPTERFIND
        // find adapter 
        //
        FIND_ADAPTERS(FASTQ_TO_BAM.out.bam)
        ch_versions = ch_versions.mix(FIND_ADAPTERS.out.versions.first())

        //
        // MODULE: BAMADAPTERCLIP
        // Cut adapter marked within BAM
        //
        CLIP_ADAPTERS(FIND_ADAPTERS.out.bam)
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
