// Copyright (C) 2026 GenRe-Mekong Core Team.

process PHENOTYPER {

    // malariagen/phenotyper 
    label "phenotyper"

    input:
        path(barcodes_grc)
        path(phenotyper_rules)

    output:
        path("*predicted.txt"), emit: phenotype
        path("versions.yml"),   emit: versions

    script:

        """
        phenotyper.r \
            --datafile=${barcodes_grc} \
            --rulesfile=${phenotyper_rules} \
            --ruleout=phenotype \
            --samplecolumn=ID \
            --output=${params.batch_id}_predicted.txt
        
        # standardize identifier column's name to "ID" for downstream compat
        sed -i '1s/sample/ID/g' ${params.batch_id}_predicted.txt

        cat <<-EOF > versions.yml
        "${task.process}":
             rbase: \$( R --version | head -n1 | sed 's/R version //g' | cut -f1,2 -d ' ')
             phenotyper: "malariagen/phenotyper (@145b52f)"
        EOF
        """
}
