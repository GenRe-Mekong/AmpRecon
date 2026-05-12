class WorkflowUtils {

    // Utility method to create a manifest file with specified header and rows.
    static String makeManifest(String name, List<String> header, List<Map> rows, String sep="\t" ) {
        def f = new File(name)
        f.parentFile?.mkdirs()

        def headerLine = header.join(sep)
        def lines = rows.collect { row ->
            header.collect { h -> row.get(h, "") }.join(sep)
        }

        f.text = ([headerLine] + lines).join("\n")
        return f.absolutePath
    }

    // Utility method to subset columns from a tab-delimited file and write to a new file.
    static String subsetColumns(String filePath, String outPath, int start = 2, int end = 149) {
        def file = new File(filePath)
        def subsetFile = new File(outPath)

        subsetFile.withWriter { writer ->
            file.eachLine { line ->
                def cols = line.split('\t')
                if (cols.size() < end) cols += (cols.size()..end-1).collect { "" }
                writer << cols[(start-1)..(end-1)].join('\t') << "\n"
            }
        }
        return subsetFile.absolutePath
    }

    // Build the 30-sample × 3-panel manifest with sample_id, primer_panel, fastq1_path, fastq2_path, and meta columns.
    static String buildMnf(String workDir, String baseDir) {
        def fastq_path = "${baseDir}/tests/data/fastq"
        return makeManifest("${workDir}/fq_mnf.tsv",
            ["sample_id", "primer_panel", "fastq1_path", "fastq2_path", "meta"],
            (1..30).collectMany { i ->
                def sampleName = String.format("sample%02d", i)
                ["GRC1", "GRC2", "Spec"].collect { panel ->
                    [
                        sample_id   : sampleName,
                        primer_panel: "PFA_${panel}",
                        fastq1_path : "${fastq_path}/${sampleName}.${panel.toLowerCase()}_1.fastq",
                        fastq2_path : "${fastq_path}/${sampleName}.${panel.toLowerCase()}_2.fastq",
                        meta        : "metadata of ${sampleName}"
                    ]
                }
            }
        )
    }

    // Build the 30-sample × 3-panel manifest with ID and vcf_path columns.
    static String buildLanelet(String workDir, String moduleTestDir) {
        def vcf_path = "${moduleTestDir}/data/vcfs"
        return makeManifest("${workDir}/lanelet_vcf_manifest.csv",
            ["ID", "vcf_path"],
            (1..30).collectMany { i ->
                def sampleName = String.format("sample%02d", i)
                ["GRC1", "GRC2", "Spec"].collect { panel ->
                    [
                        ID      : sampleName,
                        vcf_path: "${vcf_path}/${sampleName}.${panel.toLowerCase()}.vcf.gz"
                    ]
                }
            }, ","
        )
    }

    // Build the 30-sample qPCR manifest with mdr1/pm23/species columns.
    static String buildFullQpcr(String workDir) {
        return makeManifest("${workDir}/qpcr_mnf.tsv",
            ["ID", "mdr1-qpcr", "pm23-qpcr", "species-qpcr"],
            [
                [count: 4, mdr1: "WT",        pm23: "WT",        species: "pf"],
                [count: 4, mdr1: "WT",        pm23: "Amplified", species: "pf"],
                [count: 4, mdr1: "WT",        pm23: "-",         species: "pf"],
                [count: 4, mdr1: "Amplified", pm23: "WT",        species: "pf"],
                [count: 4, mdr1: "Amplified", pm23: "Amplified", species: "pf"],
                [count: 4, mdr1: "Amplified", pm23: "-",         species: "pf"],
                [count: 2, mdr1: "-",         pm23: "-",         species: "pf"],
            ].withIndex().collectMany { testCase, idx ->
                (1..testCase.count).collect { j ->
                    def sampleNum = idx * 4 + j
                    def sampleName = String.format("sample%02d", sampleNum)
                    [
                        ID             : sampleName,
                        "mdr1-qpcr"   : testCase.mdr1,
                        "pm23-qpcr"   : testCase.pm23,
                        "species-qpcr": testCase.species,
                    ]
                }
            }
        )
    }

    // Build mocking wrong formar qPCR manifest
    static String buildBadQpcr(String workDir) {
        return makeManifest("${workDir}/qpcr_mnf.tsv",
            ["ID", "pm23-qpcr", "species-qpcr"],
            [
                [count: 4, pm23: "WT",         species: "pf"],
                [count: 4, pm23: "Amplified",  species: "pf"],
                [count: 4, pm23: "-",          species: "pf"],
                [count: 4, pm23: "WT",         species: "pf"],
                [count: 4, pm23: "Amplified",  species: "pf"],
                [count: 4, pm23: "-",          species: "pf"],
                [count: 2, pm23: "-",          species: "pf"],
            ].withIndex().collectMany { testCase, idx ->
                (1..testCase.count).collect { j ->
                    def sampleNum = idx * 4 + j
                    def sampleName = String.format("sample%02d", sampleNum)
                    [
                        ID             : sampleName,
                        "pm23-qpcr"   : testCase.pm23,
                        "species-qpcr": testCase.species,
                    ]
                }
            }
        )
    }
}

