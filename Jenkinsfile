ingest_sdk_dir = "${WORKSPACE}/snowflake-ingest-java"
ingest_sdk_tag = sh(returnStdout: true, script: "cd $ingest_sdk_dir && git describe --tags").trim()

deployments = [
        "qa3": {
            build job: "SFPerf-Other-Jobs/TPCDS_BDEC_Setup",
                    parameters: [
                            string(name: 'ingest_sdk_github_branch', value: ingest_sdk_tag),
                            string(name: 'setup_branch', value: 'lthiede-SNOW-964536-Mark-complete-dataset'),
                            string(name: 'database', value: "STREAMING_INGEST_BENCHMARK_DB_${ingest_sdk_tag}"),
                            string(name: 'deployment', value: 'qa3.us-west-2.aws'),
                            string(name: 'tpcds_scale_factor', value: 'sf1')
                    ],
                    propagate: true
        },
        "preprod12": {
            build job: "SFPerf-Other-Jobs/TPCDS_BDEC_Setup",
                    parameters: [
                            string(name: 'ingest_sdk_github_branch', value: ingest_sdk_tag),
                            string(name: 'setup_branch', value: 'lthiede-SNOW-964536-Mark-complete-dataset'),
                            string(name: 'database', value: "STREAMING_INGEST_BENCHMARK_DB_${ingest_sdk_tag}"),
                            string(name: 'deployment', value: 'preprod12.us-west-2.aws'),
                            string(name: 'tpcds_scale_factor', value: 'sf1')
                    ],
                    propagate: true
        }
]

parallel deployments