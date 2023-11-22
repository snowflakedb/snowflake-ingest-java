pipeline {
    agent { label 'parallelizable-c7' }
    options { timestamps() }
    environment {
        ingest_sdk_dir = "${WORKSPACE}/snowflake-ingest-java"
        ingest_sdk_tag = sh(returnStdout: true, script: "cd $ingest_sdk_dir && git describe --tags").trim()

    }
    stages {
        stage('TriggerJobs') {
            steps {
                script {
                    def split = ingest_sdk_tag.split('.')
                    println(split)
                    def valid_db_name_tag = split.join('_')
                    println(valid_db_name_tag)
                    def deployments = [
                            /*
                            "qa3": {
                                build job: "SFPerf-Other-Jobs/TPCDS_BDEC_Setup",
                                        parameters: [
                                                string(name: 'ingest_sdk_github_branch', value: ingest_sdk_tag),
                                                string(name: 'setup_branch', value: 'lthiede-SNOW-964536-Mark-complete-dataset'),
                                                string(name: 'database', value: "STREAMING_INGEST_BENCHMARK_DB_${valid_db_name_tag}"),
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
                                                string(name: 'database', value: "STREAMING_INGEST_BENCHMARK_DB_${valid_db_name_tag}"),
                                                string(name: 'deployment', value: 'preprod12.us-west-2.aws'),
                                                string(name: 'tpcds_scale_factor', value: 'sf1')
                                        ],
                                        propagate: true
                            }
                            */
                            "preprod6": {
                                build job: "SFPerf-Other-Jobs/TPCDS_BDEC_Setup",
                                        parameters: [
                                                string(name: 'ingest_sdk_github_branch', value: ingest_sdk_tag),
                                                string(name: 'setup_branch', value: 'lthiede-SNOW-964536-Mark-complete-dataset'),
                                                string(name: 'database', value: "STREAMING_INGEST_BENCHMARK_DB_${valid_db_name_tag}"),
                                                string(name: 'deployment', value: 'preprod12.us-west-2.aws'),
                                                string(name: 'tpcds_scale_factor', value: 'sf1')
                                        ],
                                        propagate: true
                            }
                    ]

                    parallel deployments
                }
            }
        }
    }
}