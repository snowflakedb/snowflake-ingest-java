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
                println("\n\n*** Change to script 4***")
                script {
                    def valid_db_name_tag = ingest_sdk_tag.split('\\.').join('_')
                    def deployments = [
                            "qa3": {
                                build job: "SFPerf-Other-Jobs/TPCDS_BDEC_Setup",
                                        parameters: [
                                                string(name: 'ingest_sdk_github_branch', value: ingest_sdk_tag),
                                                string(name: 'database', value: "BENCHMARK_DB_BDEC_PERFORMANCE_SIGNOFF_${valid_db_name_tag}"),
                                                string(name: 'deployment', value: 'qa3.us-west-2.aws'),
                                                string(name: 'tpcds_scale_factor', value: 'sf1000')
                                        ],
                                        propagate: true
                            },
                            "preprod12": {
                                build job: "SFPerf-Other-Jobs/TPCDS_BDEC_Setup",
                                        parameters: [
                                                string(name: 'ingest_sdk_github_branch', value: ingest_sdk_tag),
                                                string(name: 'database', value: "BENCHMARK_DB_BDEC_PERFORMANCE_SIGNOFF_${valid_db_name_tag}"),
                                                string(name: 'deployment', value: 'preprod12.us-west-2.aws'),
                                                string(name: 'tpcds_scale_factor', value: 'sf1000')
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