import com.snowflake.BasePerfTest

base_perf = new com.snowflake.BasePerfTest()

pipeline {
    agent {
        node {
            label "parallelizable-c7"
        }
    }
    environment {
        jenkins_github_credential_id = 'b4f59663-ae0a-4384-9fdc-c7f2fe1c4fca'

        ingest_sdk_dir = 'snowflake-ingest-java'
        ingest_sdk_tag = sh(returnStdout: true, script: "git describe --tags").trim()

        bdec_setup_git_remote = 'https://github.com/snowflakedb/streaming-ingest-benchmark.git'
        bdec_setup_git_specifier = 'main'
        bdec_setup_relative_dir = 'streaming-ingest-benchmark'
        bdec_setup_reference = "/mnt/jenkins/git_repo/streaming-ingest-benchmark"
    }
    stages {
        stage('CheckoutSetupApplication') {
            steps {
                bdec_setup_workspace_path = base_perf.clone(bdec_setup_git_remote, bdec_setup_git_specifier, bdec_setup_relative_dir, bdec_setup_reference, jenkins_github_credential_id)
            }
        }
        stage('Build') {
            steps {
                sh "mvn -f ${ingest_sdk_dir}/pom.xml package -DskipTests"
                sh "mvn install::install-file -Dfile=${ingest_sdk_dir}/target/snowflake-ingest-sdk.jar -DgroupId=net.snowflake -DartifactId=snowflake-ingest-sdk -Dversion=${ingest_sdk_tag} -Dpackaging=jar -DgeneratePom=true"
            }
        }
        stage('SetupDataset') {
            steps {
                script {
                    println('Implement me')
                }
            }
        }
    }
}