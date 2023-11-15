pipeline {
    agent {
        node {
            label "parallelizable-c7"
        }
    }
    environment {
        jenkins_github_credential_id = 'b4f59663-ae0a-4384-9fdc-c7f2fe1c4fca'

        ingest_sdk_dir = 'snowflake-ingest-java'
        ingest_sdk_tag = sh(returnStdout: true, script: "cd $ingest_sdk_dir && git describe --tags").trim()

        setup_git_remote = 'https://github.com/snowflakedb/streaming-ingest-benchmark.git'
        setup_git_specifier = 'main'
        setup_relative_dir = 'streaming-ingest-benchmark'
        setup_reference = "/mnt/jenkins/git_repo/streaming-ingest-benchmark"
    }
    stages {
        stage('CheckoutSetupApplication') {
            steps {
                checkout(changelog: false,
                        poll: false,
                        scm: [$class: 'GitSCM',
                              branches: [[name: setup_git_specifier]],
                              doGenerateSubmoduleConfigurations: false,
                              extensions: [[$class: 'CloneOption', honorRefspec: true, noTags: false, reference: setup_reference, shallow: false],
                                           [$class: 'RelativeTargetDirectory', relativeTargetDir: setup_relative_dir]],
                              submoduleCfg: [],
                              userRemoteConfigs: [[refspec: '+refs/heads/*:refs/remotes/origin/* +refs/pull/*/head:refs/remotes/origin/pr/*', url: setup_git_remote, credentialsId: jenkins_github_credential_id]]
                        ]
                )
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