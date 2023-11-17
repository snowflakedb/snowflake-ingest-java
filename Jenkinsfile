def scale_factor_system_prop = tpcds_scale_factor ? "-Dscale_factor=${tpcds_scale_factor}" : ""
def database_system_prop = database ? "-Ddatabase=${database}" : ""
def account_system_prop = account ? "-Daccount=${account}" : ""
def port_system_prop = db_port ? "-Dport=${db_port}" : ""
def host_system_prop = host ? "-Dhost=${host}" : ""

pipeline {
    agent {
        node {
            label "parallelizable-c7"
        }
    }
    environment {
        jenkins_github_credential_id = 'b4f59663-ae0a-4384-9fdc-c7f2fe1c4fca'
        jenkins_cred_id_profile_decryption = '223990f0-cceb-449a-9275-83aa58662224'

        ingest_sdk_dir = "${WORKSPACE}/snowflake-ingest-java"
        ingest_sdk_tag = sh(returnStdout: true, script: "cd $ingest_sdk_dir && git describe --tags").trim()

        setup_git_remote = 'https://github.com/snowflakedb/streaming-ingest-benchmark.git'
        setup_dir = "${WORKSPACE}/streaming-ingest-benchmark"
        setup_git_specifier = 'main'
        setup_reference = "/mnt/jenkins/git_repo/streaming-ingest-benchmark"
    }
    stages {
        stage('CheckoutSetupApplication') {
            steps {
                deleteDir(setup_dir)
                checkout(changelog: false,
                        poll: false,
                        scm: [$class: 'GitSCM',
                              branches: [[name: setup_git_specifier]],
                              doGenerateSubmoduleConfigurations: false,
                              extensions: [[$class: 'CloneOption', honorRefspec: true, noTags: false, reference: setup_reference, shallow: false],
                                           [$class: 'RelativeTargetDirectory', relativeTargetDir: setup_dir]],
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
                sh "mvn -f ${setup_dir} -Dingest-sdk-version=${ingest_sdk_tag} clean compile assembly:single"
            }
        }
        stage('SetupDataset') {
            steps {
                dir(setup_dir) {
                    withCredentials([string(credentialsId: jenkins_cred_id_profile_decryption, variable: "DECRYPTION_PASSPHRASE")]) {
                        sh "gpg --passphrase \$DECRYPTION_PASSPHRASE --batch --output profile.json --decrypt profile.json.gpg"
                    }
                    withCredentials([usernamePassword(credentialsId: jenkins_deployment_credential_id, usernameVariable: 'USER_KEY', passwordVariable: 'PASSWORD_KEY')]) {
                        sh "java ${scale_factor_system_prop} ${database_system_prop} ${account_system_prop} ${port_system_prop} ${host_system_prop} -Djdbc_user=\\$USER_KEY -Dpassword=\\$PASSWORD_KEY -jar target/streaming-ingest-benchmark.jar"
                    }
                }
            }
        }
    }
}