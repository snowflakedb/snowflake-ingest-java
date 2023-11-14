pipeline {
    agent {
        node {
            label "regular-memory-node-c7"
        }
    }
    stages {
        stage('Checkout') {
            steps {
                echo "The current workspace is ${env.Workspace}"
                echo "hallo"
                script {
                    sh "ls" // we are in streaming-ingest-benchmark repo
                }
            }
        }
    }
}