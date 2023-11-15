pipeline {
    agent {
        node {
            label "parallelizable-c7"
        }
    }
    stages {
        stage('Checkout') {
            steps {
                echo "The current workspace is ${env.Workspace}"
                echo "test change"
                script {
                    sh "ls" // we are in streaming-ingest-benchmark repo
                }
            }
        }
    }
}