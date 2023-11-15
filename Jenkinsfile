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
                echo "passed param: $tpcds_scale_factor"
            }
        }
    }
}