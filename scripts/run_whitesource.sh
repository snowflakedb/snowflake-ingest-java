#!/usr/bin/env bash
#
# Run whitesource for components which need versioning

# SCAN_DIRECTORIES is a comma-separated list (as a string) of file paths which contain all source code and build artifacts for this project
echo "PWD:"${PWD}
SCAN_DIRECTORIES=${PWD}
echo "SCAN_DIRECTORIES:"$SCAN_DIRECTORIES


if [[ "$PWD" == *travis* ]]; then
   export PROJECT_VERSION=${TRAVIS_COMMIT}
   # if it is not a Pull request, replace PROJECT_NAME with branch Name
   if [[ -z "${TRAVIS_PULL_REQUEST}" ]]; then
       echo "TRAVIS_PULL_REQUEST is empty, using branch name"
       export PROJECT_NAME=${TRAVIS_BRANCH}
   else
       export PROJECT_NAME=${TRAVIS_PULL_REQUEST}
   fi
else
   export PROJECT_VERSION=${GIT_COMMIT}
   export PROJECT_NAME=${GIT_BRANCH}
fi
echo "PROJECT_VERSION:"$PROJECT_VERSION
echo "PROJECT_NAME:"$PROJECT_NAME
echo "GIT_COMMIT:"$GIT_COMMIT
echo "TRAVIS_COMMIT:"$TRAVIS_COMMIT

[[ -z "$WHITESOURCE_API_KEY" ]] && echo "[WARNING] No WHITESOURCE_API_KEY is set. No WhiteSource scan will occurr." && exit 0

# If your PROD_BRANCH is not master, you can define it here based on the need
PROD_BRANCH="master"

# Please refer to product naming convension on whitesource integration guide
PRODUCT_NAME="snowflake-ingest-java"

#if [[ -z "${JOB_BASE_NAME}" ]]; then
#   echo "[ERROR] No JOB_BASE_NAME is set. Run this on Jenkins"
#   exit 0
#fi

# Download the latest whitesource unified agent to do the scanning if there is no existing one
curl -LO https://github.com/whitesource/unified-agent-distribution/releases/latest/download/wss-unified-agent.jar
if [[ ! -f "wss-unified-agent.jar" ]]; then
    echo "failed to download whitesource unified agent"
    # if you want to fail the build when failing to download whitesource scanning agent, please use exit 1 
    # exit 1
fi

# whitesource will scan the folder and detect the corresponding configuration
# configuration file wss-generated-file.config will be generated under ${SCAN_DIRECTORIES}
# java -jar wss-unified-agent.jar -detect -d ${SCAN_DIRECTORIES}
# SCAN_CONFIG="${SCAN_DIRECTORIES}/wss-generated-file.config"

# SCAN_CONFIG is the path to your whitesource configuration file
SCAN_CONFIG="scripts/wss-java-maven-agent.config"
echo "SCAN_CONFIG:"$SCAN_CONFIG

echo "[INFO] Running wss.sh for ${PRODUCT_NAME}-${PROJECT_NAME} under ${SCAN_DIRECTORIES}"

if [ "$GIT_BRANCH" != "$PROD_BRANCH" ]; then
    echo "First Branch"
    java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
        -c ${SCAN_CONFIG} \
        -project ${PROJECT_NAME} \
        -product ${PRODUCT_NAME} \
        -d ${SCAN_DIRECTORIES} \
        -projectVersion ${PROJECT_VERSION}
    ERR=$?
    if [[ "$ERR" != "254" && "$ERR" != "0" ]]; then
        echo "failed to run whitesource scanning with feature branch"
        # if you want to fail the build when failing to run whitesource with projectName feature branch, please use exit 1 
        # exit 1
    fi 
else
    echo "SECOND BRANCH"
    java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
        -c ${SCAN_CONFIG} \
        -project ${PROJECT_NAME} \
        -product ${PRODUCT_NAME} \
        -d ${SCAN_DIRECTORIES} \
        -projectVersion ${PROJECT_VERSION} \
        -offline true
        ERR=$?
        if [[ "$ERR" != "254" && "$ERR" != "0" ]]; then
            echo "failed to run whitesource scanning in offline mode"
            # if you want to fail the build when failing to run whitesource scanning with offline mode, please use exit 1 
            # exit 1
        fi
    java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
        -c ${SCAN_CONFIG} \
        -project ${PROJECT_NAME} \
        -product ${PRODUCT_NAME} \
        -projectVersion baseline \
        -requestFiles whitesource/update-request.txt 
        ERR=$?
        if [[ "$ERR" != "254" && "$ERR" != "0" ]]; then
            echo "failed to run whitesource scanning with projectName baseline of master branch"
            # if you want to fail the build when failing to run whitesource with projectName baseline, please use exit 1 
            # exit 1
        fi
    java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
        -c ${SCAN_CONFIG} \
        -project ${PROJECT_NAME} \
        -product ${PRODUCT_NAME} \
        -projectVersion ${PROJECT_VERSION} \
        -requestFiles whitesource/update-request.txt
        ERR=$?
        if [[ "$ERR" != "254" && "$ERR" != "0" ]]; then
            echo "failed to run whitesource scanning with projectName GIT_COMMIT of master branch"
            # if you want to fail the build when failing to run whitesource with projectName GIT_COMMIT, please use exit 1
            # exit 1
        fi 
fi
exit 0
