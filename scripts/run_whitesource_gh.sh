#!/usr/bin/env bash
#
# Run whitesource for components which need versioning

# SCAN_DIRECTORIES is a comma-separated list (as a string) of file paths which contain all source code and build artifacts for this project
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONNECTOR_DIR="$( dirname "${THIS_DIR}")"
SCAN_DIRECTORIES="${CONNECTOR_DIR}"
echo "SCAN_DIRECTORIES:"$SCAN_DIRECTORIES

# If your PROD_BRANCH is not master, you can define it here based on the need
PROD_BRANCH="master"

PRODUCT_NAME="snowflake-ingest-java"

PROJECT_VERSION=${GITHUB_SHA}

BRANCH_OR_PR_NUMBER="$(echo "${GITHUB_REF}" | awk 'BEGIN { FS = "/" } ; { print $3 }')"

echo "PROJECT_VERSION:"$PROJECT_VERSION
echo "BRANCH_OR_PR_NUMBER:"$BRANCH_OR_PR_NUMBER

# GITHUB_EVENT_NAME should either be 'push', or 'pull_request'
if [[ "$GITHUB_EVENT_NAME" == "pull_request" ]]; then
    echo "[INFO] Pull Request"
    export PROJECT_NAME="PR-${BRANCH_OR_PR_NUMBER}"
elif [[ "${BRANCH_OR_PR_NUMBER}" == "$PROD_BRANCH" ]]; then
    echo "[INFO] Production branch"
    export PROJECT_NAME="$PROD_BRANCH"
else
    echo "[INFO] Non Production branch. Skipping wss..."
    export PROJECT_NAME=""
fi

echo "PROJECT_NAME:"$PROJECT_NAME

[[ -z "$WHITESOURCE_API_KEY" ]] && echo "[WARNING] No WHITESOURCE_API_KEY is set. No WhiteSource scan will occurr." && exit 0

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

if [[ "$PROJECT_NAME" == "$PROD_BRANCH" ]]; then
    echo "PRODUCTION"
    java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
        -c ${SCAN_CONFIG} \
        -project ${PROJECT_NAME} \
        -product ${PRODUCT_NAME} \
        -d ${SCAN_DIRECTORIES} \
        -projectVersion ${PROJECT_VERSION} \
        -offline true
        ERR=$?
        if [[ "$ERR" != "254" && "$ERR" != "0" ]]; then
            echo "failed to run wss for PROJECT_VERSION=${PROJECT_VERSION} in ${PROJECT_VERSION}..."
            exit 1
        fi
    java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
        -c ${SCAN_CONFIG} \
        -project ${PROJECT_NAME} \
        -product ${PRODUCT_NAME} \
        -projectVersion baseline \
        -requestFiles whitesource/update-request.txt
        ERR=$?
        if [[ "$ERR" != "254" && "$ERR" != "0" ]]; then
            echo "failed to run wss for PROJECT_VERSION=${PROJECT_VERSION} in baseline"
            exit 1
        fi
    java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
        -c ${SCAN_CONFIG} \
        -project ${PROJECT_NAME} \
        -product ${PRODUCT_NAME} \
        -projectVersion ${PROJECT_VERSION} \
        -requestFiles whitesource/update-request.txt
        ERR=$?
        if [[ "$ERR" != "254" && "$ERR" != "0" ]]; then
            echo "failed to run wss for PROJECT_VERSION=${PROJECT_VERSION} in ${PROJECT_VERSION}"
            exit 1
        fi
elif [[ -n "$PROJECT_NAME" ]]; then
    echo "PR"
    java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
        -c ${SCAN_CONFIG} \
        -project ${PROJECT_NAME} \
        -product ${PRODUCT_NAME} \
        -d ${SCAN_DIRECTORIES} \
        -projectVersion ${PROJECT_VERSION}
    ERR=$?
    if [[ "$ERR" != "254" && "$ERR" != "0" ]]; then
        echo "failed to run wss for PROJECT_VERSION=${PROJECT_VERSION} in ${PROJECT_VERSION}"
        exit 1
    fi
fi
exit 0
