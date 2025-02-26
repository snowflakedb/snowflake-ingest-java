#!/usr/bin/env bash

set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

GOOGLE_FORMAT_VERSION="1.20.0"
DOWNLOAD_URL="https://github.com/google/google-java-format/releases/download/v${GOOGLE_FORMAT_VERSION}/google-java-format-${GOOGLE_FORMAT_VERSION}-all-deps.jar"
JAR_FILE="./.cache/google-java-format-${GOOGLE_FORMAT_VERSION}-all-deps.jar"

if [ ! -f "${JAR_FILE}" ]; then
  mkdir -p "$(dirname "${JAR_FILE}")"
  echo "Downloading Google Java format to ${JAR_FILE}"
  curl -# -L --fail "${DOWNLOAD_URL}" --output "${JAR_FILE}"
fi

if ! command -v java > /dev/null; then
  echo "Java not installed."
  exit 1
fi

echo "Sorting pom.xml"
mvn com.github.ekryd.sortpom:sortpom-maven-plugin:sort
(cd e2e-jar-test && mvn com.github.ekryd.sortpom:sortpom-maven-plugin:sort)

echo "Formatting java sources"
if ! find ./ -type f -name "*.java" -print0 | xargs -0 java -jar "${JAR_FILE}" --replace --set-exit-if-changed; then
  echo "Java sources were not properly formatted"
  exit 1
fi
