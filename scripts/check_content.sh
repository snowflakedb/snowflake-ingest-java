#!/bin/bash -e

# scripts used to check if all dependency is shaded into snowflake internal path

set -o pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
if jar tvf $DIR/../target/snowflake-ingest-sdk.jar  | awk '{print $8}' | grep -v -E "^(net|com)/snowflake" | grep -v -E "(com|net)/\$" | grep -v -E "^META-INF" | grep -v -E "^mozilla" | grep -v mime.types | grep -v project.properties | grep -v -E "javax" | grep -v -E "^org/" | grep -v -E "^com/google" | grep -v -E "^com/sun" | grep -v "log4j.properties" | grep -v "git.properties" | grep -v "io/" | grep -v "codegen/"; then
  echo "[ERROR] Ingest SDK jar includes class not under the snowflake namespace"
  exit 1
fi