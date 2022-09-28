#!/bin/bash -e

# scripts used to check if all dependency is shaded into snowflake internal path

set -o pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
if jar tvf $DIR/../target/snowflake-ingest-sdk.jar  | awk '{print $8}' | grep -v -E "^(net|com)/snowflake" \
    | grep -v -E "(com|net)/\$" | grep -v -E "^META-INF" | grep -v -E "^mozilla" | grep -v mime.types \
    | grep -v project.properties | grep -v -E "javax" | grep -v -E "^org/" | grep -v -E "^com/google" \
    | grep -v -E "^com/sun" | grep -v "log4j.properties" | grep -v "git.properties" | grep -v "io/" \
    | grep -v "codegen/" | grep -v "com/codahale/" | grep -v "com/ibm/" | grep -v "LICENSE" | grep -v "aix/" \
    | grep -v "darwin/" | grep -v "win/" | grep -v "freebsd/" | grep -v "linux/" | grep -v "com/github/" \
    | grep -v -E "shaded/" | grep -v "webapps/"  | grep -v "microsoft/" | grep -v -E "^core-default.xml" \
    | grep -v "yarn-version-info.properties" | grep -v "yarn-version-info.properties" \
    | grep -v "common-version-info.properties" | grep -v "LocalizedFormats_fr.properties" \
    | grep -v "org.apache.hadoop.application-classloader.properties" | grep -v "assets/" | grep -v "ehcache-core.xsd" \
    | grep -v "ehcache-107ext.xsd" | grep -v "parquet.thrift" | grep -v "mapred-default.xml" \
    | grep -v "yarn-default.xml" | grep -v ".keep"  | grep -v "NOTICE" | grep -v "digesterRules.xml" \
    | grep -v "properties.dtd" | grep -v "PropertyList-1.0.dtd"  \
    ; then
  echo "[ERROR] Ingest SDK jar includes class not under the snowflake namespace"
  exit 1
fi