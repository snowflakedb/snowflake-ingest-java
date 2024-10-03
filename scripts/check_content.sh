#!/bin/bash -e

# scripts used to check if all dependency is shaded into snowflake internal path
# Do not add any additional exceptions into this script.
# If this script fails the maven build, re-configure shading relocations in pom.xml.

set -o pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

if jar tvf $DIR/../target/snowflake-ingest-sdk.jar  | awk '{print $8}' | \
    # Ignore directories
    grep -v -E '/$' | \

    # Snowflake top-level packages are allowed
    grep -v -E "^net/snowflake/ingest" | \

    # META-INF is allowed in the shaded JAR
    grep -v -E "^META-INF" | \

    # Files in the JAR root that are probably required
    grep -v mime.types | \
    grep -v project.properties | \
    grep -v arrow-git.properties | \
    grep -v core-default.xml | \
    grep -v org.apache.hadoop.application-classloader.properties | \
    grep -v common-version-info.properties | \
    grep -v PropertyList-1.0.dtd | \
    grep -v properties.dtd | \
    grep -v parquet.thrift | \
    grep -v assets/org/apache/commons/math3/random/new-joe-kuo-6.1000 | \

    # Native zstd libraries are allowed
    grep -v -E '^darwin' | \
    grep -v -E '^freebsd' | \
    grep -v -E '^linux' | \
    grep -v -E '^win' ; then

  echo "[ERROR] JDBC jar includes class not under the snowflake namespace"
  exit 1
fi