#!/bin/bash

# Script to build and setup the Snowflake Ingest Java Server

# Exit on any error
set -e

echo "=== Building Snowflake Streaming Ingest Server ==="

# Get the absolute path of the script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SDK_ROOT="$SCRIPT_DIR/.."
echo "SDK_ROOT: $SDK_ROOT"

echo "Cleaning up old SDK installations..."
rm -rf ~/.m2/repository/net/snowflake/snowflake-ingest-sdk/

echo "Building SDK..."
cd "$SDK_ROOT"
mvn clean package -DskipTests

echo "Installing SDK with perf version..."
mvn install:install-file \
    -Dfile=target/snowflake-ingest-sdk.jar \
    -DgroupId=net.snowflake \
    -DartifactId=snowflake-ingest-sdk \
    -Dversion=perf \
    -Dpackaging=jar

echo "Building server application..."
cd "$SCRIPT_DIR"
mvn clean package -DskipTests

