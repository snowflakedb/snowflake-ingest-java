#!/usr/bin/env bash

set -euo pipefail

## This script tests the SDK JARs end-to-end, i.e. not using integration tests from within the project, but from an
## external Maven project, which depends on the SDK deployed into the local maven repository. The following SDK variants are tested:
## 1. Shaded jar
## 2. Unshaded jar
## 3. FIPS-compliant jar, i.e. unshaded jar without snowflake-jdbc and bouncy castle dependencies, but with snowflake-jdbc-fips depedency

maven_repo_dir=$(mvn help:evaluate -Dexpression=settings.localRepository -q -DforceStdout)
sdk_repo_dir="${maven_repo_dir}/net/snowflake/snowflake-ingest-sdk"

cp profile.json e2e-jar-test/standard
cp profile.json e2e-jar-test/fips

echo "###################"
echo "# TEST SHADED JAR #"
echo "###################"

# Remove the SDK from local maven repository
rm -fr $sdk_repo_dir

# Prepare pom.xml for shaded JAR
project_version=$(./scripts/get_project_info_from_pom.py pom.xml version)
./scripts/update_project_version.py public_pom.xml $project_version > generated_public_pom.xml

# Build shaded SDK always with Java 8
echo "Building shaded SDK"
mvn clean package -DskipTests=true --batch-mode --show-version

# Install shaded SDK JARs into local maven repository
mvn install:install-file -Dfile=target/snowflake-ingest-sdk.jar -DpomFile=generated_public_pom.xml

# Run e2e tests with all supported LTS Java versions
cd e2e-jar-test

echo "Testing shaded JAR with Java 8"
mvn --show-version clean verify -pl standard -am # Test with Java 8

echo "Testing shaded JAR with Java 11"
JAVA_HOME="${JAVA_HOME_11_X64}" mvn --show-version clean verify -pl standard -am # Test with Java 11

echo "Testing shaded JAR with Java 17"
JAVA_HOME="${JAVA_HOME_17_X64}" mvn --show-version clean verify -pl standard -am # Test with Java 17

echo "Testing shaded JAR with Java 21"
JAVA_HOME="${JAVA_HOME_21_X64}" mvn --show-version clean verify -pl standard -am # Test with Java 21
cd ..

echo "#####################"
echo "# TEST UNSHADED JAR #"
echo "#####################"

# Remove the SDK from local maven repository
rm -r $sdk_repo_dir

# Install unshaded SDK into local maven repository with Java 8
echo "Building unshaded SDK"
mvn clean install -Dnot-shadeDep -DskipTests=true --batch-mode --show-version

# Run e2e tests with all supported LTS Java versions
cd e2e-jar-test

echo "Testing unshaded JAR with Java 8"
mvn --show-version clean verify -pl standard -am # Test with Java 8

echo "Testing unshaded JAR with Java 11"
JAVA_HOME="${JAVA_HOME_11_X64}" mvn --show-version clean verify -pl standard -am # Test with Java 11

echo "Testing unshaded JAR with Java 17"
JAVA_HOME="${JAVA_HOME_17_X64}" mvn --show-version clean verify -pl standard -am # Test with Java 17

echo "Testing unshaded JAR with Java 21"
JAVA_HOME="${JAVA_HOME_21_X64}" mvn --show-version clean verify -pl standard -am # Test with Java 21
cd ..

echo "##############"
echo "# TEST FIPS #"
echo "##############"
# Remove the SDK from local maven repository
rm -r $sdk_repo_dir

# Install unshaded SDK into local maven repository with Java
echo "Building unshaded SDK for FIPS"
mvn clean install -Dnot-shadeDep -DskipTests=true --batch-mode --show-version

# Run e2e tests on the FIPS module with all supported LTS Java versions
cd e2e-jar-test

echo "Testing FIPS JAR with Java 8"
mvn --show-version clean verify -pl standard -am # Test with Java 8

echo "Testing FIPS JAR with Java 11"
JAVA_HOME="${JAVA_HOME_11_X64}" mvn --show-version clean verify -pl standard -am # Test with Java 11

echo "Testing FIPS JAR with Java 17"
JAVA_HOME="${JAVA_HOME_17_X64}" mvn --show-version clean verify -pl standard -am # Test with Java 17

echo "Testing FIPS JAR with Java 21"
JAVA_HOME="${JAVA_HOME_21_X64}" mvn --show-version clean verify -pl standard -am # Test with Java 21
cd ..
