#!/usr/bin/env bash

set -euo pipefail

maven_repo_dir=$(mvn help:evaluate -Dexpression=settings.localRepository -q -DforceStdout)
sdk_repo_dir="${maven_repo_dir}/net/snowflake/snowflake-ingest-sdk"

## This script tests the SDK JARs end-to-end, i.e. not using integration tests from within the project, but from an
## external Maven project, which depends on the SDK deployed into the local maven repository. The following SDK variants are tested:
## 1. Shaded jar
## 2. Unshaded jar
## 3. FIPS-compliant jar, i.e. unshaded jar without snowflake-jdbc and bouncy castle dependencies, but with snowflake-jdbc-fips depedency

cp profile.json e2e-jar-test/standard
cp profile.json e2e-jar-test/fips

test_type="${test_type:-}"
java_path_for_test_execution="${!java_path_env_var}"

# Cleanup ingest SDK in the local maven repository
rm -fr "${sdk_repo_dir}"

if [[ "${test_type}" == "shaded" ]]; then
  echo "###################"
  echo "# TEST SHADED JAR #"
  echo "###################"

  # Prepare pom.xml for shaded JAR
  project_version=$(./scripts/get_project_info_from_pom.py pom.xml version)
  ./scripts/update_project_version.py public_pom.xml $project_version > generated_public_pom.xml

  # Build shaded SDK always with Java 8
  echo "Building shaded SDK"
  JAVA_HOME="${JAVA_HOME_8_X64}" mvn clean package -DskipTests=true --batch-mode --show-version

  # Install shaded SDK JARs into local maven repository
  JAVA_HOME="${JAVA_HOME_8_X64}" mvn install:install-file -Dfile=target/snowflake-ingest-sdk.jar -DpomFile=generated_public_pom.xml

  # Run e2e tests with all supported LTS Java versions
  cd e2e-jar-test

  echo "Testing shaded JAR with ${java_path_for_test_execution}"
  JAVA_HOME="${java_path_for_test_execution}" mvn --show-version clean verify -pl standard -am
  cd ..
elif [[ "${test_type}" == "unshaded" ]]; then
  echo "#####################"
  echo "# TEST UNSHADED JAR #"
  echo "#####################"

  # Install unshaded SDK into local maven repository with Java 8
  echo "Building unshaded SDK"
  JAVA_HOME="${JAVA_HOME_8_X64}" mvn clean install -Dnot-shadeDep -DskipTests=true --batch-mode --show-version

  # Run e2e tests with all supported LTS Java versions
  cd e2e-jar-test

  echo "Testing unshaded JAR with ${java_path_for_test_execution}"
  JAVA_HOME="${java_path_for_test_execution}" mvn --show-version clean verify -pl standard -am
  cd ..
elif [[ "${test_type}" == "fips" ]]; then
  echo "##############"
  echo "# TEST FIPS #"
  echo "##############"
  # Install unshaded SDK into local maven repository with Java
  echo "Building unshaded SDK for FIPS"
  JAVA_HOME="${JAVA_HOME_8_X64}" mvn clean install -Dnot-shadeDep -DskipTests=true --batch-mode --show-version

  # Run e2e tests on the FIPS module with all supported LTS Java versions
  cd e2e-jar-test

  echo "Testing FIPS JAR with ${java_path_for_test_execution}"
  JAVA_HOME="${java_path_for_test_execution}" mvn --show-version clean verify -pl fips -am
  cd ..
else
  echo "Unsupported value of \$test_type variable: \"${test_type}\"" >&2
  exit 1
fi





