#!/bin/bash

# Exit on any error and enable debug mode
set -e
set -x

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SDK_WRAPPER_DIR="$THIS_DIR/sdk-wrapper"

# Required environment variables check
required_vars=(
  "INTERNAL_NEXUS_USERNAME"
  "INTERNAL_NEXUS_PASSWORD"
  "GPG_KEY_PASSPHRASE"
  "GPG_PRIVATE_KEY"
)

for var in "${required_vars[@]}"; do
  if [ -z "${!var}" ]; then
    echo "[ERROR] Required environment variable $var is not set!"
    exit 1
  fi
done

export GPG_KEY_ID="Snowflake Computing"
export LDAP_USER="$INTERNAL_NEXUS_USERNAME"
export LDAP_PWD="$INTERNAL_NEXUS_PASSWORD"

# Clean up function
cleanup() {
    echo "Cleaning up..."
    if [ -f "$SNAPSHOT_DEPLOY_SETTINGS_XML" ]; then
        rm -f "$SNAPSHOT_DEPLOY_SETTINGS_XML"
    fi
}

# Set up trap for cleanup
trap cleanup EXIT

echo "[INFO] Import PGP Key"
if ! gpg --list-secret-key | grep "$GPG_KEY_ID" > /dev/null 2>&1; then
  if ! gpg --allow-secret-key-import --import "$GPG_PRIVATE_KEY"; then
    echo "[ERROR] Failed to import GPG key"
    exit 1
  fi
fi

# Maven repository settings
SNAPSHOT_DEPLOY_SETTINGS_XML="$THIS_DIR/mvn_settings_snapshot_deploy.xml"
MVN_REPOSITORY_ID=snapshot

# Generate Maven settings file
echo "[INFO] Generating Maven settings file"
cat > $SNAPSHOT_DEPLOY_SETTINGS_XML << SETTINGS.XML
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
     xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
  <servers>
    <server>
      <id>$MVN_REPOSITORY_ID</id>
      <username>$LDAP_USER</username>
      <password>$LDAP_PWD</password>
    </server>
  </servers>
</settings>
SETTINGS.XML

# Default Maven options
MVN_OPTIONS=(
  "--batch-mode"
  "-DskipTests"
  "--settings" "$SNAPSHOT_DEPLOY_SETTINGS_XML"
)

echo "Cleaning up old SDK installations..."
rm -rf ~/.m2/repository/net/snowflake/snowflake-ingest-sdk/

echo "Building SDK..."
cd "$THIS_DIR"
if ! mvn clean package "${MVN_OPTIONS[@]}"; then
    echo "Failed to build SDK"
    exit 1
fi

echo "Installing SDK with perf version..."
if ! mvn install:install-file \
    -Dfile=target/snowflake-ingest-sdk.jar \
    -DgroupId=net.snowflake \
    -DartifactId=snowflake-ingest-sdk \
    -Dversion=perf \
    -Dpackaging=jar; then
    echo "Failed to install SDK"
    exit 1
fi

echo "Building and deploying server wrapper..."
cd "$SDK_WRAPPER_DIR"
if ! mvn clean deploy "${MVN_OPTIONS[@]}" -Dsnapshot-deploy; then
    echo "[ERROR] Failed to deploy server wrapper"
    exit 1
fi

echo "=== Deployment Complete ==="
echo "Server JAR location: $SDK_WRAPPER_DIR/target/sdk-wrapper-1.0-SNAPSHOT.jar"
echo "Deployed to Nexus: sdk-wrapper-1.0-SNAPSHOT.jar"