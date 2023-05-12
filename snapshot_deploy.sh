#!/bin/bash -e

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export GPG_KEY_ID="Snowflake Computing"
export SONATYPE_USER="$sonatype_user"
export SONATYPE_PWD="$sonatype_password"

if [ -z "$GPG_KEY_PASSPHRASE" ]; then
  echo "[ERROR] GPG passphrase is not specified for $GPG_KEY_ID!"
  exit 1
fi

if [ -z "$GPG_PRIVATE_KEY" ]; then
  echo "[ERROR] GPG private key file is not specified!"
  exit 1
fi

echo "[INFO] Import PGP Key"
if ! gpg --list-secret-key | grep "$GPG_KEY_ID"; then
  gpg --allow-secret-key-import --import "$GPG_PRIVATE_KEY"
fi
# copy the settings.xml template and inject credential information
SNAPSHOT_DEPLOY_SETTINGS_XML="$THIS_DIR/mvn_settings_snapshot_deploy.xml"
MVN_REPOSITORY_ID=snapshot

cat > $SNAPSHOT_DEPLOY_SETTINGS_XML << SETTINGS.XML
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
     xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
  <servers>
    <server>
      <id>$MVN_REPOSITORY_ID</id>
      <username>$SONATYPE_USER</username>
      <password>$SONATYPE_PWD</password>
    </server>
  </servers>
</settings>
SETTINGS.XML

MVN_OPTIONS+=(
  "--settings" "$SNAPSHOT_DEPLOY_SETTINGS_XML"
  "--batch-mode"
)

echo "[Info] Sign package and deploy to staging area"
project_version=$($THIS_DIR/scripts/get_project_info_from_pom.py $THIS_DIR/pom.xml version)
$THIS_DIR/scripts/update_project_version.py public_pom.xml $project_version > generated_public_pom.xml

mvn deploy ${MVN_OPTIONS[@]} -Dsnapshot-deploy

echo "[INFO] Close and Release"
snowflake_repositories=$(mvn ${MVN_OPTIONS[@]} \
    org.sonatype.plugins:nexus-staging-maven-plugin:1.6.7:rc-list \
    -DserverId=$MVN_REPOSITORY_ID versions:set -DnewVersion=$project_version-SNAPSHOT
    -DnexusUrl=https://nexus.int.snowflakecomputing.com/ | grep net.snowflake | awk '{print $2}')
IFS=" "
if (( $(echo $snowflake_repositories | wc -l)!=1 )); then
    echo "[ERROR] Not single netsnowflake repository is staged. Login https://nexus.int.snowflakecomputing.com/ and make sure no netsnowflake remains there."
    exit 1
fi
if ! mvn ${MVN_OPTIONS[@]} \
    org.sonatype.plugins:nexus-staging-maven-plugin:1.6.7:rc-close \
    -DserverId=$MVN_REPOSITORY_ID versions:set -DnewVersion=$project_version-SNAPSHOT\
    -DnexusUrl=https://nexus.int.snowflakecomputing.com/ \
    -DstagingRepositoryId=$snowflake_repositories \
    -DstagingDescription="Automated Close"; then
    echo "[ERROR] Failed to close. Fix the errors and try this script again"
    mvn ${MVN_OPTIONS[@]} \
        nexus-staging:rc-drop \
        -DserverId=$MVN_REPOSITORY_ID versions:set -DnewVersion=$project_version-SNAPSHOT\
        -DnexusUrl=https://nexus.int.snowflakecomputing.com/ \
        -DstagingRepositoryId=$snowflake_repositories \
        -DstagingDescription="Failed to close. Dropping..."
fi

mvn ${MVN_OPTIONS[@]} \
    org.sonatype.plugins:nexus-staging-maven-plugin:1.6.7:rc-release \
    -DserverId=$MVN_REPOSITORY_ID versions:set -DnewVersion=$project_version-SNAPSHOT\
    -DnexusUrl=https://nexus.int.snowflakecomputing.com/ \
    -DstagingRepositoryId=$snowflake_repositories \
    -DstagingDescription="Automated Release"

rm $SNAPSHOT_DEPLOY_SETTINGS_XML