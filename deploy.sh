#!/bin/bash -e

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ -z "$GPG_KEY_ID" ]; then
  echo "[ERROR] Key Id not specified!"
  exit 1
fi

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
OSSRH_DEPLOY_SETTINGS_XML="$THIS_DIR/mvn_settings_ossrh_deploy.xml"
MVN_REPOSITORY_ID=ossrh

cat > $OSSRH_DEPLOY_SETTINGS_XML << SETTINGS.XML
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
     xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
  <profiles>
    <profile>
      <id>internal-maven</id>
      <repositories>
        <repository>
          <id>central</id>
          <name>Internal Maven Repository</name>
          <url>https://artifactory.int.snowflakecomputing.com/artifactory/development-maven-virtual</url>
        </repository>
        <repository>
          <id>deployment</id>
          <name>Internal Releases</name>
          <url>https://nexus.int.snowflakecomputing.com/repository/Releases/</url>
        </repository>
      </repositories>
      <pluginRepositories>
        <pluginRepository>
          <id>central</id>
          <name>Internal Maven Repository</name>
          <url>https://artifactory.int.snowflakecomputing.com/artifactory/development-maven-virtual</url>
        </pluginRepository>
        <pluginRepository>
          <id>deployment</id>
          <name>Internal Releases</name>
          <url>https://nexus.int.snowflakecomputing.com/repository/Releases/</url>
        </pluginRepository>
      </pluginRepositories>
    </profile>
  </profiles>
  <servers>
    <server>
      <id>$MVN_REPOSITORY_ID</id>
      <username>$SONATYPE_USER</username>
      <password>$SONATYPE_PWD</password>
    </server>
  </servers>
  <activeProfiles>
    <activeProfile>internal-maven</activeProfile>
  </activeProfiles>
</settings>
SETTINGS.XML

MVN_OPTIONS+=(
  "--settings" "$OSSRH_DEPLOY_SETTINGS_XML"
  "--batch-mode"
)

echo "[Info] Sign package and deploy to staging area"
project_version=$($THIS_DIR/scripts/get_project_info_from_pom.py $THIS_DIR/pom.xml version)
$THIS_DIR/scripts/update_project_version.py public_pom.xml $project_version > generated_public_pom.xml

mvn deploy ${MVN_OPTIONS[@]} -Dossrh-deploy -Dmaven.wagon.http.pool=false -X -e

echo "[INFO] Close and Release"
snowflake_repositories=$(mvn ${MVN_OPTIONS[@]} \
    org.sonatype.plugins:nexus-staging-maven-plugin:1.6.7:rc-list \
    -DserverId=$MVN_REPOSITORY_ID \
    -DnexusUrl=https://oss.sonatype.org/ | grep netsnowflake | awk '{print $2}')
IFS=" "
if (( $(echo $snowflake_repositories | wc -l)!=1 )); then
    echo "[ERROR] Not single netsnowflake repository is staged. Login https://oss.sonatype.org/ and make sure no netsnowflake remains there."
    exit 1
fi
if ! mvn ${MVN_OPTIONS[@]} \
    org.sonatype.plugins:nexus-staging-maven-plugin:1.6.7:rc-close \
    -DserverId=$MVN_REPOSITORY_ID \
    -DnexusUrl=https://oss.sonatype.org/ \
    -DstagingRepositoryId=$snowflake_repositories \
    -DstagingDescription="Automated Close"; then
    echo "[ERROR] Failed to close. Fix the errors and try this script again"
    mvn ${MVN_OPTIONS[@]} \
        nexus-staging:rc-drop \
        -DserverId=$MVN_REPOSITORY_ID \
        -DnexusUrl=https://oss.sonatype.org/ \
        -DstagingRepositoryId=$snowflake_repositories \
        -DstagingDescription="Failed to close. Dropping..."
fi

mvn ${MVN_OPTIONS[@]} \
    org.sonatype.plugins:nexus-staging-maven-plugin:1.6.7:rc-release \
    -DserverId=$MVN_REPOSITORY_ID \
    -DnexusUrl=https://oss.sonatype.org/ \
    -DstagingRepositoryId=$snowflake_repositories \
    -DstagingDescription="Automated Release"

rm $OSSRH_DEPLOY_SETTINGS_XML