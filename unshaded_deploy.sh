#!/bin/bash -e

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export GPG_KEY_ID="Snowflake Computing"
export SONATYPE_USER="$sonatype_user"
export SONATYPE_PWD="$sonatype_password"

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
CENTRAL_DEPLOY_SETTINGS_XML="$THIS_DIR/mvn_settings_central_deploy.xml"
MVN_REPOSITORY_ID=ossrh

cat > $CENTRAL_DEPLOY_SETTINGS_XML << SETTINGS.XML
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
          <n>Internal Maven Repository</n>
          <url>https://artifactory.int.snowflakecomputing.com/artifactory/development-maven-virtual</url>
        </repository>
        <repository>
          <id>deployment</id>
          <n>Internal Releases</n>
          <url>https://nexus.int.snowflakecomputing.com/repository/Releases/</url>
        </repository>
      </repositories>
      <pluginRepositories>
        <pluginRepository>
          <id>central</id>
          <n>Internal Maven Repository</n>
          <url>https://artifactory.int.snowflakecomputing.com/artifactory/development-maven-virtual</url>
        </pluginRepository>
        <pluginRepository>
          <id>deployment</id>
          <n>Internal Releases</n>
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
  "--settings" "$CENTRAL_DEPLOY_SETTINGS_XML"
  "--batch-mode"
)

echo "[INFO] mvn clean compile"
mvn clean compile ${MVN_OPTIONS[@]} -Dnot-shadeDep

echo "[INFO] mvn dependency resolve"
mvn dependency:resolve dependency:resolve-plugins dependency:go-offline -DmanualInclude=org.apache.maven:maven-archiver:pom:2.5 ${MVN_OPTIONS[@]} -Dnot-shadeDep

echo "[INFO] mvn test"
mvn test ${MVN_OPTIONS[@]} -Dnot-shadeDep

echo "[Info] Deploy to Central Publisher Portal"
project_version=$($THIS_DIR/scripts/get_project_info_from_pom.py $THIS_DIR/pom.xml version)
$THIS_DIR/scripts/update_project_version.py public_pom.xml $project_version-unshaded > generated_public_pom.xml

# Allow disabling auto-publish via environment variable
AUTO_PUBLISH=${AUTO_PUBLISH:-true}
mvn deploy ${MVN_OPTIONS[@]} -Dossrh-deploy -Dhttp.keepAlive=false -Dnot-shadeDep -Dauto.publish.central=$AUTO_PUBLISH

echo "[INFO] Publishing to Maven Central via Central Publisher Portal"
if [ "$AUTO_PUBLISH" = "true" ]; then
  echo "[INFO] The central-publishing-maven-plugin handles publishing automatically"
else
  echo "[INFO] Auto-publish is disabled. Please check https://central.sonatype.org/account to manually publish"
fi
echo "[INFO] Check https://central.sonatype.org/account for publication status"

rm $CENTRAL_DEPLOY_SETTINGS_XML
