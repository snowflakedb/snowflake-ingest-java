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
OSSRH_DEPLOY_SETTINGS_XML="$THIS_DIR/mvn_settings_ossrh_deploy.xml"
MVN_REPOSITORY_ID=ossrh

# For uploading to local and generate asc files
cat > $OSSRH_DEPLOY_SETTINGS_XML << SETTINGS.XML
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
     xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
  <servers>
    <server>
      <id>ossrh</id>
      <username>$SONATYPE_USER</username>
      <password>$SONATYPE_PWD</password>
    </server>
  </servers>
  <profiles>
      <profile>
        <id>ossrh</id>
        <activation>
          <activeByDefault>true</activeByDefault>
        </activation>
        <properties>
          <gpg.executable>gpg2</gpg.executable>
          <gpg.keyname>$GPG_KEY_ID</gpg.keyname>
          <gpg.passphrase>$GPG_KEY_PASSPHRASE</gpg.passphrase>
        </properties>
      </profile>
    </profiles>
</settings>
SETTINGS.XML

#release to s3
echo "[Info] Release to S3"
mvn --settings $OSSRH_DEPLOY_SETTINGS_XML -DskipTests -Dnot-shadeDep clean package
#remove the first character from version tag if it is 'v'.
[[ $github_version_tag == v* ]] && path=`echo $github_version_tag | cut -c2-` || path=$github_version_tag
cd target
aws s3 cp . s3://sfc-jenkins/repository/snowflake-ingest-sdk-unshaded/$path/ --recursive --exclude "*" --include "*.asc"
aws s3 cp . s3://sfc-jenkins/repository/snowflake-ingest-sdk-unshaded/$path/ --recursive --exclude "*" --include "*.md5"
aws s3 cp . s3://sfc-jenkins/repository/snowflake-ingest-sdk-unshaded/$path/ --recursive --exclude "*" --include "*.sha256"
aws s3 cp . s3://sfc-jenkins/repository/snowflake-ingest-sdk-unshaded/$path/ --recursive --exclude "*" --include "*.zip"
aws s3 cp . s3://sfc-jenkins/repository/snowflake-ingest-sdk-unshaded/$path/ --recursive --exclude "*" --include "*.tar.gz"
aws s3 cp . s3://sfc-jenkins/repository/snowflake-ingest-sdk-unshaded/$path/ --recursive --exclude "*" --include "snowflake-ingest-*.jar"

aws s3 cp . s3://sfc-dev1-data/client/snowflake-ingest-sdk-unshaded/releases/$path/ --recursive --exclude "*" --include "*.asc"
aws s3 cp . s3://sfc-dev1-data/client/snowflake-ingest-sdk-unshaded/releases/$path/ --recursive --exclude "*" --include "*.md5"
aws s3 cp . s3://sfc-dev1-data/client/snowflake-ingest-sdk-unshaded/releases/$path/ --recursive --exclude "*" --include "*.sha256"
aws s3 cp . s3://sfc-dev1-data/client/snowflake-ingest-sdk-unshaded/releases/$path/ --recursive --exclude "*" --include "*.zip"
aws s3 cp . s3://sfc-dev1-data/client/snowflake-ingest-sdk-unshaded/releases/$path/ --recursive --exclude "*" --include "*.tar.gz"
aws s3 cp . s3://sfc-dev1-data/client/snowflake-ingest-sdk-unshaded/releases/$path/ --recursive --exclude "*" --include "snowflake-ingest-*.jar"

rm $OSSRH_DEPLOY_SETTINGS_XML
