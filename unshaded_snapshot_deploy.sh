#!/bin/bash -e

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export GPG_KEY_ID="Snowflake Computing"
export LDAP_USER="$INTERNAL_NEXUS_USERNAME"
export LDAP_PWD="$INTERNAL_NEXUS_PASSWORD"

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
MVN_REPOSITORY_ID=snapshot

cat > $CENTRAL_DEPLOY_SETTINGS_XML << SETTINGS.XML
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

MVN_OPTIONS+=(
  "--settings" "$CENTRAL_DEPLOY_SETTINGS_XML"
  "--batch-mode"
)

echo "[INFO] Deploy to snapshot repository"
project_version=$($THIS_DIR/scripts/get_project_info_from_pom.py $THIS_DIR/pom.xml version)
$THIS_DIR/scripts/update_project_version.py pom.xml $project_version > generated_public_pom.xml

# Allow disabling auto-publish via environment variable
AUTO_PUBLISH=${AUTO_PUBLISH:-true}
mvn deploy ${MVN_OPTIONS[@]} -Dsnapshot-deploy -Dhttp.keepAlive=false -Dnot-shadeDep -Dauto.publish.central=$AUTO_PUBLISH

if [ "$AUTO_PUBLISH" = "false" ]; then
  echo "[INFO] Auto-publish is disabled. Please check https://central.sonatype.org/account to manually publish"
fi

rm $CENTRAL_DEPLOY_SETTINGS_XML