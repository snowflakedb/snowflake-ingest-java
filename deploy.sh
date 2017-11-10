#!/bin/bash -e

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ -z "$GPG_KEY_ID" ]; then
  echo "[ERROR] Key Id not specified!"
  exit 1
else
# name can include spaces
  MVN_GPG_OPTIONS+=("\"-Dgpg.keyname=$GPG_KEY_ID\"")
fi

if [ -z "$GPG_KEY_PASSPHRASE" ]; then
  echo "[ERROR] GPG passphrase is not specified for $GPG_KEY_ID!"
  exit 1
else
  MVN_GPG_OPTIONS+=("-Dgpg.passphrase=$GPG_KEY_PASSPHRASE")
fi

if [ -z "$GPG_PRIVATE_KEY"]
  echo "[ERROR] GPG private key file is not specified!"
  exit 1
fi

echo "[INFO] Import PGP Key"
if ! gpg --list-secret-key | grep "$GPG_KEY_ID"; then
  gpg --allow-secret-key-import --import "$GPG_PRIVATE_KEY"
fi

# copy the settings.xml template and inject credential information 
OSSRH_DEPLOY_SETTINGS_XML="$THIS_DIR/mvn_settings_ossrh_deploy.xml"

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
</settings>
SETTINGS.XML

MVN_OPTIONS+=(
  "--settings" "$OSSRH_DEPLOY_SETTINGS_XML"
  "--batch-mode"
)

mvn deploy ${MVN_OPTIONS[@]} ${MVN_GPG_OPTIONS[@]}
