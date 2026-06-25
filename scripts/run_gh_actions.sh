#!/bin/bash -e

#
# Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
#

set -o pipefail

# Resolve dependencies via the Google-hosted Maven Central mirror (see
# mvn_settings_ci.xml) to avoid repo.maven.apache.org rate-limiting (HTTP 429 /
# read timeouts) on the CI runners.
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
CI_SETTINGS="$THIS_DIR/../mvn_settings_ci.xml"

# Build and install shaded JAR first. check_content.sh runs here.
mvn --settings "$CI_SETTINGS" install -PcheckShadedContent -DskipTests=true --batch-mode --show-version

PARAMS=()
PARAMS+=("--settings" "$CI_SETTINGS")
PARAMS+=("-DghActionsIT")
# testing will not need shade dep. otherwise codecov cannot work
PARAMS+=("-Dnot-shadeDep")
PARAMS+=($1)
[[ -n "$JACOCO_COVERAGE" ]] && PARAMS+=("-Djacoco.skip.instrument=false")
# verify phase is after test/integration-test phase, which means both unit test
# and integration test will be run
# Rebuild package with unshaded version using clean
mvn "${PARAMS[@]}" clean verify --batch-mode

rc=$?
if [ $rc -ne 0 ] ; then
  echo Could not perform mvn verify with parameters "${PARAMS[@]}", exit code [$rc]; exit $rc
fi
