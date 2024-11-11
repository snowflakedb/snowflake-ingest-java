#!/bin/bash -e

#
# Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
#

set -o pipefail

# Build and install shaded JAR first. check_content.sh runs here.
mvn install -PcheckShadedContent -DskipTests=true --batch-mode --show-version

PARAMS=()
PARAMS+=("-DghActionsIT")
# testing will not need shade dep. otherwise codecov cannot work
PARAMS+=("-Dnot-shadeDep")
PARAMS+=($1)
[[ -n "$JACOCO_COVERAGE" ]] && PARAMS+=("-Djacoco.skip.instrument=false")
# verify phase is after test/integration-test phase, which means both unit test
# and integration test will be run
mvn "${PARAMS[@]}" verify --batch-mode

rc=$?
if [ $rc -ne 0 ] ; then
  echo Could not perform mvn verify with parameters "${PARAMS[@]}", exit code [$rc]; exit $rc
fi
