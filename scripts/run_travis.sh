mvn install -DskipTests=true --batch-mode --show-version

PARAMS=()
PARAMS+=("-DtravisIT")
# testing will not need shade dep. otherwise codecov cannot work
PARAMS+=("-Dnot-shadeDep")
echo "JDK Version: $TRAVIS_JDK_VERSION"
[[ -n "$JACOCO_COVERAGE" ]] && PARAMS+=("-Djacoco.skip.instrument=false")
# verify phase is after test/integration-test phase, which means both unit test 
# and integration test will be run
mvn "${PARAMS[@]}" verify --batch-mode

# run whitesource
echo ${PWD}
chmod 755 ./scripts/run_whitesource.sh
./scripts/run_whitesource.sh