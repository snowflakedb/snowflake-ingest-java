travis_fold_start build "Build Ingest SDK Java driver"
mvn install -DskipTests=true --batch-mode --show-version
travis_fold_end

travis_fold_start build "Test Ingest SDK Java driver"
PARAMS=()
PARAMS+=("-DtravisIT")
echo "JDK Version: $TRAVIS_JDK_VERSION"
[[ -n "$JACOCO_COVERAGE" ]] && PARAMS+=("-Djacoco.skip.instrument=false")
# verify phase is after test/integration-test phase, which means both unit test 
# and integration test will be run
mvn "${PARAMS[@]}" verify --batch-mode
travis_fold_end