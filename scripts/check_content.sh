#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

# no-op comment
: <<'EOF'
This script checks the shade tar and verifies that it only contains classes
under the package net.snowflake.ingest. This is to prevent class conflicts
when the shaded jar is used in other projects.
EOF

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

# List all the files in the shaded snowflake-ingest-sdk.jar
# filter for .class files
# filter out all files under net/snowflake/ingest
# filter out the packageless files from dnsjava
# if any files remain, throw an error

REMAINING_CLASS_FILES=$(jar tvf "${DIR}/../target/snowflake-ingest-sdk.jar" |
  awk '{print $8}' |
  fgrep .class |
  egrep -v '^net/snowflake/ingest/' |
  egrep -v '^META-INF/versions/[0-9]+/net/snowflake/ingest/' |
  egrep -v '(dig|lookup|update|jnamed(\$[0-9])?)\.class' |
  egrep -v 'META-INF/versions/9/module-info.class' || true)


# error if there are any remaining class files
# ${REMAINING_CLASS_FILES} must be quoted to print newlines instead of spaces
# echo adds a newline so "-eq 1" means empty
if ! [ $(echo "${REMAINING_CLASS_FILES}" | wc -l) -eq 1 ]; then
  echo "[ERROR] Ingest SDK jar includes classes outside net.snowflake.ingest"
  echo "Found classes in jar:"
  echo "${REMAINING_CLASS_FILES}"
  exit 1
fi
