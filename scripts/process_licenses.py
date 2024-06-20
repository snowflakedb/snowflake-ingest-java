#!/usr/bin/env python

# This script processes licenses of 3rd party dependencies and stores them in the JAR. The rules are:
# 1. Dependencies, which contains a license file should be put into the shaded JAR as-is.
# 2. Dependencies, which do not contain a license file should be mentioned in the file ADDITIONAL_LICENCES, together with the name of its license.
#
#
# The script accepts the following arguments:
# * DEPENDENCY_LIST_FILE_PATH
#     * Can be obtained by running mvn dependency:list -DincludeScope=runtime -DoutputFile=target/dependency_list.txt
# * DEPENDENCIES_DIR
#     * Directory containging the JAR files of all SDK dependencies. Automatically generated by `mvn clean package` in target/dependency-jars
# * TARGET_DIR
#     * Where to save all output, should be target/generated-sources/META-INF/third-party-licenses
#
#
# Useful mvn commands:
# * mvn clean license:add-third-party
#     * Generate dependency report; useful to find out licenses for dependencies that don't ship with a license file
# * mvn dependency:list -DincludeScope=runtime -DoutputFile=target/dependency_list.txt
#     * Used as input of this script (DEPENDENCY_LIST_FILE_PATH)
import sys
from pathlib import Path
from zipfile import ZipFile

# License name constants
APACHE_LICENSE = "Apache License 2.0"
BSD_2_CLAUSE_LICENSE = "2-Clause BSD License"
BSD_3_CLAUSE_LICENSE = "3-Clause BSD License"
EDL_10_LICENSE = "EDL 1.0"
MIT_LICENSE = "The MIT License"
GO_LICENSE = "The Go license"
BOUNCY_CASTLE_LICENSE = "Bouncy Castle Licence <https://www.bouncycastle.org/licence.html>"
CDDL_GPLv2 = "CDDL + GPLv2 with classpath exception"

# The SDK does not need to include licenses of dependencies, which aren't shaded
IGNORED_DEPENDENCIES = {"net.snowflake:snowflake-jdbc", "org.slf4j:slf4j-api"}

# List of dependencies, which don't ship with a license file.
# Only add a new record here after verifying that the dependency JAR does not contain a license!
ADDITIONAL_LICENSES_MAP = {
    "com.google.code.findbugs:jsr305": APACHE_LICENSE,
    "io.dropwizard.metrics:metrics-core": APACHE_LICENSE,
    "io.dropwizard.metrics:metrics-jmx": APACHE_LICENSE,
    "io.dropwizard.metrics:metrics-jvm": APACHE_LICENSE,
    "com.google.guava:guava": APACHE_LICENSE,
    "com.google.guava:failureaccess": APACHE_LICENSE,
    "com.google.guava:listenablefuture": APACHE_LICENSE,
    "com.google.errorprone:error_prone_annotations": APACHE_LICENSE,
    "com.google.j2objc:j2objc-annotations": APACHE_LICENSE,
    "com.nimbusds:nimbus-jose-jwt": APACHE_LICENSE,
    "com.github.stephenc.jcip:jcip-annotations": APACHE_LICENSE,
    "io.netty:netty-common": APACHE_LICENSE,
    "com.google.re2j:re2j": GO_LICENSE,
    "com.google.protobuf:protobuf-java": BSD_3_CLAUSE_LICENSE,
    "com.google.code.gson:gson": APACHE_LICENSE,
    "org.xerial.snappy:snappy-java": APACHE_LICENSE,
    "org.apache.parquet:parquet-common": APACHE_LICENSE,
    "org.apache.parquet:parquet-format-structures": APACHE_LICENSE,
    "com.github.luben:zstd-jni": BSD_2_CLAUSE_LICENSE,
    "io.airlift:aircompressor": APACHE_LICENSE,
    "org.bouncycastle:bcpkix-jdk18on": BOUNCY_CASTLE_LICENSE,
    "org.bouncycastle:bcutil-jdk18on": BOUNCY_CASTLE_LICENSE,
    "org.bouncycastle:bcprov-jdk18on": BOUNCY_CASTLE_LICENSE,
    "javax.annotation:javax.annotation-api": CDDL_GPLv2
}


def parse_cmdline_args():
    if len(sys.argv) != 4:
        raise Exception("usage: process_licenses.py DEPENDENCY_LIST_FILE_PATH DEPENDENCIES_DIR TARGET_DIR")
    dependency_list_file_path = Path(sys.argv[1]).absolute()
    dependencies_dir_path = Path(sys.argv[2]).absolute()
    target_dir = Path(sys.argv[3]).absolute()

    if not dependency_list_file_path.exists() or not dependency_list_file_path.is_file():
        raise Exception(f"File {dependency_list_file_path} does not exist")

    if not dependencies_dir_path.exists() or not dependencies_dir_path.is_dir():
        raise Exception(f"Directory {dependencies_dir_path} does not exist")
    return dependency_list_file_path, dependencies_dir_path, target_dir


def main():
    dependency_list_path, dependency_jars_path, target_dir = parse_cmdline_args()

    dependency_count = 0
    dependency_with_license_count = 0
    dependency_without_license_count = 0
    dependency_ignored_count = 0

    missing_licenses_str = ""

    target_dir.mkdir(parents=True, exist_ok=True)

    with open(dependency_list_path, "r") as dependency_file_handle:
        for line in dependency_file_handle.readlines():
            line = line.strip()
            if line == "" or line == "The following files have been resolved:":
                continue
            dependency_count += 1

            # Line is a string like: "commons-codec:commons-codec:jar:1.15:compile -- module org.apache.commons.codec [auto]"
            artifact_details = line.split()[0]
            group_id, artifact_id, _, version, scope = artifact_details.split(":")
            current_jar = Path(dependency_jars_path, f"{artifact_id}-{version}.jar")
            if not current_jar.exists() and current_jar.is_file():
                raise Exception(f"Expected JAR file does not exist: {current_jar}")
            current_jar_as_zip = ZipFile(current_jar)

            dependency_lookup_key = f"{group_id}:{artifact_id}"
            if dependency_lookup_key in IGNORED_DEPENDENCIES:
                dependency_ignored_count += 1
                continue

            license_found = False
            for zip_info in current_jar_as_zip.infolist():
                if zip_info.is_dir():
                    continue
                if zip_info.filename in ("META-INF/LICENSE.txt", "META-INF/LICENSE", "META-INF/LICENSE.md"):
                    license_found = True
                    dependency_with_license_count += 1
                    # Extract license to the target directory
                    zip_info.filename = f"LICENSE_{group_id}__{artifact_id}"
                    current_jar_as_zip.extract(zip_info, target_dir)
                    break
                if "license" in zip_info.filename.lower():  # Log potential license matches
                    print(f"Potential license match: {current_jar} {zip_info}")

            if not license_found:
                print(f"License not found {current_jar}; using value from ADDITIONAL_LICENSES_MAP")
                license_name = ADDITIONAL_LICENSES_MAP.get(dependency_lookup_key)
                if license_name:
                    dependency_without_license_count += 1
                    missing_licenses_str += f"{dependency_lookup_key}: {license_name}\n"
                else:
                    raise Exception(
                        f"The dependency {dependency_lookup_key} does not ship a license file, but neither is it not defined in ADDITIONAL_LICENSES_MAP")

    with open(Path(target_dir, "ADDITIONAL_LICENCES"), "w") as additional_licenses_handle:
        additional_licenses_handle.write(missing_licenses_str)

    if dependency_count < 30:
        raise Exception(
            f"Suspiciously low number of dependency JARs detected in {dependency_jars_path}: {dependency_count}")
    print("License generation finished")
    print(f"\tTotal dependencies: {dependency_count}")
    print(f"\tTotal dependencies (with license): {dependency_with_license_count}")
    print(f"\tTotal dependencies (without license): {dependency_without_license_count}")
    print(f"\tIgnored dependencies: {dependency_ignored_count}")


if __name__ == "__main__":
    main()
