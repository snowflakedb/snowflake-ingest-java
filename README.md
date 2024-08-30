Snowflake Ingest Service Java SDK
---

[![image](http://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)
[![image](https://github.com/snowflakedb/snowflake-ingest-java/workflows/Snowpipe%20Java%20SDK%20Tests/badge.svg)](https://github.com/snowflakedb/snowflake-ingest-java/actions)
[![image](https://maven-badges.herokuapp.com/maven-central/net.snowflake/snowflake-ingest-sdk/badge.svg?style=plastic)](https://repo.maven.apache.org/maven2/net/snowflake/snowflake-ingest-sdk/)

The Snowflake Ingest Service SDK allows users to ingest files into their
Snowflake data warehouse in a programmatic fashion via key-pair
authentication. Currently, we support ingestion through the following APIs:
1. [Snowpipe](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-rest-gs.html#client-requirement-java-or-python-sdk)
2. [Snowpipe Streaming](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview)

# Dependencies

The Snowflake Ingest Service SDK depends on the following libraries:

* snowflake-jdbc (3.16.1+) 
* slf4j-api
* com.github.luben:zstd-jni (1.5.0-1)

These dependencies will be fetched automatically by build systems like Maven or Gradle. If you don't build your project
using a build system, please make sure these dependencies are on the classpath.

## Java 8+

The Snowflake Ingest Service SDK can only be used with Java 8 or higher.
Backwards compatibility with Java 7 and prior is not planned at this time.

## A 2048-bit RSA key pair

Snowflake Authentication for the Ingest Service requires creating a 2048
bit RSA key pair and, registering the public key with Snowflake. For
detailed instructions, please visit the relevant [Snowflake
Documentation Page](https://docs.snowflake.com/en/user-guide/key-pair-auth.html).

## Maven (Developers only)

This SDK is developed as a Maven project. As a
result, you'll need to install Maven to build the projects and, run
tests.

# Adding as a Dependency

You can add the Snowflake Ingest Service SDK by adding the following to
your project

``` {.xml}
<!-- Add this to your Maven project's pom.xml -->
<dependency>
    <groupId>net.snowflake</groupId>
    <artifactId>snowflake-ingest-sdk</artifactId>
    <version>{version}</version>
</dependency>
```

``` {.groovy}
// in Gradle project
dependencies {
    compile 'net.snowflake:snowflake-ingest-sdk:{version}'
}
```

## Jar Versions

The Snowflake Ingest SDK provides shaded and unshaded versions of its jar. The shaded version bundles the dependencies into its own jar,
whereas the unshaded version declares its dependencies in `pom.xml`, which are fetched as standard transitive dependencies by the build system like Maven or Gradle.
The shaded JAR can help avoid potential dependency conflicts, but the unshaded version provides finer graned control over transitive dependencies.

## Using with snowflake-jdbc-fips

For use cases, which need to use `snowflake-jdbc-fips` instead of the default `snowflake-jdbc`, we recommend to take the following steps:

- Use the unshaded version of the Ingest SDK.
- Exclude these transitive dependencies:
    - `net.snowflake:snowflake-jdbc`
    - `org.bouncycastle:bcpkix-jdk18on`
    - `org.bouncycastle:bcprov-jdk18on`
- Add a dependency on `snowflake-jdbc-fips`.

See [this test](https://github.com/snowflakedb/snowflake-ingest-java/tree/master/e2e-jar-test/fips) for an example how to use Snowflake Ingest SDK together with Snowflake FIPS JDBC Driver.

# Example

## Snowpipe

Check out `SnowflakeIngestBasicExample.java`

## Snowpipe Streaming

Check out `SnowflakeStreamingIngestExample.java`, which performs following operations:
1. Reads a JSON file which contains details regarding Snowflake Account, User, Role and Private Key. Take a look at `profile_streaming.json.example` for more details.
    1. [Here](https://docs.snowflake.com/en/user-guide/key-pair-auth.html#configuring-key-pair-authentication) are the steps required to generate a private key.
2. Creates a `SnowflakeStreamingIngestClient` which can be used to open one or more Streaming Channels pointing to the same or different tables.
3. Creates a `SnowflakeStreamingIngestChannel` against a Database, Schema and Table.
    1. Please note: The database, schema and table is expected to be present before opening the Channel. Example SQL queries to create them:
```sql
create or replace database MY_DATABASE;
create or replace schema MY_SCHEMA;
create or replace table MY_TABLE(c1 number);
```
4. Inserts 1000 rows into the channel created in 3rd step using the `insertRows` API on the Channel object
    1. `insertRows` API also takes in an optional `offsetToken` String which can be associated to this batch of rows.
5. Calls `getLatestCommittedOffsetToken` on the channel until the appropriate offset is found in Snowflake.
6. Close the channel when the ingestion is done to make sure everything is committed.

# Building From Source

If you would like to build this project from source you can run the
following to install the artifact to your local maven repository.

``` {.bash}
mvn install
```

If you would just like to build the jar in the source directory, you can
run

``` {.bash}
mvn package
```

However, for general usage, pulling a pre-built jar from maven is
recommended.

If you would like to run SnowflakeIngestBasicExample.java or SnowflakeStreamingIngestExample.java in the example folder, 
please edit `pom.xml` and change the scope of the dependency `slf4j-simple` from `test` to `runtime` in order to enable
console log output.


# Testing (SimpleIngestIT Test)

-   Modify `TestUtils.java` file and replace *PROFILE_PATH* with `profile.json.example` for testing.

    -   `profile.json` is used because an encrypted file will be
            decrypted for Github Actions testing. Check `End2EndTest.yml`

-   Use an unencrypted version(Only for testing) of private key while generating keys(private and public pair) using OpenSSL.

    -   Here is the link for documentation [Key Pair
            Generator](https://docs.snowflake.com/en/user-guide/key-pair-auth.html)

# Contributing to this repo

Each PR must pass all required github action merge gates before approval and merge. In addition to those tests, you will need:

- Formatter: run this script [`./format.sh`](https://github.com/snowflakedb/snowflake-ingest-java/blob/master/format.sh) from root
- CLA: all contributers must sign the Snowflake CLA. This is a one time signature, please provide your email so we can work with you to get this signed after you open a PR.

Thank you for contributing! We will review and approve PRs as soon as we can.
