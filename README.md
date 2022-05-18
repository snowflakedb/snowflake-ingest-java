Snowflake Ingest Service Java SDK
---

[![image](http://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)
[![image](https://github.com/snowflakedb/snowflake-ingest-java/workflows/Snowpipe%20Java%20SDK%20Tests/badge.svg)](https://github.com/snowflakedb/snowflake-ingest-java/actions)
[![image](https://codecov.io/gh/snowflakedb/snowflake-ingest-java/branch/master/graph/badge.svg)](https://codecov.io/gh/snowflakedb/snowflake-ingest-java)
[![image](https://maven-badges.herokuapp.com/maven-central/net.snowflake/snowflake-ingest-sdk/badge.svg?style=plastic)](https://repo.maven.apache.org/maven2/net/snowflake/snowflake-ingest-sdk/)

The Snowflake Ingest Service SDK allows users to ingest files into their
Snowflake data warehouse in a programmatic fashion via key-pair
authentication.

# Prerequisites

## Java 8+

The Snowflake Ingest Service SDK can only be used with Java 8 or higher.
Backwards compatibility with Java 7 and prior is not planned at this
time.

## A 2048-bit RSA key pair

Snowflake Authentication for the Ingest Service requires creating a 2048
bit RSA key pair and, registering the public key with Snowflake. For
detailed instructions, please visit the relevant [Snowflake
Documentation Page](docs.snowflake.net).

## Maven (Developers only)

This SDK is developed as a [Maven](maven.apache.org) project. As a
result, you'll need to install maven to build the projects and, run
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
you would need to remove the following scope limits in pom.xml

<pre>
&lt;!-- Remove provided scope from slf4j-api --&gt;
&lt;dependency&gt;
    &lt;groupId&gt;org.slf4j&lt;/groupId&gt;
    &lt;artifactId&gt;slf4j-api&lt;/artifactId&gt;
    <s>&lt;scope&gt;provided&lt;/scope&gt;</s>
&lt;/dependency&gt;
</pre>

# Testing (SimpleIngestIT Test)

-   Modify `TestUtils.java` file and replace *PROFILE_PATH* with `profile.json.example` for testing.

    -   `profile.json` is used because an encrypted file will be
            decrypted for Github Actions testing. Check `End2EndTest.yml`

-   Use an unencrypted version(Only for testing) of private key while generating keys(private and public pair) using OpenSSL.

    -   Here is the link for documentation [Key Pair
            Generator](https://docs.snowflake.net/manuals/user-guide/python-connector-example.html#using-key-pair-authentication)

# Code style

We use [Google Java format](https://github.com/google/google-java-format) to format the code. To format all files, run:
```bash
./format.sh
````

# Snowpipe Streaming Example (Still in Preview)

Run File `SnowflakeStreamingIngestExample.java` which performs following operations.
1. Reads a JSON file which contains details regarding Snowflake Account, User, Role and Private Key. Take a look at `profile_streaming.json.example` for more details.
   1. [Here](https://docs.snowflake.com/en/user-guide/key-pair-auth.html#configuring-key-pair-authentication) are the steps required to generate a private key.
2. Creates a `SnowflakeStreamingIngestClient` which can be used to open one or more Streaming Channels against a table.
3. Creates a `SnowflakeStreamingIngestChannel` against a Database, Schema and Table name.
   1. Please note: A Table is expected to be present before opening a Channel. Use following SQL queries and place respective Database, Schema and Table names in `profile_streaming.json` file
```sql
create or replace database MY_DATABASE;
create or replace schema MY_SCHEMA;
create or replace table MY_TABLE(c1 number);
```
4. Inserts a few rows (1000) into a channel created in 3rd step using the `insertRows` API on the Channel object
   1. `insertRows` API also takes in an optional `offsetToken` String which can be associated to this batch of rows. 
5. Calls `getLatestCommittedOffsetToken` on the channel until the appropriate offset is found in Snowflake.