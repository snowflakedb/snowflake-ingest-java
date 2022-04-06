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
result, you\'ll need to install maven to build the projects and, run
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

# Google Java Format

- Download the formatter jar file from https://github.com/google/google-java-format, then run it with 
```
java -jar ~/path-to/google-java-format-1.10.0-all-deps.jar  -i $(find . -type f -name "*.java" | grep ".*/src/.*java")
```
