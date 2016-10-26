Snowflake Ingest Service Java SDK
**********************************

.. image:: http://img.shields.io/:license-Apache%202-brightgreen.svg
    :target: http://www.apache.org/licenses/LICENSE-2.0.txt

The Snowflake Ingest Service SDK allows users to ingest files
into their Snowflake data warehouse in a programmatic fashion via key-pair
authentication.

Prerequisites
=============

Java 8
-------

The Snowflake Ingest Service SDK can only be used with Java 8 or higher. Backwards
compatibility with Java 7 and prior is not planned at this time.

A 2048-bit RSA key pair
-------
Snowflake Authentication for the Ingest Service requires creating a 2048 bit
RSA key pair and, registering the public key with Snowflake. For detailed instructions,
please visit the relevant `Snowflake Documentation Page <docs.snowflake.net>`_.

Maven (Developers only)
------
This SDK is developed as a `Maven <maven.apache.org>`_ project.
As a result, you'll need to install maven to build the projects and, run tests.


Adding as a Maven Dependency
======================
You can add the Snowflake Ingest Service SDK by adding the following to your pom.xml

    ..code-block:: xml

        <dependency>
            <groupId>net.snowflake</groupId>
            <artifactId>snowflake-ingest-sdk</artifactId>
            <version>{version}</version>
        </dependency>


