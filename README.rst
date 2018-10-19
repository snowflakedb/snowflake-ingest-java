Snowflake Ingest Service Java SDK
*********************************

.. image:: http://img.shields.io/:license-Apache%202-brightgreen.svg
    :target: http://www.apache.org/licenses/LICENSE-2.0.txt
.. image:: https://travis-ci.org/snowflakedb/snowflake-ingest-java.svg?branch=master
    :target: https://travis-ci.org/snowflakedb/snowflake-ingest-java
.. image:: https://codecov.io/gh/snowflakedb/snowflake-ingest-java/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/snowflakedb/snowflake-ingest-java
.. image:: https://maven-badges.herokuapp.com/maven-central/net.snowflake/snowflake-ingest-sdk/badge.svg?style=plastic   
    :target: http://repo2.maven.org/maven2/net/snowflake/snowflake-ingest-sdk/

The Snowflake Ingest Service SDK allows users to ingest files
into their Snowflake data warehouse in a programmatic fashion via key-pair
authentication.

Prerequisites
=============

Java 8+
-------

The Snowflake Ingest Service SDK can only be used with Java 8 or higher. Backwards
compatibility with Java 7 and prior is not planned at this time.

A 2048-bit RSA key pair
-----------------------
Snowflake Authentication for the Ingest Service requires creating a 2048 bit
RSA key pair and, registering the public key with Snowflake. For detailed instructions,
please visit the relevant `Snowflake Documentation Page <docs.snowflake.net>`_.

Maven (Developers only)
-----------------------
This SDK is developed as a `Maven <maven.apache.org>`_ project.
As a result, you'll need to install maven to build the projects and, run tests.


Adding as a Dependency
======================
You can add the Snowflake Ingest Service SDK by adding the following to your project

.. code-block:: xml

    <!-- Add this to your Maven project's pom.xml -->
    <dependency>
        <groupId>net.snowflake</groupId>
        <artifactId>snowflake-ingest-sdk</artifactId>
        <version>{version}</version>
    </dependency>


.. code-block:: groovy

    // in Gradle project
    dependencies {
        compile 'net.snowflake:snowflake-ingest-sdk:{version}'
    }


Building From Source
====================
If you would like to build this project from source you can run the following to install
the artifact to your local maven repository.

.. code-block:: bash

    mvn install

If you would just like to build the jar in the source directory, you can run

.. code-block:: bash

    mvn package

However, for general usage, pulling a pre-built jar from maven is recommended.


