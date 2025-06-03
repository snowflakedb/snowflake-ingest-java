# Snowflake Ingest Java SDK Server Setup Guide

This guide provides step-by-step instructions for building and running the Snowflake Ingest Java SDK server.

## Prerequisites

- Java 11 or higher
- Maven
- Git

## Building the SDK and Server

### 1. Build the Snowflake Ingest SDK JAR

First, build the main SDK JAR file:

```bash
# Navigate to the SDK root directory
cd /Users/SSV1\ -\ Duplicate/SSv1/snowflake-ingest-java

# Build the SDK using Maven
mvn clean package -DskipTests

# Copy the built JAR to the e2e-tests directory
cp target/snowflake-ingest-sdk.jar e2e-tests/snowflake-ingest-sdk.jar
```

### 2. Build the Java SDK App Server

Next, build the server application:

```bash
# Navigate to the common module directory
cd e2e-tests/common
mvn clean install -DskipTests

# Navigate to the java-sdk-app directory
cd ../sdk-apps/java-sdk-app
mvn clean package -DskipTests
```

## Running the Server

To run the server:

```bash
# From the java-sdk-app directory
java -jar target/java-sdk-app-1.0-SNAPSHOT.jar
```

The server will start and automatically select an available port. By default, it will use port 50231 if available.

## Testing the Server

Once the server is running, you can test it using curl commands:

### 1. Register a Client

```bash
curl -X PUT "http://localhost:50231/clients/test_client_2" \
  -H "Content-Type: application/json" \
  -d '{
    "clientId": "test_client_1",
    "parameterOverrides": {
      "private_key": "YOUR_PRIVATE_KEY",
      "schema": "PUBLIC",
      "database": "REGRESS",
      "ssl": "on",
      "warehouse": "xsmall",
      "role": "ACCOUNTADMIN",
      "user": "rowset_sdk_gh_action",
      "account": "Streamingesttestqa6",
      "url": "https://Streamingesttestqa6.qa6.us-west-2.aws.snowflakecomputing.com:443",
      "port": "443",
      "host": "streamingesttestqa6.qa6.us-west-2.aws.snowflakecomputing.com",
      "connect_string": "jdbc:snowflake://streamingesttestqa6.qa6.snowflakecomputing.com:443",
      "scheme": "https",
      "ROWSET_DEV_VM_TEST_MODE": "false"
    }
  }'
```

### 2. Create a Channel

```bash
curl -X PUT "http://localhost:50231/clients/test_client_1/channels/test_channel" \
  -H "Content-Type: application/json" \
  -d '{
    "channelName": "test_channel",
    "offsetToken": "0"
  }'
```

### 3. Insert Data

```bash
curl -X POST "http://localhost:50231/clients/test_client_1/channels/test_channel/insertRow" \
  -H "Content-Type: application/json" \
  -d '{
    "content": "your_data_here"
  }'
```

## Available Endpoints

The server provides the following endpoints:

- `PUT /clients/:clientId` - Register a new client
- `PUT /clients/:clientId/channels/:channelName` - Create a new channel
- `POST /clients/:clientId/channels/:channelName/insertRow` - Insert data into a channel
- `GET /clients/:clientId/channels/:channelName/offset` - Get channel offset
- `PUT /clients/:clientId/channels/:channelName/close` - Close a channel
- `PUT /clients/:clientId/close` - Close a client
- `GET /health` - Health check endpoint

## Troubleshooting

1. If the server fails to start, check:
   - Port availability
   - Java version (should be 11 or higher)
   - All dependencies are properly installed

2. If client registration fails:
   - Verify the server is running
   - Check the client configuration parameters
   - Ensure proper JSON formatting in the request

3. For connection issues:
   - Verify the server port
   - Check network connectivity
   - Ensure proper SSL configuration if enabled