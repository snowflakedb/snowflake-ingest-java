# Snowflake Streaming Ingest Java Server

This is a standalone server application that wraps the Snowflake Streaming Ingest Java SDK and exposes its functionality through HTTP endpoints.

## Quick Start

### Building the Server

The easiest way to build the server is using the provided build script:

```bash
# From the java-sdk-app directory
./build-server.sh
```

This script will:
1. Clean up any old SDK installations
2. Build the SDK from source
3. Install the SDK with a performance testing version
4. Build the common module
5. Build the server application

Note: If the script fails or you want to ensure a completely clean build:
1. Clean up existing files: `rm -rf ~/.m2/repository/net/snowflake/snowflake-ingest-sdk/`
2. Run the build script again: `./build-server.sh`

### Running the Server

To run the server:

```bash
java -jar target/java-sdk-app-1.0-SNAPSHOT.jar [OPTIONS]
```

Available options:
- `--port <number>` : Port to run the server on (default: 8080)
- `--host <address>` : Host address to bind to (default: 0.0.0.0)

Example:
```bash
# Run on port 9090
java -jar target/java-sdk-app-1.0-SNAPSHOT.jar --server.port=9090


```

## API Endpoints

### Health Check
```
GET /health
```
Returns 200 OK if the server is running.

### Create Client
```
PUT /clients/{clientId}
```
Creates a new Snowflake Streaming Ingest client.

Request body:
```json
{
  "parameterOverrides": {
    "user": "your_user",
    "url": "your_account_url",
    "account": "your_account",
    "private_key": "your_private_key",
    "host": "your_host",
    "schema": "your_schema",
    "database": "your_database",
    "warehouse": "your_warehouse",
    "role": "your_role"
  }
}
```

### Open Channel
```
PUT /clients/{clientId}/channels/{channelName}
```
Opens a new channel for streaming data.

Request body:
```json
{
  "dbName": "your_database",
  "schemaName": "your_schema",
  "tableName": "your_table",
  "onErrorOption": "CONTINUE"  // or "ABORT"
}
```

### Insert Row
```
POST /clients/{clientId}/channels/{channelName}/insertRow
```
Inserts a row of data into the channel.

Request body: JSON object representing the row data
Query parameter: `offsetToken` (optional)

### Get Latest Offset
```
GET /clients/{clientId}/channels/{channelName}/offset
```
Returns the latest committed offset token for the channel.

### Close Channel
```
POST /clients/{clientId}/channels/{channelName}/close
```
Closes the specified channel.

### Close Client
```
POST /clients/{clientId}/close
```
Closes the client and all its associated channels.

