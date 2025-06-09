# Snowflake Streaming Ingest Java Server

This is a standalone server application that wraps the Snowflake Streaming Ingest Java SDK and exposes its functionality through HTTP endpoints.

## Quick Start

### Building the Server

The server can be built using the provided `build-server.sh` script:

```bash
# Make the script executable
chmod +x build-server.sh

# Run the build script
./build-server.sh
```

This script will:
1. Clean up any old SDK installations
2. Build the SDK from source
3. Install the SDK with perf version
4. Build the server application

### Running the Server

To run the server:

```bash
java -jar target/sdk-wrapper-1.0-SNAPSHOT.jar [OPTIONS]
```

Available options:
- `--server.port=<number>` : Port to run the server on. If not specified, the server will choose a random available port.
- `--enable.access.logging` : Enable access logging (disabled by default)

Example:
```bash
# Run on port 9090
java -jar target/sdk-wrapper-1.0-SNAPSHOT.jar --server.port=9090

# Run with access logging enabled
java -jar target/sdk-wrapper-1.0-SNAPSHOT.jar --enable.access.logging

# Run on specific port with access logging
java -jar target/sdk-wrapper-1.0-SNAPSHOT.jar --server.port=9090 --enable.access.logging
```

The server will log its port number on startup:
```
INFO c.s.i.s.s.StreamingIngestJavaServer - Java StreamingIngestServer started successfully on port: <port_number>
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
  "database": "your_database",
  "schema": "your_schema",
  "table": "your_table",
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

