# Snowflake Streaming Ingest Example - Docker Image

This Docker image runs the SnowflakeStreamingIngestExample application for ingesting data into Snowflake using the Streaming Ingest API.

## Building the Docker Image

```bash
docker build -t snowflake-streaming-ingest .
```

## Configuration

The application requires a `profile.json` file with your Snowflake connection details. Create this file based on the example below:

```json
{
  "user": "your_username",
  "url": "https://your_account.snowflakecomputing.com:443",
  "private_key": "-----BEGIN PRIVATE KEY-----\nYOUR_PRIVATE_KEY_HERE\n-----END PRIVATE KEY-----",
  "port": 443,
  "host": "your_account.snowflakecomputing.com",
  "scheme": "https",
  "role": "your_role_name"
}
```

## Running the Container

### Option 1: Mount the config file as a volume

```bash
docker run -v /path/to/your/profile.json:/app/config/profile.json snowflake-streaming-ingest
```

### Option 2: Copy config file into container

```bash
# Copy your profile.json to the container
docker run -it --name temp-container snowflake-streaming-ingest /bin/bash
# In another terminal:
docker cp /path/to/your/profile.json temp-container:/app/config/profile.json
docker start temp-container
```

### Option 3: Use environment variables (if you modify the application)

You can extend the application to read from environment variables instead of a file:

```bash
docker run -e SNOWFLAKE_USER=your_user \
           -e SNOWFLAKE_URL=https://your_account.snowflakecomputing.com:443 \
           -e SNOWFLAKE_PRIVATE_KEY="your_private_key" \
           -e SNOWFLAKE_ROLE=your_role \
           snowflake-streaming-ingest
```

## Prerequisites

Before running the application, ensure you have:

1. **Snowflake Account**: A valid Snowflake account with appropriate permissions
2. **Database and Table**: Create the target database and table in Snowflake:
   ```sql
   CREATE DATABASE alhuang_db;
   USE DATABASE alhuang_db;
   CREATE SCHEMA PUBLIC;
   USE SCHEMA PUBLIC;
   CREATE TABLE t1 (c1 NUMBER);
   ```
3. **Key-pair Authentication**: Set up key-pair authentication for your Snowflake user
4. **Role Permissions**: Ensure your role has INSERT permissions on the target table

## Application Behavior

The example application will:
- Connect to Snowflake using the provided credentials
- Create a streaming ingest channel for table `alhuang_db.PUBLIC.t1`
- Insert 1000 rows with sequential numbers (0-999)
- Verify that all rows were successfully committed
- Close the channel and exit

## Customization

To customize the application behavior, you can:

1. **Modify the table/database**: Edit the `OpenChannelRequest` in the source code
2. **Change data volume**: Modify the `totalRowsInTable` variable
3. **Add different data types**: Extend the row mapping logic
4. **Add error handling**: Implement custom error handling logic

## Troubleshooting

### Common Issues

1. **Connection Errors**: Verify your Snowflake credentials and network connectivity
2. **Permission Errors**: Ensure your role has the necessary permissions
3. **Table Not Found**: Create the target table before running the application
4. **Private Key Issues**: Verify the private key format and permissions

### Logs

The application logs will show:
- Connection status
- Row insertion progress
- Final success/failure status

### Debug Mode

To run with debug output:

```bash
docker run -e JAVA_OPTS="-Xmx1g -Xms512m -Dlog.level=DEBUG" \
           -v /path/to/your/profile.json:/app/config/profile.json \
           snowflake-streaming-ingest
```

## Security Considerations

- Never include real credentials in Docker images
- Use secrets management for production deployments
- Consider using environment variables or mounted secrets
- Run containers with non-root user (already configured in the image)

## Development

To modify the application:

1. Edit the source code in `src/main/java/net/snowflake/ingest/streaming/example/`
2. Rebuild the Docker image
3. Test with your configuration

The Docker image uses a multi-stage build for efficiency and includes only the necessary runtime components in the final image. 