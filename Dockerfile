# Multi-stage build for Snowflake Streaming Ingest Example
FROM maven:3.8.4-openjdk-8 AS builder

# Set working directory
WORKDIR /app

# Copy pom.xml first to cache dependencies
COPY pom.xml .

# Download dependencies
RUN mvn dependency:go-offline -B

# Copy source code
COPY src ./src

# Build the application
RUN mvn clean package -DskipTests=true -Dexec.skip=true

# Runtime stage
FROM openjdk:8-jre-slim

# Set working directory
WORKDIR /app

# Copy the built shaded JAR and the excluded dependencies from builder stage
COPY --from=builder /app/target/snowflake-ingest-sdk.jar snowflake-ingest-sdk.jar
COPY --from=builder /app/target/dependency-jars/slf4j-api-1.7.36.jar /app/lib/
COPY --from=builder /app/target/dependency-jars/slf4j-simple-1.7.36.jar /app/lib/
COPY --from=builder /app/target/dependency-jars/jcl-over-slf4j-1.7.36.jar /app/lib/
COPY --from=builder /app/target/dependency-jars/zstd-jni-1.5.6-5.jar /app/lib/

# Create directory for configuration
RUN mkdir -p /app/config

# Copy example profile for reference
COPY profile_streaming.json.example /app/config/

# OPTION 1: Embed the actual config file (less secure)
# Uncomment the next line to embed profile.json into the image
COPY profile.json /app/config/profile.json

# Set environment variables
ENV JAVA_OPTS="-Xmx1g -Xms512m"
ENV PROFILE_PATH="/app/config/profile.json"

# Expose any ports if needed (optional)
# EXPOSE 8080

# Create a non-root user
RUN groupadd -r snowflake && useradd -r -g snowflake snowflake
RUN chown -R snowflake:snowflake /app
USER snowflake

# Entry point to run the SnowflakeStreamingIngestExample
ENTRYPOINT ["sh", "-c", "cp /app/config/profile.json . && java $JAVA_OPTS -cp 'snowflake-ingest-sdk.jar:lib/*' net.snowflake.ingest.streaming.example.SnowflakeStreamingIngestExample"] 