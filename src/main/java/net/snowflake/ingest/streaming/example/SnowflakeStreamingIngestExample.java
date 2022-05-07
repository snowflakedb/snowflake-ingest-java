/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;

/**
 * Example on how to use the Streaming Ingest client APIs.
 *
 * <p>Please read README.md file for detailed steps
 */
public class SnowflakeStreamingIngestExample {
  private static String PROFILE_PATH = "profile.json";
  private static final ObjectMapper mapper = new ObjectMapper();

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    Iterator<Map.Entry<String, JsonNode>> propIt =
        mapper.readTree(new String(Files.readAllBytes(Paths.get(PROFILE_PATH)))).fields();
    while (propIt.hasNext()) {
      Map.Entry<String, JsonNode> prop = propIt.next();
      props.put(prop.getKey(), prop.getValue().asText());
    }

    // Create a streaming ingest client
    try (SnowflakeStreamingIngestClient client =
        SnowflakeStreamingIngestClientFactory.builder("CLIENT").setProperties(props).build()) {
      // Create an open channel request on table T_STREAMINGINGEST
      OpenChannelRequest request1 =
          OpenChannelRequest.builder("CHANNEL")
              .setDBName("DB_STREAMINGINGEST")
              .setSchemaName("SCHEMA_STREAMINGINGEST")
              .setTableName("T_STREAMINGINGEST")
              .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
              .build();

      // Open a streaming ingest channel from the given client
      SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);

      final int totalRowsInTable = 1000;

      // Insert a few rows into the channel,
      for (int val = 0; val < totalRowsInTable; val++) {
        Map<String, Object> row = new HashMap<>();

        // c1 corresponds to the column name in table
        row.put("c1", val);

        // Along with row, we are also passing in offset Token which corresponds to the row number
        // for simplicity
        InsertValidationResponse response = channel1.insertRow(row, String.valueOf(val));
        if (response.hasErrors()) {
          // Simply throw if there is an exception
          throw response.getInsertErrors().get(0).getException();
        }
      }
      final int expectedOffsetTokenInSnowflake = 999; // because it goes from 0 to 999
      String offsetTokenFromSnowflake = channel1.getLatestCommittedOffsetToken();

      // Note: This is just an upper bound used as an example. Please don't use this for
      // benchmarking.
      final int maxRetries = 10;
      int retryCount = 0;
      while (offsetTokenFromSnowflake == null
          || !offsetTokenFromSnowflake.equals(String.valueOf(expectedOffsetTokenInSnowflake))) {

        // Sleeping for 1 second to Commit all rows in the table.
        Thread.sleep(1_000);
        // Used as a polling
        offsetTokenFromSnowflake = channel1.getLatestCommittedOffsetToken();
        retryCount++;
        System.out.println(
            String.format(
                "OffsetToken found in Snowflake:%s with retryCount:%s",
                offsetTokenFromSnowflake, retryCount));
        if (retryCount >= maxRetries) {
          System.out.println(
              String.format(
                  "Failed to look for required OffsetToken in Snowflake:%s after MaxRetryCounts:%s",
                  expectedOffsetTokenInSnowflake, maxRetries));
          System.exit(1);
        }
      }
      System.out.println("SUCCESSFULLY inserted " + totalRowsInTable + " rows");
    }
  }
}
