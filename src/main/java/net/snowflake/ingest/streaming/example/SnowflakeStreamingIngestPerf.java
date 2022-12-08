/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * Example on how to use the Streaming Ingest client APIs.
 *
 * <p>Please read the README.md file for detailed steps
 */
public class SnowflakeStreamingIngestPerf {
  // Please follow the example in profile_streaming.json.example to see the required properties, or
  // if you have already set up profile.json with Snowpipe before, all you need is to add the "role"
  // property.
  private static String PROFILE_PATH = "profile.json";
  private static final ObjectMapper mapper = new ObjectMapper();

  private static List<Map<String, Object>> generateRandRows(int rowCount) {
    List<Map<String, Object>> rows = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    for (int i = 0; i < rowCount; i++) {
      row.put("c1", randInt());
      row.put("c2", randInt());
      row.put("c3", randNumericString());
      row.put("c4", randNumericString());
      row.put("c5", randAlphabeticString());
      row.put("c6", randAlphabeticString());
      row.put("c7", randAlphabeticString());
      rows.add(row);
    }
    return rows;
  }

  private static int randInt() {
    return ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE);
  }

  private static String randAlphabeticString() {
    return RandomStringUtils.randomAlphabetic(8, 64);
  }

  private static String randNumericString() {
    return RandomStringUtils.randomNumeric(8, 128);
  }

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    Iterator<Map.Entry<String, JsonNode>> propIt =
        mapper.readTree(new String(Files.readAllBytes(Paths.get(PROFILE_PATH)))).fields();
    while (propIt.hasNext()) {
      Map.Entry<String, JsonNode> prop = propIt.next();
      props.put(prop.getKey(), prop.getValue().asText());
    }

    int batchSize = Integer.parseInt(args[0]);
    List<Map<String, Object>> rows = generateRandRows(batchSize);

    // Create a streaming ingest client
    try (SnowflakeStreamingIngestClient client =
        SnowflakeStreamingIngestClientFactory.builder("MY_CLIENT").setProperties(props).build()) {

      // Create an open channel request on table MY_TABLE, note that the corresponding
      // db/schema/table needs to be present
      // Example: create or replace table MY_TABLE(c1 number);
      OpenChannelRequest request1 =
          OpenChannelRequest.builder("MY_CHANNEL")
              .setDBName("DB_STREAMINGINGEST")
              .setSchemaName("PUBLIC")
              .setTableName("T_STREAMINGINGEST")
              .setOnErrorOption(
                  OpenChannelRequest.OnErrorOption.CONTINUE) // Another ON_ERROR option is ABORT
              .build();

      // Open a streaming ingest channel from the given client
      SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);

      // Insert rows into the channel (Using insertRows API)
      int iteration = Integer.parseInt(args[1]);
      for (int val = 0; val < iteration; val++) {
        // Insert the row with the current offset_token
        InsertValidationResponse response = channel1.insertRows(rows, String.valueOf(val));
        if (response.hasErrors()) {
          // Simply throw if there is an exception, or you can do whatever you want with the
          // erroneous row
          throw response.getInsertErrors().get(0).getException();
        }
      }

      // If needed, you can check the offset_token registered in Snowflake to make sure everything
      // is committed
      final int expectedOffsetTokenInSnowflake = iteration; // 0 based offset_token
      final int maxRetries = 10;
      int retryCount = 0;

      do {
        String offsetTokenFromSnowflake = channel1.getLatestCommittedOffsetToken();
        if (offsetTokenFromSnowflake != null
            && offsetTokenFromSnowflake.equals(String.valueOf(expectedOffsetTokenInSnowflake))) {
          System.out.println("SUCCESSFULLY inserted " + iteration * batchSize + " rows");
          break;
        }
        retryCount++;
      } while (retryCount < maxRetries);

      // Close the channel, the function internally will make sure everything is committed (or throw
      // an exception if there is any issue)
      channel1.close().get();
    }
  }
}
