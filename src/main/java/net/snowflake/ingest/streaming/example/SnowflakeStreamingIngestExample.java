/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;

/** Examples on how to use the Streaming Ingest client APIs */
public class SnowflakeStreamingIngestExample {
  private static String PROFILE_PATH = "profile.properties";

  public static void main(String[] args)
      throws ExecutionException, InterruptedException, IOException {
    Properties prop = new Properties();
    prop.load(new FileInputStream(PROFILE_PATH));

    // Create a streaming ingest client
    SnowflakeStreamingIngestClient client =
        SnowflakeStreamingIngestClientFactory.builder("CLIENT").setProperties(prop).build();

    try {
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

      // Insert a few rows into the channel,
      for (int val = 0; val < 1000; val++) {
        Map<String, Object> row = new HashMap<>();
        row.put("c1", val);
        InsertValidationResponse response = channel1.insertRow(row, null);
        if (response.hasErrors()) {
          // Simply throw if there is an exception
          throw response.getInsertErrors().get(0).getException();
        }
      }
    } finally {
      client.close().get();
    }
  }
}
