/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.example;

import static net.snowflake.ingest.utils.Constants.INTERNAL_STAGE_DB_NAME;
import static net.snowflake.ingest.utils.Constants.INTERNAL_STAGE_SCHEMA_NAME;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
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
        SnowflakeStreamingIngestClientFactory.builder("client1").setProperties(prop).build();

    try {
      // Create a open channel request on table T_STREAMINGINGEST
      OpenChannelRequest request1 =
          OpenChannelRequest.builder("CHANNEL1")
              .setDBName(INTERNAL_STAGE_DB_NAME)
              .setSchemaName(INTERNAL_STAGE_SCHEMA_NAME)
              .setTableName("T_STREAMINGINGEST")
              .build();

      // Open a streaming ingest channel from the given client
      SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);

      for (int val = 0; val < 1000; val++) {
        Map<String, Object> row = new HashMap<>();
        row.put("c1", Integer.toString(val));
        row.put("c2", val + 1);
        channel1.insertRow(row, null);
      }
    } finally {
      client.close().get();
    }
  }
}
