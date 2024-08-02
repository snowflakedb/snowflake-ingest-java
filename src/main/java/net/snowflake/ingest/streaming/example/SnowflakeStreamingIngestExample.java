/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.example;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeFileTransferConfig;
import net.snowflake.client.jdbc.SnowflakeFileTransferMetadataV1;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;

/**
 * Example on how to use the Streaming Ingest client APIs.
 *
 * <p>Please read the README.md file for detailed steps
 */
public class SnowflakeStreamingIngestExample {
  // Please follow the example in profile_streaming.json.example to see the required properties, or
  // if you have already set up profile.json with Snowpipe before, all you need is to add the "role"
  // property. If the "role" is not specified, the default user role will be applied.
  private static String PROFILE_PATH = "profile.json";
  private static final ObjectMapper mapper = new ObjectMapper();

  public static void testUpload(SnowflakeStreamingIngestClient client) throws Exception {
    JsonNode node = mapper.readTree("");
    SnowflakeFileTransferMetadataV1 fileTransferMetadata =
        (SnowflakeFileTransferMetadataV1)
            SnowflakeFileTransferAgent.getFileTransferMetadatas(node).get(0);

    SnowflakeFileTransferAgent.uploadWithoutConnection(
        SnowflakeFileTransferConfig.Builder.newInstance()
            .setSnowflakeFileTransferMetadata(fileTransferMetadata)
            .setUploadStream(new ByteArrayInputStream("test".getBytes()))
            .setRequireCompress(false)
            .setOcspMode(OCSPMode.FAIL_OPEN)
            .setStreamingIngestClientKey(client.getName())
            .setStreamingIngestClientName(client.getName())
            .setDestFileName("test")
            .build());
  }

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
        SnowflakeStreamingIngestClientFactory.builder("MY_CLIENT")
            .setProperties(props)
            .setIsIceberg(false)
            .build()) {

      // Create an open channel request on table MY_TABLE, note that the corresponding
      // db/schema/table needs to be present
      // Example: create or replace table MY_TABLE(c1 number);
      OpenChannelRequest request1 =
          OpenChannelRequest.builder("MY_CHANNEL")
              .setDBName(props.get("database").toString())
              .setSchemaName(props.get("schema").toString())
              .setTableName(props.get("table").toString())
              .setOnErrorOption(
                  OpenChannelRequest.OnErrorOption.CONTINUE) // Another ON_ERROR option is ABORT
              .build();

      // Open a streaming ingest channel from the given client
      SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);
      //
      //      // Insert rows into the channel (Using insertRows API)
      //      final int totalRowsInTable = 1000;
      //      for (int val = 0; val < totalRowsInTable; val++) {
      //        Map<String, Object> row = new HashMap<>();
      //
      //        // c1 corresponds to the column name in table
      //        row.put("c1", val);
      //
      //        // Insert the row with the current offset_token
      //        InsertValidationResponse response = channel1.insertRow(row, String.valueOf(val));
      //        if (response.hasErrors()) {
      //          // Simply throw if there is an exception, or you can do whatever you want with the
      //          // erroneous row
      //          throw response.getInsertErrors().get(0).getException();
      //        }
      //      }

      // If needed, you can check the offset_token registered in Snowflake to make sure everything
      // is committed
      //      final int expectedOffsetTokenInSnowflake = totalRowsInTable - 1; // 0 based
      // offset_token
      //      final int maxRetries = 10;
      //      int retryCount = 0;
      //
      //      do {
      //        String offsetTokenFromSnowflake = channel1.getLatestCommittedOffsetToken();
      //        if (offsetTokenFromSnowflake != null
      //            &&
      // offsetTokenFromSnowflake.equals(String.valueOf(expectedOffsetTokenInSnowflake))) {
      //          System.out.println("SUCCESSFULLY inserted " + totalRowsInTable + " rows");
      //          break;
      //        }
      //        retryCount++;
      //      } while (retryCount < maxRetries);

      Map<String, Object> row = new HashMap<>();
      row.put("boolean_col", false);
      row.put("int_col", 1234567890);

      InsertValidationResponse response = channel1.insertRow(row, "1");

      if (response.hasErrors()) {
        // Simply throw if there is an exception, or you can do whatever you want with the
        // erroneous row
        throw response.getInsertErrors().get(0).getException();
      }

      final int maxRetries = 10;
      int retryCount = 0;

      do {
        String offsetTokenFromSnowflake = channel1.getLatestCommittedOffsetToken();
        if (offsetTokenFromSnowflake != null && offsetTokenFromSnowflake.equals("1")) {
          System.out.println("SUCCESSFULLY inserted " + "1" + " rows");
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
