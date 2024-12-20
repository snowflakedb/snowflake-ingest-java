/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

  private static final String jsonStr = "{\"txid\":\"15975ed1-b09c-4e15-93a2-3c232dd5ea5c\",\"rfid\":\"0x46abc5ff4f7f13e0750462ef\",\"resort\":\"Whistler Blackcomb\",\"purchase_time\":\"2024-12-12T19:57:59.188489\",\"expiration_time\":\"2023-06-01\",\"days\":2,\"name\":\"Shannon Mccarthy\",\"address\":null,\"phone\":\"001-652-728-5967x865\",\"email\":null,\"emergency_contact\":{\"name\":\"Douglas Rowland\",\"phone\":\"314.829.3980x99274\"}}\n";

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
        SnowflakeStreamingIngestClientFactory.builder("MY_CLIENT").setProperties(props).build()) {

      OpenChannelRequest request1 =
          OpenChannelRequest.builder("MY_CHANNEL")
              .setDBName("TESTDB_KAFKA")
              .setSchemaName("KAFKA_TEST")
              .setTableName("REVI_TABLE")
              .setOnErrorOption(
                  OpenChannelRequest.OnErrorOption.CONTINUE) // Another ON_ERROR option is ABORT
              .build();

      // Open a streaming ingest channel from the given client
      SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);
      int offset = 0;

      // null string
      Map<String, Object> row = new HashMap<>();
      row.put("c1", "'null' string");
      row.put("c2", "null");
      channel1.insertRow(row, String.valueOf(offset));
      offset++;

      // empty string
      row.put("c1", "empty string");
      row.put("c2", "");
      channel1.insertRow(row, String.valueOf(offset));
      offset++;

      // java null
      row.put("c1", "java null");
      row.put("c2", null);
      channel1.insertRow(row, String.valueOf(offset));
      offset++;

      // json nullnode null
      ObjectNode root = mapper.createObjectNode();
      root.set("nullfield", NullNode.instance);
      row.put("c1", "json null");
      row.put("c2", root.get("nullfield").asText());
      channel1.insertRow(row, String.valueOf(offset));
      offset++;

      // given example data
      Map<String, Object> jsonNode = mapper.readValue(jsonStr, Map.class);
      for (Map.Entry<String, Object> entry : jsonNode.entrySet()) {
        row.put("c1", entry.getKey());
        row.put("c2", entry.getValue());

        channel1.insertRow(row, String.valueOf(offset));
        offset++;
      }

      channel1.close().get();
    }
  }
}
