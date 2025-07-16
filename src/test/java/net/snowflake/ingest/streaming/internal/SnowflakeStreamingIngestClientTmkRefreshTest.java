/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.HashMap;
import java.util.Properties;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ParameterProvider;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpStatus;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

/**
 * Unit tests for encryption key (TMK - Table Master Key) refresh functionality in the openChannel
 * method of SnowflakeStreamingIngestClientInternal.
 */
@RunWith(Parameterized.class)
public class SnowflakeStreamingIngestClientTmkRefreshTest {

  @Parameterized.Parameter public boolean enableIcebergStreaming;

  @Parameterized.Parameters(name = "enableIcebergStreaming: {0}")
  public static Object[] enableIcebergStreaming() {
    return new Object[] {false, true};
  }

  private static final String DB_NAME = "test_db";
  private static final String SCHEMA_NAME = "test_schema";
  private static final String TABLE_NAME = "test_table";
  private static final String TABLE2_NAME = "test_table2";
  private static final String CHANNEL_NAME = "test_channel";
  private static final String ENCRYPTION_KEY_OLD = "old_encryption_key";
  private static final String ENCRYPTION_KEY_NEW = "new_encryption_key";
  private static final long ENCRYPTION_KEY_ID_OLD = 1000L;
  private static final long ENCRYPTION_KEY_ID_NEW = 2000L;

  SnowflakeStreamingIngestClientInternal<StubChunkData> clientInternal;
  private MockSnowflakeServiceClient.ApiOverride apiOverride;
  RequestBuilder requestBuilder;

  @Before
  public void setup() throws Exception {
    Properties enableIcebergStreamingProp = new Properties();
    enableIcebergStreamingProp.setProperty(
        ParameterProvider.ENABLE_ICEBERG_STREAMING, String.valueOf(enableIcebergStreaming));

    apiOverride = new MockSnowflakeServiceClient.ApiOverride();
    CloseableHttpClient httpClient = MockSnowflakeServiceClient.createHttpClient(apiOverride);
    requestBuilder =
        Mockito.spy(
            MockSnowflakeServiceClient.createRequestBuilder(httpClient, enableIcebergStreaming));
    clientInternal =
        new SnowflakeStreamingIngestClientInternal<>(
            "client",
            null,
            enableIcebergStreamingProp,
            httpClient,
            true,
            requestBuilder,
            new HashMap<>());
  }

  @Test
  public void testOpenChannelEncryptionKeyRefresh_UpdatesExistingCacheEntry() throws Exception {
    // Test that encryption key cache is updated when opening a channel with new encryption key
    // and an existing cache entry exists

    // Pre-populate the cache with an existing encryption key
    FullyQualifiedTableName fqn = new FullyQualifiedTableName(DB_NAME, SCHEMA_NAME, TABLE_NAME);
    EncryptionKey existingKey =
        new EncryptionKey(
            DB_NAME, SCHEMA_NAME, TABLE_NAME, ENCRYPTION_KEY_OLD, ENCRYPTION_KEY_ID_OLD);
    clientInternal.getEncryptionKeysPerTable().put(fqn, existingKey);

    // Setup mock response with new encryption key
    String openChannelResponse =
        createOpenChannelResponse(ENCRYPTION_KEY_NEW, ENCRYPTION_KEY_ID_NEW);

    apiOverride.addSerializedJsonOverride(
        Constants.OPEN_CHANNEL_ENDPOINT, request -> Pair.of(HttpStatus.SC_OK, openChannelResponse));

    // Open channel
    OpenChannelRequest request =
        OpenChannelRequest.builder(CHANNEL_NAME)
            .setDBName(DB_NAME)
            .setSchemaName(SCHEMA_NAME)
            .setTableName(TABLE_NAME)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    clientInternal.openChannel(request);

    // Verify that the encryption key was updated in the cache
    EncryptionKey updatedKey = clientInternal.getEncryptionKeysPerTable().get(fqn);
    Assert.assertNotNull("Encryption key should exist in cache", updatedKey);
    Assert.assertEquals("Database name should match", DB_NAME, updatedKey.getDatabaseName());
    Assert.assertEquals("Schema name should match", SCHEMA_NAME, updatedKey.getSchemaName());
    Assert.assertEquals("Table name should match", TABLE_NAME, updatedKey.getTableName());
    Assert.assertEquals(
        "Encryption key should be updated", ENCRYPTION_KEY_NEW, updatedKey.getEncryptionKey());
    Assert.assertEquals(
        "Encryption key ID should be updated",
        ENCRYPTION_KEY_ID_NEW,
        updatedKey.getEncryptionKeyId());
  }

  @Test
  public void testOpenChannelEncryptionKeyRefresh_NoUpdateWhenNoCacheEntry() throws Exception {
    // Test that encryption key cache is NOT updated when opening a channel with new encryption key
    // but no existing cache entry exists (computeIfPresent behavior)

    // Setup mock response with encryption key
    String openChannelResponse =
        createOpenChannelResponse(ENCRYPTION_KEY_NEW, ENCRYPTION_KEY_ID_NEW);

    apiOverride.addSerializedJsonOverride(
        Constants.OPEN_CHANNEL_ENDPOINT, request -> Pair.of(HttpStatus.SC_OK, openChannelResponse));

    // Open channel
    OpenChannelRequest request =
        OpenChannelRequest.builder(CHANNEL_NAME)
            .setDBName(DB_NAME)
            .setSchemaName(SCHEMA_NAME)
            .setTableName(TABLE_NAME)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    clientInternal.openChannel(request);

    // Verify that no encryption key was added to the cache
    Assert.assertTrue(
        "Encryption key should not be added to cache when no existing entry",
        clientInternal.getEncryptionKeysPerTable().isEmpty());
  }

  @Test
  public void testOpenChannelEncryptionKeyRefresh_MultipleTablesIndependent() throws Exception {
    // Test that encryption key refresh works independently for multiple tables

    // Setup keys for both tables
    FullyQualifiedTableName fqn1 = new FullyQualifiedTableName(DB_NAME, SCHEMA_NAME, TABLE_NAME);
    FullyQualifiedTableName fqn2 = new FullyQualifiedTableName(DB_NAME, SCHEMA_NAME, TABLE2_NAME);

    EncryptionKey table1Key =
        new EncryptionKey(DB_NAME, SCHEMA_NAME, TABLE_NAME, "old_key1", 1000L);
    EncryptionKey table2Key =
        new EncryptionKey(DB_NAME, SCHEMA_NAME, TABLE2_NAME, "old_key2", 2000L);

    clientInternal.getEncryptionKeysPerTable().put(fqn1, table1Key);
    clientInternal.getEncryptionKeysPerTable().put(fqn2, table2Key);

    // Open channel for table1 with new encryption key
    String newEncryptionKey = "new_key1";
    long newEncryptionKeyId = 3000L;

    String openChannelResponse = createOpenChannelResponse(newEncryptionKey, newEncryptionKeyId);

    apiOverride.addSerializedJsonOverride(
        Constants.OPEN_CHANNEL_ENDPOINT, request -> Pair.of(HttpStatus.SC_OK, openChannelResponse));

    OpenChannelRequest request =
        OpenChannelRequest.builder(CHANNEL_NAME)
            .setDBName(DB_NAME)
            .setSchemaName(SCHEMA_NAME)
            .setTableName(TABLE_NAME)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    clientInternal.openChannel(request);

    // Verify that only table1's encryption key was updated
    EncryptionKey updatedTable1Key = clientInternal.getEncryptionKeysPerTable().get(fqn1);
    EncryptionKey unchangedTable2Key = clientInternal.getEncryptionKeysPerTable().get(fqn2);

    Assert.assertNotNull("Table1 encryption key should exist", updatedTable1Key);
    Assert.assertEquals(
        "Table1 encryption key should be updated",
        newEncryptionKey,
        updatedTable1Key.getEncryptionKey());
    Assert.assertEquals(
        "Table1 encryption key ID should be updated",
        newEncryptionKeyId,
        updatedTable1Key.getEncryptionKeyId());

    Assert.assertNotNull("Table2 encryption key should still exist", unchangedTable2Key);
    Assert.assertEquals(
        "Table2 encryption key should remain unchanged",
        "old_key2",
        unchangedTable2Key.getEncryptionKey());
    Assert.assertEquals(
        "Table2 encryption key ID should remain unchanged",
        (long) 2000L,
        unchangedTable2Key.getEncryptionKeyId());
  }

  /** Helper method to create an OpenChannel response JSON string */
  private String createOpenChannelResponse(String encryptionKey, Long encryptionKeyId) {
    StringBuilder response = new StringBuilder();
    response.append("{\n");
    response.append("  \"status_code\" : 0,\n");
    response.append("  \"message\" : \"Success\",\n");
    response.append("  \"database\" : \"").append(DB_NAME).append("\",\n");
    response.append("  \"schema\" : \"").append(SCHEMA_NAME).append("\",\n");
    response.append("  \"table\" : \"").append(TABLE_NAME).append("\",\n");
    response.append("  \"channel\" : \"").append(TABLE2_NAME).append("\",\n");
    response.append("  \"client_sequencer\" : 123,\n");
    response.append("  \"row_sequencer\" : 123,\n");
    response.append("  \"offset_token\" : \"test_offset_token\",\n");
    response.append("  \"encryption_key\" : \"").append(encryptionKey).append("\",\n");
    response.append("  \"encryption_key_id\" : ").append(encryptionKeyId).append(",\n");

    if (enableIcebergStreaming) {
      response.append("  \"iceberg_serialization_policy\" : \"OPTIMIZED\",\n");
    }
    response.append("  \"table_columns\" : [ {\n");
    response.append("    \"ordinal\" : 1,\n");
    response.append("    \"name\" : \"C1\",\n");
    response.append("    \"type\" : \"NUMBER(38,0)\",\n");
    response.append("    \"logical_type\" : \"fixed\",\n");
    response.append("    \"physical_type\" : \"SB16\",\n");
    if (enableIcebergStreaming) {
      response.append("  \"source_iceberg_data_type\" : \"\\\"long\\\"\",\n");
    }
    response.append("    \"precision\" : 38,\n");
    response.append("    \"scale\" : 0,\n");
    response.append("    \"byte_length\" : null,\n");
    response.append("    \"length\" : null,\n");
    response.append("    \"nullable\" : true\n");
    response.append("  } ]\n");
    response.append("}");

    return response.toString();
  }
}
