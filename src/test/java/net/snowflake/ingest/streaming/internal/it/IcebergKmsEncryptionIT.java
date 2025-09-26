/*
 * Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import net.snowflake.ingest.IcebergIT;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.internal.FileLocationInfo;
import net.snowflake.ingest.streaming.internal.IStorageManager;
import net.snowflake.ingest.streaming.internal.SnowflakeStreamingIngestClientInternal;
import net.snowflake.ingest.streaming.internal.TableRef;
import net.snowflake.ingest.utils.Constants;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for KMS encryption with Iceberg streaming ingest. */
@Ignore("SNOW-2302576 KMS encryption support is not in prod yet, targeting 9.31 release")
@Category(IcebergIT.class)
public class IcebergKmsEncryptionIT {
  private static final Logger logger = LoggerFactory.getLogger(IcebergKmsEncryptionIT.class);

  private String database;
  private String schema;
  private Connection conn;
  private SnowflakeStreamingIngestClient client;

  @Before
  public void before() throws Exception {
    database = String.format("SDK_ICEBERG_KMS_ENCRYPTION_IT_DB_%d", System.nanoTime());
    schema = "PUBLIC";
    conn = TestUtils.getConnection(true);
    client =
        TestUtils.setUp(
            conn, database, schema, true, "ZSTD", Constants.IcebergSerializationPolicy.COMPATIBLE);
  }

  @After
  public void after() throws Exception {
    if (client != null) {
      client.close();
    }
    if (conn != null) {
      conn.createStatement().execute(String.format("drop database if exists %s;", database));
      conn.close();
    }
  }

  @Test
  public void testKmsEncryptionDataIngestion() throws Exception {
    String tableName = "test_kms_encryption_table";

    // Create Iceberg table with external volume with KMS encryption
    String createTableSql =
        String.format(
            "create or replace iceberg table %s(id int) "
                + "catalog = 'SNOWFLAKE' "
                + "external_volume = 'streaming_ingest_kms' "
                + "base_location = 'SDK_IT/%s/%s';",
            tableName, database, tableName);

    conn.createStatement().execute(createTableSql);
    conn.createStatement()
        .execute(
            String.format(
                "alter iceberg table %s set ENABLE_FIX_2302576_ICEBERG_VOLUME_KMS_ENCRYPTION ="
                    + " true",
                tableName));
    verifyVolumeEncryption(tableName, true);

    OpenChannelRequest request =
        OpenChannelRequest.builder("test_kms_channel")
            .setDBName(database)
            .setSchemaName(schema)
            .setTableName(tableName)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    SnowflakeStreamingIngestChannel channel = client.openChannel(request);

    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      Map<String, Object> row = ImmutableMap.of("id", i);
      rows.add(row);
    }

    TestUtils.verifyInsertValidationResponse(channel.insertRows(rows, "offset_kms"));
    TestUtils.waitForOffset(channel, "offset_kms");

    String selectSql = String.format("select * from %s order by id", tableName);

    ResultSet rs = conn.createStatement().executeQuery(selectSql);

    for (int i = 1; i <= 10; i++) {
      assertThat(rs.next()).isTrue();
      assertThat(rs.getInt(1)).isEqualTo(i);
    }
    assertThat(rs.next()).isFalse();

    conn.createStatement()
        .execute(
            String.format(
                "alter iceberg table %s set ENABLE_FIX_2302576_ICEBERG_VOLUME_KMS_ENCRYPTION ="
                    + " false",
                tableName));
    verifyVolumeEncryption(tableName, false);

    channel.close();
  }

  private void verifyVolumeEncryption(String tableName, boolean isKmsEncryption) {
    SnowflakeStreamingIngestClientInternal<?> internalClient =
        (SnowflakeStreamingIngestClientInternal<?>) client;
    IStorageManager storageManager = internalClient.getStorageManager();

    TableRef tableRef = new TableRef(database, schema, tableName);
    FileLocationInfo fileLocationInfo =
        storageManager.getRefreshedLocation(tableRef, Optional.empty());

    String expectedVolumeEncryptionMode = null;
    if (isKmsEncryption) {
      switch (fileLocationInfo.getLocationType()) {
        case "S3":
          expectedVolumeEncryptionMode = "AWS_SSE_KMS";
          break;
        case "GCS":
          expectedVolumeEncryptionMode = "GCS_SSE_KMS";
          break;
        case "AZURE":
          expectedVolumeEncryptionMode =
              "NONE"; // Snoflake doesn't support kms encryption for Azure
          break;
        default:
          throw new IllegalArgumentException(
              "Unexpected location type: " + fileLocationInfo.getLocationType());
      }
      assertThat(fileLocationInfo.getEncryptionKmsKeyId()).isNotNull();
    } else {
      assertThat(fileLocationInfo.getEncryptionKmsKeyId()).isNull();
    }

    assertThat(fileLocationInfo.getVolumeEncryptionMode()).isEqualTo(expectedVolumeEncryptionMode);
  }
}
