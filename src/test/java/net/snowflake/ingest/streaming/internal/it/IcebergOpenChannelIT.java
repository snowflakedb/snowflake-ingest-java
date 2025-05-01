/*
 * Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.it;

import java.sql.Connection;
import net.snowflake.ingest.IcebergIT;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.SFException;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Ignore("SNOW-1903895 ENABLE_FIX_1633616_ALLOW_ROLLOUT_PARTITIONED_ICEBERG is not set in prod yet")
@Category(IcebergIT.class)
@RunWith(Parameterized.class)
public class IcebergOpenChannelIT {
  @Parameterized.Parameters(name = "icebergSerializationPolicy={0}")
  public static Object[] parameters() {
    return new Object[] {
      Constants.IcebergSerializationPolicy.COMPATIBLE,
      Constants.IcebergSerializationPolicy.OPTIMIZED
    };
  }

  @Parameterized.Parameter
  public static Constants.IcebergSerializationPolicy icebergSerializationPolicy;

  private String database;
  private String schema;
  private Connection conn;
  private SnowflakeStreamingIngestClient client;

  @Before
  public void before() throws Exception {
    database = String.format("SDK_ICEBERG_OPEN_CHANNEL_IT_DB_%d", System.nanoTime());
    schema = "PUBLIC";
    conn = TestUtils.getConnection(true);
    client = TestUtils.setUp(conn, database, schema, true, "ZSTD", icebergSerializationPolicy);
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
  public void testOpenChannelToIcebergPartitioned() throws Exception {
    String tableName = "test_open_channel_to_iceberg_partitioned";

    conn.createStatement().execute("call SYSTEM$SET_DEPLOYMENT_TYPE_PARAMS('PROD');");
    conn.createStatement()
        .execute("alter session set ENABLE_FIX_1633616_ALLOW_ROLLOUT_PARTITIONED_ICEBERG = True;");
    conn.createStatement().execute("alter session set FEATURE_PARTITIONED_ICEBERG = Enabled;");

    conn.createStatement()
        .execute(
            String.format(
                "create or replace iceberg table %s(col int)"
                    + "partition by (col) "
                    + "catalog = 'SNOWFLAKE' "
                    + "external_volume = 'streaming_ingest' "
                    + "base_location = 'SDK_IT/%s/%s';",
                tableName, database, tableName));

    OpenChannelRequest request =
        OpenChannelRequest.builder("test_channel")
            .setDBName(database)
            .setSchemaName(schema)
            .setTableName(tableName)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    Assertions.assertThatThrownBy(() -> client.openChannel(request))
        .isInstanceOf(SFException.class)
        .hasMessageContaining("\"status_code\" : 84")
        .hasMessageContaining("Snowpipe Streaming doesn't support Iceberg Partitioned Tables");
  }
}
