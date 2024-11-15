/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.datatypes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.SFException;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;

public abstract class AbstractDataTypeTest {
  private static final String SOURCE_COLUMN_NAME = "source";
  private static final String VALUE_COLUMN_NAME = "value";

  private static final String SOURCE_STREAMING_INGEST = "STREAMING_INGEST";
  private static final String SOURCE_JDBC = "JDBC";
  private static final String SCHEMA_NAME = "PUBLIC";

  protected static BigInteger MAX_ALLOWED_BIG_INTEGER =
      new BigInteger("99999999999999999999999999999999999999");
  protected static BigInteger MIN_ALLOWED_BIG_INTEGER =
      new BigInteger("-99999999999999999999999999999999999999");

  protected static BigDecimal MAX_ALLOWED_BIG_DECIMAL = new BigDecimal(MAX_ALLOWED_BIG_INTEGER);
  protected static BigDecimal MIN_ALLOWED_BIG_DECIMAL = new BigDecimal(MIN_ALLOWED_BIG_INTEGER);
  protected static final ObjectMapper objectMapper =
      JsonMapper.builder().enable(JsonReadFeature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER).build();

  protected Connection conn;
  private String databaseName;

  /**
   * Default timezone for newly created channels. If empty, the test will not explicitly set default
   * timezone in the channel builder.
   */
  private Optional<ZoneId> defaultTimezone = Optional.empty();

  private String schemaName = "PUBLIC";
  protected SnowflakeStreamingIngestClient client = null;
  protected Boolean enableIcebergStreaming = null;
  protected String compressionAlgorithm = null;
  protected Constants.IcebergSerializationPolicy serializationPolicy = null;

  protected void setUp(
      boolean enableIcebergStreaming,
      String compressionAlgorithm,
      Constants.IcebergSerializationPolicy serializationPolicy)
      throws Exception {
    this.enableIcebergStreaming = enableIcebergStreaming;
    this.compressionAlgorithm = compressionAlgorithm;
    this.serializationPolicy = serializationPolicy;
    this.databaseName = String.format("SDK_DATATYPE_COMPATIBILITY_IT_%s", getRandomIdentifier());
    this.conn = TestUtils.getConnection(true);
    this.client =
        TestUtils.setUp(
            conn,
            databaseName,
            schemaName,
            enableIcebergStreaming,
            compressionAlgorithm,
            serializationPolicy);
  }

  @After
  public void after() throws Exception {
    this.defaultTimezone = Optional.empty();
    if (conn != null) {
      conn.createStatement().executeQuery(String.format("drop database %s", databaseName));
    }
    if (client != null) {
      client.close();
    }
    if (conn != null) {
      conn.close();
    }
  }

  void setChannelDefaultTimezone(ZoneId defaultTimezone) {
    this.defaultTimezone = Optional.of(defaultTimezone);
  }

  protected String createTable(String dataType) throws SQLException {
    String tableName = getRandomIdentifier();
    conn.createStatement()
        .execute(
            String.format(
                "create or replace table %s (%s string, %s %s)",
                tableName, SOURCE_COLUMN_NAME, VALUE_COLUMN_NAME, dataType));

    return tableName;
  }

  protected String createIcebergTable(String dataType) throws SQLException {
    String tableName = getRandomIdentifier();
    String baseLocation = String.format("SDK_IT/%s/%s", databaseName, tableName);
    conn.createStatement()
        .execute(
            String.format(
                "create or replace iceberg table %s (%s string, %s %s) %s",
                tableName,
                SOURCE_COLUMN_NAME,
                VALUE_COLUMN_NAME,
                dataType,
                getIcebergTableConfig(tableName)));

    return tableName;
  }

  protected String createIcebergTableWithColumns(String columns) throws SQLException {
    String tableName = getRandomIdentifier();
    String baseLocation =
        String.format("SDK_IT/%s/%s/%s", databaseName, columns.replace(" ", "_"), tableName);
    conn.createStatement()
        .execute(
            String.format(
                "create or replace iceberg table %s (%s) %s",
                tableName, columns, getIcebergTableConfig(tableName)));

    return tableName;
  }

  protected String getIcebergTableConfig(String tableName) {
    String baseLocation = String.format("SDK_IT/%s/%s", databaseName, tableName);
    return String.format(
        "catalog = 'SNOWFLAKE' "
            + "external_volume = 'streaming_ingest' "
            + "base_location = '%s';",
        baseLocation);
  }

  protected String getRandomIdentifier() {
    return String.format("test_%s", UUID.randomUUID()).replace('-', '_');
  }

  protected SnowflakeStreamingIngestChannel openChannel(String tableName) {
    return openChannel(tableName, OpenChannelRequest.OnErrorOption.ABORT);
  }

  protected SnowflakeStreamingIngestChannel openChannel(
      String tableName, OpenChannelRequest.OnErrorOption onErrorOption) {
    OpenChannelRequest.OpenChannelRequestBuilder requestBuilder =
        OpenChannelRequest.builder("CHANNEL")
            .setDBName(databaseName)
            .setSchemaName(SCHEMA_NAME)
            .setTableName(tableName)
            .setOnErrorOption(onErrorOption);
    defaultTimezone.ifPresent(requestBuilder::setDefaultTimezone);
    OpenChannelRequest openChannelRequest = requestBuilder.build();
    return client.openChannel(openChannelRequest);
  }

  protected Map<String, Object> createStreamingIngestRow(Object value) {
    Map<String, Object> row = new HashMap<>();
    row.put(SOURCE_COLUMN_NAME, SOURCE_STREAMING_INGEST);
    row.put(VALUE_COLUMN_NAME, value);
    return row;
  }

  private <T> void expectError(String dataType, T value, Predicate<Exception> errorMatcher)
      throws Exception {
    String tableName = createTable(dataType);
    SnowflakeStreamingIngestChannel channel = null;
    try {
      channel = openChannel(tableName);
      channel.insertRow(createStreamingIngestRow(value), "0");
      Assert.fail(
          String.format("Inserting value %s for data type %s should fail", value, dataType));
    } catch (Exception e) {
      if (errorMatcher.test(e)) {
        // all good, expected exception has been thrown
      } else {
        e.printStackTrace();
        Assert.fail(String.format("Unexpected exception thrown: %s", e.getMessage()));
      }
    } finally {
      if (channel != null) {
        channel.close().get();
      }
    }
  }

  protected <T> void expectNumberOutOfRangeError(
      String dataType, T value, int maxPowerOf10Exclusive) throws Exception {
    expectError(
        dataType,
        value,
        x ->
            (x instanceof SFException
                && x.getMessage()
                    .contains(
                        String.format(
                            "Number out of representable exclusive range of (-1e%d..1e%d)",
                            maxPowerOf10Exclusive, maxPowerOf10Exclusive))));
  }

  protected <T> void expectNotSupported(String dataType, T value) throws Exception {
    expectError(
        dataType,
        value,
        x ->
            (x instanceof SFException
                && x.getMessage()
                    .contains("The given row cannot be converted to the internal format")));
  }

  /**
   * Simplified version, which does not insert using JDBC. Useful for testing non-JDBC types like
   * BigInteger, java.time.* types, etc.
   */
  <VALUE> void testIngestion(String dataType, VALUE expectedValue, Provider<VALUE> selectProvider)
      throws Exception {
    ingestAndAssert(
        dataType,
        expectedValue,
        null,
        expectedValue,
        null,
        selectProvider,
        false /* enableIcebergStreaming */);
  }

  <VALUE> void testIcebergIngestion(
      String dataType, VALUE expectedValue, Provider<VALUE> selectProvider) throws Exception {
    ingestAndAssert(
        dataType,
        expectedValue,
        null,
        expectedValue,
        null,
        selectProvider,
        true /* enableIcebergStreaming */);
  }

  /**
   * Simplified version, which does not insert using JDBC. Useful for testing non-JDBC types like
   * BigInteger, java.time.* types, etc.
   */
  <STREAMING_INGEST_WRITE, JDBC_READ> void testIngestion(
      String dataType,
      STREAMING_INGEST_WRITE streamingIngestWriteValue,
      JDBC_READ expectedValue,
      Provider<JDBC_READ> selectProvider)
      throws Exception {
    ingestAndAssert(
        dataType,
        streamingIngestWriteValue,
        null,
        expectedValue,
        null,
        selectProvider,
        false /* enableIcebergStreaming */);
  }

  <STREAMING_INGEST_WRITE, JDBC_READ> void testIcebergIngestion(
      String dataType,
      STREAMING_INGEST_WRITE streamingIngestWriteValue,
      JDBC_READ expectedValue,
      Provider<JDBC_READ> selectProvider)
      throws Exception {
    ingestAndAssert(
        dataType,
        streamingIngestWriteValue,
        null,
        expectedValue,
        null,
        selectProvider,
        true /* enableIcebergStreaming */);
  }

  /**
   * Simplified version where streaming ingest write type, JDBC write type and JDBC read type are
   * the same type
   */
  <T> void testJdbcTypeCompatibility(String typeName, T value, Provider<T> provider)
      throws Exception {
    ingestAndAssert(
        typeName, value, value, value, provider, provider, false /* enableIcebergStreaming */);
  }

  /** Simplified version where write value for streaming ingest and JDBC are the same */
  <WRITE, READ> void testJdbcTypeCompatibility(
      String typeName,
      WRITE writeValue,
      READ expectedValue,
      Provider<WRITE> insertProvider,
      Provider<READ> selectProvider)
      throws Exception {
    ingestAndAssert(
        typeName,
        writeValue,
        writeValue,
        expectedValue,
        insertProvider,
        selectProvider,
        false /* enableIcebergStreaming */);
  }

  /**
   * Ingests values with streaming ingest and JDBC driver, SELECTs them back with WHERE condition
   * and asserts they exist.
   *
   * @param <STREAMING_INGEST_WRITE> Type ingested by streaming ingest
   * @param <JDBC_WRITE> Type written by JDBC driver
   * @param <JDBC_READ> Type read by JDBC driver
   * @param dataType Snowflake data type
   * @param streamingIngestWriteValue Value ingested by streaming ingest
   * @param jdbcWriteValue Value written by JDBC driver
   * @param expectedValue Expected value received from JDBC driver SELECT
   * @param insertProvider JDBC parameter provider for INSERT
   * @param selectProvider JDBC parameter provider for SELECT ... WHERE
   * @param enableIcebergStreaming whether the table is an iceberg table
   */
  <STREAMING_INGEST_WRITE, JDBC_WRITE, JDBC_READ> void ingestAndAssert(
      String dataType,
      STREAMING_INGEST_WRITE streamingIngestWriteValue,
      JDBC_WRITE jdbcWriteValue,
      JDBC_READ expectedValue,
      Provider<JDBC_WRITE> insertProvider,
      Provider<JDBC_READ> selectProvider,
      boolean enableIcebergStreaming)
      throws Exception {
    if (jdbcWriteValue == null ^ insertProvider == null)
      throw new IllegalArgumentException(
          "jdbcWriteValue and provider must be both null or not null");
    boolean insertAlsoWithJdbc = jdbcWriteValue != null;
    String tableName =
        enableIcebergStreaming ? createIcebergTable(dataType) : createTable(dataType);
    String offsetToken = UUID.randomUUID().toString();

    // Insert using JDBC
    if (insertAlsoWithJdbc) {
      String query = "insert into %s (select ?, ?)";
      PreparedStatement insertStatement = conn.prepareStatement(String.format(query, tableName));
      insertStatement.setString(1, SOURCE_JDBC);
      insertProvider.provide(insertStatement, 2, jdbcWriteValue);
      insertStatement.execute();
    }

    // Ingest using streaming ingest
    SnowflakeStreamingIngestChannel channel = openChannel(tableName);
    channel.insertRow(createStreamingIngestRow(streamingIngestWriteValue), offsetToken);
    TestUtils.waitForOffset(channel, offsetToken);

    // Select expected value and assert that expected number of results is returned (selecting with
    // WHERE also tests pruning).
    // The following is evaluated as true in Snowflake:
    //     select '2020-01-01 14:00:00 +0000'::timestamp_tz = '2020-01-01 12:00:00
    // -0200'::timestamp_tz;
    // Therefore, to correctly test TIMESTAMP_TZ values, we need to compare them as strings.
    // We can do that for all date/time types, not just TIMESTAMP_TZ.
    String selectQuery;
    if (expectedValue == null) {
      selectQuery =
          String.format("select count(*) from %s where %s is NULL", tableName, VALUE_COLUMN_NAME);
    } else if (dataType.toUpperCase().startsWith("TIMESTAMP")) {
      selectQuery =
          String.format(
              "select count(*) from %s where to_varchar(%s, 'YYYY-MM-DD HH24:MI:SS.FF TZHTZM') ="
                  + " ?;",
              tableName, VALUE_COLUMN_NAME);
    } else if (dataType.toUpperCase().startsWith("TIME")) {
      selectQuery =
          String.format(
              "select count(*) from %s where to_varchar(%s, 'HH24:MI:SS.FF TZHTZM') = ?;",
              tableName, VALUE_COLUMN_NAME);
    } else {
      selectQuery = "select count(*) from %s where %s = ?";
    }
    PreparedStatement selectStatement =
        conn.prepareStatement(String.format(selectQuery, tableName, VALUE_COLUMN_NAME));
    if (expectedValue != null) {
      selectProvider.provide(selectStatement, 1, expectedValue);
    }
    ResultSet resultSet = selectStatement.executeQuery();
    Assert.assertTrue(resultSet.next());
    int count = resultSet.getInt(1);
    Assert.assertEquals(insertAlsoWithJdbc ? 2 : 1, count);
    if (!enableIcebergStreaming) {
      migrateTable(tableName); // migration should always succeed
    }
  }

  <STREAMING_INGEST_WRITE> void assertVariant(
      String dataType,
      STREAMING_INGEST_WRITE streamingIngestWriteValue,
      String expectedValue,
      String expectedType)
      throws Exception {
    assertVariant(
        dataType,
        streamingIngestWriteValue,
        expectedValue,
        expectedType,
        false /*Not max variant or max array*/);
  }

  <STREAMING_INGEST_WRITE> void assertVariant(
      String dataType,
      STREAMING_INGEST_WRITE streamingIngestWriteValue,
      String expectedValue,
      String expectedType,
      boolean isAtMaxValueForDataType)
      throws Exception {

    String tableName = createTable(dataType);
    String offsetToken = UUID.randomUUID().toString();

    // Ingest using streaming ingest
    SnowflakeStreamingIngestChannel channel = openChannel(tableName);
    channel.insertRow(createStreamingIngestRow(streamingIngestWriteValue), offsetToken);
    TestUtils.waitForOffset(channel, offsetToken);

    String query;
    // if added row value is using max possible value, we will not verify its returned type since
    // JDBC doesnt parse it properly because of Jackson Databind version 2.15
    // we will only verify type of it.
    // Check this: https://github.com/snowflakedb/snowflake-sdks-drivers-issues-teamwork/issues/819
    // TODO:: SNOW-1051731
    if (isAtMaxValueForDataType) {
      query = String.format("select typeof(%s) from %s", VALUE_COLUMN_NAME, tableName);
    } else {
      query =
          String.format(
              "select typeof(%s), %s  from %s", VALUE_COLUMN_NAME, VALUE_COLUMN_NAME, tableName);
    }
    ResultSet resultSet = conn.createStatement().executeQuery(query);
    int counter = 0;
    String value = null;
    String typeof = null;
    while (resultSet.next()) {
      counter++;
      typeof = resultSet.getString(1);
      if (!isAtMaxValueForDataType) value = resultSet.getString(2);
    }

    Assert.assertEquals(1, counter);
    if (!isAtMaxValueForDataType) {
      if (expectedValue == null) {
        Assert.assertNull(value);
      } else {
        Assert.assertEquals(objectMapper.readTree(expectedValue), objectMapper.readTree(value));
      }
    }
    Assert.assertEquals(expectedType, typeof);
    migrateTable(tableName); // migration should always succeed
  }

  void assertVariantLiterally(
      String dataType, String writeValue, String expectedValue, String expectedType)
      throws Exception {

    String tableName = createTable(dataType);
    String offsetToken = UUID.randomUUID().toString();

    // Ingest using streaming ingest
    SnowflakeStreamingIngestChannel channel = openChannel(tableName);
    channel.insertRow(createStreamingIngestRow(writeValue), offsetToken);
    TestUtils.waitForOffset(channel, offsetToken);

    final String query =
        String.format(
            "select %s as v1, typeof(v1) as v1_type, parse_json('%s') as v2, typeof(v2) as v2_type"
                + " from %s",
            VALUE_COLUMN_NAME, writeValue, tableName);
    try (ResultSet resultSet = conn.createStatement().executeQuery(query)) {
      resultSet.next();
      Assert.assertEquals(expectedValue, resultSet.getString("V1"));
      Assert.assertEquals(expectedValue, resultSet.getString("V2"));
      Assert.assertEquals(expectedType, resultSet.getString("V1_TYPE"));
      Assert.assertEquals(expectedType, resultSet.getString("V2_TYPE"));
    }
    ;
  }

  protected void migrateTable(String tableName) throws SQLException {
    conn.createStatement().execute(String.format("alter table %s migrate;", tableName));
  }

  /**
   * Ingest multiple values, wait for the latest offset to be committed, migrate the table and
   * assert no errors have been thrown.
   */
  @SafeVarargs
  protected final <STREAMING_INGEST_WRITE> void ingestManyAndMigrate(
      String datatype, final STREAMING_INGEST_WRITE... values) throws Exception {
    String tableName = createTable(datatype);
    SnowflakeStreamingIngestChannel channel = openChannel(tableName);
    String offsetToken = null;
    for (int i = 0; i < values.length; i++) {
      offsetToken = String.format("offsetToken%d", i);
      channel.insertRow(createStreamingIngestRow(values[i]), offsetToken);
    }

    TestUtils.waitForOffset(channel, offsetToken);
    migrateTable(tableName); // migration should always succeed
  }

  protected void testIcebergIngestAndQuery(
      String dataType,
      Iterable<Object> values,
      String queryTemplate,
      Iterable<Object> expectedValues)
      throws Exception {
    String tableName = createIcebergTable(dataType);
    SnowflakeStreamingIngestChannel channel = openChannel(tableName);
    String offsetToken = null;
    for (Object value : values) {
      offsetToken = UUID.randomUUID().toString();
      channel.insertRow(createStreamingIngestRow(value), offsetToken);
    }
    TestUtils.waitForOffset(channel, offsetToken);

    String verificationQuery =
        queryTemplate.replace("{tableName}", tableName).replace("{columnName}", VALUE_COLUMN_NAME);
    ResultSet resultSet = conn.createStatement().executeQuery(verificationQuery);

    for (Object expectedValue : expectedValues) {
      Assertions.assertThat(resultSet.next()).isTrue();
      Object res = resultSet.getObject(1);
      assertEqualValues(expectedValue, res);
    }
    Assertions.assertThat(resultSet.next()).isFalse();
  }

  protected void verifyMultipleColumns(
      String tableName,
      SnowflakeStreamingIngestChannel channel,
      List<Map<String, Object>> values,
      List<Map<String, Object>> expectedValues,
      String orderBy)
      throws Exception {

    Set<String> keySet = new HashSet<>();
    String offsetToken = null;
    for (Map<String, Object> value : values) {
      offsetToken = UUID.randomUUID().toString();
      channel.insertRow(value, offsetToken);
    }
    TestUtils.waitForOffset(channel, offsetToken);

    for (Map<String, Object> value : expectedValues) {
      keySet.addAll(value.keySet());
    }

    List<String> keyList = new ArrayList<>(keySet);
    String query =
        String.format(
            "select %s from %s order by %s", StringUtils.join(keyList, ", "), tableName, orderBy);
    ResultSet resultSet = conn.createStatement().executeQuery(query);

    for (Map<String, Object> expectedValue : expectedValues) {
      Assertions.assertThat(resultSet.next()).isTrue();
      for (String key : keyList) {
        assertEqualValues(expectedValue.get(key), resultSet.getObject(keyList.indexOf(key) + 1));
      }
    }
    Assertions.assertThat(resultSet.next()).isFalse();
  }

  private void assertEqualValues(Object expectedValue, Object actualValue)
      throws JsonProcessingException {
    if (expectedValue instanceof BigDecimal) {
      Assertions.assertThat(actualValue)
          .usingComparatorForType(BigDecimal::compareTo, BigDecimal.class)
          .usingRecursiveComparison()
          .isEqualTo(expectedValue);
    } else if (expectedValue instanceof Map) {
      Assertions.assertThat(objectMapper.readTree((String) actualValue))
          .isEqualTo(objectMapper.valueToTree(expectedValue));
    } else if (expectedValue instanceof Timestamp) {
      Assertions.assertThat(actualValue.toString()).isEqualTo(expectedValue.toString());
    } else {
      Assertions.assertThat(actualValue).isEqualTo(expectedValue);
    }
  }
}
