package net.snowflake.ingest.streaming.internal.datatypes;

import static net.snowflake.ingest.utils.Constants.ROLE;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Predicate;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.SFException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class AbstractDataTypeTest {
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> bdecVersion() {
    return Arrays.asList(
        new Object[][] {
          {"Arrow", Constants.BdecVersion.ONE},
          // {"Parquet", Constants.BdecVersion.THREE}
        });
  }

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

  protected Connection conn;
  private String databaseName;

  private String schemaName = "PUBLIC";
  private SnowflakeStreamingIngestClient client;

  protected String randomString() {
    return UUID.randomUUID().toString().replace("-", "_");
  }

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final Constants.BdecVersion bdecVersion;

  public AbstractDataTypeTest(
      @SuppressWarnings("unused") String name, Constants.BdecVersion bdecVersion) {
    this.bdecVersion = bdecVersion;
  }

  @Before
  public void before() throws Exception {
    databaseName = String.format("SDK_DATATYPE_COMPATIBILITY_IT_%s", randomString());
    conn = TestUtils.getConnection(true);
    conn.createStatement().execute(String.format("create or replace database %s;", databaseName));
    conn.createStatement().execute(String.format("use database %s;", databaseName));
    conn.createStatement().execute(String.format("use schema %s;", schemaName));

    // Set to a random time zone not to interfere with any of the tests
    conn.createStatement().execute("alter session set timezone = 'America/New_York';");

    conn.createStatement().execute(String.format("use warehouse %s;", TestUtils.getWarehouse()));

    if (bdecVersion == Constants.BdecVersion.THREE) {
      // TODO: encryption and interleaved mode are not yet supported by server side's Parquet
      // scanner if local file cache is enabled (SNOW-656500)
      conn.createStatement().execute("alter session set disable_parquet_cache=true;");
    }

    Properties props = TestUtils.getProperties(bdecVersion);
    if (props.getProperty(ROLE).equals("DEFAULT_ROLE")) {
      props.setProperty(ROLE, "ACCOUNTADMIN");
    }
    client = SnowflakeStreamingIngestClientFactory.builder("client1").setProperties(props).build();
  }

  @After
  public void after() throws Exception {
    conn.createStatement().executeQuery(String.format("drop database %s", databaseName));
    if (client != null) {
      client.close();
    }
    if (conn != null) {
      conn.close();
    }
  }

  protected String createTable(String dataType) throws SQLException {
    String tableName =
        String.format("test_%s_%s", dataType, UUID.randomUUID())
            .replace('-', '_')
            .replace(' ', '_')
            .replace('(', '_')
            .replace(')', '_')
            .replace(',', '_');

    //    System.out.printf("Creating table %s.%s.%s%n", databaseName, schemaName, tableName);
    conn.createStatement()
        .execute(
            String.format(
                "create or replace table %s (%s string, %s %s)",
                tableName, SOURCE_COLUMN_NAME, VALUE_COLUMN_NAME, dataType));
    return tableName;
  }

  protected SnowflakeStreamingIngestChannel openChannel(String tableName) {
    OpenChannelRequest openChannelRequest =
        OpenChannelRequest.builder("CHANNEL")
            .setDBName(databaseName)
            .setSchemaName(SCHEMA_NAME)
            .setTableName(tableName)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.ABORT)
            .build();
    return client.openChannel(openChannelRequest);
  }

  private Map<String, Object> createStreamingIngestRow(Object value) {
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

  protected <T> void expectArrowNotSupported(String dataType, T value) throws Exception {
    expectError(
        dataType,
        value,
        x ->
            (x instanceof SFException
                && x.getMessage().contains("The given row cannot be converted to Arrow format")));
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
    ingestAndAssert(dataType, streamingIngestWriteValue, null, expectedValue, null, selectProvider);
  }

  /**
   * Simplified version where streaming ingest write type, JDBC write type and JDBC read type are
   * the same type
   */
  <T> void testJdbcTypeCompatibility(String typeName, T value, Provider<T> provider)
      throws Exception {
    ingestAndAssert(typeName, value, value, value, provider, provider);
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
        typeName, writeValue, writeValue, expectedValue, insertProvider, selectProvider);
  }

  /**
   * Ingests values with streaming ingest and JDBC driver, SELECTs them back with WHERE condition
   * and asserts they exist.
   *
   * @param dataType Snowflake data type
   * @param streamingIngestWriteValue Value ingested by streaming ingest
   * @param jdbcWriteValue Value written by JDBC driver
   * @param expectedValue Expected value received from JDBC driver SELECT
   * @param insertProvider JDBC parameter provider for INSERT
   * @param selectProvider JDBC parameter provider for SELECT ... WHERE
   * @param <STREAMING_INGEST_WRITE> Type ingested by streaming ingest
   * @param <JDBC_WRITE> Type written by JDBC driver
   * @param <JDBC_READ> Type read by JDBC driver
   */
  <STREAMING_INGEST_WRITE, JDBC_WRITE, JDBC_READ> void ingestAndAssert(
      String dataType,
      STREAMING_INGEST_WRITE streamingIngestWriteValue,
      JDBC_WRITE jdbcWriteValue,
      JDBC_READ expectedValue,
      Provider<JDBC_WRITE> insertProvider,
      Provider<JDBC_READ> selectProvider)
      throws Exception {
    if (jdbcWriteValue == null ^ insertProvider == null)
      throw new IllegalArgumentException(
          "jdbcWriteValue and provider must be both null or not null");
    boolean insertAlsoWithJdbc = jdbcWriteValue != null;
    String tableName = createTable(dataType);
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
    } else if (dataType.startsWith("TIMESTAMP_")) {
      selectQuery =
          String.format(
              "select count(*) from %s where to_varchar(%s, 'YYYY-MM-DD HH24:MI:SS.FF TZHTZM') ="
                  + " ?;",
              tableName, VALUE_COLUMN_NAME);
    } else if (dataType.startsWith("TIME")) {
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
  }

  <STREAMING_INGEST_WRITE> void assertVariant(
      String dataType,
      STREAMING_INGEST_WRITE streamingIngestWriteValue,
      String expectedValue,
      String expectedType)
      throws Exception {

    String tableName = createTable(dataType);
    String offsetToken = UUID.randomUUID().toString();

    // Ingest using streaming ingest
    SnowflakeStreamingIngestChannel channel = openChannel(tableName);
    channel.insertRow(createStreamingIngestRow(streamingIngestWriteValue), offsetToken);
    TestUtils.waitForOffset(channel, offsetToken);

    String query =
        String.format(
            "select %s, typeof(%s) from %s", VALUE_COLUMN_NAME, VALUE_COLUMN_NAME, tableName);
    ResultSet resultSet = conn.createStatement().executeQuery(query);
    int counter = 0;
    String value = null;
    String typeof = null;
    while (resultSet.next()) {
      counter++;
      value = resultSet.getString(1);
      typeof = resultSet.getString(2);
    }

    Assert.assertEquals(1, counter);
    Assert.assertEquals(objectMapper.readTree(expectedValue), objectMapper.readTree(value));
    Assert.assertEquals(expectedType, typeof);
  }
}
