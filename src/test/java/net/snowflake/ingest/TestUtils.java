/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest;

import static net.snowflake.ingest.utils.Constants.ACCOUNT;
import static net.snowflake.ingest.utils.Constants.ACCOUNT_URL;
import static net.snowflake.ingest.utils.Constants.CONNECT_STRING;
import static net.snowflake.ingest.utils.Constants.DATABASE;
import static net.snowflake.ingest.utils.Constants.HOST;
import static net.snowflake.ingest.utils.Constants.PORT;
import static net.snowflake.ingest.utils.Constants.PRIVATE_KEY;
import static net.snowflake.ingest.utils.Constants.ROLE;
import static net.snowflake.ingest.utils.Constants.SCHEMA;
import static net.snowflake.ingest.utils.Constants.SCHEME;
import static net.snowflake.ingest.utils.Constants.SSL;
import static net.snowflake.ingest.utils.Constants.USER;
import static net.snowflake.ingest.utils.Constants.WAREHOUSE;
import static net.snowflake.ingest.utils.ParameterProvider.BDEC_PARQUET_COMPRESSION_ALGORITHM;
import static net.snowflake.ingest.utils.ParameterProvider.BLOB_FORMAT_VERSION;
import static net.snowflake.ingest.utils.ParameterProvider.ENABLE_ICEBERG_STREAMING;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.function.Supplier;
import net.snowflake.client.jdbc.internal.apache.http.client.utils.URIBuilder;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.internal.SnowflakeStreamingIngestClientInternal;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.Constants.IcebergSerializationPolicy;
import net.snowflake.ingest.utils.ParameterProvider;
import net.snowflake.ingest.utils.SnowflakeURL;
import net.snowflake.ingest.utils.Utils;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;

public class TestUtils {
  // profile path, follow readme for the format
  private static final String PROFILE_PATH = "profile.json";

  private static final String OAUTH_INTEGRATION = "OAUTH_INTEGRATION";

  private static final ObjectMapper mapper = new ObjectMapper();

  private static boolean isInitialized = false;

  private static String user = "";

  private static String role = "";

  private static String privateKeyPem = "";

  private static PrivateKey privateKey = null;

  private static KeyPair keyPair = null;

  private static String account = "";

  private static String ssl = "";

  private static String database = "";

  private static String schema = "";

  private static String warehouse = "";

  private static String connectString = "";

  private static String scheme = "";

  private static String host = "";

  private static int port = 0;

  // Keep separate test connections for snowpipe and snowpipe streaming so that session state is
  // isolated
  private static Connection snowpipeConn = null;

  private static Connection streamingConn = null;

  private static String dummyUser = "user";
  private static int dummyPort = 443;
  private static String dummyHost = "snowflake.qa1.int.snowflakecomputing.com";
  private static String dummyScheme = "http";

  /**
   * load all login info from profile
   *
   * @throws IOException if can't read profile
   */
  private static void init() throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {
    String testProfilePath = getTestProfilePath();
    Path path = Paths.get(testProfilePath);

    if (Files.exists(path)) {
      ObjectNode profile = (ObjectNode) mapper.readTree(new String(Files.readAllBytes(path)));

      user = profile.get(USER).asText();
      account = profile.get(ACCOUNT).asText();
      port = profile.get(PORT).asInt();
      ssl = profile.get(SSL).asText();
      database = profile.get(DATABASE) == null ? null : profile.get(DATABASE).asText();
      connectString = profile.get(CONNECT_STRING).asText();
      schema = profile.get(SCHEMA) == null ? null : profile.get(SCHEMA).asText();
      warehouse = profile.get(WAREHOUSE).asText();
      host = profile.get(HOST).asText();
      scheme = profile.get(SCHEME).asText();
      role = Optional.ofNullable(profile.get(ROLE)).map(r -> r.asText()).orElse("DEFAULT_ROLE");
      privateKeyPem = profile.get(PRIVATE_KEY).asText();

      byte[] encoded = Base64.decodeBase64(privateKeyPem);
      KeyFactory kf = KeyFactory.getInstance("RSA");

      PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
      privateKey = kf.generatePrivate(keySpec);
      keyPair = Utils.createKeyPairFromPrivateKey(privateKey);
    } else {
      user = dummyUser;
      port = dummyPort;
      host = dummyHost;
      scheme = dummyScheme;
      KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
      kpg.initialize(2048);
      keyPair = kpg.generateKeyPair();
      privateKey = keyPair.getPrivate();
      privateKeyPem = java.util.Base64.getEncoder().encodeToString(privateKey.getEncoded());
    }

    isInitialized = true;
  }

  /** @return profile path that will be used for tests. */
  private static String getTestProfilePath() {
    String testProfilePath =
        System.getProperty("testProfilePath") != null
            ? System.getProperty("testProfilePath")
            : PROFILE_PATH;
    return testProfilePath;
  }

  public static String getUser() throws Exception {
    if (!isInitialized) {
      init();
    }
    return user;
  }

  public static String getAccount() throws Exception {
    if (!isInitialized) {
      init();
    }
    return account;
  }

  public static String getAccountURL() throws Exception {
    if (!isInitialized) {
      init();
    }

    return Utils.constructAccountUrl(scheme, host, port);
  }

  public static String getRole() throws Exception {
    if (!isInitialized) {
      init();
    }

    return role;
  }

  public static String getWarehouse() throws Exception {
    if (!isInitialized) {
      init();
    }
    return warehouse;
  }

  public static String getHost() throws Exception {
    if (!isInitialized) {
      init();
    }
    return host;
  }

  public static String getPrivateKey() throws Exception {
    if (!isInitialized) {
      init();
    }
    return privateKeyPem;
  }

  public static KeyPair getKeyPair() throws Exception {
    if (!isInitialized) {
      init();
    }
    return keyPair;
  }

  public static String getDatabase() throws Exception {
    if (!isInitialized) {
      init();
    }
    return database;
  }

  public static String getSchema() throws Exception {
    if (!isInitialized) {
      init();
    }
    return schema;
  }

  public static Properties getProperties(Constants.BdecVersion bdecVersion, boolean useDefaultRole)
      throws Exception {
    if (!isInitialized) {
      init();
    }
    Properties props = new Properties();

    props.put(USER, user);
    props.put(ACCOUNT, account);
    props.put(SSL, ssl);
    props.put(DATABASE, database);
    props.put(SCHEMA, schema);
    props.put(WAREHOUSE, warehouse);
    props.put(PRIVATE_KEY, privateKeyPem);
    if (!useDefaultRole) {
      props.put(ROLE, role);
    }
    props.put(ACCOUNT_URL, getAccountURL());
    props.put(BLOB_FORMAT_VERSION, bdecVersion.toByte());
    return props;
  }

  /**
   * Create snowflake jdbc connection
   *
   * @return jdbc connection
   * @throws Exception
   */
  public static Connection getConnection() throws Exception {
    return getConnection(false);
  }

  /**
   * Create snowflake jdbc connection
   *
   * @param isStreamingConnection: is true will return a separate connection for streaming ingest
   *     tests
   * @return jdbc connection
   * @throws Exception
   */
  public static Connection getConnection(boolean isStreamingConnection) throws Exception {
    if (!isStreamingConnection && snowpipeConn != null && !snowpipeConn.isClosed()) {
      return snowpipeConn;
    }
    if (isStreamingConnection && streamingConn != null && !streamingConn.isClosed()) {
      return streamingConn;
    }

    if (!isInitialized) {
      init();
    }

    // check first to see if we have the Snowflake JDBC
    Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");

    // build our properties
    Properties props = new Properties();
    props.put("user", user);
    props.put("account", account);
    props.put("ssl", ssl);
    props.put("db", database);
    props.put("schema", schema);
    props.put("warehouse", warehouse);
    props.put("client_session_keep_alive", "true");
    props.put("privateKey", privateKey);
    props.put("role", role);

    if (isStreamingConnection) {
      streamingConn = DriverManager.getConnection(connectString, props);
      // fire off the connection
      return streamingConn;
    }
    snowpipeConn = DriverManager.getConnection(connectString, props);
    // fire off the connection
    return snowpipeConn;
  }

  /**
   * execute sql query
   *
   * @param query sql query string
   * @return result set
   */
  public static ResultSet executeQuery(String query) {
    try (Statement statement = getConnection().createStatement()) {
      return statement.executeQuery(query);
    }
    // if ANY exceptions occur, an illegal state has been reached
    catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * create ingest manager
   *
   * @param pipe pipe name
   * @return ingest manager object
   * @throws Exception
   */
  public static SimpleIngestManager getManager(String pipe) throws Exception {
    if (!isInitialized) {
      init();
    }

    return new SimpleIngestManager(
        account, user, database + "." + schema + "." + pipe, privateKey, scheme, host, port);
  }

  /**
   * Use this constructor to test the new userAgentSuffix code path
   *
   * @param pipe pipe name
   * @param userAgentSuffix suffix we want to add in all request header of user-agent to the
   *     snowpipe API.
   * @return ingest manager object
   * @throws Exception
   */
  public static SimpleIngestManager getManager(String pipe, final String userAgentSuffix)
      throws Exception {
    if (!isInitialized) {
      init();
    }

    return new SimpleIngestManager(
        account,
        user,
        database + "." + schema + "." + pipe,
        privateKey,
        scheme,
        host,
        port,
        userAgentSuffix);
  }

  /**
   * Test for using builder pattern of SimpleIngestManager. In most of the cases we will not require
   * to pass in scheme and port since they are supposed to be kept https and 443 respectively
   */
  public static SimpleIngestManager getManagerUsingBuilderPattern(
      String pipe, final String userAgentSuffix) throws Exception {
    if (!isInitialized) {
      init();
    }

    return new SimpleIngestManager.Builder()
        .setAccount(account)
        .setUser(user)
        .setPipe(database + "." + schema + "." + pipe)
        .setKeypair(keyPair)
        .setUserAgentSuffix(userAgentSuffix)
        .build();
  }

  /**
   * Given a channel and expected offset, this method waits up to 60 seconds until the last
   * committed offset is equal to the passed offset
   */
  public static void waitForOffset(SnowflakeStreamingIngestChannel channel, String expectedOffset)
      throws InterruptedException {
    waitForOffset(channel, expectedOffset, 60);
  }

  public static void waitForOffset(
      SnowflakeStreamingIngestChannel channel, String expectedOffset, int seconds)
      throws InterruptedException {
    int counter = 0;
    String lastCommittedOffset = null;
    while (counter < seconds * 10) {
      String currentOffset = channel.getLatestCommittedOffsetToken();
      if (expectedOffset.equals(currentOffset)) {
        return;
      }
      lastCommittedOffset = currentOffset;
      counter++;
      Thread.sleep(100);
    }
    Assert.fail(
        String.format(
            "Timeout exceeded while waiting for offset %s. Last committed offset: %s",
            expectedOffset, lastCommittedOffset));
  }

  public static void waitChannelFlushed(SnowflakeStreamingIngestChannel channel, int numberOfRows) {
    String latestCommittedOffsetToken = null;
    for (int i = 1; i < 40; i++) {
      latestCommittedOffsetToken = channel.getLatestCommittedOffsetToken();
      if (latestCommittedOffsetToken != null
          && latestCommittedOffsetToken.equals(Integer.toString(numberOfRows - 1))) {
        return;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(
            "Interrupted waitChannelFlushed for " + numberOfRows + " rows", e);
      }
    }
    Assert.fail(
        "Row sequencer not updated before timeout, latestCommittedOffsetToken: "
            + latestCommittedOffsetToken);
  }

  /** Verify the insert validation response and throw the exception if needed */
  public static void verifyInsertValidationResponse(InsertValidationResponse response) {
    if (response.hasErrors()) {
      throw response.getInsertErrors().get(0).getException();
    }
  }

  public static void verifyTableRowCount(
      int rowNumber, Connection jdbcConnection, String database, String schema, String tableName) {
    try {
      ResultSet resultCount =
          jdbcConnection
              .createStatement()
              .executeQuery(
                  String.format("select count(*) from %s.%s.%s", database, schema, tableName));
      resultCount.next();
      Assert.assertEquals(rowNumber, resultCount.getLong(1));
    } catch (SQLException e) {
      throw new RuntimeException("Cannot verifyTableRowCount for " + tableName, e);
    }
  }

  /**
   * Creates a string from a certain number of concatenated strings e.g. buildString("ab", 2) =>
   * abab
   */
  public static String buildString(String str, int count) {
    StringBuilder sb = new StringBuilder(count);
    for (int i = 0; i < count; i++) {
      sb.append(str);
    }
    return sb.toString();
  }

  public static Map<String, Object> getRandomRow(Random r, boolean nullable) {
    Map<String, Object> row = new HashMap<>();

    row.put("num_2_1", nullOrIfNullable(nullable, r, () -> r.nextInt(100) / 10.0));
    row.put("num_4_2", nullOrIfNullable(nullable, r, () -> r.nextInt(10000) / 100.0));
    row.put(
        "num_9_4", nullOrIfNullable(nullable, r, () -> r.nextInt(1000000000) / Math.pow(10, 4)));
    row.put(
        "num_18_7",
        nullOrIfNullable(nullable, r, () -> nextLongOfPrecision(r, 18) / Math.pow(10, 7)));
    row.put(
        "num_38_15",
        nullOrIfNullable(
            nullable,
            r,
            () ->
                new BigDecimal(
                    "" + nextLongOfPrecision(r, 18) + "." + Math.abs(nextLongOfPrecision(r, 15)))));

    row.put("num_float", nullOrIfNullable(nullable, r, () -> nextFloat(r)));
    row.put("str", nullOrIfNullable(nullable, r, () -> nextString(r)));
    row.put("bin", nullOrIfNullable(nullable, r, () -> nextBytes(r)));

    return row;
  }

  public static URIBuilder getBaseURIBuilder() {
    return new URIBuilder().setScheme(scheme).setHost(host).setPort(port);
  }

  public static URI getTokenRequestURI() {
    URI tokenRequestURI = null;
    try {
      tokenRequestURI = getBaseURIBuilder().setPath("/oauth/token-request").build();
    } catch (URISyntaxException e) {
      throw new RuntimeException("Fail to construct token request uri", e);
    }

    return tokenRequestURI;
  }

  public static ParameterProvider createParameterProvider(
      Map<String, Object> parameterOverrides, Properties props, boolean enableIcebergStreaming) {
    if (parameterOverrides != null) {
      parameterOverrides.put(ENABLE_ICEBERG_STREAMING, enableIcebergStreaming);
    }
    return new ParameterProvider(parameterOverrides, props);
  }

  public static ParameterProvider createParameterProvider(boolean enableIcebergStreaming) {
    return createParameterProvider(new HashMap<>(), null, enableIcebergStreaming);
  }

  public static Properties createProps(boolean enableIcebergStreaming) {
    Properties prop = new Properties();
    prop.setProperty(
        ParameterProvider.ENABLE_ICEBERG_STREAMING, String.valueOf(enableIcebergStreaming));
    return prop;
  }

  public static SnowflakeStreamingIngestClient setUp(
      Connection conn,
      String databaseName,
      String schemaName,
      boolean enableIcebergStreaming,
      String compressionAlgorithm,
      IcebergSerializationPolicy serializationPolicy)
      throws Exception {
    conn.createStatement().execute(String.format("create or replace database %s;", databaseName));
    conn.createStatement().execute(String.format("use database %s;", databaseName));
    conn.createStatement().execute(String.format("use schema %s;", schemaName));

    if (enableIcebergStreaming) {
      switch (serializationPolicy) {
        case COMPATIBLE:
          conn.createStatement()
              .execute(
                  String.format(
                      "alter schema %s set STORAGE_SERIALIZATION_POLICY = 'COMPATIBLE';",
                      schemaName));
          break;
        case OPTIMIZED:
          conn.createStatement()
              .execute(
                  String.format(
                      "alter schema %s set STORAGE_SERIALIZATION_POLICY = 'OPTIMIZED';",
                      schemaName));
          break;
      }
    }

    conn.createStatement().execute(String.format("use warehouse %s;", TestUtils.getWarehouse()));

    Properties props = TestUtils.getProperties(Constants.BdecVersion.THREE, false);
    props.setProperty(
        ParameterProvider.ENABLE_ICEBERG_STREAMING, String.valueOf(enableIcebergStreaming));
    if (props.getProperty(ROLE).equals("DEFAULT_ROLE")) {
      props.setProperty(ROLE, "ACCOUNTADMIN");
    }
    props.setProperty(BDEC_PARQUET_COMPRESSION_ALGORITHM, compressionAlgorithm);

    // Override Iceberg mode client lag to 1 second for faster test execution
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, 1000L);

    Properties prop = Utils.createProperties(props);
    SnowflakeURL accountURL = new SnowflakeURL(prop.getProperty(Constants.ACCOUNT_URL));
    return new SnowflakeStreamingIngestClientInternal<>(
        "client1", accountURL, prop, parameterMap, false);
  }

  private static <T> T nullOrIfNullable(boolean nullable, Random r, Supplier<T> value) {
    return !nullable ? value.get() : (r.nextBoolean() ? value.get() : null);
  }

  private static long nextLongOfPrecision(Random r, int precision) {
    return r.nextLong() % Math.round(Math.pow(10, precision));
  }

  private static String nextString(Random r) {
    return new String(nextBytes(r));
  }

  private static byte[] nextBytes(Random r) {
    byte[] bin = new byte[128];
    r.nextBytes(bin);
    for (int i = 0; i < bin.length; i++) {
      bin[i] = (byte) (Math.abs(bin[i]) % 25 + 97); // ascii letters
    }
    return bin;
  }

  private static double nextFloat(Random r) {
    return (r.nextLong() % Math.round(Math.pow(10, 10))) / 100000d;
  }
}
