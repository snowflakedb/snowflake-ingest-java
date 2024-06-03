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
import static net.snowflake.ingest.utils.ParameterProvider.BLOB_FORMAT_VERSION;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
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
import java.util.stream.Collectors;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.CloseableHttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpPost;
import net.snowflake.client.jdbc.internal.apache.http.client.utils.URIBuilder;
import net.snowflake.client.jdbc.internal.apache.http.entity.ContentType;
import net.snowflake.client.jdbc.internal.apache.http.entity.StringEntity;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.HttpClientBuilder;
import net.snowflake.client.jdbc.internal.apache.http.util.EntityUtils;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.jdbc.internal.google.gson.JsonObject;
import net.snowflake.client.jdbc.internal.google.gson.JsonParser;
import net.snowflake.ingest.connection.JWTManager;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.Utils;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;

public class TestUtils {
  // profile path, follow readme for the format
  private static final String PROFILE_PATH = "profile.json";

  private static final String OAUTH_INTEGRATION = "OAUTH_INTEGRATION";

  private static final ObjectMapper mapper = new ObjectMapper();

  private static ObjectNode profile = null;

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
  private static void init() throws Exception {
    String testProfilePath = getTestProfilePath();
    Path path = Paths.get(testProfilePath);

    if (Files.exists(path)) {
      profile = (ObjectNode) mapper.readTree(new String(Files.readAllBytes(path)));

      user = profile.get(USER).asText();
      account = profile.get(ACCOUNT).asText();
      port = profile.get(PORT).asInt();
      ssl = profile.get(SSL).asText();
      database = profile.get(DATABASE).asText();
      connectString = profile.get(CONNECT_STRING).asText();
      schema = profile.get(SCHEMA).asText();
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
    if (profile == null) {
      init();
    }
    return user;
  }

  public static String getAccount() throws Exception {
    if (profile == null) {
      init();
    }
    return account;
  }

  public static String getAccountURL() throws Exception {
    if (profile == null) {
      init();
    }

    return Utils.constructAccountUrl(scheme, host, port);
  }

  public static String getRole() throws Exception {
    if (profile == null) {
      init();
    }

    return role;
  }

  public static String getWarehouse() throws Exception {
    if (profile == null) {
      init();
    }
    return warehouse;
  }

  public static String getHost() throws Exception {
    if (profile == null) {
      init();
    }
    return host;
  }

  public static String getPrivateKey() throws Exception {
    if (profile == null) {
      init();
    }
    return privateKeyPem;
  }

  public static KeyPair getKeyPair() throws Exception {
    if (profile == null) {
      init();
    }
    return keyPair;
  }

  public static String getDatabase() throws Exception {
    if (profile == null) {
      init();
    }
    return database;
  }

  public static String getSchema() throws Exception {
    if (profile == null) {
      init();
    }
    return schema;
  }

  public static Properties getProperties(Constants.BdecVersion bdecVersion, boolean useDefaultRole)
      throws Exception {
    if (profile == null) {
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

    if (profile == null) init();
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
    if (profile == null) init();
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
    if (profile == null) init();
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
    if (profile == null) init();
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
    int counter = 0;
    String lastCommittedOffset = null;
    while (counter < 600) {
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

  public static String getOAuthAccessToken() throws Exception {
    // Setup security integration
    Connection jdbcConnection = TestUtils.getConnection(true);
    jdbcConnection
        .createStatement()
        .execute(
            "create or replace security integration SDK_TEST\n"
                + "        type=oauth\n"
                + "        oauth_client=CUSTOM\n"
                + "        oauth_client_type=CONFIDENTIAL\n"
                + "        oauth_redirect_uri='https://localhost.com/oauth'\n"
                + "        oauth_issue_refresh_tokens=true\n"
                + "        enabled=true\n"
                + "        oauth_refresh_token_validity=7776000;\n");
    jdbcConnection
        .createStatement()
        .execute(
            String.format(
                "alter user %s add delegated authorization of role %s to security integration"
                    + " SDK_TEST;",
                user, role));
    ResultSet resultSet =
        jdbcConnection
            .createStatement()
            .executeQuery("select SYSTEM$SHOW_OAUTH_CLIENT_SECRETS('SDK_TEST');");
    resultSet.next();
    JsonObject obj = JsonParser.parseString(resultSet.getString(1)).getAsJsonObject();
    String clientId = obj.get("OAUTH_CLIENT_ID").getAsString();
    String clientSecret = obj.get("OAUTH_CLIENT_SECRET").getAsString();
    String jwtToken = new JWTManager(account, user, keyPair, null).getToken();

    URI loginRequestURI = getBaseURIBuilder().setPath("/session/authenticate-request").build();
    URI authRequestURI = getBaseURIBuilder().setPath("/oauth/authorization-request").build();
    URI tokenRequestURI = getBaseURIBuilder().setPath("/oauth/token-request").build();

    CloseableHttpClient httpClient = HttpClientBuilder.create().build();

    // Request master token
    HttpPost loginPost = new HttpPost(loginRequestURI);
    loginPost.setHeader("Content-type", "application/json");
    String loginPayload =
        String.format(
            "{"
                + "\"data\":{"
                + "\"ACCOUNT_NAME\":\"%s\","
                + "\"LOGIN_NAME\":\"%s\","
                + "\"TOKEN\":\"%s\","
                + "\"AUTHENTICATOR\":\"SNOWFLAKE_JWT\","
                + "\"clientId\":\"%s\","
                + "\"responseType\":\"code\","
                + "\"scope\":\"refresh_token session:role:%s\""
                + "}"
                + "}",
            account.toUpperCase(), user, jwtToken, clientId, role.toUpperCase());
    loginPost.setEntity(new StringEntity(loginPayload, ContentType.APPLICATION_JSON));

    CloseableHttpResponse response = httpClient.execute(loginPost);
    String respBodyString = EntityUtils.toString(response.getEntity());
    obj = JsonParser.parseString(respBodyString).getAsJsonObject();
    String masterToken =
        obj.getAsJsonObject("data").get("masterToken").toString().replaceAll("^\"|\"$", "");

    // Get AZCode using masterToken
    HttpPost authPost = new HttpPost(authRequestURI);
    authPost.setHeader("Content-type", "application/json");
    String authPayload =
        String.format(
            "{"
                + "\"clientId\":\"%s\","
                + "\"masterToken\":\"%s\","
                + "\"responseType\":\"code\","
                + "\"scope\":\"refresh_token session:role:%s\""
                + "}",
            clientId, masterToken, role.toUpperCase());
    authPost.setEntity(new StringEntity(authPayload, ContentType.APPLICATION_JSON));
    response = httpClient.execute(authPost);
    respBodyString = EntityUtils.toString(response.getEntity());
    obj = JsonParser.parseString(respBodyString).getAsJsonObject();
    String authCode =
        obj.getAsJsonObject("data")
            .get("redirectUrl")
            .toString()
            .replaceAll("^\"|\"$", "")
            .split("=")[1];

    // Get access token using AZCode
    HttpPost tokenPost = new HttpPost(tokenRequestURI);
    tokenPost.setHeader("Content-type", "application/x-www-form-urlencoded");
    tokenPost.setHeader(
        "Authorization",
        "Basic "
            + java.util.Base64.getEncoder()
                .encodeToString((clientId + ":" + clientSecret).getBytes()));
    HashMap<String, String> tokenPayload = new HashMap<>();
    tokenPayload.put("grant_type", "authorization_code");
    tokenPayload.put("code", authCode);
    tokenPayload.put("redirect_uri", "https://localhost.com/oauth");
    String tokenPayloadString =
        tokenPayload.entrySet().stream()
            .map(
                e -> {
                  try {
                    return e.getKey() + "=" + URLEncoder.encode(e.getValue(), "UTF-8");
                  } catch (UnsupportedEncodingException ex) {
                    throw new RuntimeException(ex);
                  }
                })
            .collect(Collectors.joining("&"));
    tokenPost.setEntity(
        new StringEntity(tokenPayloadString, ContentType.APPLICATION_FORM_URLENCODED));
    response = httpClient.execute(tokenPost);
    respBodyString = EntityUtils.toString(response.getEntity());
    obj = JsonParser.parseString(respBodyString).getAsJsonObject();
    return obj.get("access_token").toString().replaceAll("^\"|\"$", "");
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
