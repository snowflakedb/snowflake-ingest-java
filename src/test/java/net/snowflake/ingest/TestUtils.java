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

import java.io.IOException;
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
import java.sql.Statement;
import java.util.Optional;
import java.util.Properties;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.jdbc.internal.org.bouncycastle.jce.provider.BouncyCastleProvider;
import net.snowflake.ingest.utils.Utils;
import org.apache.commons.codec.binary.Base64;

public class TestUtils {
  // profile path, follow readme for the format
  private static final String PROFILE_PATH = "profile.json";

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

  private static Connection conn = null;

  private static String dummyUser = "user";
  private static int dummyPort = 443;
  private static String dummyHost = "snowflake.qa1.int.snowflakecomputing.com";

  /**
   * load all login info from profile
   *
   * @throws IOException if can't read profile
   */
  private static void init() throws Exception {
    Path path = Paths.get(PROFILE_PATH);

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

      java.security.Security.addProvider(new BouncyCastleProvider());

      byte[] encoded = Base64.decodeBase64(privateKeyPem);
      KeyFactory kf = KeyFactory.getInstance("RSA");

      PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
      privateKey = kf.generatePrivate(keySpec);
      keyPair = Utils.createKeyPairFromPrivateKey(privateKey);
    } else {
      user = dummyUser;
      port = dummyPort;
      host = dummyHost;
      KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
      kpg.initialize(2048);
      keyPair = kpg.generateKeyPair();
      privateKey = keyPair.getPrivate();
      privateKeyPem = java.util.Base64.getEncoder().encodeToString(privateKey.getEncoded());
    }
  }

  public static String getDatabase() throws Exception {
    if (profile == null) {
      init();
    }
    return database;
  }

  public static String getUser() throws Exception {
    if (profile == null) {
      init();
    }
    return user;
  }

  public static String getAccountURL() throws Exception {
    if (profile == null) {
      init();
    }

    return Utils.constructAccountUrl(scheme, host, port);
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

  public static Properties getProperties() throws Exception {
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
    props.put(ROLE, role);
    props.put(ACCOUNT_URL, getAccountURL());
    return props;
  }

  /**
   * Create snowflake jdbc connection
   *
   * @return jdbc connection
   * @throws Exception
   */
  public static Connection getConnection() throws Exception {
    if (conn != null) return conn;

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

    conn = DriverManager.getConnection(connectString, props);

    // fire off the connection
    return conn;
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
}
