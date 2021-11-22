package net.snowflake.ingest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.codec.binary.Base64;

public class TestUtils {
  // profile path, follow readme for the format
  private static final String PROFILE_PATH = "profile.json.example";

  private static final ObjectMapper mapper = new ObjectMapper();

  private static ObjectNode profile = null;

  private static String user = "";

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

  /**
   * load all login info from profile
   *
   * @throws IOException if can't read profile
   */
  private static void init() throws Exception {
    profile = (ObjectNode) mapper.readTree(new String(Files.readAllBytes(Paths.get(PROFILE_PATH))));

    user = profile.get("user").asText();
    account = profile.get("account").asText();
    port = profile.get("port").asInt();
    ssl = profile.get("ssl").asText();
    database = profile.get("database").asText();
    connectString = profile.get("connect_string").asText();
    schema = profile.get("schema").asText();
    warehouse = profile.get("warehouse").asText();
    host = profile.get("host").asText();
    scheme = profile.get("scheme").asText();

    String privateKeyPem = profile.get("private_key").asText();

    java.security.Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

    byte[] encoded = Base64.decodeBase64(privateKeyPem);
    KeyFactory kf = KeyFactory.getInstance("RSA");

    PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
    privateKey = kf.generatePrivate(keySpec);
    keyPair = SimpleIngestManager.createKeyPairFromPrivateKey(privateKey);
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
