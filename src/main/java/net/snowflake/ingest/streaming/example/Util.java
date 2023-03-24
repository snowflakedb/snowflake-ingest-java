package net.snowflake.ingest.streaming.example;

import static net.snowflake.ingest.utils.Constants.*;
import static net.snowflake.ingest.utils.ParameterProvider.BLOB_FORMAT_VERSION;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.*;
import java.util.*;
import java.util.function.Supplier;
import net.snowflake.client.jdbc.internal.org.bouncycastle.jce.provider.BouncyCastleProvider;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.internal.SnowflakeStreamingIngestClientInternal;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.Utils;

public class Util {
  // profile path, follow readme for the format
  private static final String PROFILE_PATH = "profile.json";

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

  private static final ObjectMapper mapper = new ObjectMapper();

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

      byte[] encoded = org.apache.commons.codec.binary.Base64.decodeBase64(privateKeyPem);
      KeyFactory kf = KeyFactory.getInstance("RSA");

      PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
      privateKey = kf.generatePrivate(keySpec);
      keyPair = Utils.createKeyPairFromPrivateKey(privateKey);
    }
  }

  public static Properties getProperties(Constants.BdecVersion bdecVersion) throws Exception {
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
    props.put(ACCOUNT_URL, Utils.constructAccountUrl(scheme, host, port));
    props.put(BLOB_FORMAT_VERSION, bdecVersion.toByte());
    return props;
  }

  public static Connection getConnection() throws Exception {
    // check first to see if we have the Snowflake JDBC
    Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");

    // build our properties
    Properties props = new Properties();
    props.put("user", user);
    props.put("role", role);
    props.put("account", account);
    props.put("ssl", ssl);
    props.put("db", database);
    props.put("schema", schema);
    props.put("warehouse", warehouse);
    props.put("client_session_keep_alive", "true");
    props.put("privateKey", privateKey);

    return DriverManager.getConnection(connectString, props);
  }

  /**
   * This method converts a result set into list of rows where each row is represented in a name -> value map
   *
   * @param rs result set
   * @return list of rows as hashmap
   * @throws SQLException
   */
  public static List resultSetToArrayListAndIngestInBatches(ResultSet rs, int batchSize, SnowflakeStreamingIngestChannel channel, long totalCount) throws SQLException {
    ResultSetMetaData md = rs.getMetaData();
    int columns = md.getColumnCount();
    List batchedRows = new ArrayList();
    int ctr = 0;
    while (rs.next()){
      Map<String, Object> row = new HashMap(columns);
      for(int i=1; i<=columns; ++i){
        Object val = rs.getObject(i);
        if(val instanceof java.sql.Date) {
          val = val.toString();
        }
        row.put(md.getColumnName(i), val);
      }
      batchedRows.add(row);
    }
    return batchedRows;
  }

  public static SnowflakeStreamingIngestChannel openChannel(String databaseName, String schemaName, String tableName, String channelName, SnowflakeStreamingIngestClientInternal clientInternal) {
    OpenChannelRequest request =
            OpenChannelRequest.builder(channelName)
                    .setDBName(databaseName)
                    .setSchemaName(schemaName)
                    .setTableName(tableName)
                    .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
                    .build();

    // Open a streaming ingest channel from the given client
    return clientInternal.openChannel(request);
  }

  /** Verify the insert validation response and throw the exception if needed */
  public static void verifyInsertValidationResponse(InsertValidationResponse response) {
    if (response.hasErrors()) {
      throw response.getInsertErrors().get(0).getException();
    }
  }


  public static void waitChannelFlushed(SnowflakeStreamingIngestChannel channel, int numberOfRows) {
    String latestCommittedOffsetToken = null;
    for (int i = 1; i < 15; i++) {
      latestCommittedOffsetToken = channel.getLatestCommittedOffsetToken();
      if (latestCommittedOffsetToken != null
              && latestCommittedOffsetToken.equals(Integer.toString(numberOfRows - 1))) {
        return;
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        throw new RuntimeException(
                "Interrupted waitChannelFlushed for " + numberOfRows + " rows", e);
      }
    }
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

  public static String getWarehouse() {
    return warehouse;
  }
}
