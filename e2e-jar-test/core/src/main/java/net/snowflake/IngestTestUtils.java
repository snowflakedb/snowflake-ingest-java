package net.snowflake;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestTestUtils {
  private static final String PROFILE_PATH = "profile.json";

  private final Connection connection;

  private final String database;
  private final String table;

  private final String testId;

  private static final Logger logger = LoggerFactory.getLogger(IngestTestUtils.class);

  private final SnowflakeStreamingIngestClient client;

  private final SnowflakeStreamingIngestChannel channel;

  private final ObjectMapper objectMapper = new ObjectMapper();

  private final Base64.Decoder base64Decoder = Base64.getDecoder();

  public IngestTestUtils(String testName)
      throws SQLException, IOException, ClassNotFoundException, NoSuchAlgorithmException,
          InvalidKeySpecException {
    testId = String.format("%s_%s", testName, UUID.randomUUID().toString().replace("-", "_"));
    connection = getConnection();
    database = String.format("ingest_sdk_e2e_jar_database_%s", testId);
    table = String.format("ingest_sdk_e2e_jar_table_%s", testId);

    connection.createStatement().execute(String.format("create database %s", database));
    connection
        .createStatement()
        .execute(
            String.format(
                "create table %s ("
                    + "boolean_col boolean,"
                    + "int_col int,"
                    + "number_col number(10, 5),"
                    + "float_col float,"
                    + "text_col text,"
                    + "binary_col binary,"
                    + "variant_col variant,"
                    + "array_col array,"
                    + "object_col object,"
                    + "time_col time,"
                    + "date_col date,"
                    + "timestamp_ntz_col timestamp_ntz,"
                    + "timestamp_ltz_col timestamp_ltz,"
                    + "timestamp_tz_col timestamp_tz"
                    + ");",
                table));
    client =
        SnowflakeStreamingIngestClientFactory.builder("TestClient01")
            .setProperties(loadProperties())
            .build();

    channel =
        client.openChannel(
            OpenChannelRequest.builder(String.format("channel_%s", this.testId))
                .setDBName(database)
                .setSchemaName("PUBLIC")
                .setTableName(table)
                .setOnErrorOption(OpenChannelRequest.OnErrorOption.ABORT)
                .build());
  }

  private Properties loadProperties() throws IOException {
    Properties props = new Properties();
    Iterator<Map.Entry<String, JsonNode>> propIt =
        objectMapper.readTree(new String(Files.readAllBytes(Paths.get(PROFILE_PATH)))).fields();
    while (propIt.hasNext()) {
      Map.Entry<String, JsonNode> prop = propIt.next();
      props.put(prop.getKey(), prop.getValue().asText());
    }
    return props;
  }

  private Connection getConnection()
      throws IOException, ClassNotFoundException, SQLException, NoSuchAlgorithmException,
          InvalidKeySpecException {
    Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");

    Properties loadedProps = loadProperties();

    byte[] decoded = base64Decoder.decode(loadedProps.getProperty("private_key"));
    KeyFactory kf = KeyFactory.getInstance("RSA");

    PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(decoded);
    PrivateKey privateKey = kf.generatePrivate(keySpec);

    Properties props = new Properties();
    props.putAll(loadedProps);
    props.put("client_session_keep_alive", "true");
    props.put("privateKey", privateKey);

    return DriverManager.getConnection(loadedProps.getProperty("connect_string"), props);
  }

  private Map<String, Object> createRow() {
    Map<String, Object> row = new HashMap<>();
    row.put("boolean_col", false);
    row.put("int_col", 1);
    row.put("number_col", new BigDecimal("11111.11111"));
    row.put("float_col", 1.234);
    row.put("text_col", "test");
    row.put("binary_col", new byte[] {1, 2, 3, 4, 5});
    row.put("variant_col", "\"Hello, World!\"");
    row.put("array_col", "[{\"k1\": \"v1\"}]");
    row.put("object_col", "{\"k1\": \"v1\"}");
    row.put("time_col", "00:00");
    row.put("date_col", "2000-01-01");
    row.put("timestamp_ntz_col", "2000-01-01T11:00");
    row.put("timestamp_ltz_col", "2000-01-01T11:00");
    row.put("timestamp_tz_col", "2000-01-01T11:00-07:00");
    return row;
  }

  /**
   * Given a channel and expected offset, this method waits up to 60 seconds until the last
   * committed offset is equal to the passed offset
   */
  private void waitForOffset(SnowflakeStreamingIngestChannel channel, String expectedOffset)
      throws InterruptedException {
    int counter = 0;
    String lastCommittedOffset = null;
    while (counter < 600) {
      String currentOffset = channel.getLatestCommittedOffsetToken();
      if (expectedOffset.equals(currentOffset)) {
        return;
      }
      System.out.printf(
          "Waiting for offset expected=%s actual=%s%n", expectedOffset, currentOffset);
      lastCommittedOffset = currentOffset;
      counter++;
      Thread.sleep(100);
    }
    throw new RuntimeException(
        String.format(
            "Timeout exceeded while waiting for offset %s. Last committed offset: %s",
            expectedOffset, lastCommittedOffset));
  }

  public void runBasicTest() throws InterruptedException {
    // Insert few rows one by one
    for (int offset = 0; offset < 1000; offset++) {
      offset++;
      channel.insertRow(createRow(), String.valueOf(offset));
    }

    // Insert a batch of rows
    String offset = "final-offset";
    channel.insertRows(
        Arrays.asList(createRow(), createRow(), createRow(), createRow(), createRow()), offset);
    waitForOffset(channel, offset);
  }

  public void runLongRunningTest(Duration testDuration) throws InterruptedException {
    final Instant testStart = Instant.now();
    int counter = 0;
    while (true) {
      counter++;

      channel.insertRow(createRow(), String.valueOf(counter));

      if (!channel.isValid()) {
        throw new IllegalStateException("Channel has been invalidated");
      }
      Thread.sleep(60000);

      final Duration elapsed = Duration.between(testStart, Instant.now());

      logger.info(
          "Test loop_nr={} duration={}s/{}s committed_offset={}",
          counter,
          elapsed.get(ChronoUnit.SECONDS),
          testDuration.get(ChronoUnit.SECONDS),
          channel.getLatestCommittedOffsetToken());

      if (elapsed.compareTo(testDuration) > 0) {
        break;
      }
    }
    waitForOffset(channel, String.valueOf(counter));
  }

  public void close() throws Exception {
    channel.close().get();
    client.close();
    connection
        .createStatement()
        .execute(String.format(String.format("drop database %s", database)));
    connection.close();
  }
}
