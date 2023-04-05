/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import org.apache.hadoop.util.StopWatch;

/**
 * Example on how to use the Streaming Ingest client APIs.
 *
 * <p>Please read the README.md file for detailed steps
 */
public class SnowflakeStreamingIngestExample2 {

  private static String PROFILE_PATH = "profile.json";
  private static final ObjectMapper mapper = new ObjectMapper();

  private enum ETabType {
    VARCHAR,
    INT,
    NUM38,
    DATE
  }

  // Below are the seeting which we can control.

  /** Indicates how many columns are required for the table */
  private static final int nDataCols = 10;
  // Col len must be at least 30
  /** Indicates column length */
  private static final int dataColLen = 100;
  /** Indicates how many rows are needed */
  private static final int numRows = 2000000;

  /** Indicates the data type for each column */
  private static final ETabType tabType = ETabType.VARCHAR;
  /** setting to true will drop the existing table */
  private boolean DROP_TABLES = true;
  /** setting to true will create a new table */
  private boolean CREATE_TABLES = true;
  /** setting to true will truncate a existing table */
  private boolean TRUNCATE_TABLES = false;

  /** setting to true will insert data into the table via snowpipe streaming */
  private boolean INSERT_TABLES = true;

  /** setting to true will use the quotes for the column during table creation and data insert */
  private static boolean ENABLE_QUOTES = false;
  /**
   * setting to true will use the ArrowBuffer. This flag is only needed when using SDK version
   * >1.1.0
   */
  private static boolean USE_ARROW = false;

  // Connection properties
  private static String USER_NAME = "NOUFALBA";
  private static String URL = "https://informatica.eu-central-1.snowflakecomputing.com:443";
  private static String PRIVATE_KEY_FILE_LOCATION = "C:\\snowflake\\key\\rsa_streaming_key.p8";
  private static String PORT = "443";
  private static String SCHEME = "https";
  private static String ROLE = "SYSADMIN";
  private static String DATA_BASE = "testdb_kafka";
  private static String SCHEMA = "kafka_test";
  private static String WARE_HOUSE = "DBMI_WH1";
  private String pad;
  private String columnNamesArray[];

  public static void main(String[] args) throws Exception {
    new SnowflakeStreamingIngestExample2().doIt();
  }

  private static Properties getKeysPairAuthParams(boolean isStreamConnection) throws IOException {
    Properties props = new Properties();
    Iterator<Map.Entry<String, JsonNode>> propIt =
        mapper.readTree(new String(Files.readAllBytes(Paths.get(PROFILE_PATH)))).fields();
    while (propIt.hasNext()) {
      Map.Entry<String, JsonNode> prop = propIt.next();
      props.put(prop.getKey(), prop.getValue().asText());
    }
    return props;
  }

  public void doIt() throws Exception {

    if (dataColLen < 30) {
      throw new IllegalArgumentException("Col len must be >=30");
    }

    if (dataColLen % 10 != 0) {
      throw new IllegalArgumentException("Col len must be a multiple of 10");
    }

    final StringBuilder padBuilder = new StringBuilder();
    for (int i = 0; i < dataColLen; ++i) {
      padBuilder.append("X");
    }
    pad = padBuilder.toString();

    // get all column names and cache it
    columnNamesArray = new String[nDataCols];
    for (int i = 0; i < nDataCols; ++i) {
      columnNamesArray[i] = getColName(i + 1);
    }

    if (INSERT_TABLES) {
      new Inserter().doInserts();
    }

    System.out.println("Done");
  }

  private String getColDef() {
    String colDef;
    switch (tabType) {
      case VARCHAR:
        colDef = String.format("varchar(%s)", dataColLen);
        break;
      case NUM38:
        colDef = String.format("NUMBER(%s)", 38);
        break;
      case INT:
        colDef = "INTEGER";
        break;
      case DATE:
        colDef = "DATE";
        break;
      default:
        throw new RuntimeException("Unsupported : " + tabType);
    }

    return colDef;
  }

  private String getFullyQualifiedTableName() {
    return String.format("%s.%s", SCHEMA, getTabName());
  }

  private String getTabName() {
    int tabNum = 1;
    String tabName;

    switch (tabType) {
      case VARCHAR:
        tabName = String.format("tabL%06d", tabNum);
        break;
      default:
        throw new RuntimeException("Unsupported : " + tabType);
    }
    return tabName;
  }

  private String getColName(int colNum) {
    if (ENABLE_QUOTES) {
      return wrap(String.format("Col_%04d", colNum));
    } else {
      return String.format("Col_%04d", colNum);
    }
  }

  public static String wrap(String identifier) {
    final String quote = "\"";
    return new StringBuilder(quote).append(identifier).append(quote).toString();
  }

  ///////////////////////////////
  private class Inserter {

    public Inserter() {}

    public void doInserts() throws Exception {
      try (SnowflakeStreamingIngestClient client =
          SnowflakeStreamingIngestClientFactory.builder("INFA_CLIENT")
              .setProperties(getKeysPairAuthParams(true))
              .build()) {
        // Open a streaming ingest channel from the given client
        OpenChannelRequest request1 =
            OpenChannelRequest.builder("MSSQL_TEST_RS_84")
                .setDBName(DATA_BASE)
                .setSchemaName(SCHEMA)
                .setTableName("t_streamingingest")
                .setOnErrorOption(
                    OpenChannelRequest.OnErrorOption.CONTINUE) // Another ON_ERROR option is ABORT
                .build();

        // Open a streaming ingest channel from the given client
        SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);

        String previousOffsetTokenFromSnowflake = channel1.getLatestCommittedOffsetToken();

        System.out.println(
            "==============================================================================");
        System.out.println(
            "********************************  STARTING OFFSET IS "
                + previousOffsetTokenFromSnowflake);
        System.out.println(
            "======================================f========================================");

        // Insert rows into the channel (Using insertRows API)
        StopWatch watch = new StopWatch();
        watch.start();

        for (int val = 0; val < numRows; val++) {
          Map<String, Object> row = new HashMap<>();
          for (int bc = 0; bc < nDataCols; ++bc) {

            row.put(columnNamesArray[bc], buildDataCol());
          }
          InsertValidationResponse response = channel1.insertRow(row, String.valueOf(val + 1));
          if (response.hasErrors()) {
            // Simply throw if there is an exception, or you can do whatever you want with the
            // erroneous row
            throw response.getInsertErrors().get(0).getException();
          }
        }

        System.out.println("aaaaaaaaa Elapsed Time in Seconds: " + watch.now(TimeUnit.SECONDS));

        // If needed, you can check the offset_token registered in Snowflake to make sure everything
        // is committed
        final String expectedOffsetTokenInSnowflake = String.valueOf(numRows);
        final int maxRetries = 60;
        int retryCount = 0;

        do {
          String offsetTokenFromSnowflake = channel1.getLatestCommittedOffsetToken();
          System.out.println(
              "==============================================================================");
          System.out.println(
              "+++++++++++++++++++++++++++++++++++++++++  CURRENT OFFSET IS "
                  + offsetTokenFromSnowflake);
          System.out.println(
              "==============================================================================");
          if (offsetTokenFromSnowflake != null
              && offsetTokenFromSnowflake.equals(String.valueOf(expectedOffsetTokenInSnowflake))) {
            System.out.println(
                "==============================================================================");
            System.out.println(
                "+++++++++++++++++++++++++++++++++++++++++  SUCCESSFULLY inserted "
                    + numRows
                    + " rows");
            System.out.println(
                "==============================================================================");
            break;
          }

          retryCount++;
        } while (true);
        watch.stop();
        System.out.println("aaaaaaaaa Elapsed Time in Seconds: " + watch.now(TimeUnit.SECONDS));
      }
    }

    private Object buildDataCol() {
      Object dataVal;
      switch (tabType) {
        case VARCHAR:
          dataVal = pad;
          break;
        default:
          throw new RuntimeException("Unsupported : " + tabType);
      }

      return dataVal;
    }
  }
}
