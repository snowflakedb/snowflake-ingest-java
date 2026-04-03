/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/telemetry/TelemetryClient.java
 */
package net.snowflake.ingest.connection.telemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.Future;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.JdbcHttpUtil;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.ObjectMapperFactory;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.SnowflakeSQLException;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.TelemetryThreadPool;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLogger;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLoggerFactory;
import net.snowflake.ingest.utils.Stopwatch;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;

/** Telemetry Service Interface */
public class TelemetryClient implements Telemetry {
  private static final SFLogger logger = SFLoggerFactory.getLogger(TelemetryClient.class);

  private static final String SF_PATH_TELEMETRY_SESSIONLESS = "/telemetry/send/sessionless";

  // if the number of cached logs is larger than this threshold,
  // the telemetry connector will flush the buffer automatically.
  private final int forceFlushSize;

  private static final int DEFAULT_FORCE_FLUSH_SIZE = 100;

  private final String serverUrl;
  private final String telemetryUrl;

  private LinkedList<TelemetryData> logBatch;
  private static final ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();

  private boolean isClosed;

  // HTTP client object used to communicate with other machine
  private final CloseableHttpClient httpClient;

  // the authorization type speficied in sessionless header
  private String authType;

  // JWT/OAuth token
  private String token;

  private Object locker = new Object();

  // false if meet any error when sending metrics
  private boolean isTelemetryServiceAvailable = true;

  // Retry timeout for the HTTP request
  private static final int TELEMETRY_HTTP_RETRY_TIMEOUT_IN_SEC = 1000;

  /**
   * Constructor for creating a sessionless telemetry client
   *
   * @param httpClient client object used to communicate with other machine
   * @param serverUrl server url
   * @param authType authorization type, should be either KEYPAIR_JWY or OAUTH
   * @param flushSize maximum size of telemetry batch before flush
   */
  private TelemetryClient(
      CloseableHttpClient httpClient, String serverUrl, String authType, int flushSize) {
    this.serverUrl = serverUrl;
    this.httpClient = httpClient;

    if (!Objects.equals(authType, "KEYPAIR_JWT") && !Objects.equals(authType, "OAUTH")) {
      throw new IllegalArgumentException(
          "Invalid authType, should be \"KEYPAIR_JWT\" or \"OAUTH\"");
    }
    this.authType = authType;

    if (this.serverUrl.endsWith("/")) {
      this.telemetryUrl =
          this.serverUrl.substring(0, this.serverUrl.length() - 1) + SF_PATH_TELEMETRY_SESSIONLESS;
    } else {
      this.telemetryUrl = this.serverUrl + SF_PATH_TELEMETRY_SESSIONLESS;
    }

    this.logBatch = new LinkedList<>();
    this.isClosed = false;
    this.forceFlushSize = flushSize;
    logger.debug(
        "Initializing telemetry client with telemetry url: {}, flush size: {}, auth type: {}",
        telemetryUrl,
        forceFlushSize,
        authType);
  }

  /**
   * Return whether the client can be used to add/send metrics
   *
   * @return whether client is enabled
   */
  public boolean isTelemetryEnabled() {
    return this.isTelemetryServiceAvailable;
  }

  /** Disable any use of the client to add/send metrics */
  public void disableTelemetry() {
    logger.debug("Disabling telemetry");
    this.isTelemetryServiceAvailable = false;
  }

  /**
   * Initialize the sessionless telemetry connector using KEYPAIR_JWT as the default auth type
   *
   * @param httpClient client object used to communicate with other machine
   * @param serverUrl server url
   * @return a telemetry connector
   */
  public static Telemetry createSessionlessTelemetry(
      CloseableHttpClient httpClient, String serverUrl) {
    // By default, use KEYPAIR_JWT as the auth type
    return createSessionlessTelemetry(
        httpClient, serverUrl, "KEYPAIR_JWT", DEFAULT_FORCE_FLUSH_SIZE);
  }

  /**
   * Initialize the sessionless telemetry connector
   *
   * @param httpClient client object used to communicate with other machine
   * @param serverUrl server url
   * @param authType authorization type for sessionless telemetry
   * @return a telemetry connector
   */
  public static Telemetry createSessionlessTelemetry(
      CloseableHttpClient httpClient, String serverUrl, String authType) {
    return createSessionlessTelemetry(httpClient, serverUrl, authType, DEFAULT_FORCE_FLUSH_SIZE);
  }

  /**
   * Initialize the sessionless telemetry connector
   *
   * @param httpClient client object used to communicate with other machine
   * @param serverUrl server url
   * @param authType authorization type for sessionless telemetry
   * @param flushSize maximum size of telemetry batch before flush
   * @return a telemetry connector
   */
  public static Telemetry createSessionlessTelemetry(
      CloseableHttpClient httpClient, String serverUrl, String authType, int flushSize) {
    return new TelemetryClient(httpClient, serverUrl, authType, flushSize);
  }

  /**
   * Add log to batch to be submitted to telemetry. Send batch if forceFlushSize reached
   *
   * @param log entry to add
   */
  @Override
  public void addLogToBatch(TelemetryData log) {
    if (isClosed) {
      logger.debug("Telemetry already closed", false);
      return;
    }

    if (!isTelemetryEnabled()) {
      return; // if disable, do nothing
    }

    synchronized (locker) {
      this.logBatch.add(log);
    }

    int logBatchSize = this.logBatch.size();
    if (logBatchSize >= this.forceFlushSize) {
      logger.debug("Force flushing telemetry batch of size: {}", logBatchSize);
      this.sendBatchAsync();
    }
  }

  /**
   * Add log to batch to be submitted to telemetry. Send batch if forceFlushSize reached
   *
   * @param message json node of log
   * @param timeStamp timestamp to use for log
   */
  public void addLogToBatch(ObjectNode message, long timeStamp) {
    this.addLogToBatch(new TelemetryData(message, timeStamp));
  }

  /** Close telemetry connector and send any unsubmitted logs */
  @Override
  public void close() {
    if (isClosed) {
      logger.debug("Telemetry client already closed", false);
      return;
    }

    try {
      // sendBatch when close is synchronous, otherwise client might be closed
      // before data was sent.
      sendBatchAsync().get();
    } catch (Throwable e) {
      logger.debug("Error when sending batch data, {}", e);
    } finally {
      this.isClosed = true;
    }
  }

  /**
   * Return whether the client has been closed
   *
   * @return whether client is closed
   */
  public boolean isClosed() {
    return this.isClosed;
  }

  @Override
  public Future<Boolean> sendBatchAsync() {
    return TelemetryThreadPool.getInstance()
        .submit(
            () -> {
              try {
                return this.sendBatch();
              } catch (Throwable e) {
                logger.debug("Failed to send telemetry data, {}", e);
                return false;
              }
            });
  }

  @Override
  public void postProcess(String queryId, String sqlState, int vendorCode, Throwable ex) {
    // This is a no-op.
  }

  /**
   * Send all cached logs to server
   *
   * @return whether the logs were sent successfully
   * @throws IOException if closed or uploading batch fails
   */
  private boolean sendBatch() throws IOException {
    if (isClosed) {
      throw new IOException("Telemetry connector is closed");
    }
    if (!isTelemetryEnabled()) {
      return false;
    }

    LinkedList<TelemetryData> tmpList;
    synchronized (locker) {
      tmpList = this.logBatch;
      this.logBatch = new LinkedList<>();
    }

    if (!tmpList.isEmpty()) {
      Stopwatch stopwatch = new Stopwatch();
      stopwatch.start();
      String payload = logsToString(tmpList);

      logger.debugNoMask("Payload of telemetry is : " + payload);

      HttpPost post = new HttpPost(this.telemetryUrl);
      post.setEntity(new StringEntity(payload));
      post.setHeader("Content-type", "application/json");

      post.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + this.token);
      post.setHeader("X-Snowflake-Authorization-Token-Type", this.authType);
      post.setHeader(HttpHeaders.ACCEPT, "application/json");

      String response = null;

      try {
        response =
            JdbcHttpUtil.executeGeneralRequest(
                post,
                TELEMETRY_HTTP_RETRY_TIMEOUT_IN_SEC,
                0,
                (int) JdbcHttpUtil.getSocketTimeout().toMillis(),
                0,
                this.httpClient);
        stopwatch.stop();
        logger.debug(
            "Sending telemetry took {} ms. Batch size: {}",
            stopwatch.elapsedMillis(),
            tmpList.size());
      } catch (SnowflakeSQLException e) {
        disableTelemetry(); // when got error like 404 or bad request, disable telemetry in this
        // telemetry instance
        logger.error(
            "Telemetry request failed, response: {}, exception: {}", response, e.getMessage());
        return false;
      }
    }
    return true;
  }

  /**
   * Send a log to the server, along with any existing logs waiting to be sent
   *
   * @param log entry to send
   * @return whether the logs were sent successfully
   * @throws IOException if closed or uploading batch fails
   */
  public boolean sendLog(TelemetryData log) throws IOException {
    addLogToBatch(log);
    return sendBatch();
  }

  /**
   * Send a log to the server, along with any existing logs waiting to be sent
   *
   * @param message json node of log
   * @param timeStamp timestamp to use for log
   * @return whether the logs were sent successfully
   * @throws IOException if closed or uploading batch fails
   */
  public boolean sendLog(ObjectNode message, long timeStamp) throws IOException {
    return this.sendLog(new TelemetryData(message, timeStamp));
  }

  /**
   * convert a list of log to a JSON object
   *
   * @param telemetryData a list of log
   * @return the result json string
   */
  static ObjectNode logsToJson(LinkedList<TelemetryData> telemetryData) {
    ObjectNode node = mapper.createObjectNode();
    ArrayNode logs = mapper.createArrayNode();
    for (TelemetryData data : telemetryData) {
      logs.add(data.toJson());
    }
    node.set("logs", logs);

    return node;
  }

  /**
   * convert a list of log to a JSON String
   *
   * @param telemetryData a list of log
   * @return the result json string
   */
  static String logsToString(LinkedList<TelemetryData> telemetryData) {
    return logsToJson(telemetryData).toString();
  }

  /**
   * For test use only
   *
   * @return the number of cached logs
   */
  public int bufferSize() {
    return this.logBatch.size();
  }

  /**
   * For test use only
   *
   * @return a copy of the logs currently in the buffer
   */
  public LinkedList<TelemetryData> logBuffer() {
    return new LinkedList<>(this.logBatch);
  }

  /**
   * Refresh the JWT/OAuth token
   *
   * @param token latest JWT/OAuth token
   */
  public void refreshToken(String token) {
    this.token = token;
  }
}
