/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming;

import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import net.snowflake.ingest.streaming.internal.ColumnProperties;

/**
 * A logical partition that represents a connection to a single Snowflake table, data will be
 * ingested into the channel, and then flushed to Snowflake table periodically in the background.
 *
 * <p>Channels are identified by their name and only one channel with the same name may ingest data
 * at the same time. When a new channel is opened, all previously opened channels with the same name
 * are invalidated (this applies for the table globally. not just in a single JVM). In order to
 * ingest data from multiple threads/clients/applications, we recommend opening multiple channels,
 * each with a different name.
 *
 * <p>Thread safety note: Implementations of this interface are required to be thread safe.
 */
public interface SnowflakeStreamingIngestChannel {
  /**
   * Get the fully qualified channel name
   *
   * @return fully qualified name of the channel, in the format of
   *     dbName.schemaName.tableName.channelName
   */
  String getFullyQualifiedName();

  /**
   * Get the name of the channel
   *
   * @return name of the channel
   */
  String getName();

  /**
   * Get the database name
   *
   * @return name of the database
   */
  String getDBName();

  /**
   * Get the schema name
   *
   * @return name of the schema
   */
  String getSchemaName();

  /**
   * Get the table name
   *
   * @return name of the table
   */
  String getTableName();

  /**
   * Get the fully qualified table name that the channel belongs to
   *
   * @return fully qualified table name, in the format of dbName.schemaName.tableName
   */
  String getFullyQualifiedTableName();

  /** @return a boolean which indicates whether the channel is valid */
  boolean isValid();

  /** @return a boolean which indicates whether the channel is closed */
  boolean isClosed();

  /**
   * Close the channel, this function will make sure all the data in this channel is committed
   *
   * @return a completable future which will be completed when the channel is closed
   */
  CompletableFuture<Void> close();

  /**
   * Close the channel, this function will make sure all the data in this channel is committed
   *
   * <p>Note that this call with drop=true will delete <a
   * href=https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview#offset-tokens>Offset
   * Token</a> information from Snowflake servers unless the channel has already been opened in
   * another client. So only use it if you are completely done ingesting data for this channel. If
   * you open a channel with the same name in the future, it will behave like a new channel.
   *
   * @param drop if true, the channel will be dropped after all data is successfully committed.
   * @return a completable future which will be completed when the channel is closed
   */
  CompletableFuture<Void> close(boolean drop);

  /**
   * Insert one row into the channel, the row is represented using Map where the key is column name
   * and the value is a row of data. The following table summarizes supported value types and their
   * formats:
   *
   * <table>
   *     <tr>
   *     <th>Snowflake Column Type</th>
   *     <th>Allowed Java Data Type</th>
   *     </tr>
   *     <tr>
   *         <td>CHAR, VARCHAR</td>
   *         <td>
   *             <ul>
   *                 <li>String</li>
   *                 <li>primitive data types (int, boolean, char, …)</li>
   *             </ul>
   *         </td>
   *     </tr>
   *     <tr>
   *         <td>BINARY</td>
   *         <td>
   *             <ul>
   *                 <li>byte[]</li>
   *                 <li>String (hex-encoded)</li>
   *             </ul>
   *         </td>
   *     </tr>
   *     <tr>
   *         <td>NUMBER, FLOAT</td>
   *         <td>
   *             <ul>
   *                 <li>numeric types (BigInteger, BigDecimal, byte, int, double, …)</li>
   *                 <li>String</li>
   *             </ul>
   *         </td>
   *     </tr>
   *     <tr>
   *         <td>BOOLEAN</td>
   *         <td>
   *             <ul>
   *                 <li>boolean</li>
   *                 <li>numeric types (BigInteger, BigDecimal, byte, int, double, …)</li>
   *                 <li>String</li>
   *             </ul>
   *             See <a href="https://docs.snowflake.com/en/sql-reference/data-types-logical.html#label-boolean-conversion">boolean conversion details.</a>
   *         </td>
   *     </tr>
   *     <tr>
   *         <td>TIME</td>
   *         <td>
   *             <ul>
   *                 <li>{@link java.time.LocalTime}</li>
   *                 <li>{@link java.time.OffsetTime}</li>
   *                 <li>
   *                     String (in one of the following formats):
   *                     <ul>
   *                         <li>{@link java.time.format.DateTimeFormatter#ISO_LOCAL_TIME}</li>
   *                         <li>{@link java.time.format.DateTimeFormatter#ISO_OFFSET_TIME}</li>
   *                         <li>Integer-stored time (see <a href="https://docs.snowflake.com/en/user-guide/date-time-input-output.html#auto-detection-of-integer-stored-date-time-and-timestamp-values">Snowflake Docs</a> for more details)</li>
   *                     </ul>
   *                 </li>
   *
   *             </ul>
   *         </td>
   *     </tr>
   *     <tr>
   *         <td>DATE</td>
   *         <td>
   *             <ul>
   *                 <li>{@link java.time.LocalDate}</li>
   *                 <li>{@link java.time.LocalDateTime}</li>
   *                 <li>{@link java.time.OffsetDateTime}</li>
   *                 <li>{@link java.time.ZonedDateTime}</li>
   *                 <li>{@link java.time.Instant}</li>
   *                 <li>
   *                     String (in one of the following formats):
   *                     <ul>
   *                         <li>{@link java.time.format.DateTimeFormatter#ISO_LOCAL_DATE}</li>
   *                         <li>{@link java.time.format.DateTimeFormatter#ISO_LOCAL_DATE_TIME}</li>
   *                         <li>{@link java.time.format.DateTimeFormatter#ISO_OFFSET_DATE_TIME}</li>
   *                         <li>{@link java.time.format.DateTimeFormatter#ISO_ZONED_DATE_TIME}</li>
   *                         <li>Integer-stored date (see <a href="https://docs.snowflake.com/en/user-guide/date-time-input-output.html#auto-detection-of-integer-stored-date-time-and-timestamp-values">Snowflake Docs</a> for more details)</li>
   *                     </ul>
   *                 </li>
   *             </ul>
   *         </td>
   *     </tr>
   *     <tr>
   *         <td>TIMESTAMP_NTZ, TIMESTAMP_LTZ, TIMESTAMP_TZ</td>
   *         <td>
   *             <ul>
   *                 <li>{@link java.time.LocalDate}</li>
   *                 <li>{@link java.time.LocalDateTime}</li>
   *                 <li>{@link java.time.OffsetDateTime}</li>
   *                 <li>{@link java.time.ZonedDateTime}</li>
   *                 <li>{@link java.time.Instant}</li>
   *                 <li>
   *                     String (in one of the following formats):
   *                     <ul>
   *                         <li>{@link java.time.format.DateTimeFormatter#ISO_LOCAL_DATE}</li>
   *                         <li>{@link java.time.format.DateTimeFormatter#ISO_LOCAL_DATE_TIME}</li>
   *                         <li>{@link java.time.format.DateTimeFormatter#ISO_OFFSET_DATE_TIME}</li>
   *                         <li>{@link java.time.format.DateTimeFormatter#ISO_ZONED_DATE_TIME}</li>
   *                         <li>Integer-stored timestamp (see <a href="https://docs.snowflake.com/en/user-guide/date-time-input-output.html#auto-detection-of-integer-stored-date-time-and-timestamp-values">Snowflake Docs</a> for more details)</li>
   *                     </ul>
   *                 </li>
   *
   *             </ul>
   *
   *             For TIMESTAMP_LTZ and TIMESTAMP_TZ, all input without timezone will be by default interpreted in the timezone "America/Los_Angeles". This can be changed by calling {@link net.snowflake.ingest.streaming.OpenChannelRequest.OpenChannelRequestBuilder#setDefaultTimezone(ZoneId)}.
   *         </td>
   *         <tr>
   *             <td>VARIANT, ARRAY</td>
   *             <td>
   *                 <ul>
   *                     <li>String (must be a valid JSON value)</li>
   *                     <li>primitive data types and their arrays</li>
   *                     <li>BigInteger, BigDecimal</li>
   *                     <li>{@link java.time.LocalDate}</li>
   *                     <li>{@link java.time.LocalDateTime}</li>
   *                     <li>{@link java.time.OffsetDateTime}</li>
   *                     <li>{@link java.time.ZonedDateTime}</li>
   *                     <li>Map&lt;String, T&gt; where T is a valid VARIANT type</li>
   *                     <li>T[] where T is a valid VARIANT type</li>
   *                     <li>List&lt;T&gt; where T is a valid VARIANT type</li>
   *                 </ul>
   *             </td>
   *         </tr>
   *         <tr>
   *             <td>OBJECT</td>
   *             <td>
   *                 <ul>
   *                     <li>String (must be a valid JSON object)</li>
   *                     <li>Map&lt;String, T&gt; where T is a valid variant type</li>
   *                 </ul>
   *             </td>
   *         </tr>
   *         <tr>
   *            <td>GEOGRAPHY, GEOMETRY</td>
   *            <td>Not supported</td>
   *         </tr>
   *     </tr>
   *
   * </table>
   *
   * @param row object data to write. For predictable results, we recommend not to concurrently
   *     modify the input row data.
   * @param offsetToken offset of given row, used for replay in case of failures. It could be null
   *     if you don't plan on replaying or can't replay
   * @return insert response that possibly contains errors because of insertion failures
   */
  InsertValidationResponse insertRow(Map<String, Object> row, @Nullable String offsetToken);

  /**
   * Insert a batch of rows into the channel, each row is represented using Map where the key is
   * column name and the value is a row of data. See {@link
   * SnowflakeStreamingIngestChannel#insertRow(Map, String)} for more information about accepted
   * values.
   *
   * @param rows object data to write
   * @param startOffsetToken start offset of the batch/row-set
   * @param endOffsetToken end offset of the batch/row-set, used for replay in case of failures, *
   *     It could be null if you don't plan on replaying or can't replay
   * @return insert response that possibly contains errors because of insertion failures
   */
  InsertValidationResponse insertRows(
      Iterable<Map<String, Object>> rows,
      @Nullable String startOffsetToken,
      @Nullable String endOffsetToken);

  /**
   * Insert a batch of rows into the channel with the end offset token only, please see {@link
   * SnowflakeStreamingIngestChannel#insertRows(Iterable, String, String)} for more information.
   */
  InsertValidationResponse insertRows(
      Iterable<Map<String, Object>> rows, @Nullable String offsetToken);

  /**
   * Get the latest committed offset token from Snowflake
   *
   * @return the latest committed offset token
   */
  @Nullable
  String getLatestCommittedOffsetToken();

  /**
   * Gets the table schema associated with this channel. Note that this is the table schema at the
   * time of a channel open event. The schema may be changed on the Snowflake side in which case
   * this will continue to show an old schema version until the channel is re-opened.
   *
   * @return map representing Column Name to Column Properties
   */
  Map<String, ColumnProperties> getTableSchema();
}
