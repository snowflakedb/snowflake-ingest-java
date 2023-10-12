/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.Map;
import net.snowflake.ingest.streaming.InsertValidationResponse;

/** Interface to a batch of rows into the row buffer based on different on error options */
public interface IngestionStrategy<T> {
  /**
   * Insert a batch of rows into the row buffer
   *
   * @param rows input row
   * @param offsetToken offset token of the latest row in the batch
   * @return insert response that possibly contains errors because of insertion failures
   */
  InsertValidationResponse insertRows(
      AbstractRowBuffer<T> rowBuffer, Iterable<Map<String, Object>> rows, String offsetToken);
}
