/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming;

public interface OffsetTokenVerificationFunction {
  /**
   * @param prevBatchEndOffset
   * @param curBatchStartOffset
   * @param curBatchEndOffset
   * @param rowCount
   * @return a boolean indicates whether the verification passed or not
   */
  boolean verify(
      String prevBatchEndOffset,
      String curBatchStartOffset,
      String curBatchEndOffset,
      long rowCount);
}
