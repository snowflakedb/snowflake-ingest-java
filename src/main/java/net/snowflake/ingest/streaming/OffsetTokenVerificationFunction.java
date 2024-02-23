/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming;

/**
 * Interface to provide a custom offset verification logic. If specified, verification failures will
 * be logged as warnings and reported to Snowflake. This interface could be used when there are
 * certain assumption about the offset token behavior and please reach out to Snowflake if you
 * notice any unexpected behaviors.
 *
 * <p>Below is an example that verifies that all offset tokens are monotonically increasing numbers:
 *
 * <pre>
 *     <code>
 *       private static final OffsetTokenVerificationFunction offsetTokenVerificationFunction =
 *       (prevBatchEndOffset, curBatchStartOffset, curBatchEndOffset, rowCount) -> {
 *         boolean hasMismatch = false;
 *
 *         if (curBatchStartOffset != null) {
 *           try {
 *             long curStart = Long.parseLong(curBatchStartOffset);
 *             long curEnd = Long.parseLong(curBatchEndOffset);
 *
 *             // We verify that the end_offset - start_offset + 1 = row_count
 *             if (curEnd - curStart + 1 != rowCount) {
 *               hasMismatch = true;
 *             }
 *
 *             // We verify that start_offset_of_current_batch = end_offset_of_previous_batch+1
 *             if (prevBatchEndOffset != null) {
 *               long prevEnd = Long.parseLong(prevBatchEndOffset);
 *               if (curStart != prevEnd + 1) {
 *                 hasMismatch = true;
 *               }
 *             }
 *           } catch (NumberFormatException ignored) {
 *             // Do nothing since we can't compare the offset
 *           }
 *         }
 *
 *         return hasMismatch;
 *       };
 *     </code>
 * </pre>
 */
public interface OffsetTokenVerificationFunction {
  /**
   * @param prevBatchEndOffset end offset token of the previous batch
   * @param curBatchStartOffset start offset token of the current batch
   * @param curBatchEndOffset end offset token of the current batch
   * @param rowCount number of rows in the current batch
   * @return a boolean indicates whether the verification passed or not, if not, we will log a
   *     warning and report it to SF
   */
  boolean verify(
      String prevBatchEndOffset,
      String curBatchStartOffset,
      String curBatchEndOffset,
      long rowCount);
}
