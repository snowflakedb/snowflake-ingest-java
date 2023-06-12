/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import org.junit.Assert;
import org.junit.Test;

public class StreamingIngestResponseCodeTest {
  @Test
  public void testGetMessageNullCode() {
    Assert.assertEquals(
        StreamingIngestResponseCode.UNKNOWN_STATUS_MESSAGE,
        StreamingIngestResponseCode.getMessageByCode(null));
  }

  @Test
  public void testGetMessageUnknownCode() {
    Assert.assertEquals(
        StreamingIngestResponseCode.UNKNOWN_STATUS_MESSAGE,
        StreamingIngestResponseCode.getMessageByCode(-1L));
  }

  @Test
  public void testGetMessageKnownCodes() {
    for (StreamingIngestResponseCode code : StreamingIngestResponseCode.values()) {
      Assert.assertEquals(
          code.getMessage(), StreamingIngestResponseCode.getMessageByCode(code.getStatusCode()));
    }
  }
}
