/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming;

import net.snowflake.ingest.utils.Utils;

import javax.annotation.Nullable;
import java.util.Map;

public class InsertRowsRequest {

  // builder for KC to call to create insertRowRequest
  public static InsertRowsRequestBuilder builder(Map<String, Object> rows, @Nullable String offsetToken) {
    return new InsertRowsRequestBuilder(rows, offsetToken);
  }

  public static class InsertRowsRequestBuilder {
    // required parameters
    private final Map<String, Object> rows;
    private final @Nullable String offsetToken;

    // optional parameters
    private KcFlushReason kcFlushReason;

    public InsertRowsRequestBuilder(Map<String, Object> rows, @Nullable String offsetToken) {
      this.rows = rows;
      this.offsetToken = offsetToken;
    }

    public InsertRowsRequestBuilder setKcFlushReason(KcFlushReason kcFlushReason) {
      this.kcFlushReason = kcFlushReason;
      return this;
    }

    public InsertRowsRequest build() {
      return new InsertRowsRequest(this);
    }
  }

  // actual insert row request
  private final Map<String, Object> rows;
  private final @Nullable String offsetToken;
  private final KcFlushReason kcFlushReason;

  private InsertRowsRequest(InsertRowsRequestBuilder builder) {
    // ensure required values are not null
    Utils.assertNotNull("rows", builder.rows);

    // default values
    this.rows = builder.rows;
    this.offsetToken = builder.offsetToken;

    // optional values
    KcFlushReason kcFlushReason = builder.kcFlushReason;
    this.kcFlushReason = (kcFlushReason == null || kcFlushReason.getFlushReason() == KcFlushReason.FlushReason.NONE) ?
      new KcFlushReason(KcFlushReason.FlushReason.NONE, -1, "", "") :
      kcFlushReason;
  }

  public Map<String, Object> getRows() {
    return this.rows;
  }

  public @Nullable String getOffsetToken() {
    return this.offsetToken;
  }

  public KcFlushReason getKcFlushReason() {
    return this.kcFlushReason;
  }
}
