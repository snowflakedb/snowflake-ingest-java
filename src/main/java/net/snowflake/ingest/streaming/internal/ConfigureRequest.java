/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

/** Interface for {@link ChannelConfigureRequest} and {@link ClientConfigureRequest} */
interface ConfigureRequest extends StreamingIngestRequest {
  String getRole();

  void setFileName(String fileName);
}
