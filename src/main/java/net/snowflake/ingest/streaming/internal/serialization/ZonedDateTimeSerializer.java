/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.serialization;

import java.io.IOException;
import java.time.ZonedDateTime;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.core.JsonGenerator;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonSerializer;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.SerializerProvider;

/** Snowflake does not support parsing zones, so serialize it in offset instead */
public class ZonedDateTimeSerializer extends JsonSerializer<ZonedDateTime> {
  @Override
  public void serialize(ZonedDateTime value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    gen.writeString(value.toOffsetDateTime().toString());
  }
}
