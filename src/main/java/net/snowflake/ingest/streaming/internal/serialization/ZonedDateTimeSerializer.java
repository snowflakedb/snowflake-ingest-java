/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.serialization;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.time.ZonedDateTime;

/** Snowflake does not support parsing zones, so serialize it in offset instead */
public class ZonedDateTimeSerializer extends JsonSerializer<ZonedDateTime> {
  @Override
  public void serialize(ZonedDateTime value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    gen.writeString(value.toOffsetDateTime().toString());
  }
}
