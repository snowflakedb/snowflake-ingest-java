/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.serialization;

import java.io.IOException;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.core.JsonGenerator;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonSerializer;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.SerializerProvider;

/**
 * Serialize Java byte arrays as JSON arrays of numbers instead of the default Jackson
 * base64-encoding.
 */
public class ByteArraySerializer extends JsonSerializer<byte[]> {
  @Override
  public void serialize(byte[] value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    gen.writeStartArray();
    for (byte v : value) {
      gen.writeNumber(v);
    }
    gen.writeEndArray();
  }
}
