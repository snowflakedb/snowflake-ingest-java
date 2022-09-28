package net.snowflake.ingest.streaming.internal.serialization;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;

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
