// Ported from snowflake-jdbc: net.snowflake.client.jdbc.SnowflakeFileTransferConfigTest
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import net.snowflake.ingest.utils.OCSPMode;
import org.junit.Test;

public class SnowflakeFileTransferConfigTest {

  private static final SnowflakeFileTransferMetadataV1 SNOWFLAKE_FILE_TRANSFER_METADATA =
      new SnowflakeFileTransferMetadataV1(
          "", "", "", "", 0L, SFBaseFileTransferAgent.CommandType.UPLOAD, null);
  private static final ByteArrayInputStream UPLOAD_STREAM = new ByteArrayInputStream(new byte[0]);

  @Test
  public void shouldBuildDefaultConfig() {
    SnowflakeFileTransferConfig config = createObligatoryConfigPartInBuilder().build();

    assertObligatoryParameters(config);
    assertFalse(config.isSilentException());
  }

  private static void assertObligatoryParameters(SnowflakeFileTransferConfig config) {
    assertNotNull(config);
    assertEquals(SNOWFLAKE_FILE_TRANSFER_METADATA, config.getSnowflakeFileTransferMetadata());
    assertEquals(UPLOAD_STREAM, config.getUploadStream());
    assertEquals(OCSPMode.DISABLE_OCSP_CHECKS, config.getOcspMode());
  }

  @Test
  public void shouldBuildConfig() {
    SnowflakeFileTransferConfig config =
        createObligatoryConfigPartInBuilder().setSilentException(true).build();

    assertObligatoryParameters(config);
    assertTrue(config.isSilentException());
  }

  private static SnowflakeFileTransferConfig.Builder createObligatoryConfigPartInBuilder() {
    return SnowflakeFileTransferConfig.Builder.newInstance()
        .setSnowflakeFileTransferMetadata(SNOWFLAKE_FILE_TRANSFER_METADATA)
        .setUploadStream(UPLOAD_STREAM)
        .setOcspMode(OCSPMode.DISABLE_OCSP_CHECKS);
  }
}
