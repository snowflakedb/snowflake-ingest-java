package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.Constants.*;

import java.io.IOException;
import java.io.StringWriter;
import java.security.PrivateKey;
import java.security.Security;
import java.util.Properties;
import net.snowflake.client.jdbc.internal.org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import net.snowflake.client.jdbc.internal.org.bouncycastle.operator.OperatorCreationException;
import net.snowflake.client.jdbc.internal.org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfoBuilder;
import net.snowflake.client.jdbc.internal.org.bouncycastle.pkcs.jcajce.JcaPKCS8EncryptedPrivateKeyInfoBuilder;
import net.snowflake.client.jdbc.internal.org.bouncycastle.pkcs.jcajce.JcePKCSPBEOutputEncryptorBuilder;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.StreamingUtils;
import org.bouncycastle.asn1.nist.NISTObjectIdentifiers;
import org.junit.Assert;
import org.junit.Test;

public class SnowflakeStreamingIngestClientTest {

  @Test
  public void testClientFactoryMissingName() throws Exception {
    Properties prop = new Properties();
    prop.put(ACCOUNT_URL, TestUtils.getHost());
    prop.put(PRIVATE_KEY, TestUtils.getPrivateKey());

    try {
      SnowflakeStreamingIngestClient client =
          SnowflakeStreamingIngestClientFactory.builder("client").setProperties(prop).build();
      Assert.fail("Client creation should fail.");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.MISSING_CONFIG.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testClientFactoryMissingURL() throws Exception {
    Properties prop = new Properties();
    prop.put(USER_NAME, TestUtils.getUser());
    prop.put(PRIVATE_KEY, TestUtils.getPrivateKey());

    try {
      SnowflakeStreamingIngestClient client =
          SnowflakeStreamingIngestClientFactory.builder("client").setProperties(prop).build();
      Assert.fail("Client creation should fail.");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.MISSING_CONFIG.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testClientFactoryInvalidURL() throws Exception {
    Properties prop = new Properties();
    prop.put(USER_NAME, TestUtils.getUser());
    prop.put(ACCOUNT_URL, "invalid");
    prop.put(PRIVATE_KEY, TestUtils.getPrivateKey());

    try {
      SnowflakeStreamingIngestClient client =
          SnowflakeStreamingIngestClientFactory.builder("client").setProperties(prop).build();
      Assert.fail("Client creation should fail.");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_URL.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testClientFactoryInvalidPrivateKey() throws Exception {
    Properties prop = new Properties();
    prop.put(USER_NAME, TestUtils.getUser());
    prop.put(ACCOUNT_URL, TestUtils.getHost());
    prop.put(PRIVATE_KEY, "invalid");

    try {
      SnowflakeStreamingIngestClient client =
          SnowflakeStreamingIngestClientFactory.builder("client").setProperties(prop).build();
      Assert.fail("Client creation should fail.");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_PRIVATE_KEY.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testClientFactorySuccess() throws Exception {
    Properties prop = new Properties();
    prop.put(USER_NAME, TestUtils.getUser());
    prop.put(ACCOUNT_URL, TestUtils.getHost());
    prop.put(PRIVATE_KEY, TestUtils.getPrivateKey());

    SnowflakeStreamingIngestClient client =
        SnowflakeStreamingIngestClientFactory.builder("client").setProperties(prop).build();

    Assert.assertEquals("client", client.getName());
  }

  @Test
  public void testEncryptedPrivateKey() throws Exception {
    String testPassphrase = "TestPassword1234!";
    String testKey =
        generateAESKey(
            StreamingUtils.parsePrivateKey(TestUtils.getPrivateKey()),
            testPassphrase.toCharArray());

    try {
      StreamingUtils.parseEncryptedPrivateKey(testKey, testPassphrase.substring(1));
      Assert.fail("Decryption should fail.");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_ENCRYPTED_KEY.getMessageCode(), e.getVendorCode());
    }

    StreamingUtils.parseEncryptedPrivateKey(testKey, testPassphrase);
  }

  private String generateAESKey(PrivateKey key, char[] passwd)
      throws IOException, OperatorCreationException {
    Security.addProvider(new BouncyCastleFipsProvider());
    StringWriter writer = new StringWriter();
    JcaPEMWriter pemWriter = new JcaPEMWriter(writer);
    PKCS8EncryptedPrivateKeyInfoBuilder pkcs8EncryptedPrivateKeyInfoBuilder =
        new JcaPKCS8EncryptedPrivateKeyInfoBuilder(key);
    pemWriter.writeObject(
        pkcs8EncryptedPrivateKeyInfoBuilder.build(
            new JcePKCSPBEOutputEncryptorBuilder(NISTObjectIdentifiers.id_aes256_CBC)
                .setProvider("BCFIPS")
                .build(passwd)));
    pemWriter.close();
    return writer.toString();
  }
}
