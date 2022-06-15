package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.SnowflakeURL;
import org.junit.Assert;
import org.junit.Test;

public class SnowflakeURLTest {

  @Test
  public void testParseSane() throws Exception {
    SnowflakeURL url = new SnowflakeURL("https://honksrus.snowflakecomputing.com:8080");
    Assert.assertEquals("https", url.getScheme());
    Assert.assertEquals(8080, url.getPort());
    Assert.assertEquals("honksrus", url.getAccount());
    Assert.assertEquals("honksrus.snowflakecomputing.com:8080", url.getFullUrl());
    Assert.assertEquals("honksrus.snowflakecomputing.com", url.getUrlWithoutPort());
    Assert.assertEquals("jdbc:snowflake://honksrus.snowflakecomputing.com:8080", url.getJdbcUrl());

    url = new SnowflakeURL("sfctest0.snowflakecomputing.com");
    Assert.assertEquals(443, url.getPort());
    Assert.assertEquals("https", url.getScheme());
    Assert.assertEquals("sfctest0.snowflakecomputing.com:443", url.getFullUrl());

    url = new SnowflakeURL("sfctest0.snowflakecomputing.com:80");
    Assert.assertEquals(80, url.getPort());
    Assert.assertEquals("https", url.getScheme());
    Assert.assertEquals("sfctest0.snowflakecomputing.com:80", url.getFullUrl());

    url = new SnowflakeURL("http://sfctest0.snowflakecomputing.com:443");
    Assert.assertEquals(443, url.getPort());
    Assert.assertEquals("http", url.getScheme());
    Assert.assertEquals("sfctest0.snowflakecomputing.com:443", url.getFullUrl());

    url = new SnowflakeURL("http://snowflake.dev.local:8082/");
    Assert.assertEquals(8082, url.getPort());
    Assert.assertEquals("http", url.getScheme());
    Assert.assertEquals("snowflake.dev.local:8082", url.getFullUrl());

    url = new SnowflakeURL("https://pm-connectors.snowflakecomputing.com:443");
    Assert.assertEquals(443, url.getPort());
    Assert.assertEquals("https", url.getScheme());
    Assert.assertEquals("pm-connectors.snowflakecomputing.com:443", url.getFullUrl());
    Assert.assertEquals("pm-connectors", url.getAccount());

    url = new SnowflakeURL("https://pm_connectors.snowflakecomputing.com:443");
    Assert.assertEquals(443, url.getPort());
    Assert.assertEquals("https", url.getScheme());
    Assert.assertEquals("pm_connectors.snowflakecomputing.com:443", url.getFullUrl());
    Assert.assertEquals("pm_connectors", url.getAccount());
  }

  @Test
  public void testBuilder() throws Exception {
    SnowflakeURL url =
        new SnowflakeURL.SnowflakeURLBuilder()
            .setUrl("honksrus.snowflakecomputing.com")
            .setSsl(true)
            .setPort(8080)
            .setAccount("honksrus")
            .build();

    Assert.assertEquals("https", url.getScheme());
    Assert.assertEquals(8080, url.getPort());
    Assert.assertEquals("honksrus", url.getAccount());
    Assert.assertEquals("honksrus.snowflakecomputing.com:8080", url.getFullUrl());
    Assert.assertEquals("honksrus.snowflakecomputing.com", url.getUrlWithoutPort());
    Assert.assertEquals("jdbc:snowflake://honksrus.snowflakecomputing.com:8080", url.getJdbcUrl());
  }

  @Test
  public void testInvalidURL() throws Exception {
    try {
      SnowflakeURL url = new SnowflakeURL("sfctest0");
      Assert.fail("URL should be invalid.");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_URL.getMessageCode(), e.getVendorCode());
    }

    try {
      SnowflakeURL url = new SnowflakeURL("sfctest0.b");
      Assert.fail("URL should be invalid.");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_URL.getMessageCode(), e.getVendorCode());
    }

    try {
      SnowflakeURL url = new SnowflakeURL(".snowflakecomputing.com");
      Assert.fail("URL should be invalid.");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_URL.getMessageCode(), e.getVendorCode());
    }

    try {
      SnowflakeURL url = new SnowflakeURL("..snowflakecomputing.com");
      Assert.fail("URL should be invalid.");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_URL.getMessageCode(), e.getVendorCode());
    }
  }
}
