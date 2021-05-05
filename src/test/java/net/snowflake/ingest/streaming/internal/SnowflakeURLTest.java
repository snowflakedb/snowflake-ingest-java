package net.snowflake.ingest.streaming.internal;

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
}
