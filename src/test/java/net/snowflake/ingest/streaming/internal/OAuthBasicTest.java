package net.snowflake.ingest.streaming.internal;

import java.util.Properties;
import java.util.UUID;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.connection.MockOAuthClient;
import net.snowflake.ingest.connection.OAuthManager;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.junit.Assert;
import org.junit.Test;

/**
 * This test only contains basic construction of client using OAuth authentication. Further
 * integration test would be added in dew.
 */
public class OAuthBasicTest {

  /** Create client with invalid authorization type, this should fail. */
  @Test
  public void invalidAuthType() throws Exception {
    Properties props = TestUtils.getProperties(Constants.BdecVersion.THREE, false);
    props.put(Constants.AUTHORIZATION_TYPE, "INVALID_AUTH_TYPE");
    SFException e =
        Assert.assertThrows(
            SFException.class,
            () ->
                SnowflakeStreamingIngestClientFactory.builder("MY_CLIENT")
                    .setProperties(props)
                    .build());
    Assert.assertEquals(e.getVendorCode(), ErrorCode.INVALID_CONFIG_PARAMETER.getMessageCode());
  }

  /** Create client with missing config, this should fail. */
  @Test
  public void missingOAuthParam() throws Exception {
    Properties props = TestUtils.getProperties(Constants.BdecVersion.THREE, false);
    props.put(Constants.AUTHORIZATION_TYPE, Constants.OAUTH);
    props.put(Constants.OAUTH_AUTO_REFRESH, "true");

    // Missing oauth_client_id
    assertMissingKey(Constants.OAUTH_CLIENT_ID, props);

    // Missing oauth_client_secret
    props.put(Constants.OAUTH_CLIENT_ID, "MOCK_CLIENT_ID");
    assertMissingKey(Constants.OAUTH_CLIENT_SECRET, props);

    // Missing oauth_refresh_token
    props.put(Constants.OAUTH_CLIENT_SECRET, "MOCK_CLIENT_SECRET");
    assertMissingKey(Constants.OAUTH_REFRESH_TOKEN, props);

    // Missing oauth_access_token
    props.put(Constants.OAUTH_AUTO_REFRESH, "false");
    assertMissingKey(Constants.OAUTH_ACCESS_TOKEN, props);
  }

  void assertMissingKey(String missingKey, Properties props) {
    SFException e =
        Assert.assertThrows(
            SFException.class,
            () ->
                SnowflakeStreamingIngestClientFactory.builder("MY_CLIENT")
                    .setProperties(props)
                    .build());
    Assert.assertEquals(
        e.getMessage(), new SFException(ErrorCode.MISSING_CONFIG, missingKey).getMessage());
  }

  /**
   * Create a client with mock credential using snowflake oauth, should fail when refreshing token
   */
  @Test(expected = SecurityException.class)
  public void testCreateSnowflakeOAuthClient() throws Exception {
    Properties props = getSnowflakeOAuthConfig(true);
    SnowflakeStreamingIngestClient client =
        SnowflakeStreamingIngestClientFactory.builder("MY_CLIENT").setProperties(props).build();
  }

  /**
   * Create a client with mock credential using external oauth, should fail when refreshing token
   */
  @Test(expected = SecurityException.class)
  public void testCreateOAuthClient() throws Exception {
    Properties props = getSnowflakeOAuthConfig(true);
    props.put(Constants.OAUTH_TOKEN_ENDPOINT, "https://mockexternaloauthendpoint.test/token");
    SnowflakeStreamingIngestClient client =
        SnowflakeStreamingIngestClientFactory.builder("MY_CLIENT").setProperties(props).build();
  }

  @Test
  public void testSetRefreshToken() throws Exception {
    SnowflakeStreamingIngestClientInternal<StubChunkData> client =
        new SnowflakeStreamingIngestClientInternal<>("TEST_CLIENT");
    MockOAuthClient mockOAuthClient = new MockOAuthClient(true);
    client.injectRequestBuilder(getRequestBuilder(mockOAuthClient));

    String newToken = UUID.randomUUID().toString();
    client.setRefreshToken(newToken);
  }

  @Test
  public void testSetAccessToken() throws Exception {
    SnowflakeStreamingIngestClientInternal<StubChunkData> client =
        new SnowflakeStreamingIngestClientInternal<>("TEST_CLIENT");
    MockOAuthClient mockOAuthClient = new MockOAuthClient(false);
    client.injectRequestBuilder(getRequestBuilder(mockOAuthClient));

    String newToken = UUID.randomUUID().toString();
    client.setAccessToken(newToken);
  }

  private static Properties getSnowflakeOAuthConfig(boolean autoRefresh) throws Exception {
    Properties props = TestUtils.getProperties(Constants.BdecVersion.THREE, false);
    props.remove(Constants.PRIVATE_KEY);
    props.put(Constants.AUTHORIZATION_TYPE, Constants.OAUTH);
    props.put(Constants.OAUTH_AUTO_REFRESH, autoRefresh);
    props.put(Constants.OAUTH_CLIENT_ID, "MOCK_CLIENT_ID");
    props.put(Constants.OAUTH_CLIENT_SECRET, "MOCK_CLIENT_SECRET");
    props.put(Constants.OAUTH_REFRESH_TOKEN, "MOCK_REFRESH_TOKEN");
    props.put(Constants.OAUTH_ACCESS_TOKEN, "MOCK_ACCESS_TOKEN");
    return props;
  }

  private static RequestBuilder getRequestBuilder(MockOAuthClient mockOAuthClient)
      throws Exception {
    OAuthManager oAuthManager =
        new OAuthManager(TestUtils.getAccount(), TestUtils.getUser(), mockOAuthClient, 0.8);
    return new RequestBuilder(
        "MOCK_ACCOUNTNAME",
        "MOCK_USERNAME",
        "MOCK_CREDENTIAL",
        "https",
        "MOCK_HOST_NAME",
        443,
        null,
        oAuthManager,
        null,
        null);
  }
}
