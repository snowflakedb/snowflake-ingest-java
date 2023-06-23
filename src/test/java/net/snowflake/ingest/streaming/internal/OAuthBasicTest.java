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
import org.junit.Before;
import org.junit.Test;

/**
 * This test only contains basic construction of client using OAuth authentication. Further
 * integration test would be added in dew.
 */
public class OAuthBasicTest {
  private Properties props;

  @Before
  public void setUp() throws Exception {
    props = TestUtils.getProperties(Constants.BdecVersion.THREE);

    props.remove(Constants.PRIVATE_KEY);
    props.put(Constants.AUTHORIZATION_TYPE, Constants.OAUTH);
    props.put(Constants.OAUTH_CLIENT_ID, "MOCK_CLIENT_ID");
    props.put(Constants.OAUTH_CLIENT_SECRET, "MOCK_CLIENT_SECRET");
    props.put(Constants.OAUTH_REFRESH_TOKEN, "MOCK_REFRESH_TOKEN");
  }

  /** Create client with mock credential, should fail when refreshing token */
  @Test(expected = SecurityException.class)
  public void testCreateOAuthClient() {
    SnowflakeStreamingIngestClient client =
        SnowflakeStreamingIngestClientFactory.builder("MY_CLIENT").setProperties(props).build();
  }

  @Test
  public void testSetRefreshToken() throws Exception {
    SnowflakeStreamingIngestClientInternal<StubChunkData> client =
        new SnowflakeStreamingIngestClientInternal<>("TEST_CLIENT");
    MockOAuthClient mockOAuthClient = new MockOAuthClient();

    OAuthManager oAuthManager =
        new OAuthManager(TestUtils.getAccount(), TestUtils.getUser(), mockOAuthClient, 0.8);
    RequestBuilder requestBuilder = new RequestBuilder(oAuthManager);
    client.injectRequestBuilder(requestBuilder);

    String newToken = UUID.randomUUID().toString();
    client.setRefreshToken(newToken);
  }
}
