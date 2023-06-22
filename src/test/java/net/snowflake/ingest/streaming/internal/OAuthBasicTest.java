package net.snowflake.ingest.streaming.internal;

import java.util.Properties;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.*;
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
}
