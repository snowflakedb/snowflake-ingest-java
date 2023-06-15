package net.snowflake.ingest.streaming.internal;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.connection.OAuthCredential;
import net.snowflake.ingest.streaming.*;
import net.snowflake.ingest.utils.Constants;
import org.junit.Before;
import org.junit.Test;

public class OAuthTest {
  private static final String TABLE = "t2";
  private Connection jdbcConn;
  private SnowflakeStreamingIngestClient client;

  @Before
  public void setUp() throws Exception {
    jdbcConn = TestUtils.getConnection(true);
    OAuthCredential oAuthCredential = TestUtils.generateOAuthCredential(jdbcConn);
    Properties props = TestUtils.getProperties(Constants.BdecVersion.THREE);

    jdbcConn
        .createStatement()
        .execute(String.format("create or replace database %s;", TestUtils.getDatabase()));
    jdbcConn
        .createStatement()
        .execute(
            String.format(
                "create or replace table %s.%s.%s (col int)",
                TestUtils.getDatabase().toUpperCase(), TestUtils.getSchema().toUpperCase(), TABLE));

    props.remove(Constants.PRIVATE_KEY);
    props.put(Constants.AUTHORIZATION_TYPE, Constants.OAUTH);
    props.put(Constants.OAUTH_CLIENT_ID, oAuthCredential.getClientId());
    props.put(Constants.OAUTH_CLIENT_SECRET, oAuthCredential.getClientSecret());
    props.put(Constants.OAUTH_REFRESH_TOKEN, oAuthCredential.getRefreshToken());
    client =
        SnowflakeStreamingIngestClientFactory.builder("MY_CLIENT").setProperties(props).build();
  }

  @Test
  public void testOAuthBasic() throws Exception {
    // Open a streaming ingest channel from the given client
    OpenChannelRequest request1 =
        OpenChannelRequest.builder("MY_CHANNEL")
            .setDBName(TestUtils.getDatabase())
            .setSchemaName(TestUtils.getSchema())
            .setTableName(TABLE)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();
    SnowflakeStreamingIngestChannel channel = client.openChannel(request1);

    // Stream data to target table
    final int totalRowsInTable = 1000;
    for (int val = 0; val < totalRowsInTable; val++) {
      Map<String, Object> row = new HashMap<>();
      row.put("col", val);
      InsertValidationResponse response = channel.insertRow(row, String.valueOf(val));
      if (response.hasErrors()) {
        throw response.getInsertErrors().get(0).getException();
      }
    }

    // Check commit
    final int expectedOffsetTokenInSnowflake = totalRowsInTable - 1;
    final int maxRetries = 10;
    int retryCount;
    for (retryCount = 0; retryCount < maxRetries; retryCount++) {
      String offsetTokenFromSnowflake = channel.getLatestCommittedOffsetToken();
      System.out.println(offsetTokenFromSnowflake);
      if (offsetTokenFromSnowflake != null
          && offsetTokenFromSnowflake.equals(String.valueOf(expectedOffsetTokenInSnowflake))) {
        break;
      }
    }
    if (retryCount == maxRetries) {
      throw new RuntimeException("Commit fail.");
    }

    channel.close();
  }
}
