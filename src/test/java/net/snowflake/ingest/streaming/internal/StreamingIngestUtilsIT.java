/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.connection.ServiceResponseHandler.ApiName.STREAMING_CLIENT_CONFIGURE;
import static net.snowflake.ingest.streaming.internal.StreamingIngestUtils.executeWithRetries;
import static net.snowflake.ingest.utils.Constants.CLIENT_CONFIGURE_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.RESPONSE_SUCCESS;

import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.connection.JWTManager;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.utils.HttpUtil;
import net.snowflake.ingest.utils.SnowflakeURL;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({JWTManager.class})
@PowerMockIgnore({
  "javax.net.ssl.*",
  "javax.security.*"
}) /* Avoid ssl related exception from power mockito*/
public class StreamingIngestUtilsIT {
  @Test
  public void testJWTRetries() throws Exception {
    SnowflakeURL url = new SnowflakeURL(TestUtils.getAccountURL());
    CloseableHttpClient httpClient = HttpUtil.getHttpClient(TestUtils.getAccount());

    JWTManager manager =
        new JWTManager(url.getAccount(), TestUtils.getUser(), TestUtils.getKeyPair(), null);
    JWTManager spyManager = PowerMockito.spy(manager);

    // inject spy manager
    RequestBuilder requestBuilder =
        PowerMockito.spy(
            new RequestBuilder(
                url.getAccount(),
                TestUtils.getUser(),
                TestUtils.getKeyPair(),
                url.getScheme(),
                TestUtils.getHost(),
                url.getPort(),
                "testJWTRetries",
                spyManager,
                httpClient,
                false /* enableIcebergMode */,
                "testJWTRetries"));

    // build payload
    ClientConfigureRequest request =
        new ClientConfigureRequest(
            !TestUtils.getRole().isEmpty() && !TestUtils.getRole().equals("DEFAULT_ROLE")
                ? TestUtils.getRole()
                : null);

    // request wih invalid token, should get 401 3 times
    PowerMockito.doReturn("invalid_token").when(spyManager).getToken();
    try {
      ChannelsStatusResponse response =
          executeWithRetries(
              ChannelsStatusResponse.class,
              CLIENT_CONFIGURE_ENDPOINT,
              request,
              "client configure",
              STREAMING_CLIENT_CONFIGURE,
              httpClient,
              requestBuilder);
      Assert.fail("Expected error for invalid token");
    } catch (SecurityException ignored) {
    }
    Mockito.verify(requestBuilder, Mockito.times(1)).refreshToken();
    Mockito.clearInvocations(requestBuilder);

    // request wih invalid header, should get 400 once and never refresh
    PowerMockito.doReturn("invalid header").when(spyManager).getToken();
    try {
      ChannelsStatusResponse response =
          executeWithRetries(
              ChannelsStatusResponse.class,
              CLIENT_CONFIGURE_ENDPOINT,
              request,
              "client configure",
              STREAMING_CLIENT_CONFIGURE,
              httpClient,
              requestBuilder);
      Assert.fail("Expected error for invalid token");
    } catch (IngestResponseException ignored) {
    }
    Mockito.verify(requestBuilder, Mockito.times(0)).refreshToken();
    Mockito.clearInvocations(requestBuilder);

    // request with invalid token first time but with valid token second time after refresh
    PowerMockito.doReturn("invalid_token").doReturn(manager.getToken()).when(spyManager).getToken();
    ChannelsStatusResponse response =
        executeWithRetries(
            ChannelsStatusResponse.class,
            CLIENT_CONFIGURE_ENDPOINT,
            request,
            "client configure",
            STREAMING_CLIENT_CONFIGURE,
            httpClient,
            requestBuilder);

    assert (response.getStatusCode() == RESPONSE_SUCCESS);
    Mockito.verify(requestBuilder, Mockito.times(1)).refreshToken();
  }
}
