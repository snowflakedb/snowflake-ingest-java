package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.connection.ServiceResponseHandler.ApiName.STREAMING_CHANNEL_STATUS;
import static net.snowflake.ingest.connection.ServiceResponseHandler.ApiName.STREAMING_CLIENT_CONFIGURE;
import static net.snowflake.ingest.streaming.internal.StreamingIngestUtils.executeWithRetries;
import static net.snowflake.ingest.streaming.internal.StreamingIngestUtils.getSleepForRetryMs;
import static net.snowflake.ingest.utils.Constants.CHANNEL_STATUS_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.CLIENT_CONFIGURE_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.RESPONSE_ERR_GENERAL_EXCEPTION_RETRY_REQUEST;
import static net.snowflake.ingest.utils.Constants.RESPONSE_SUCCESS;
import static org.powermock.api.mockito.PowerMockito.doReturn;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.jdbc.internal.apache.commons.io.IOUtils;
import net.snowflake.client.jdbc.internal.apache.http.HttpEntity;
import net.snowflake.client.jdbc.internal.apache.http.StatusLine;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.CloseableHttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.connection.JWTManager;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.utils.HttpUtil;
import net.snowflake.ingest.utils.SnowflakeURL;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class StreamingIngestUtilsTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void testSuccessNoRetries() throws Exception {
    ChannelsStatusResponse response = new ChannelsStatusResponse();
    response.setStatusCode(0L);
    response.setMessage("honk");
    response.setChannels(new ArrayList<>());
    String responseString = objectMapper.writeValueAsString(response);

    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    StatusLine statusLine = Mockito.mock(StatusLine.class);
    HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(200);
    Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
    Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);
    Mockito.when(httpEntity.getContent()).thenReturn(IOUtils.toInputStream(responseString));
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);

    RequestBuilder requestBuilder =
        Mockito.spy(
            new RequestBuilder(TestUtils.getHost(), TestUtils.getUser(), TestUtils.getKeyPair()));

    executeWithRetries(
        ChannelsStatusResponse.class,
        CHANNEL_STATUS_ENDPOINT,
        "{}",
        "channel status",
        STREAMING_CHANNEL_STATUS,
        httpClient,
        requestBuilder);

    Mockito.verify(requestBuilder, Mockito.times(1))
        .generateStreamingIngestPostRequest(Mockito.anyString(), Mockito.any(), Mockito.any());
  }

  InputStream getInputStream(String value) {
    return IOUtils.toInputStream(value);
  }

  @Test
  public void testRetries() throws Exception {
    ChannelsStatusResponse response = new ChannelsStatusResponse();
    response.setStatusCode(RESPONSE_ERR_GENERAL_EXCEPTION_RETRY_REQUEST);
    //    response.setStatusCode(7L);

    response.setMessage("honk");
    response.setChannels(new ArrayList<>());
    String responseString = objectMapper.writeValueAsString(response);

    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    StatusLine statusLine = Mockito.mock(StatusLine.class);
    HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(200);
    Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
    Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);

    Mockito.when(httpEntity.getContent())
        .thenAnswer(
            new Answer<InputStream>() {
              @Override
              public InputStream answer(InvocationOnMock invocation) throws Throwable {
                return IOUtils.toInputStream(responseString);
              }
            });

    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);

    RequestBuilder requestBuilder =
        Mockito.spy(
            new RequestBuilder(TestUtils.getHost(), TestUtils.getUser(), TestUtils.getKeyPair()));

    ChannelsStatusResponse result =
        executeWithRetries(
            ChannelsStatusResponse.class,
            CHANNEL_STATUS_ENDPOINT,
            "{}",
            "channel status",
            STREAMING_CHANNEL_STATUS,
            httpClient,
            requestBuilder);

    Mockito.verify(requestBuilder, Mockito.times(4))
        .generateStreamingIngestPostRequest(Mockito.anyString(), Mockito.any(), Mockito.any());
    Assert.assertEquals("honk", result.getMessage());
  }

  @Test
  public void testJWTRetries() throws Exception {
    SnowflakeURL url = new SnowflakeURL(TestUtils.getAccountURL());
    CloseableHttpClient httpClient = HttpUtil.getHttpClient(TestUtils.getAccount());

    JWTManager manager =
        new JWTManager(url.getAccount(), TestUtils.getUser(), TestUtils.getKeyPair(), null);
    JWTManager spyManager = Mockito.spy(manager);

    // inject spy manager
    RequestBuilder requestBuilder =
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
            "testJWTRetries");

    // build payload
    Map<Object, Object> payload = new HashMap<>();
    if (!TestUtils.getRole().isEmpty() && !TestUtils.getRole().equals("DEFAULT_ROLE")) {
      payload.put("role", TestUtils.getRole());
    }
    ObjectMapper mapper = new ObjectMapper();

    // request wih invalid token
    doReturn("invalid_token").when(spyManager).getToken();
    try {
      ChannelsStatusResponse response =
          executeWithRetries(
              ChannelsStatusResponse.class,
              CLIENT_CONFIGURE_ENDPOINT,
              mapper.writeValueAsString(payload),
              "client configure",
              STREAMING_CLIENT_CONFIGURE,
              httpClient,
              requestBuilder);
      Assert.fail("Expected error for invalid token");
    } catch (SecurityException ignored) {
    }

    // request with invalid token first time but with valid token second time after refresh
    doReturn("invalid_token").doReturn(manager.getToken()).when(spyManager).getToken();
    ChannelsStatusResponse response =
        executeWithRetries(
            ChannelsStatusResponse.class,
            CLIENT_CONFIGURE_ENDPOINT,
            mapper.writeValueAsString(payload),
            "client configure",
            STREAMING_CLIENT_CONFIGURE,
            httpClient,
            requestBuilder);

    assert (response.getStatusCode() == RESPONSE_SUCCESS);
  }

  @Test
  public void testRetriesRecovery() throws Exception {
    ChannelsStatusResponse errorResponse = new ChannelsStatusResponse();
    errorResponse.setStatusCode(RESPONSE_ERR_GENERAL_EXCEPTION_RETRY_REQUEST);

    errorResponse.setMessage("honkFailure");
    errorResponse.setChannels(new ArrayList<>());
    String errorResponseString = objectMapper.writeValueAsString(errorResponse);

    ChannelsStatusResponse successfulResponse = new ChannelsStatusResponse();
    successfulResponse.setStatusCode(RESPONSE_SUCCESS);

    successfulResponse.setMessage("honkSuccess");
    successfulResponse.setChannels(new ArrayList<>());
    String successfulResponseString = objectMapper.writeValueAsString(successfulResponse);

    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    StatusLine statusLine = Mockito.mock(StatusLine.class);
    HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(200);
    Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
    Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);

    Mockito.when(httpEntity.getContent())
        .thenReturn(
            IOUtils.toInputStream(errorResponseString),
            IOUtils.toInputStream(errorResponseString),
            IOUtils.toInputStream(successfulResponseString));
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);

    RequestBuilder requestBuilder =
        Mockito.spy(
            new RequestBuilder(TestUtils.getHost(), TestUtils.getUser(), TestUtils.getKeyPair()));

    ChannelsStatusResponse result =
        executeWithRetries(
            ChannelsStatusResponse.class,
            CHANNEL_STATUS_ENDPOINT,
            "{}",
            "channel status",
            STREAMING_CHANNEL_STATUS,
            httpClient,
            requestBuilder);

    Mockito.verify(requestBuilder, Mockito.times(3))
        .generateStreamingIngestPostRequest(Mockito.anyString(), Mockito.any(), Mockito.any());

    Assert.assertEquals("honkSuccess", result.getMessage());
  }

  @Test
  public void testGetSleepForRetry() {
    Assert.assertEquals(0, getSleepForRetryMs(0));
    Assert.assertEquals(0, getSleepForRetryMs(1));
    Assert.assertEquals(1000, getSleepForRetryMs(2));
    Assert.assertEquals(2000, getSleepForRetryMs(3));
    Assert.assertEquals(4000, getSleepForRetryMs(4));
    Assert.assertEquals(4000, getSleepForRetryMs(5));
    Assert.assertEquals(4000, getSleepForRetryMs(100000));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetSleepForRetryNegative() {
    getSleepForRetryMs(-1);
  }
}
